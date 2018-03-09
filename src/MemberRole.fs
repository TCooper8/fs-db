module MemberRole

open Consensus

let retryResult maxRetries (agent:Agent) binding =
  let rec loop retries = async {
    if retries > maxRetries then return ServiceUnavailable
    else
      let! res = agent.PostAndTryAsyncReply(binding, timeout=1500)
      match res with
      | Some res -> return res
      | None -> return! loop (retries + 1)
  }

  loop 0

let awaitResult (agent:Agent) binding = retryResult 5 agent binding

type StateInfo = {
  inbox: Agent
  members: NodeInfo list
  id: string
  commits: Commit list
  lookupAgent: NodeInfo -> Agent
}

type State =
  | Idle of StateInfo
  | AwaitingCommit of string * StateInfo

let info = function
  | Idle info -> info
  | AwaitingCommit(_, info) -> info

let version state =
  match state.commits with
  | [] -> failwithf "No base commit"
  | head::_ -> head.id

let majority state =
  let count = Seq.length state.members
  let limit = count / 2 + 1
  state.members
  |> List.take limit

let minority state =
  let count = Seq.length state.members
  let limit = count / 4 + 1
  state.members
  |> List.take limit

let findCommit state id =
  state.commits
  |> Seq.tryFind (fun commit -> commit.id = id)

let reportNode state (info:NodeInfo) =
  majority state
  |> Seq.map state.lookupAgent
  |> Seq.iter (fun agent -> agent.Post (ReportNode info))

let commitsFrom state id =
  state.commits
  |> List.fold (fun (found, acc) commit ->
    if found then found, acc
    else if commit.id = id then true, acc
    else false, commit::acc
  ) (false, [])
  |> fun (found, commits) ->
    if found then commits
    else []

let handleSwimProposal state (info:NodeInfo) proposal = async {
  let handler =
    state.members
    |> Seq.tryFind (fun _info -> _info.id <> info.id)
  match handler with
  | None -> return ServiceUnavailable
  | Some handler ->
    let agent = state.lookupAgent handler
    let! res = awaitResult agent (fun reply ->
      SwimProposal(info, proposal, reply)
    )
    return res
}

let printMembers =
  function
    | Idle state -> state
    | AwaitingCommit (_, state) -> state
  >> fun state ->
    state.members
    |> printfn "[%s] Members = %A" state.id

let resync state = async {
  printfn "[%s] Resyncing state..." state.id
  // Get the commit stacks from each member.

  let! commitStacks =
    state.members
    |> Seq.map (fun info -> async {
      let agent = state.lookupAgent info
      let! res = awaitResult agent (fun reply ->
        RequestCommits(version state, reply)
      )
      match res with
      | Ok stack -> return Some stack
      | ServiceUnavailable ->
        do reportNode state info
        return None
    })
    |> Async.Parallel

  // Take the largest stack.
  let commits =
    commitStacks
    |> Seq.choose id
    |> Seq.maxBy (fun stack -> stack.Length)
  // Update with these commits.
  let state =
    { state with
        commits = commits @ state.commits
    }
  printfn "[%s] New commits = %A" state.id state.commits
  return state
}

let doJoin state = async {
  printfn "Joining cluster"
  // Just join someone on the cluster.
  let leader = state.members |> Seq.head
  let leaderAgent = state.lookupAgent leader
  let! res = awaitResult leaderAgent Ping
  match res with
  | Ok () -> ()
  | any -> failwithf "Unable to join leader, unexpected %A" any

  let info = { id = state.id }

  let! members, commits = leaderAgent.PostAndAsyncReply(fun reply ->
    Join(info, reply)
  )
  printfn "Joining %A" members

  // Run through each of these and ping them.
  let! pings =
    members
    |> Seq.map (fun info -> async {
      let agent = state.lookupAgent info
      let! res = awaitResult agent Ping
      match res with
      | Ok () -> return info, true
      | _ -> return info, false
    })
    |> Async.Parallel
  let validMembers, invalidMembers =
    pings
    |> Seq.fold (fun (valid, invalid) (info, success) ->
      if success then info::valid, invalid
      else valid, info::invalid
    ) ([], [])

  let members =
    if Seq.exists (fun (info:NodeInfo) -> info.id = leader.id) validMembers then validMembers
    else leader::validMembers

  match members with
  | [] -> failwithf "Unable to connect to any other nodes"
  | _ -> ()

  let state =
    { state with
        members = members
        commits = commits
    }

  do
    invalidMembers
    |> Seq.iter (reportNode state)

  return state
}

let rec handleMsg binding state = function
  | RequestCommits (commitId, reply) ->
    let commits = commitsFrom state commitId
    reply.Reply (Ok commits)
    async { return binding state }

  | Join (info, reply) ->
    printfn "Got join"
    reply.Reply (state.members, state.commits)
    async {
      // Inform the other members that the node has joined.
      do
        state.members
        |> Seq.map state.lookupAgent
        |> Seq.iter (fun agent -> agent.Post (Joined info))

      let state =
        { state with
            members = info::state.members
        }
      return binding state
    }

  | Joined info ->
    if info.id = state.id then async { return binding state }
    else
      printfn "[%s] Agent %s joined" state.id info.id
      if state.members |> Seq.exists(fun _info -> _info.id = info.id) then
        async { return binding state }
      else
        let state =
          { state with
              members = info::state.members
          }
        async { return binding state }

  | IsCurrent (commitId, reply) ->
    reply.Reply (version state = commitId)
    async { return binding state }

  | GetCurrent reply ->
    async {
      let! oks =
        minority state
        |> Seq.map state.lookupAgent
        |> Seq.map (fun agent ->
          agent.PostAndAsyncReply(fun reply ->
            IsCurrent(version state, reply)
          )
        )
        |> Async.Parallel
      let ok = oks |> Seq.fold (fun a b -> a && b) true
      if not ok then
        let! state = resync state
        return! handleMsg binding state (GetCurrent reply)
      else
        let commit = List.head state.commits
        reply.Reply commit.data
        return binding state
    }

  | SwimProposal (info, proposal, reply) ->
    async {
      let agent = state.lookupAgent info
      let! res = awaitResult agent (fun reply ->
        Proposal(proposal, reply)
      )
      reply.Reply res
    } |> Async.Start
    async { return binding state }

  | ReportNode info ->
    if info.id = state.id then
      async {
        let! state = doJoin state
        return binding state
      }
    else
      async {
        let agent = state.lookupAgent info
        let! res = awaitResult agent Ping
        match res with
        | Ok () ->
          return binding state
        | ServiceUnavailable ->
          let state =
            { state with
                members = state.members |> List.filter (fun _info -> _info.id <> info.id)
            }
          return binding state
      }

  | Ping reply ->
    async {
      do reply.Reply (Ok ())
      return binding state
    }

  | Latest reply ->
    async {
      let commit = List.head state.commits
      do reply.Reply commit.data
      return binding state
    }

  | msg ->
    printfn "[%s] Dead letter %A" state.id msg
    async { return binding state }


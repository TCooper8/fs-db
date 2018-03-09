module OwnerRole

open System
open System.Security.Cryptography
open Consensus
open MemberRole

let rec handleIdle state = function
  | Proposal (proposal, reply) ->
    async {
      if proposal.commit.lastCommit <> version state then
        let! state = resync state
        if proposal.commit.lastCommit <> version state then
          reply.Reply (Ok <| Reject (proposal.id, "bad version"))
        else
          reply.Reply (Ok <| Accept proposal.id)
        return Idle state
      else
        reply.Reply (Ok <| Accept proposal.id)
        return AwaitingCommit (proposal.id, state)
    }

  | Consensus (Approved info) ->
    if findCommit state info.id |> Option.isSome then
      printfn "Commit %A already approved" info
      async { return Idle state }
    else
      async {
        printfn "[%s] resyncing" state.id
        let! state = resync state
        printfn "[%s] Updated commits %A" state.id state.commits
        return Idle state
      }

  | Consensus (Rejected _) ->
    async { return Idle state }

  | Update body ->
    async {
      printfn "[%s] Updating..." state.id

      let md5 = MD5.Create()
      let buffer = md5.ComputeHash body
      let hash = BitConverter.ToString(buffer)
      let commit =
        { id = hash
          lastCommit = version state
        }
      let proposal =
        { id = hash
          leader = state.id
          commit = commit
        }

      printfn "[%s] Creating proposal" state.id
      let! votes =
        state.members
        |> Seq.map (fun info -> async {
          let agent = state.lookupAgent info
          let! res = awaitResult agent (fun reply ->
            Proposal (proposal, reply)
          )

          match res with
          | Ok vote -> return Some vote
          | ServiceUnavailable ->
            let! res = handleSwimProposal state info proposal
            match res with
            | Ok vote -> return Some vote
            | ServiceUnavailable ->
              do reportNode state info
              return None
        })
        |> Async.Parallel

      let consensus =
        { id = proposal.id
          commit =
            { id = commit.id
              lastCommit = Some commit.lastCommit
              data = body
            }
          members = state.members |> Seq.map (fun info -> info.id) |> Set
        }

      printfn "Votes %A" votes
      let accepted =
        votes
        |> Seq.choose id
        |> Seq.fold (fun acc -> function
          | Accept _ -> true && acc
          | Reject (_, reason) ->
            printfn "[%s] vote rejected, %s" state.id reason
            false && acc
        ) true
      let accepted, rejected =
        votes
        |> Seq.choose id
        |> Seq.fold (fun (accepted, rejected) -> function
          | Accept _ -> accepted + 1, rejected
          | Reject (_, reason) ->
            printfn "[%s] Vote rejected due to : %s" state.id reason
            accepted, rejected + 1
        ) (0, 0)

      if rejected = 0 then
        printfn "Commit approved!!!"
        state.members
        |> Seq.map state.lookupAgent
        |> Seq.iter (fun agent -> agent.Post (Consensus <| Approved consensus))

        let! state = resync state

        return Idle state
      else
        printfn "[%s] Accepted %i Rejected %i" state.id accepted rejected
        state.members
        |> Seq.map state.lookupAgent
        |> Seq.iter (fun agent -> agent.Post (Consensus <| Rejected consensus))

        // In this case, our commit was rejected and we need to resync.
        let! state = resync state
        // Try updating again.
        return! handleIdle state (Update body)
    }

  | msg -> handleMsg Idle state msg

let handleAwaiting id state = function
  | Consensus (Approved info) ->
    if id = info.id then
      printfn "Received approved"
      let state =
        { state with
            commits = info.commit::state.commits
        }
      printfn "[%s] State = %A" state.id state.commits
      async { return Idle state }
    else
      printfn "Expected commit %A but got %A" id info.id
      async { return AwaitingCommit (id, state) }

  | Proposal (proposal, reply) ->
    reply.Reply(Ok (Reject(proposal.id, "unexpected")))
    async { return AwaitingCommit (id, state) }

  | Consensus (Rejected info) ->
    if id = info.id then async { return Idle state }
    else async { return AwaitingCommit (id, state) }

  | Update body ->
    async { return AwaitingCommit(id, state) }

  | msg -> handleMsg (fun s -> AwaitingCommit(id, s)) state msg

let handle = function
  | Idle state -> handleIdle state
  | AwaitingCommit (id, state) -> handleAwaiting id state

let agent lookupAgent (info:NodeInfo) = MailboxProcessor.Start(fun inbox ->
  printfn "Running leader"
  let state =
    { inbox = inbox
      lookupAgent = lookupAgent
      members = []
      id = info.id
      commits =
        [ { id = "base"
            lastCommit = None
            data = Array.empty
          }
        ]
    }

  let rec loop state = async {
    //do printMembers state
    let! msg = inbox.Receive()
    printfn "[%s] Received %A" info.id msg
    let! state = handle state msg
    return! loop state
  }

  loop (Idle state)
)

let join (agentLookup:NodeInfo -> Agent) info (leader:NodeInfo) = MailboxProcessor.Start(fun inbox -> async {
  printfn "Joining cluster"
  let leaderAgent = agentLookup leader
  let! res = awaitResult leaderAgent Ping
  match res with
  | Ok () -> ()
  | any -> failwithf "Unable to join leader, unexpected %A" any

  let! members, commits = leaderAgent.PostAndAsyncReply(fun reply ->
    Join(info, reply)
  )
  printfn "Joining %A" members

  // Run through each of these and ping them.
  let! pings =
    members
    |> Seq.map (fun info -> async {
      let agent = agentLookup info
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

  let state =
    { inbox = inbox
      lookupAgent = agentLookup
      id = info.id
      members = members
      commits = commits
    }

  do
    invalidMembers
    |> Seq.iter (reportNode state)

  let rand = new System.Random()
  let rec loop state = async {
    //do printMembers state
    let! msg = inbox.Receive()
    //do! Async.Sleep (rand.Next 16)
    printfn "[%s] Received %A" info.id msg
    let! state = handle state msg
    return! loop state
  }

  return! loop (Idle state)
})


module Storage =
  type DType =
    | Text
    | Float

module Transport =
  open Consensus

  [<Interface>]
  type ITransport =
    abstract Connect: NodeInfo -> Agent

module Http =
  open Consensus
  open OwnerRole
  open Transport
  open System.Net
  open System.Text
  open System.IO
  open System.Net.Http
  open Newtonsoft.Json

  type private Req = HttpListenerRequest
  type private Resp = HttpListenerResponse

  type private State = {
    transport: ITransport
    self: Agent
  }

  let private enc = Encoding.UTF8

  let private decode (req:Req) = async {
    use stream = req.InputStream
    use reader = new StreamReader(stream)
    let! body = reader.ReadToEndAsync() |> Async.AwaitTask
    return JsonConvert.DeserializeObject<'a>(body)
  }

  let private respond (resp:Resp) output = async {
    use resp = resp
    use stream = resp.OutputStream
    use writer = new StreamWriter(stream)

    let body = JsonConvert.SerializeObject(output :> obj)
    do! writer.WriteAsync(body) |> Async.AwaitTask
  }

  let private consensus state req resp = async {
    let! input = decode<Consensus> req
    printfn "Got input %A" input
    state.self.Post (Consensus input)
    do! respond resp "ok"
  }

  let private ping state req resp = async {
    do! respond resp "ok"
  }

  let private join state req resp = async {
    let! input = decode<NodeInfo> req
    printfn "Got input %A" input
    let! result = state.self.PostAndAsyncReply(fun reply ->
      Join(input, reply)
    )
    do! respond resp result
  }

  let private joined state req resp = async {
    let! input = decode req
    do state.self.Post (Joined input)
    do! respond resp ""
  }

  let private update state req resp = async {
    let! data = decode req
    do state.self.Post (Update data)
    do! respond resp ""
  }

  let private getCurrent state req resp = async {
    let! data = state.self.PostAndAsyncReply(fun reply ->
      GetCurrent reply
    )
    do! respond resp data
  }

  let private proposal state req resp = async {
    let! proposal = decode req
    let! result = state.self.PostAndAsyncReply(fun reply ->
      Proposal(proposal, reply)
    )
    do! respond resp result
  }

  let private requestCommits state req resp = async {
    let! commitId = decode req
    let! commits = state.self.PostAndAsyncReply(fun reply ->
      RequestCommits(commitId, reply)
    )
    do! respond resp commits
  }

  let private isCurrent state req resp = async {
    let! input = decode req
    let! result = state.self.PostAndAsyncReply(fun reply ->
      IsCurrent(input, reply)
    )
    do! respond resp result
  }

  let private latest state req resp = async {
    let! result = state.self.PostAndAsyncReply(fun reply ->
      Latest reply
    )
    do! respond resp result
  }

  let private route state req resp = function
    | "/consensus" -> consensus state req resp
    | "/ping" -> ping state req resp
    | "/join" -> join state req resp
    | "/joined" -> joined state req resp
    | "/update" -> update state req resp
    | "/getCurrent" -> getCurrent state req resp
    | "/proposal" -> proposal state req resp
    | "/requestCommits" -> requestCommits state req resp
    | "/isCurrent" -> isCurrent state req resp
    | "/latest" -> latest state req resp
    | route ->
      async {
        printfn "Unhandled route %s" route
        use resp = resp
        return ()
      }

  let agent (transport:ITransport) (host, port) leader = MailboxProcessor.Start(fun inbox -> async {
    let info =
      { id = sprintf "http://%s:%i/" host port
      }
    let self =
      match leader with
      | None -> OwnerRole.agent transport.Connect info
      | Some leader ->
        OwnerRole.join transport.Connect info leader

    use listener = new HttpListener()
    listener.Prefixes.Add info.id
    listener.Start()

    let state =
      { transport = transport
        self = self
      }

    let rec loop state = async {
      let! ctx = listener.GetContextAsync() |> Async.AwaitTask
      async {
        let req = ctx.Request
        use resp = ctx.Response
        let path = req.RawUrl
        do! route state req resp path
      } |> Async.Start
      return! loop state
    }

    return! loop state
  })

  let private client () =
    new HttpClient()

  let private post (info:NodeInfo) path msg = async {
    let json = JsonConvert.SerializeObject(msg :> obj)
    let body = enc.GetBytes json
    let content = ByteArrayContent body
    use client = client ()
    let! _ = client.PostAsync(info.id + path, content) |> Async.AwaitTask
    return ()
  }

  let private postAndReply (info:NodeInfo) path msg = async {
    let json = JsonConvert.SerializeObject(msg :> obj)
    let body = enc.GetBytes json
    let content = ByteArrayContent body
    use client = client ()
    use! resp = client.PostAsync(info.id + path, content) |> Async.AwaitTask
    use content = resp.Content
    let! body = content.ReadAsStringAsync() |> Async.AwaitTask
    let result = JsonConvert.DeserializeObject<'b> body
    return result
  }

  let private clientAgent info = MailboxProcessor.Start(fun inbox ->
    let rec loop () = async {
      let! msg = inbox.Receive()
      printfn "Msg %A" msg
      match msg with
      | Consensus output -> do! post info "consensus" output
      | Ping reply ->
        let! _ = postAndReply info "ping" ""
        reply.Reply(Ok ())
      | Join (memberInfo, reply) ->
        let! (members, commits) = postAndReply info "join" memberInfo
        reply.Reply(members, commits)

      | Joined nodeInfo ->
        do! post info "joined" nodeInfo

      | Update data ->
        do! post info "update" data

      | GetCurrent reply ->
        let! data = postAndReply info "getCurrent" ""
        reply.Reply data

      | Proposal(proposal, reply) ->
        let! result = postAndReply info "proposal" proposal
        reply.Reply result

      | RequestCommits(id, reply) ->
        let! commits = postAndReply info "requestCommits" id
        reply.Reply commits

      | IsCurrent(id, reply) ->
        let! result = postAndReply info "isCurrent" id
        reply.Reply result

      | Latest reply ->
        let! result = postAndReply info "latest" ""
        reply.Reply result

      | msg ->
        printfn "Unhandled %A" msg

      return! loop()
    }

    loop ()
  )

  type Transport () =
    interface ITransport with
      member __.Connect info =
        clientAgent info

open Consensus

[<EntryPoint>]
let main argv =
  let transport = Http.Transport() :> Transport.ITransport
  //let agents = ref Map.empty<string, Agent>

  //let agentLookup (info:NodeInfo) =
  //  Map.find info.id !agents

  let leader =
  //  agent
  //    agentLookup
      //{ id = "leader"
      //}
    Http.agent
      transport
      ("localhost", 8080)
      None

  //agents := Map.add "leader" leader !agents

  let leaderInfo =
    { id = "http://localhost:8080/"
    }
  use sub = leader.Error.Subscribe(printfn "Error %A")

  async {
    async {
      for i in 0 .. 5 do
        let follower = Http.agent transport ("localhost", 8081 + i) (Some leaderInfo)
        printfn "Follower joined"
        follower.Error.Subscribe(printfn "Error %A")
        //agents := Map.add info.id follower !agents
    } |> Async.Start

    let rand = System.Random()
    let agent = transport.Connect leaderInfo

    while true do
      //let agents = !agents |> Map.toSeq |> Seq.map snd |> Seq.toArray
      //let agent = agents.[rand.Next agents.Length]
      let input = System.Console.ReadLine()
      let bytes = System.Text.Encoding.UTF8.GetBytes input
      do agent.Post(Update bytes)

      let bytes = agent.PostAndReply(GetCurrent)
      let msg = System.Text.Encoding.UTF8.GetString bytes
      printfn "current > %s" msg
  }
  |> Async.RunSynchronously
  0

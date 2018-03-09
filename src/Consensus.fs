module Consensus

open System
open System.Security.Cryptography

type NodeId = string
type NodeInfo = {
  id: NodeId
}

type CommitInfo = {
  id: string
  lastCommit: string
}

type Proposal = {
  id: string
  leader: NodeId
  commit: CommitInfo
}

type Vote =
  | Accept of string
  | Reject of string * string

type Commit = {
  id: string
  lastCommit: string option
  data: byte[]
}

type ConsensusInfo = {
  id: string
  commit: Commit
  members: NodeId Set
}

type Consensus =
  | Approved of ConsensusInfo
  | Rejected of ConsensusInfo

type 'a Result =
  | Ok of 'a
  | ServiceUnavailable

type Msg =
  | Consensus of Consensus
  | Proposal of Proposal * Vote Result AsyncReplyChannel
  | RequestCommits of string * Commit list Result AsyncReplyChannel
  | Join of NodeInfo * (NodeInfo list * Commit list) AsyncReplyChannel
  | Update of byte[]
  | LatestCommit of string
  | Joined of NodeInfo
  | GetCurrent of byte[] AsyncReplyChannel
  | IsCurrent of string * bool AsyncReplyChannel
  | SwimProposal of NodeInfo * Proposal * Vote Result AsyncReplyChannel
  | Ping of unit Result AsyncReplyChannel
  | ReportNode of NodeInfo
  | Latest of byte[] AsyncReplyChannel

type Agent = Msg MailboxProcessor


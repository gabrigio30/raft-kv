package raft

// Transport is the interface the Raft node uses to send RPCs to peers.
type Transport interface {
	SendRequestVote(toID int, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(toID int, args AppendEntriesArgs) (AppendEntriesReply, error)
}
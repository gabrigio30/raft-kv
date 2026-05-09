package network

import (
	"fmt"
	"sync"

	"github.com/gabrigio30/raft-kv/raft"
)

// RaftNode is the interface a node must implement to receive inbound RPCs.
type RaftNode interface {
	RequestVote(args raft.RequestVoteArgs) (raft.RequestVoteReply, error)
	AppendEntries(args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error)
}

// InMemTransport implements raft.Transport using directly in-process method calls.
type InMemTransport struct {
	mu sync.RWMutex
	peers map[int]RaftNode
}

// NewInMemTransport returns an empty InMemTransport.
func NewInMemTransport() *InMemTransport {
	return &InMemTransport{
		peers: make(map[int]RaftNode),
	}
}

// AddPeer registers a node under the given ID.
func (t *InMemTransport) AddPeer(id int, node RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()		// used to guarantee the mutex is always unlocked even if function panics
	t.peers[id] = node
}

// SendRequestVote forwards a RequestVote RPC to the target node.
func (t *InMemTransport) SendRequestVote(toID int, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	t.mu.RLock()
	node, ok := t.peers[toID]
	t.mu.RUnlock()
	if !ok {
		return raft.RequestVoteReply{}, fmt.Errorf("unknown peer %d", toID)
	}
	return node.RequestVote(args)
}

// SendAppendEntries forwards an AppendEntries RPC to the target node.
func (t *InMemTransport) SendAppendEntries(toID int, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	t.mu.RLock()
	node, ok := t.peers[toID]
	t.mu.RUnlock()
	if !ok {
		return raft.AppendEntriesReply{}, fmt.Errorf("unknown peer %d", toID)
	}
	return node.AppendEntries(args)
}
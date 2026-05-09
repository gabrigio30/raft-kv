package raft

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/gabrigio30/raft-kv/storage"
)

const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	heartbeatInterval = 50 * time.Millisecond
)

// Transport is the interface the Raft node uses to send RPCs to peers.
type Transport interface {
	SendRequestVote(toID int, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(toID int, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Node is a single Raft consensus node.
type Node struct {
	mu sync.Mutex
	id int
	peers []int
	state NodeState
	currentTerm int
	votedFor int
	log []LogEntry
	commitIndex int
	lastApplied int
	nextIndex map[int]int
	matchIndex map[int]int
	transport Transport
	storage *storage.Storage
	applyCh chan LogEntry
	electionTimer *time.Timer
	stopCh chan struct{}
	rand *rand.Rand
}

// NewNode creates and starts a Raft node.
func NewNode (id int, peers []int, transport Transport, store *storage.Storage, applyCh chan LogEntry) (*Node, error) {
	n := &Node{
		id: id,
		peers: peers,
		state: Follower,
		votedFor: -1,
		transport: transport,
		storage: store,
		applyCh: applyCh,
		stopCh: make(chan struct{}),
		rand: rand.New(rand.NewSource(int64(id))),
	}

	if err := n.loadState(); err != nil {
		return nil, err
	}
	
	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	go n.run()
	return n, nil
}

func (n *Node) randomElectionTimeout() time.Duration {
	delta := maxElectionTimeout - minElectionTimeout
	return minElectionTimeout + time.Duration(n.rand.Int63n(int64(delta)))
}

func (n *Node) loadState() error {
	state, err := n.storage.Load()
	if err != nil {
		return err
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	if len(state.Log) > 0 {
		if err := json.Unmarshal(state.Log, &n.log); err != nil {
			return err
		}
	}
	return nil
}

// Stop shuts down the node's background goroutine.
func (n *Node) Stop() {
	close(n.stopCh)
	n.electionTimer.Stop()
}

func (n *Node) run() {
	for {
		select {
		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.state != Leader {
				n.startElection()
			} else {
				n.resetElectionTimer()
			}
			n.mu.Unlock()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) resetElectionTimer() {
	n.electionTimer.Reset(n.randomElectionTimeout())
}

func (n *Node) saveState() error {
	logBytes, err := json.Marshal(n.log)
	if err != nil {
		return err
	}
	return n.storage.Save(storage.HardState{
		CurrentTerm: n.currentTerm,
		VotedFor: n.votedFor,
		Log: logBytes,
	})
}

// startElection is fired whenever a node does not receive a heartbeat
// from the leader before its internal electionTimer runs out
func (n *Node) startElection() {
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	n.resetElectionTimer()

	if err := n.saveState(); err != nil {
		return
	}

	term := n.currentTerm
	lastIndex, lastTerm := lastLogIndexAndTerm(n.log)
	votes := 1

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term: term,
				CandidateID: n.id,
				LastLogIndex: lastIndex,
				LastLogTerm: lastTerm,
			}
			reply, err := n.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}
			n.mu.Lock()		// acquire lock after the RPC returns
			defer n.mu.Unlock()		// will unlock when this instance of the goroutine returns
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted && n.state == Candidate && n.currentTerm == term {
				mu.Lock()
				votes++
				hasQuorum := votes > (len(n.peers) + 1)/2
				mu.Unlock()
				if hasQuorum {
					n.becomeLeader()
				}
			}
		}(peer)
	}
	n.mu.Unlock()	// release the mutex locked before calling startElection(), so goroutines can acquire it
	wg.Wait()		// wait for all goroutines to finish
	n.mu.Lock()		//re-acquire it so run() can unlock it
}

func (n *Node) becomeFollower(term int) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = -1
	n.resetElectionTimer()
	n.saveState()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	lastIndex, _ := lastLogIndexAndTerm(n.log)
	n.nextIndex = make(map[int]int)
	n.matchIndex = make(map[int]int)
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <- ticker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			for _, peer := range n.peers {
				go n.sendAppendEntries(peer)
			}
		case <- n.stopCh:
			return
		}
	}
}

func (n *Node) sendAppendEntries(peer int) {

}
# Raft-based distributed key-value store
 
![Go](https://img.shields.io/badge/go-1.21+-00ADD8?logo=go)
 
A fault-tolerant distributed key-value store built from scratch in Go, implementing the Raft consensus algorithm as described in the [extended Raft paper](https://raft.github.io/raft.pdf). Designed for correctness and clarity over production completeness.
 
---
 
## Table of Contents
 
- [Overview](#overview)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Getting Started](#getting-started)
- [Testing and Benchmarks](#testing-and-benchmarks)
- [Design Decisions](#design-decisions)
- [Known Limitations](#known-limitations)
---
 
## Overview
 
Distributed consensus is the problem of getting a cluster of nodes to agree on a sequence of values even in the presence of node crashes. Raft solves this by electing a single leader that serializes all writes through a replicated log — every write is committed only after a quorum of nodes has durably recorded it, making the system tolerant to the failure of any minority of nodes.
 
This project implements Raft from scratch and layers a key-value store on top of it, exposing a simple `Put`, `Get`, and `Delete` API. It covers the core protocol — leader election, log replication, and crash-safe persistence — and adds at-most-once write semantics via per-client deduplication, a correctness concern that most toy implementations skip.
 
---
 
## Architecture
 
```
┌─────────────────────────────────────────────────────────────────┐
│                          Client                                 │
│                    kvstore.Client                               │
│         (routes writes to leader, reads to any node)            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
          ┌─────────────▼──────────────┐
          │         KVStore            │
          │  • applies committed log   │
          │    entries to in-memory    │
          │    map                     │
          │  • deduplicates via        │
          │    (ClientID, SeqNo)       │
          │  • signals pending writes  │
          │    via per-index channels  │
          └─────────────┬──────────────┘
                        │ Submit / applyCh
          ┌─────────────▼──────────────┐
          │         Raft Node          │
          │  • leader election         │
          │  • log replication         │
          │  • commit index advance    │
          └──────┬──────────┬──────────┘
                 │          │
    ┌────────────▼───┐  ┌───▼────────────┐
    │   Transport    │  │    Storage     │
    │  (InMem / TCP) │  │  bbolt B-tree  │
    │  RequestVote   │  │  HardState:    │
    │  AppendEntries │  │  term/vote/log │
    └────────────────┘  └────────────────┘
```
 
### Project Structure
 
```
raft-kv/
├── raft/
│   ├── raft.go        # Node struct, election, log replication, RPC handlers
│   ├── log.go         # LogEntry type and lastLogIndexAndTerm helper
│   └── state.go       # NodeState enum, RequestVote/AppendEntries RPC types
├── kvstore/
│   ├── kvstore.go     # KVStore: apply loop, deduplication, pending channel map
│   └── client.go      # Client: leader-aware routing for Put/Get/Delete
├── storage/
│   └── storage.go     # HardState persistence via bbolt atomic transactions
├── transport/
│   └── network.go     # InMemTransport: in-process RPC for testing
├── server/
│   └── server.go      # HTTP server exposing the KV API
└── tests/
    └── raft_test.go   # End-to-end leader failover test
```
 
---
 
## How It Works
 
### Leader Election
 
Each node starts as a follower with a randomized election timeout between 150ms and 300ms. If no `AppendEntries` heartbeat is received before the timer fires, the node increments its term, transitions to candidate, and broadcasts `RequestVote` RPCs to all peers in parallel goroutines.
 
A candidate wins the election if it receives votes from a strict majority of the cluster. Votes are only granted if the candidate's log is at least as up-to-date as the voter's — compared first by last log term, then by last log index. This is the critical safety property that prevents a node with a stale log from winning an election and overwriting committed entries.
 
Once elected, the leader immediately begins sending heartbeats at 50ms intervals to suppress new elections for the duration of its term.
 
### Log Replication
 
The leader serializes all writes through its log. When a client submits a command via `Submit()`, the leader appends it as a new `LogEntry` and replicates it to followers via `AppendEntries` RPCs. Each RPC carries a `PrevLogIndex` and `PrevLogTerm` that the follower checks to detect log inconsistencies.
 
The leader tracks per-peer `nextIndex` (the next log index to send) and `matchIndex` (the highest index known to be replicated on that peer). Once a majority of `matchIndex` values reach a given index, and that index belongs to the current term, the leader advances `commitIndex` and notifies the KV state machine via `applyCh`.
 
The restriction that only entries from the current term can be directly committed — with older entries committed indirectly — is the Raft leader completeness property (§5.4.2 of the paper). It prevents a scenario where a re-elected leader overwrites entries that were committed during a previous term.
 
### Crash-Safe Persistence
 
Before acting on any state change, the node persists its hard state to disk via `storage.Save()`. Hard state consists of three fields that must survive crashes:
 
- `currentTerm` — prevents a restarted node from voting in a stale term
- `votedFor` — prevents a restarted node from casting a second vote in the same term
- `log` — the full replicated log, JSON-serialized and written atomically via a bbolt `Update` transaction
bbolt's `Update` wraps the write in an ACID transaction, guaranteeing that no partial state is ever written. On restart, `loadState()` reads this state back before the node joins the cluster.
 
### At-Most-Once Write Semantics
 
Raft itself guarantees that committed entries are applied exactly once in log order. However, the client layer introduces a subtlety: if a leader commits a write and then crashes before replying, the client will retry — potentially sending the same command to a new leader, which would apply it again.
 
`KVStore` handles this by maintaining a `lastSeq map[string]uint64` keyed by `ClientID`. Each command carries a monotonically increasing `SeqNo`, and a command is only applied if `SeqNo > lastSeq[ClientID]`. Retried commands with the same `SeqNo` are silently deduplicated, providing at-most-once semantics across leader failovers.
 
### Write Backpressure via Channel Notification
 
When `KVStore.submit()` calls `node.Submit()`, it receives the log index at which the command was appended. It registers a `chan struct{}` in a `pending` map keyed by that index and blocks on it. When `applyLoop` processes an entry at that index, it closes the channel, unblocking the caller. This avoids polling and provides clean backpressure between the Raft layer and the application layer.
 
---
 
## Getting Started
 
### Prerequisites
 
- Go 1.21+
### Build
 
```bash
git clone https://github.com/gabrigio30/raft-kv
cd raft-kv
go build ./...
```
 
### Run the tests
 
```bash
go test ./tests/ -v
```
 
---
 
## Testing and Benchmarks
 
The integration test `TestLeaderFailover` in `tests/raft_test.go` spins up a full 3-node cluster in a single process using an `InMemTransport` and real bbolt storage in temporary directories. It exercises the following scenario:
 
1. Wait for a leader to be elected
2. Write 5 key-value pairs and verify they are readable
3. Stop the current leader (simulating a crash)
4. Wait for a new leader to be elected among the remaining two nodes
5. Verify all 5 pre-failover keys are still readable with correct values (zero data loss)
6. Write and read a new key to confirm the cluster accepts writes after failover
**Results over 10 runs:**
 
| Metric | Value |
|---|---|
| Median leader failover latency | ~520ms |
| Data loss across all runs | 0 entries |
| Cluster size | 3 nodes |
| Election timeout range | 150–300ms |
| Heartbeat interval | 50ms |
 
The ~520ms failover time is consistent with the expected worst-case: up to 300ms for the election timer to fire on a follower, plus one round of `AppendEntries` for the new leader to confirm its log with the remaining follower before accepting writes.
 
---
 
## Design Decisions
 
### Transport as an interface
 
The `Transport` interface exposes only two methods — `SendRequestVote` and `SendAppendEntries`. The Raft node never imports the transport package directly; it depends on the abstraction. This decoupling means the same consensus logic runs against `InMemTransport` in tests and a real TCP/gRPC transport in production, without any changes to `raft.go`.
 
### Mutex release across RPC calls
 
`startElection` releases the node's mutex before issuing `RequestVote` RPCs and re-acquires it inside each goroutine after the RPC returns. Holding the mutex across a network call would serialize all vote collection, defeating the purpose of parallel RPC dispatch, and would risk deadlock if a peer's RPC handler tries to acquire the same lock on a shared transport. The re-acquire pattern is carefully documented in the code to make the lock discipline explicit.
 
### bbolt over a flat file
 
bbolt provides ACID transactions with `fsync`-backed durability. A flat file approach (e.g. JSON written with `os.WriteFile`) is not atomic — a crash mid-write leaves a corrupt file. bbolt's `Update` transaction guarantees that either the full hard state is committed or nothing is, which is exactly what Raft requires.
 
### Separate `applyCh` per node
 
Rather than having the KV store poll the Raft node for new committed entries, committed entries are pushed to an `applyCh chan LogEntry` that the KV store consumes in a dedicated `applyLoop` goroutine. This separates the apply concern from the consensus concern and avoids any coupling between the Raft timing loop and the application layer.
 
---
 
## Known Limitations
 
**No log compaction or snapshotting.** The full log is serialized to bbolt on every state save. In a long-running cluster, this grows unboundedly and restart time grows linearly with log length. The Raft paper covers snapshotting in §7 as the standard solution; this is the most significant missing production feature.
 
**Stale reads are possible.** `KVStore.Get()` reads directly from the in-memory map without going through the Raft log. A partitioned leader that has not yet discovered it is deposed will serve stale values to clients. The standard fixes are read-index (the leader confirms it is still leader by checking a quorum before serving a read) or lease-based reads. Neither is implemented.
 
**Graceful shutdown only.** The failover test stops nodes via `node.Stop()`, which is a clean channel close. SIGKILL, network partitions, and partial message delivery are not tested. A more adversarial test harness would use a controllable transport that can drop, delay, or reorder messages.
 
**Linear `nextIndex` backoff.** When a follower rejects an `AppendEntries` due to a log inconsistency, the leader decrements `nextIndex[peer]` by one and retries. For a follower that is far behind, this requires O(n) round trips to converge. The paper suggests returning conflict metadata to allow the leader to skip to the correct index in one step; this optimisation is not implemented.
 
---
 
## Author
 
**Gabriele Giordanelli**
 
M.Sc. Computer Science and Engineering — Politecnico di Milano
 
[LinkedIn](https://linkedin.com/in/gabrigio30) · [GitHub](https://github.com/gabrigio30)

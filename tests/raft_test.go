package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gabrigio30/raft-kv/kvstore"
	"github.com/gabrigio30/raft-kv/raft"
	"github.com/gabrigio30/raft-kv/storage"
	network "github.com/gabrigio30/raft-kv/transport"
)

func TestLeaderFailover(t *testing.T) {
	const n = 3

	dirs := make([]string, n)
	for i := range dirs {
		dir, err := os.MkdirTemp("", "raft-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		dirs[i] = dir
	}

	transports := make([]*network.InMemTransport, n)
	for i := range transports {
		transports[i] = network.NewInMemTransport()
	}

	stores := make([]*storage.Storage, n)
	for i := range stores {
		s, err := storage.NewStorage(fmt.Sprintf("%s/raft.db", dirs[i]))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		stores[i] = s
	}

	applyChans := make([]chan raft.LogEntry, n)
	for i := range applyChans {
		applyChans[i] = make(chan raft.LogEntry, 100)
	}

	nodes := make([]*raft.Node, n)
	for i := range nodes {
		peers := make([]int, 0, n-1)
		for id := 0; id < n; id++ {
			if id != i {
				peers = append(peers, id)
			}
		}
		node, err := raft.NewNode(i, peers, transports[i], stores[i], applyChans[i])
		if err != nil {
			t.Fatal(err)
		}
		defer node.Stop()
		nodes[i] = node
	}

	kvStores := make([]*kvstore.KVStore, n)
	for i := range kvStores {
		kvStores[i] = kvstore.NewKVStore(nodes[i], applyChans[i])
	}

	for i, tr := range transports {
		for j, node := range nodes {
			if i != j {
				tr.AddPeer(j, node)
			}
		}
	}

	client := kvstore.NewClient(kvStores)

	waitForLeader(t, nodes, 3*time.Second)

	for k := 0; k < 5; k++ {
		if err := client.Put(fmt.Sprintf("key%d", k), fmt.Sprintf("val%d", k)); err != nil {
			t.Fatalf("Put key%d: %v", k, err)
		}
	}

	for k := 0; k < 5; k++ {
		val, err := client.Get(fmt.Sprintf("key%d", k))
		if err != nil {
			t.Fatalf("Get key%d: %v", k, err)
		}
		if val != fmt.Sprintf("val%d", k) {
			t.Fatalf("Get key%d = %q, want %q", k, val, fmt.Sprintf("val%d", k))
		}
	}

	leaderIdx := -1
	for i, node := range nodes {
		if node.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader found before failover")
	}
	nodes[leaderIdx].Stop()

	remaining := make([]*raft.Node, 0, n-1)
	for i, node := range nodes {
		if i != leaderIdx {
			remaining = append(remaining, node)
		}
	}
	waitForLeader(t, remaining, 3*time.Second)

	for k := 0; k < 5; k++ {
		val, err := client.Get(fmt.Sprintf("key%d", k))
		if err != nil {
			t.Fatalf("after failover, Get key%d: %v", k, err)
		}
		if val != fmt.Sprintf("val%d", k) {
			t.Fatalf("after failover, Get key%d = %q, want %q", k, val, fmt.Sprintf("val%d", k))
		}
	}

	if err := client.Put("post-failover", "yes"); err != nil {
		t.Fatalf("Put after failover: %v", err)
	}
	val, err := client.Get("post-failover")
	if err != nil {
		t.Fatalf("Get post-failover: %v", err)
	}
	if val != "yes" {
		t.Fatalf("Get post-failover = %q, want %q", val, "yes")
	}
}

func waitForLeader(t *testing.T, nodes []*raft.Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.IsLeader() {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader election")
}
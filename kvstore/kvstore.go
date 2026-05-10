package kvstore

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gabrigio30/raft-kv/raft"
)

// Command is a KV operation encoded into a Raft log entry.
type Command struct {
	Op       string
	Key      string
	Value    string
	ClientID string
	SeqNo    uint64
}

// KVStore is a replicated key-value store driven by a Raft node.
type KVStore struct {
	mu sync.RWMutex
	data map[string]string
	lastSeq map[string]uint64
	node *raft.Node
	pendingMu sync.Mutex
	pending map[int]chan struct{}
	applyCh chan raft.LogEntry
}

// NewKVStore creates a KVStore and starts its apply loop.
func NewKVStore(node *raft.Node, applyCh chan raft.LogEntry) *KVStore {
	kv := &KVStore{
		data: make(map[string]string),
		lastSeq: make(map[string]uint64),
		node: node,
		pending: make(map[int]chan struct{}),
		applyCh: applyCh,
	}
	go kv.applyLoop()
	return kv
}

func (kv *KVStore) applyLoop() {
	for entry := range kv.applyCh {
		var cmd Command
		if err := json.Unmarshal(entry.Command, &cmd); err != nil {
			continue
		}

		kv.mu.Lock()
		if cmd.SeqNo > kv.lastSeq[cmd.ClientID] {
			switch cmd.Op {
			case "put":
				kv.data[cmd.Key] = cmd.Value
			case "delete":
				delete(kv.data, cmd.Key)
			}
			kv.lastSeq[cmd.ClientID] = cmd.SeqNo
		}
		kv.mu.Unlock()

		kv.pendingMu.Lock()
		ch, ok := kv.pending[entry.Index]
		if ok {
			delete(kv.pending, entry.Index)
		}
		kv.pendingMu.Unlock()

		if ok {
			close(ch)
		}
	}
}

func (kv *KVStore) submit(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	index, err := kv.node.Submit(data)
	if err != nil {
		return err
	}
	ch := make(chan struct{})
	kv.pendingMu.Lock()
	kv.pending[index] = ch
	kv.pendingMu.Unlock()
	<-ch
	return nil
}

// Put sets key to value.
func (kv *KVStore) Put(clientID string, seqNo uint64, key, value string) error {
	return kv.submit(Command{Op: "put", Key: key, Value: value, ClientID: clientID, SeqNo: seqNo})
}

// Get returns the value for the key.
func (kv *KVStore) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return val, nil
}

// Delete removes the key from the store.
func (kv *KVStore) Delete(clientID string, seqNo uint64, key string) error {
	return kv.submit(Command{Op: "delete", Key: key, ClientID: clientID, SeqNo: seqNo})
}
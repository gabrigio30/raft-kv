package kvstore

import(
	"fmt"
	"math/rand"
	"sync"
)

// Client routes operations to the KV cluster leader.
type Client struct {
	id string
	seqNo uint64
	stores []*KVStore
	leader int
	mu sync.Mutex
}

// NewClient creates a client connected to the given stores.
func NewClient(stores []*KVStore) *Client {
	return &Client{
		id: fmt.Sprintf("client-&d", rand.Int63()),
		stores: stores,
	}
}

// Put sets key to value.
func (c *Client) Put(key, value string) error {
	c.mu.Lock()
	c.seqNo++
	seqNo := c.seqNo
	c.mu.Unlock()
	return c.retry(func(store *KVStore) error {
		return store.Put(c.id, seqNo, key, value)
	})
}

// Get returns the value for the key.
func (c *Client) Get(key string) (string, error) {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()
	return c.stores[leader].Get(key)
}

// Delete removes key from the store.
func (c *Client) Delete(key string) error {
	c.mu.Lock()
	c.seqNo++
	seqNo := c.seqNo
	c.mu.Unlock()
	return c.retry(func(store *KVStore) error {
		return store.Delete(c.id, seqNo, key)
	})
}

func (c *Client) retry(fn func(*KVStore) error) error {
	c.mu.Lock()
	start := c.leader
	c.mu.Unlock()
	for i := 0; i < len(c.stores); i++ {
		idx := (start + i) % len(c.stores)
		err := fn(c.stores[idx])
		if err == nil {
			c.mu.Lock()
			c.leader = idx
			c.mu.Unlock()
			return nil
		}
		if err.Error() != "not leader" {
			return err
		}
	}
	return fmt.Errorf("no leader found")
}
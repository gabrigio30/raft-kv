package storage

import (
	"encoding/json"
	"fmt"

	bbolt "go.etcd.io/bbolt"
)

var (
	bucketName = []byte("raft")
	keyState = []byte("state")
)

// HardState is the Raft state that must survive crashes
type HardState struct {
	CurrentTerm int
	VotedFor int	// -1 means no vote cast yet
	Log []byte
}

// Storage persists HardState to a bbolt database
type Storage struct {
	db *bbolt.DB
}

// NewStorage opens or creates the bbolt file at path
func NewStorage(path string) (*Storage, error) {
	db, err := bbolt.Open(path, 0600, nil) // 0600 is a Unix file permission (owner r/w only)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}
	return &Storage{db: db}, nil
}
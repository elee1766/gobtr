package btdu

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

// PebbleStore manages a single shared PebbleDB for all btdu sessions.
type PebbleStore struct {
	db      *pebble.DB
	baseDir string
	mu      sync.Mutex
}

// NewPebbleStore creates a new store backed by a single PebbleDB.
func NewPebbleStore(baseDir string) (*PebbleStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}

	dbPath := filepath.Join(baseDir, "btdu.db")

	opts := &pebble.Options{
		// Optimize for sustained write-heavy workload
		MemTableSize:                128 << 20, // 128MB memtable
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       8,
		L0StopWritesThreshold:       24,
		MaxConcurrentCompactions:    func() int { return 4 },
		Levels: []pebble.LevelOptions{
			{Compression: pebble.SnappyCompression},
			{Compression: pebble.SnappyCompression},
			{Compression: pebble.SnappyCompression},
			{Compression: pebble.SnappyCompression},
			{Compression: pebble.SnappyCompression},
			{Compression: pebble.SnappyCompression},
			{Compression: pebble.SnappyCompression},
		},
		DisableWAL: false,
		// Suppress noisy logs
		Logger: &silentLogger{},
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	return &PebbleStore{
		db:      db,
		baseDir: baseDir,
	}, nil
}

// silentLogger suppresses Pebble's info logs
type silentLogger struct{}

func (l *silentLogger) Infof(format string, args ...interface{})  {}
func (l *silentLogger) Errorf(format string, args ...interface{}) {}
func (l *silentLogger) Fatalf(format string, args ...interface{}) {}

// Close closes the store.
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// pathToKey converts a filesystem path to a short hash prefix.
func pathToKey(fsPath string) string {
	h := sha256.Sum256([]byte(fsPath))
	return hex.EncodeToString(h[:8])
}

// Has returns true if a session exists for the given filesystem path.
func (s *PebbleStore) Has(fsPath string) bool {
	key := []byte("fs:" + pathToKey(fsPath) + ":m:fs_path")
	_, closer, err := s.db.Get(key)
	if err == nil {
		closer.Close()
		return true
	}
	return false
}

// Open opens an existing session.
func (s *PebbleStore) Open(fsPath string) (*PebbleSession, error) {
	prefix := "fs:" + pathToKey(fsPath) + ":"
	return newPebbleSessionWithDB(s.db, prefix, fsPath, 0, false)
}

// OpenOrCreate opens or creates a session.
func (s *PebbleStore) OpenOrCreate(fsPath string, totalSize uint64) (*PebbleSession, bool, error) {
	existed := s.Has(fsPath)
	prefix := "fs:" + pathToKey(fsPath) + ":"
	session, err := newPebbleSessionWithDB(s.db, prefix, fsPath, totalSize, !existed)
	if err != nil {
		return nil, false, err
	}
	return session, existed, nil
}

// Delete removes a session's data.
func (s *PebbleStore) Delete(fsPath string) error {
	prefixStr := "fs:" + pathToKey(fsPath) + ":"
	prefix := []byte(prefixStr)

	// Create proper upper bound (prefix with last byte incremented)
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	upperBound[len(upperBound)-1]++

	// Use DeleteRange for efficiency, with Sync to ensure it's persisted
	if err := s.db.DeleteRange(prefix, upperBound, pebble.Sync); err != nil {
		return err
	}

	// Flush to ensure deletes are visible
	return s.db.Flush()
}

// List returns all filesystem paths with sessions.
func (s *PebbleStore) List() ([]string, error) {
	var paths []string
	seen := make(map[string]bool)

	prefix := []byte("fs:")
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		// Extract the hash part and check if it's a fs_path meta key
		keyStr := string(key)
		if len(keyStr) > 20 && keyStr[19:] == ":m:fs_path" {
			fsPath := string(iter.Value())
			if !seen[fsPath] {
				seen[fsPath] = true
				paths = append(paths, fsPath)
			}
		}
	}

	return paths, nil
}

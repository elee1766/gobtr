package btdu

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	// BoltExtension is the file extension for bbolt database files.
	BoltExtension = ".db"
)

// BoltStore manages BBolt-backed session storage on disk.
type BoltStore struct {
	dir string
}

// NewBoltStore creates a new BBolt store at the given directory.
func NewBoltStore(dir string) (*BoltStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	return &BoltStore{dir: dir}, nil
}

// fsPathToDBName converts a filesystem path to a safe database filename.
func fsPathToDBName(fsPath string) string {
	fsPath = filepath.Clean(fsPath)

	if fsPath == "/" {
		return "root.db"
	}

	name := strings.TrimPrefix(fsPath, "/")
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ":", "_")

	const maxLen = 64
	if len(name) > maxLen {
		hash := sha256.Sum256([]byte(fsPath))
		hashStr := hex.EncodeToString(hash[:8])
		name = name[:maxLen-17] + "-" + hashStr
	}

	return name + ".db"
}

// Path returns the database path for a given filesystem path.
func (s *BoltStore) Path(fsPath string) string {
	return filepath.Join(s.dir, fsPathToDBName(fsPath))
}

// Has returns true if a session exists for the given filesystem path.
func (s *BoltStore) Has(fsPath string) bool {
	_, err := os.Stat(s.Path(fsPath))
	return err == nil
}

// Open opens an existing session or returns an error if it doesn't exist.
func (s *BoltStore) Open(fsPath string) (*BoltSession, error) {
	if !s.Has(fsPath) {
		return nil, fmt.Errorf("session not found for %s", fsPath)
	}
	return OpenBoltSession(s.Path(fsPath))
}

// OpenOrCreate opens an existing session or creates a new one.
func (s *BoltStore) OpenOrCreate(fsPath string, totalSize uint64) (*BoltSession, bool, error) {
	existed := s.Has(fsPath)

	session, err := NewBoltSession(s.Path(fsPath), fsPath, totalSize)
	if err != nil {
		return nil, false, err
	}

	// Verify it's for the same filesystem
	if session.FSPath() != fsPath {
		session.Close()
		return nil, false, fmt.Errorf("session mismatch: stored %s, requested %s", session.FSPath(), fsPath)
	}

	return session, existed, nil
}

// Delete removes the session for the given filesystem path.
func (s *BoltStore) Delete(fsPath string) error {
	path := s.Path(fsPath)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove session: %w", err)
	}
	return nil
}

// BoltSessionInfo contains metadata about a stored session.
type BoltSessionInfo struct {
	FSPath   string
	FilePath string
	Size     int64
}

// List returns info about all stored sessions.
func (s *BoltStore) List() ([]BoltSessionInfo, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read store directory: %w", err)
	}

	var sessions []BoltSessionInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}

		filePath := filepath.Join(s.dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Try to recover fs path by opening the database
		var fsPath string
		if session, err := OpenBoltSession(filePath); err == nil {
			fsPath = session.FSPath()
			session.Close()
		}

		sessions = append(sessions, BoltSessionInfo{
			FSPath:   fsPath,
			FilePath: filePath,
			Size:     info.Size(),
		})
	}

	return sessions, nil
}

// Dir returns the store directory path.
func (s *BoltStore) Dir() string {
	return s.dir
}

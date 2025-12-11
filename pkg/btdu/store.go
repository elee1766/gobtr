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
	// SampleExtension is the file extension for sample files.
	SampleExtension = ".gob"
)

// Store manages usage sample storage on disk.
// Samples are stored one per filesystem, named by the filesystem path.
type Store struct {
	dir string // Directory where usage samples are stored
}

// NewStore creates a new usage sample store at the given directory.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	return &Store{dir: dir}, nil
}

// fsPathToFilename converts a filesystem path to a safe filename.
// Example: "/mnt/data" -> "mnt-data.gob"
// Example: "/" -> "root.gob"
// For long or complex paths, uses a hash suffix for uniqueness.
func fsPathToFilename(fsPath string) string {
	// Normalize the path
	fsPath = filepath.Clean(fsPath)

	// Handle root specially
	if fsPath == "/" {
		return "root.gob"
	}

	// Remove leading slash and replace separators with dashes
	name := strings.TrimPrefix(fsPath, "/")
	name = strings.ReplaceAll(name, "/", "-")

	// Replace other problematic characters
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ":", "_")

	// If the name is too long, truncate and add hash for uniqueness
	const maxLen = 64
	if len(name) > maxLen {
		hash := sha256.Sum256([]byte(fsPath))
		hashStr := hex.EncodeToString(hash[:8]) // 16 chars
		name = name[:maxLen-17] + "-" + hashStr
	}

	return name + ".gob"
}

// filenameToFSPath attempts to recover the fs path from a filename.
// This is a best-effort reverse of fsPathToFilename.
// For hashed names, returns empty string (use Load to get FSPath from metadata).
func filenameToFSPath(filename string) string {
	name := strings.TrimSuffix(filename, ".gob")
	if name == "root" {
		return "/"
	}
	// Check if it's a hashed name (contains a hash suffix)
	// Hashed names are unreliable to reverse, return empty
	if len(name) > 17 && name[len(name)-17] == '-' {
		return ""
	}
	return "/" + strings.ReplaceAll(name, "-", "/")
}

// Path returns the file path for a given filesystem path.
func (s *Store) Path(fsPath string) string {
	return filepath.Join(s.dir, fsPathToFilename(fsPath))
}

// Has returns true if a session exists for the given filesystem path.
func (s *Store) Has(fsPath string) bool {
	_, err := os.Stat(s.Path(fsPath))
	return err == nil
}

// Load loads a session for the given filesystem path.
func (s *Store) Load(fsPath string) (*Session, error) {
	return Load(s.Path(fsPath))
}

// Save saves a session for its filesystem path.
func (s *Store) Save(session *Session) error {
	return session.Save(s.Path(session.FSPath))
}

// Delete removes the session for the given filesystem path.
func (s *Store) Delete(fsPath string) error {
	path := s.Path(fsPath)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove session: %w", err)
	}
	return nil
}

// LoadOrCreate loads an existing session or creates a new one.
func (s *Store) LoadOrCreate(fsPath string, totalSize uint64) (*Session, bool, error) {
	if s.Has(fsPath) {
		session, err := s.Load(fsPath)
		if err != nil {
			return nil, false, err
		}
		// Verify it's for the same filesystem
		if session.FSPath != fsPath {
			return nil, false, fmt.Errorf("session mismatch: stored %s, requested %s", session.FSPath, fsPath)
		}
		return session, true, nil
	}
	return NewSession(fsPath, totalSize), false, nil
}

// SessionInfo contains metadata about a stored session without loading the full data.
type SessionInfo struct {
	FSPath   string // Filesystem path
	FilePath string // Path to the session file
	Size     int64  // File size in bytes
}

// List returns info about all stored sessions.
func (s *Store) List() ([]SessionInfo, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read store directory: %w", err)
	}

	var sessions []SessionInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".gob") {
			continue
		}

		filePath := filepath.Join(s.dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Try to recover fs path from filename
		fsPath := filenameToFSPath(entry.Name())

		// If we can't recover it, load the session to get the real path
		if fsPath == "" {
			if session, err := Load(filePath); err == nil {
				fsPath = session.FSPath
			}
		}

		sessions = append(sessions, SessionInfo{
			FSPath:   fsPath,
			FilePath: filePath,
			Size:     info.Size(),
		})
	}

	return sessions, nil
}

// Dir returns the store directory path.
func (s *Store) Dir() string {
	return s.dir
}

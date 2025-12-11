package btdu

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"time"
)

// Session represents a sampling session with metadata and collected data.
type Session struct {
	// Metadata
	FSPath      string        // Filesystem path being sampled
	TotalSize   uint64        // Total size of the filesystem in bytes
	StartedAt   time.Time     // When the session was originally created
	LastUpdated time.Time     // When the session was last updated
	SampleCount uint64        // Total number of samples collected
	RunningTime time.Duration // Cumulative time spent actively sampling

	// Runtime state (not persisted)
	runStartedAt time.Time // When current run started (zero if not running)

	// Data
	Root *PathNode // Root of the path trie

	mu sync.RWMutex // Protects concurrent access
}

// NewSession creates a new sampling session.
func NewSession(fsPath string, totalSize uint64) *Session {
	return &Session{
		FSPath:      fsPath,
		TotalSize:   totalSize,
		StartedAt:   time.Now(),
		LastUpdated: time.Now(),
		Root:        NewRootNode(),
	}
}

// AddSample adds a sample to the session.
func (s *Session) AddSample(path string, sampleType SampleType, offset Offset, duration time.Duration) {
	node := s.Root.GetOrCreatePath(path)
	node.AddSample(sampleType, offset, duration)

	s.mu.Lock()
	s.SampleCount++
	now := time.Now()
	s.LastUpdated = now
	// Update running time if we're actively sampling
	if !s.runStartedAt.IsZero() {
		s.RunningTime += now.Sub(s.runStartedAt)
		s.runStartedAt = now
	}
	s.mu.Unlock()
}

// GetPath returns the node for a path, or nil if not found.
func (s *Session) GetPath(path string) *PathNode {
	return s.Root.GetPath(path)
}

// StartRun marks the beginning of an active sampling run.
func (s *Session) StartRun() {
	s.mu.Lock()
	s.runStartedAt = time.Now()
	s.mu.Unlock()
}

// StopRun marks the end of an active sampling run and accumulates running time.
func (s *Session) StopRun() {
	s.mu.Lock()
	if !s.runStartedAt.IsZero() {
		s.RunningTime += time.Since(s.runStartedAt)
		s.runStartedAt = time.Time{}
	}
	s.mu.Unlock()
}

// GetRunningTime returns the total cumulative running time.
// If currently running, includes time from the current run.
func (s *Session) GetRunningTime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := s.RunningTime
	if !s.runStartedAt.IsZero() {
		total += time.Since(s.runStartedAt)
	}
	return total
}

// Duration returns how long the session has been running.
func (s *Session) Duration() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastUpdated.Sub(s.StartedAt)
}

// SerializedSession is the gob-friendly version of Session.
type SerializedSession struct {
	FSPath      string
	TotalSize   uint64
	StartedAt   time.Time
	LastUpdated time.Time
	SampleCount uint64
	RunningTime time.Duration
	Root        SerializedNode
}

// Save persists the session to a file using gob encoding.
func (s *Session) Save(path string) error {
	s.mu.RLock()
	// Calculate current running time including any active run
	runningTime := s.RunningTime
	if !s.runStartedAt.IsZero() {
		runningTime += time.Since(s.runStartedAt)
	}
	serialized := SerializedSession{
		FSPath:      s.FSPath,
		TotalSize:   s.TotalSize,
		StartedAt:   s.StartedAt,
		LastUpdated: s.LastUpdated,
		SampleCount: s.SampleCount,
		RunningTime: runningTime,
		Root:        s.Root.Serialize(),
	}
	s.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create session file: %w", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(&serialized); err != nil {
		return fmt.Errorf("encode session: %w", err)
	}

	return nil
}

// Load loads a session from a file.
func Load(path string) (*Session, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open session file: %w", err)
	}
	defer f.Close()

	var serialized SerializedSession
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&serialized); err != nil {
		return nil, fmt.Errorf("decode session: %w", err)
	}

	session := &Session{
		FSPath:      serialized.FSPath,
		TotalSize:   serialized.TotalSize,
		StartedAt:   serialized.StartedAt,
		LastUpdated: serialized.LastUpdated,
		SampleCount: serialized.SampleCount,
		RunningTime: serialized.RunningTime,
		Root:        serialized.Root.Deserialize(nil),
	}

	return session, nil
}

// LoadOrCreate loads an existing session or creates a new one.
func LoadOrCreate(path, fsPath string, totalSize uint64) (*Session, bool, error) {
	if _, err := os.Stat(path); err == nil {
		session, err := Load(path)
		if err != nil {
			return nil, false, err
		}
		// Verify it's for the same filesystem
		if session.FSPath != fsPath {
			return nil, false, fmt.Errorf("session is for different filesystem: %s (expected %s)", session.FSPath, fsPath)
		}
		return session, true, nil // resumed
	}

	return NewSession(fsPath, totalSize), false, nil // new session
}

// Stats returns summary statistics for the session.
type SessionStats struct {
	FSPath       string
	TotalSize    uint64
	SampleCount  uint64
	Duration     time.Duration
	UniquePathCount int
	MaxDepth     int
}

// Stats computes summary statistics for the session.
func (s *Session) Stats() SessionStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := SessionStats{
		FSPath:      s.FSPath,
		TotalSize:   s.TotalSize,
		SampleCount: s.SampleCount,
		Duration:    s.LastUpdated.Sub(s.StartedAt),
	}

	s.Root.Walk(func(node *PathNode, depth int) bool {
		stats.UniquePathCount++
		if depth > stats.MaxDepth {
			stats.MaxDepth = depth
		}
		return true
	})

	return stats
}

// TopPaths returns the top N paths by sample count for a given sample type.
type RankedPath struct {
	Path    string
	Samples uint64
	Size    uint64 // Estimated size based on samples
}

func (s *Session) TopPaths(sampleType SampleType, n int) []RankedPath {
	var paths []RankedPath

	s.Root.Walk(func(node *PathNode, depth int) bool {
		samples := node.Stats.Data[sampleType].Samples
		if samples > 0 && node.IsLeaf() {
			// Estimate size: (samples / total_samples) * total_size
			var estimatedSize uint64
			if s.SampleCount > 0 {
				estimatedSize = (samples * s.TotalSize) / s.SampleCount
			}
			paths = append(paths, RankedPath{
				Path:    node.FullPath(),
				Samples: samples,
				Size:    estimatedSize,
			})
		}
		return true
	})

	// Sort by samples descending
	for i := 0; i < len(paths); i++ {
		for j := i + 1; j < len(paths); j++ {
			if paths[j].Samples > paths[i].Samples {
				paths[i], paths[j] = paths[j], paths[i]
			}
		}
	}

	if n > len(paths) {
		n = len(paths)
	}
	return paths[:n]
}

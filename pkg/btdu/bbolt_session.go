package btdu

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	// Bucket names
	metaBucket  = []byte("meta")
	pathsBucket = []byte("paths")

	// Meta keys
	metaFSPath      = []byte("fs_path")
	metaTotalSize   = []byte("total_size")
	metaStartedAt   = []byte("started_at")
	metaLastUpdated = []byte("last_updated")
	metaSampleCount = []byte("sample_count")
	metaRunningTime = []byte("running_time")
)

// BoltSession represents a sampling session backed by BBolt.
// Instead of keeping all paths in memory, paths are stored on disk
// and loaded on demand.
type BoltSession struct {
	db *bolt.DB

	// Cached metadata (frequently accessed)
	fsPath      string
	totalSize   uint64
	startedAt   time.Time
	lastUpdated time.Time
	sampleCount uint64
	runningTime time.Duration

	// Runtime state
	runStartedAt time.Time
	dirty        bool // metadata needs to be flushed

	mu sync.RWMutex
}

// OpenBoltSession opens or creates a BBolt-backed session.
func OpenBoltSession(dbPath string) (*BoltSession, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout:      1 * time.Second,
		NoGrowSync:   false,
		NoSync:       true, // Don't fsync on every write - much faster, acceptable data loss on crash
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, fmt.Errorf("open bbolt: %w", err)
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(metaBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(pathsBucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create buckets: %w", err)
	}

	session := &BoltSession{db: db}

	// Load metadata
	if err := session.loadMetadata(); err != nil {
		db.Close()
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	return session, nil
}

// NewBoltSession creates a new BBolt session with the given parameters.
func NewBoltSession(dbPath, fsPath string, totalSize uint64) (*BoltSession, error) {
	session, err := OpenBoltSession(dbPath)
	if err != nil {
		return nil, err
	}

	// Initialize if new
	if session.fsPath == "" {
		session.fsPath = fsPath
		session.totalSize = totalSize
		session.startedAt = time.Now()
		session.lastUpdated = time.Now()
		session.dirty = true
		if err := session.flushMetadata(); err != nil {
			session.Close()
			return nil, err
		}
	}

	return session, nil
}

// loadMetadata loads cached metadata from the database.
func (s *BoltSession) loadMetadata() error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		if b == nil {
			return nil
		}

		if v := b.Get(metaFSPath); v != nil {
			s.fsPath = string(v)
		}
		if v := b.Get(metaTotalSize); v != nil {
			s.totalSize = decodeUint64(v)
		}
		if v := b.Get(metaStartedAt); v != nil {
			s.startedAt = decodeTime(v)
		}
		if v := b.Get(metaLastUpdated); v != nil {
			s.lastUpdated = decodeTime(v)
		}
		if v := b.Get(metaSampleCount); v != nil {
			s.sampleCount = decodeUint64(v)
		}
		if v := b.Get(metaRunningTime); v != nil {
			s.runningTime = time.Duration(decodeInt64(v))
		}
		return nil
	})
}

// flushMetadata writes cached metadata to the database.
func (s *BoltSession) flushMetadata() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty {
		return nil
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)

		if err := b.Put(metaFSPath, []byte(s.fsPath)); err != nil {
			return err
		}
		if err := b.Put(metaTotalSize, encodeUint64(s.totalSize)); err != nil {
			return err
		}
		if err := b.Put(metaStartedAt, encodeTime(s.startedAt)); err != nil {
			return err
		}
		if err := b.Put(metaLastUpdated, encodeTime(s.lastUpdated)); err != nil {
			return err
		}
		if err := b.Put(metaSampleCount, encodeUint64(s.sampleCount)); err != nil {
			return err
		}
		if err := b.Put(metaRunningTime, encodeInt64(int64(s.runningTime))); err != nil {
			return err
		}
		return nil
	})

	if err == nil {
		s.dirty = false
	}
	return err
}

// AddSample adds a sample to the session.
func (s *BoltSession) AddSample(path string, sampleType SampleType, offset Offset, duration time.Duration) error {
	// Update path stats in database
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)

		// Update all ancestors (including the path itself)
		segments := splitPath(path)
		for i := 0; i <= len(segments); i++ {
			var currentPath string
			if i == 0 {
				currentPath = "/"
			} else {
				currentPath = "/" + joinPath(segments[:i])
			}

			key := []byte(currentPath)
			var stats PathStats

			// Load existing stats
			if v := b.Get(key); v != nil {
				if err := decodeStats(v, &stats); err != nil {
					return fmt.Errorf("decode stats for %s: %w", currentPath, err)
				}
			}

			// Update stats
			stats.AddSample(sampleType, offset, duration)

			// Save stats
			encoded, err := encodeStats(&stats)
			if err != nil {
				return fmt.Errorf("encode stats for %s: %w", currentPath, err)
			}
			if err := b.Put(key, encoded); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Update metadata
	s.mu.Lock()
	s.sampleCount++
	now := time.Now()
	s.lastUpdated = now
	if !s.runStartedAt.IsZero() {
		s.runningTime += now.Sub(s.runStartedAt)
		s.runStartedAt = now
	}
	s.dirty = true
	s.mu.Unlock()

	return nil
}

// AddSampleBatch adds multiple samples efficiently in a single transaction.
func (s *BoltSession) AddSampleBatch(samples []SampleRecord) error {
	if len(samples) == 0 {
		return nil
	}

	// Collect all path updates
	pathUpdates := make(map[string]*PathStats)

	for _, sample := range samples {
		// Update all ancestors
		segments := splitPath(sample.Path)
		for i := 0; i <= len(segments); i++ {
			var currentPath string
			if i == 0 {
				currentPath = "/"
			} else {
				currentPath = "/" + joinPath(segments[:i])
			}

			stats, ok := pathUpdates[currentPath]
			if !ok {
				stats = &PathStats{}
				pathUpdates[currentPath] = stats
			}
			stats.AddSample(sample.Type, sample.Offset, sample.Duration)
		}
	}

	// Write all updates in one transaction
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)

		for path, newStats := range pathUpdates {
			key := []byte(path)
			var stats PathStats

			// Load existing stats
			if v := b.Get(key); v != nil {
				if err := decodeStats(v, &stats); err != nil {
					return fmt.Errorf("decode stats for %s: %w", path, err)
				}
			}

			// Merge new stats
			for i := 0; i < int(NumSampleTypes); i++ {
				stats.Data[i].Samples += newStats.Data[i].Samples
				stats.Data[i].Duration += newStats.Data[i].Duration
				// Copy last offsets from new stats
				if newStats.Data[i].Samples > 0 {
					stats.Data[i].Offsets = newStats.Data[i].Offsets
				}
			}
			stats.DistributedSamples += newStats.DistributedSamples
			stats.DistributedDuration += newStats.DistributedDuration

			// Save stats
			encoded, err := encodeStats(&stats)
			if err != nil {
				return fmt.Errorf("encode stats for %s: %w", path, err)
			}
			if err := b.Put(key, encoded); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Update metadata
	s.mu.Lock()
	s.sampleCount += uint64(len(samples))
	now := time.Now()
	s.lastUpdated = now
	if !s.runStartedAt.IsZero() {
		s.runningTime += now.Sub(s.runStartedAt)
		s.runStartedAt = now
	}
	s.dirty = true
	s.mu.Unlock()

	return nil
}

// SampleRecord represents a single sample for batch operations.
type SampleRecord struct {
	Path     string
	Type     SampleType
	Offset   Offset
	Duration time.Duration
}

// GetPathStats returns stats for a specific path.
func (s *BoltSession) GetPathStats(path string) (*PathStats, error) {
	var stats PathStats
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		v := b.Get([]byte(path))
		if v == nil {
			return nil // Path not found, return zero stats
		}
		return decodeStats(v, &stats)
	})
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

// GetChildren returns all direct children of a path.
func (s *BoltSession) GetChildren(parentPath string) ([]ChildInfo, error) {
	var children []ChildInfo

	// Normalize parent path
	if parentPath == "" {
		parentPath = "/"
	}

	prefix := parentPath
	if prefix != "/" {
		prefix += "/"
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		c := b.Cursor()

		// Seek to prefix
		for k, v := c.Seek([]byte(prefix)); k != nil; k, v = c.Next() {
			path := string(k)

			// Stop if we've moved past the prefix
			if !hasPrefix(path, prefix) && path != parentPath {
				break
			}

			// Skip the parent itself
			if path == parentPath {
				continue
			}

			// Get the relative part after prefix
			relative := path[len(prefix):]
			if relative == "" {
				continue
			}

			// Check if this is a direct child (no more slashes)
			slashIdx := indexOf(relative, '/')
			if slashIdx != -1 {
				continue // Not a direct child
			}

			// This is a direct child
			var stats PathStats
			if err := decodeStats(v, &stats); err != nil {
				continue
			}

			children = append(children, ChildInfo{
				Name:  relative,
				Path:  path,
				Stats: stats,
			})
		}
		return nil
	})

	return children, err
}

// ChildInfo contains information about a child path.
type ChildInfo struct {
	Name  string
	Path  string
	Stats PathStats
}

// StartRun marks the beginning of an active sampling run.
func (s *BoltSession) StartRun() {
	s.mu.Lock()
	s.runStartedAt = time.Now()
	s.mu.Unlock()
}

// StopRun marks the end of an active sampling run.
func (s *BoltSession) StopRun() {
	s.mu.Lock()
	if !s.runStartedAt.IsZero() {
		s.runningTime += time.Since(s.runStartedAt)
		s.runStartedAt = time.Time{}
		s.dirty = true
	}
	s.mu.Unlock()
}

// Flush writes any pending changes to disk.
func (s *BoltSession) Flush() error {
	return s.flushMetadata()
}

// Sync forces a sync of the database to disk.
func (s *BoltSession) Sync() error {
	return s.db.Sync()
}

// Close closes the database.
func (s *BoltSession) Close() error {
	if err := s.flushMetadata(); err != nil {
		s.db.Close()
		return err
	}
	// Force sync before close since we use NoSync
	s.db.Sync()
	return s.db.Close()
}

// FSPath returns the filesystem path.
func (s *BoltSession) FSPath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fsPath
}

// TotalSize returns the total filesystem size.
func (s *BoltSession) TotalSize() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalSize
}

// SampleCount returns the total number of samples.
func (s *BoltSession) SampleCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sampleCount
}

// GetRunningTime returns the total running time.
func (s *BoltSession) GetRunningTime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := s.runningTime
	if !s.runStartedAt.IsZero() {
		total += time.Since(s.runStartedAt)
	}
	return total
}

// StartedAt returns when the session was created.
func (s *BoltSession) StartedAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startedAt
}

// LastUpdated returns when the session was last updated.
func (s *BoltSession) LastUpdated() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastUpdated
}

// PathCount returns the number of unique paths stored.
func (s *BoltSession) PathCount() int {
	var count int
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		count = b.Stats().KeyN
		return nil
	})
	return count
}

// TopPaths returns the top N paths by sample count for a given sample type.
func (s *BoltSession) TopPaths(sampleType SampleType, n int) ([]RankedPath, error) {
	var paths []RankedPath

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(pathsBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var stats PathStats
			if err := decodeStats(v, &stats); err != nil {
				continue
			}

			samples := stats.Data[sampleType].Samples
			if samples > 0 {
				// Check if this is a leaf (no children)
				path := string(k)
				isLeaf := true

				// Quick check: see if there's anything after this path
				prefix := path + "/"
				nextK, _ := c.Seek([]byte(prefix))
				if nextK != nil && hasPrefix(string(nextK), prefix) {
					isLeaf = false
				}
				// Restore cursor position
				c.Seek(k)

				if isLeaf {
					var estimatedSize uint64
					if s.sampleCount > 0 {
						estimatedSize = (samples * s.totalSize) / s.sampleCount
					}
					paths = append(paths, RankedPath{
						Path:    path,
						Samples: samples,
						Size:    estimatedSize,
					})
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

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
	return paths[:n], nil
}

// Encoding helpers

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
	return b
}

func decodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func encodeInt64(v int64) []byte {
	return encodeUint64(uint64(v))
}

func decodeInt64(b []byte) int64 {
	return int64(decodeUint64(b))
}

func encodeTime(t time.Time) []byte {
	return encodeInt64(t.UnixNano())
}

func decodeTime(b []byte) time.Time {
	return time.Unix(0, decodeInt64(b))
}

// Binary encoding for PathStats - much faster than gob
// Format: [5 x uint64 sample counts] + [5 x int64 durations] + [2 x float64 distributed]
// = 40 + 40 + 16 = 96 bytes fixed size
const statsEncodedSize = 96

func encodeStats(stats *PathStats) ([]byte, error) {
	buf := make([]byte, statsEncodedSize)
	off := 0

	// Sample counts (5 x uint64)
	for i := 0; i < int(NumSampleTypes); i++ {
		putUint64(buf[off:], stats.Data[i].Samples)
		off += 8
	}

	// Durations (5 x int64)
	for i := 0; i < int(NumSampleTypes); i++ {
		putInt64(buf[off:], int64(stats.Data[i].Duration))
		off += 8
	}

	// Distributed values (2 x float64)
	putFloat64(buf[off:], stats.DistributedSamples)
	off += 8
	putFloat64(buf[off:], stats.DistributedDuration)

	return buf, nil
}

func decodeStats(data []byte, stats *PathStats) error {
	if len(data) < statsEncodedSize {
		// Handle old gob-encoded data or short data
		if len(data) > 0 && data[0] != 0 {
			// Might be gob encoded, try that
			return gob.NewDecoder(bytes.NewReader(data)).Decode(stats)
		}
		return nil
	}

	off := 0

	// Sample counts
	for i := 0; i < int(NumSampleTypes); i++ {
		stats.Data[i].Samples = getUint64(data[off:])
		off += 8
	}

	// Durations
	for i := 0; i < int(NumSampleTypes); i++ {
		stats.Data[i].Duration = time.Duration(getInt64(data[off:]))
		off += 8
	}

	// Distributed values
	stats.DistributedSamples = getFloat64(data[off:])
	off += 8
	stats.DistributedDuration = getFloat64(data[off:])

	return nil
}

func putUint64(b []byte, v uint64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

func getUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func putInt64(b []byte, v int64) {
	putUint64(b, uint64(v))
}

func getInt64(b []byte) int64 {
	return int64(getUint64(b))
}

func putFloat64(b []byte, v float64) {
	putUint64(b, math.Float64bits(v))
}

func getFloat64(b []byte) float64 {
	return math.Float64frombits(getUint64(b))
}

// String helpers

func joinPath(segments []string) string {
	if len(segments) == 0 {
		return ""
	}
	result := segments[0]
	for i := 1; i < len(segments); i++ {
		result += "/" + segments[i]
	}
	return result
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

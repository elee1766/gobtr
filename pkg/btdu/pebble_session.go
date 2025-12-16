package btdu

import (
	"bytes"
	"encoding/gob"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleSession represents a sampling session backed by a shared PebbleDB.
type PebbleSession struct {
	db     *pebble.DB
	prefix string // Key prefix for this session (e.g., "fs:abc123:")

	// Cached metadata
	fsPath      string
	totalSize   uint64
	startedAt   time.Time
	lastUpdated time.Time
	sampleCount uint64
	runningTime time.Duration

	// Runtime state
	runStartedAt time.Time
	dirty        bool

	// In-memory accumulator for batching writes
	accumulator     map[string]*PathStats
	accumulatorSize int
	accumulatorMu   sync.Mutex

	mu sync.RWMutex
}

const (
	// Flush accumulator when it reaches this many unique paths
	accumulatorFlushThreshold = 10000
)

// Key helpers
func (s *PebbleSession) metaKey(name string) []byte {
	return []byte(s.prefix + "m:" + name)
}

func (s *PebbleSession) pathKey(path string) []byte {
	return []byte(s.prefix + "p:" + path)
}

// newPebbleSessionWithDB creates a session using a shared DB.
func newPebbleSessionWithDB(db *pebble.DB, prefix, fsPath string, totalSize uint64, isNew bool) (*PebbleSession, error) {
	session := &PebbleSession{
		db:          db,
		prefix:      prefix,
		accumulator: make(map[string]*PathStats),
	}

	if err := session.loadMetadata(); err != nil {
		return nil, err
	}

	if isNew || session.fsPath == "" {
		session.fsPath = fsPath
		session.totalSize = totalSize
		session.startedAt = time.Now()
		session.lastUpdated = time.Now()
		session.dirty = true
		if err := session.flushMetadata(); err != nil {
			return nil, err
		}
	}

	return session, nil
}

func (s *PebbleSession) loadMetadata() error {
	if v, closer, err := s.db.Get(s.metaKey("fs_path")); err == nil {
		s.fsPath = string(v)
		closer.Close()
	}
	if v, closer, err := s.db.Get(s.metaKey("total_size")); err == nil {
		s.totalSize = decodeUint64(v)
		closer.Close()
	}
	if v, closer, err := s.db.Get(s.metaKey("started_at")); err == nil {
		s.startedAt = decodeTime(v)
		closer.Close()
	}
	if v, closer, err := s.db.Get(s.metaKey("last_updated")); err == nil {
		s.lastUpdated = decodeTime(v)
		closer.Close()
	}
	if v, closer, err := s.db.Get(s.metaKey("sample_count")); err == nil {
		s.sampleCount = decodeUint64(v)
		closer.Close()
	}
	if v, closer, err := s.db.Get(s.metaKey("running_time")); err == nil {
		s.runningTime = time.Duration(decodeInt64(v))
		closer.Close()
	}
	return nil
}

func (s *PebbleSession) flushMetadata() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty {
		return nil
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	batch.Set(s.metaKey("fs_path"), []byte(s.fsPath), pebble.NoSync)
	batch.Set(s.metaKey("total_size"), encodeUint64(s.totalSize), pebble.NoSync)
	batch.Set(s.metaKey("started_at"), encodeTime(s.startedAt), pebble.NoSync)
	batch.Set(s.metaKey("last_updated"), encodeTime(s.lastUpdated), pebble.NoSync)
	batch.Set(s.metaKey("sample_count"), encodeUint64(s.sampleCount), pebble.NoSync)
	batch.Set(s.metaKey("running_time"), encodeInt64(int64(s.runningTime)), pebble.NoSync)

	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	s.dirty = false
	return nil
}

// AddSampleBatch adds multiple samples to the in-memory accumulator.
func (s *PebbleSession) AddSampleBatch(samples []SampleRecord) error {
	if len(samples) == 0 {
		return nil
	}

	s.accumulatorMu.Lock()

	for _, sample := range samples {
		segments := splitPath(sample.Path)
		for i := 0; i <= len(segments); i++ {
			var currentPath string
			if i == 0 {
				currentPath = "/"
			} else {
				currentPath = "/" + joinPath(segments[:i])
			}

			stats, ok := s.accumulator[currentPath]
			if !ok {
				stats = &PathStats{}
				s.accumulator[currentPath] = stats
				s.accumulatorSize++
			}
			stats.AddSample(sample.Type, sample.Offset, sample.Duration)
		}
	}

	shouldFlush := s.accumulatorSize >= accumulatorFlushThreshold
	s.accumulatorMu.Unlock()

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

	if shouldFlush {
		return s.FlushAccumulator()
	}

	return nil
}

// FlushAccumulator writes accumulated stats to disk.
func (s *PebbleSession) FlushAccumulator() error {
	s.accumulatorMu.Lock()
	if len(s.accumulator) == 0 {
		s.accumulatorMu.Unlock()
		return nil
	}

	toFlush := s.accumulator
	s.accumulator = make(map[string]*PathStats)
	s.accumulatorSize = 0
	s.accumulatorMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()

	for path, newStats := range toFlush {
		key := s.pathKey(path)
		var stats PathStats

		if v, closer, err := s.db.Get(key); err == nil {
			decodePebbleStats(v, &stats)
			closer.Close()
		}

		for i := 0; i < int(NumSampleTypes); i++ {
			stats.Data[i].Samples += newStats.Data[i].Samples
			stats.Data[i].Duration += newStats.Data[i].Duration
			if newStats.Data[i].Samples > 0 {
				stats.Data[i].Offsets = newStats.Data[i].Offsets
			}
		}
		stats.DistributedSamples += newStats.DistributedSamples
		stats.DistributedDuration += newStats.DistributedDuration

		batch.Set(key, encodePebbleStats(&stats), pebble.NoSync)
	}

	return batch.Commit(pebble.NoSync)
}

// GetPathStats returns stats for a specific path.
func (s *PebbleSession) GetPathStats(path string) (*PathStats, error) {
	var stats PathStats

	key := s.pathKey(path)
	if v, closer, err := s.db.Get(key); err == nil {
		decodePebbleStats(v, &stats)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, err
	}

	s.accumulatorMu.Lock()
	if accStats, ok := s.accumulator[path]; ok {
		for i := 0; i < int(NumSampleTypes); i++ {
			stats.Data[i].Samples += accStats.Data[i].Samples
			stats.Data[i].Duration += accStats.Data[i].Duration
			if accStats.Data[i].Samples > 0 {
				stats.Data[i].Offsets = accStats.Data[i].Offsets
			}
		}
		stats.DistributedSamples += accStats.DistributedSamples
		stats.DistributedDuration += accStats.DistributedDuration
	}
	s.accumulatorMu.Unlock()

	return &stats, nil
}

// GetChildren returns all direct children of a path.
func (s *PebbleSession) GetChildren(parentPath string) ([]ChildInfo, error) {
	if parentPath == "" {
		parentPath = "/"
	}

	prefix := parentPath
	if prefix != "/" {
		prefix += "/"
	}

	childMap := make(map[string]*PathStats)

	// Scan disk
	prefixKey := s.pathKey(prefix)
	upperBound := make([]byte, len(prefixKey))
	copy(upperBound, prefixKey)
	upperBound[len(upperBound)-1]++

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixKey,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	pathsPrefixLen := len(s.prefix) + 2 // "fs:xxx:p:"
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= pathsPrefixLen {
			continue
		}

		path := string(key[pathsPrefixLen:])

		if !hasPrefix(path, prefix) && path != parentPath {
			break
		}
		if path == parentPath {
			continue
		}

		relative := path[len(prefix):]
		if relative == "" || indexOf(relative, '/') != -1 {
			continue
		}

		stats := &PathStats{}
		decodePebbleStats(iter.Value(), stats)
		childMap[path] = stats
	}

	// Merge accumulator
	s.accumulatorMu.Lock()
	for accPath, accStats := range s.accumulator {
		if !hasPrefix(accPath, prefix) || accPath == parentPath {
			continue
		}

		relative := accPath[len(prefix):]
		if relative == "" || indexOf(relative, '/') != -1 {
			continue
		}

		if existing, ok := childMap[accPath]; ok {
			for i := 0; i < int(NumSampleTypes); i++ {
				existing.Data[i].Samples += accStats.Data[i].Samples
				existing.Data[i].Duration += accStats.Data[i].Duration
				if accStats.Data[i].Samples > 0 {
					existing.Data[i].Offsets = accStats.Data[i].Offsets
				}
			}
			existing.DistributedSamples += accStats.DistributedSamples
			existing.DistributedDuration += accStats.DistributedDuration
		} else {
			statsCopy := *accStats
			childMap[accPath] = &statsCopy
		}
	}
	s.accumulatorMu.Unlock()

	children := make([]ChildInfo, 0, len(childMap))
	for path, stats := range childMap {
		relative := path[len(prefix):]
		children = append(children, ChildInfo{
			Name:  relative,
			Path:  path,
			Stats: *stats,
		})
	}

	return children, nil
}

// StartRun marks the beginning of an active sampling run.
func (s *PebbleSession) StartRun() {
	s.mu.Lock()
	s.runStartedAt = time.Now()
	s.mu.Unlock()
}

// StopRun marks the end of an active sampling run.
func (s *PebbleSession) StopRun() {
	s.mu.Lock()
	if !s.runStartedAt.IsZero() {
		s.runningTime += time.Since(s.runStartedAt)
		s.runStartedAt = time.Time{}
		s.dirty = true
	}
	s.mu.Unlock()
}

// Flush writes pending changes.
func (s *PebbleSession) Flush() error {
	if err := s.FlushAccumulator(); err != nil {
		return err
	}
	return s.flushMetadata()
}

// Sync forces a sync to disk.
func (s *PebbleSession) Sync() error {
	return s.db.Flush()
}

// Close flushes pending data (but doesn't close the shared DB).
func (s *PebbleSession) Close() error {
	if err := s.FlushAccumulator(); err != nil {
		return err
	}
	return s.flushMetadata()
}

// Accessors
func (s *PebbleSession) FSPath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fsPath
}

func (s *PebbleSession) TotalSize() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalSize
}

func (s *PebbleSession) SetTotalSize(size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totalSize != size {
		s.totalSize = size
		s.dirty = true
	}
}

func (s *PebbleSession) SampleCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sampleCount
}

func (s *PebbleSession) GetRunningTime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := s.runningTime
	if !s.runStartedAt.IsZero() {
		total += time.Since(s.runStartedAt)
	}
	return total
}

func (s *PebbleSession) StartedAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startedAt
}

func (s *PebbleSession) LastUpdated() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastUpdated
}

func (s *PebbleSession) PathCount() int {
	count := 0
	prefix := s.pathKey("")
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	upperBound[len(upperBound)-1]++

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count
}

// Encoding helpers
func encodePebbleStats(stats *PathStats) []byte {
	buf := make([]byte, statsEncodedSize)
	off := 0

	for i := 0; i < int(NumSampleTypes); i++ {
		putUint64(buf[off:], stats.Data[i].Samples)
		off += 8
	}

	for i := 0; i < int(NumSampleTypes); i++ {
		putInt64(buf[off:], int64(stats.Data[i].Duration))
		off += 8
	}

	putFloat64(buf[off:], stats.DistributedSamples)
	off += 8
	putFloat64(buf[off:], stats.DistributedDuration)

	return buf
}

func decodePebbleStats(data []byte, stats *PathStats) {
	if len(data) < statsEncodedSize {
		if len(data) > 0 && data[0] != 0 {
			gob.NewDecoder(bytes.NewReader(data)).Decode(stats)
		}
		return
	}

	off := 0

	for i := 0; i < int(NumSampleTypes); i++ {
		stats.Data[i].Samples = getUint64(data[off:])
		off += 8
	}

	for i := 0; i < int(NumSampleTypes); i++ {
		stats.Data[i].Duration = time.Duration(getInt64(data[off:]))
		off += 8
	}

	stats.DistributedSamples = getFloat64(data[off:])
	off += 8
	stats.DistributedDuration = getFloat64(data[off:])
}

func init() {
	_ = math.Float64bits
	_ = math.Float64frombits
}

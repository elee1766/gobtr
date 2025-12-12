package btdu

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dennwc/btrfs"
)

// BoltSampler performs disk usage sampling with BBolt-backed storage.
// This uses significantly less memory than the in-memory sampler.
type BoltSampler struct {
	fsPath  string
	fs      *btrfs.FS
	fsFile  *os.File
	session *BoltSession
	store   *BoltStore
	chunks  *ChunkList

	// State
	running     atomic.Bool
	cancelFunc  context.CancelFunc
	recentPaths [32]atomic.Value
	pathIndex   atomic.Uint32

	// Root ID to subvolume path lookup
	rootPaths sync.Map

	// Stats
	samplesPerSec   atomic.Int64
	lastSampleCount uint64
	lastSampleTime  time.Time

	// Batching for better write performance
	sampleBatch   []SampleRecord
	batchMu       sync.Mutex
	batchSize     int
	lastBatchTime time.Time
}

// NewBoltSampler creates a new sampler with BBolt-backed storage.
func NewBoltSampler(fsPath string, store *BoltStore, resume bool) (*BoltSampler, error) {
	fs, err := btrfs.Open(fsPath, true)
	if err != nil {
		return nil, fmt.Errorf("open btrfs filesystem: %w", err)
	}

	fsFile, err := os.OpenFile(fsPath, os.O_RDONLY, 0)
	if err != nil {
		fs.Close()
		return nil, fmt.Errorf("open fs for ioctl: %w", err)
	}

	chunks, err := EnumerateChunks(fsFile)
	if err != nil {
		fs.Close()
		fsFile.Close()
		return nil, fmt.Errorf("enumerate chunks: %w", err)
	}

	totalSize := chunks.TotalSize
	if totalSize == 0 {
		fs.Close()
		fsFile.Close()
		return nil, fmt.Errorf("no chunks found in filesystem")
	}

	// Open or create BBolt session
	var session *BoltSession
	if resume && store != nil && store.Has(fsPath) {
		session, err = store.Open(fsPath)
		if err != nil {
			// Failed to load, create new
			session, _, err = store.OpenOrCreate(fsPath, totalSize)
			if err != nil {
				fs.Close()
				fsFile.Close()
				return nil, fmt.Errorf("create session: %w", err)
			}
		}
	} else if store != nil {
		session, _, err = store.OpenOrCreate(fsPath, totalSize)
		if err != nil {
			fs.Close()
			fsFile.Close()
			return nil, fmt.Errorf("create session: %w", err)
		}
	} else {
		fs.Close()
		fsFile.Close()
		return nil, fmt.Errorf("store is required for BoltSampler")
	}

	s := &BoltSampler{
		fsPath:    fsPath,
		fs:        fs,
		fsFile:    fsFile,
		store:     store,
		session:   session,
		chunks:    chunks,
		batchSize: 100, // Batch 100 samples before writing
	}

	for i := range s.recentPaths {
		s.recentPaths[i].Store("")
	}

	s.refreshRootPaths()

	return s, nil
}

// Session returns the underlying BBolt session.
func (s *BoltSampler) Session() *BoltSession {
	return s.session
}

// refreshRootPaths updates the root ID to path lookup map.
func (s *BoltSampler) refreshRootPaths() {
	subvols, err := s.fs.ListSubvolumes(nil)
	if err != nil {
		return
	}

	s.rootPaths.Store(uint64(5), "")

	for _, sv := range subvols {
		s.rootPaths.Store(sv.RootID, sv.Path)
	}
}

func (s *BoltSampler) getRootPath(rootID uint64) string {
	if v, ok := s.rootPaths.Load(rootID); ok {
		return v.(string)
	}

	s.refreshRootPaths()

	if v, ok := s.rootPaths.Load(rootID); ok {
		return v.(string)
	}

	return ""
}

// Close closes the sampler and releases resources.
func (s *BoltSampler) Close() error {
	s.Stop()
	if s.session != nil {
		s.session.Close()
	}
	if s.fsFile != nil {
		s.fsFile.Close()
	}
	if s.fs != nil {
		return s.fs.Close()
	}
	return nil
}

// Start starts sampling.
func (s *BoltSampler) Start(ctx context.Context) (bool, error) {
	if s.running.Load() {
		return false, fmt.Errorf("sampler already running")
	}

	resumed := s.session.SampleCount() > 0

	ctx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel
	s.running.Store(true)
	s.lastSampleTime = time.Now()
	s.lastSampleCount = s.session.SampleCount()
	s.lastBatchTime = time.Now()

	s.session.StartRun()

	go s.sampleLoop(ctx, s.session.TotalSize())

	return resumed, nil
}

// Stop stops the sampler and saves the session.
func (s *BoltSampler) Stop() {
	if !s.running.Load() {
		return
	}

	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
	}
	s.running.Store(false)

	// Flush any remaining samples
	s.flushBatch()

	s.session.StopRun()
	s.session.Flush()
}

// IsRunning returns whether the sampler is currently running.
func (s *BoltSampler) IsRunning() bool {
	return s.running.Load()
}

// CurrentPath returns the most recent path being sampled.
func (s *BoltSampler) CurrentPath() string {
	idx := s.pathIndex.Load()
	if idx == 0 {
		idx = recentPathsSize - 1
	} else {
		idx = (idx - 1) % recentPathsSize
	}
	v := s.recentPaths[idx].Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// RecentPaths returns the last N sampled paths.
func (s *BoltSampler) RecentPaths(n int) []string {
	if n > recentPathsSize {
		n = recentPathsSize
	}
	result := make([]string, 0, n)
	idx := s.pathIndex.Load() % recentPathsSize

	for i := 0; i < n; i++ {
		if idx == 0 {
			idx = recentPathsSize - 1
		} else {
			idx--
		}
		v := s.recentPaths[idx].Load()
		if v == nil {
			continue
		}
		path := v.(string)
		if path != "" {
			result = append(result, path)
		}
	}
	return result
}

func (s *BoltSampler) addRecentPath(path string) {
	idx := s.pathIndex.Add(1) % recentPathsSize
	s.recentPaths[idx].Store(path)
}

// SamplesPerSecond returns the current sampling rate.
func (s *BoltSampler) SamplesPerSecond() float64 {
	return float64(s.samplesPerSec.Load())
}

// Clear resets the session data.
func (s *BoltSampler) Clear() error {
	s.Stop()

	// Close current session
	if s.session != nil {
		s.session.Close()
	}

	// Delete and recreate
	if s.store != nil {
		s.store.Delete(s.fsPath)
		session, _, err := s.store.OpenOrCreate(s.fsPath, s.chunks.TotalSize)
		if err != nil {
			return err
		}
		s.session = session
	}

	return nil
}

// sampleLoop is the main sampling loop.
func (s *BoltSampler) sampleLoop(ctx context.Context, totalSize uint64) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	statsTicker := time.NewTicker(time.Second)
	defer statsTicker.Stop()

	// Channel for flushing batches asynchronously
	flushChan := make(chan []SampleRecord, 2)
	flushDone := make(chan struct{})

	// Background goroutine for flushing batches
	go func() {
		defer close(flushDone)
		for batch := range flushChan {
			s.session.AddSampleBatch(batch)
		}
	}()

	defer func() {
		close(flushChan)
		<-flushDone // Wait for flush goroutine to finish
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTicker.C:
			currentCount := s.session.SampleCount()
			elapsed := time.Since(s.lastSampleTime).Seconds()
			if elapsed > 0 {
				rate := float64(currentCount-s.lastSampleCount) / elapsed
				s.samplesPerSec.Store(int64(rate))
			}
			s.lastSampleCount = currentCount
			s.lastSampleTime = time.Now()
		default:
			s.sampleOnceAsync(rng, totalSize, flushChan)
		}
	}
}

// flushBatch writes accumulated samples to the database.
func (s *BoltSampler) flushBatch() {
	s.batchMu.Lock()
	if len(s.sampleBatch) == 0 {
		s.batchMu.Unlock()
		return
	}
	batch := s.sampleBatch
	s.sampleBatch = nil
	s.batchMu.Unlock()

	s.session.AddSampleBatch(batch)
}

// sampleOnceAsync samples a single random block and sends batches to flush channel.
func (s *BoltSampler) sampleOnceAsync(rng *rand.Rand, totalSize uint64, flushChan chan<- []SampleRecord) {
	start := time.Now()

	pos := uint64(rng.Int63n(int64(totalSize)))
	logicalAddr := s.chunks.SamplePosition(pos)

	path, sampleType := s.resolveLogicalAddress(logicalAddr)

	s.addRecentPath(path)

	duration := time.Since(start)

	offset := Offset{
		Physical: 0,
		Logical:  logicalAddr,
	}

	// Add to batch
	s.batchMu.Lock()
	s.sampleBatch = append(s.sampleBatch, SampleRecord{
		Path:     path,
		Type:     sampleType,
		Offset:   offset,
		Duration: duration,
	})

	// Flush if batch is full
	if len(s.sampleBatch) >= s.batchSize {
		batch := s.sampleBatch
		s.sampleBatch = make([]SampleRecord, 0, s.batchSize)
		s.batchMu.Unlock()

		// Non-blocking send - if channel is full, continue sampling
		select {
		case flushChan <- batch:
		default:
			// Channel full, flush synchronously
			s.session.AddSampleBatch(batch)
		}
	} else {
		s.batchMu.Unlock()
	}
}

// sampleOnce samples a single random block (synchronous version for compatibility).
func (s *BoltSampler) sampleOnce(rng *rand.Rand, totalSize uint64) {
	start := time.Now()

	pos := uint64(rng.Int63n(int64(totalSize)))
	logicalAddr := s.chunks.SamplePosition(pos)

	path, sampleType := s.resolveLogicalAddress(logicalAddr)

	s.addRecentPath(path)

	duration := time.Since(start)

	offset := Offset{
		Physical: 0,
		Logical:  logicalAddr,
	}

	// Add to batch
	s.batchMu.Lock()
	s.sampleBatch = append(s.sampleBatch, SampleRecord{
		Path:     path,
		Type:     sampleType,
		Offset:   offset,
		Duration: duration,
	})

	// Flush if batch is full
	shouldFlush := len(s.sampleBatch) >= s.batchSize
	s.batchMu.Unlock()

	if shouldFlush {
		s.flushBatch()
	}
}

// resolveLogicalAddress resolves a logical address to a file path.
func (s *BoltSampler) resolveLogicalAddress(logicalAddr uint64) (string, SampleType) {
	inodes, err := s.logicalIno(logicalAddr)
	if err != nil || len(inodes) == 0 {
		return "<free>", Unresolved
	}

	var allPaths []string
	for _, inode := range inodes {
		path, err := s.inodeLookup(inode.Root, inode.Inum)
		if err != nil {
			continue
		}

		if path == "" {
			continue
		}

		rootPath := s.getRootPath(inode.Root)

		var fullPath string
		if rootPath == "" {
			fullPath = "/" + path
		} else {
			if path == "" {
				fullPath = "/" + rootPath
			} else {
				fullPath = "/" + rootPath + "/" + path
			}
		}

		allPaths = append(allPaths, fullPath)
	}

	if len(allPaths) == 0 {
		return "<unreachable>", Unreachable
	}

	representativePath := selectRepresentativePath(allPaths)

	sampleType := Represented
	if len(allPaths) > 1 {
		sampleType = Shared
	}

	return representativePath, sampleType
}

// logicalIno calls BTRFS_IOC_LOGICAL_INO
func (s *BoltSampler) logicalIno(logical uint64) ([]InodeResult, error) {
	// Reuse the parent sampler's implementation via package-level function
	return logicalInoImpl(s.fsFile, logical)
}

// inodeLookup calls BTRFS_IOC_INO_LOOKUP
func (s *BoltSampler) inodeLookup(treeID, objectID uint64) (string, error) {
	return inodeLookupImpl(s.fsFile, treeID, objectID)
}

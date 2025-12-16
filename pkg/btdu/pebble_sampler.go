package btdu

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dennwc/btrfs"
)

// PebbleSampler performs disk usage sampling with PebbleDB-backed storage.
type PebbleSampler struct {
	fsPath  string
	fs      *btrfs.FS
	fsFile  *os.File
	session *PebbleSession
	store   *PebbleStore
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
}

// NewPebbleSampler creates a new sampler with PebbleDB-backed storage.
func NewPebbleSampler(fsPath string, store *PebbleStore, resume bool) (*PebbleSampler, error) {
	fs, err := btrfs.Open(fsPath, true)
	if err != nil {
		return nil, fmt.Errorf("open btrfs filesystem: %w", err)
	}

	fsFile, err := os.OpenFile(fsPath, os.O_RDONLY, 0)
	if err != nil {
		fs.Close()
		return nil, fmt.Errorf("open fs for ioctl: %w", err)
	}

	// Only enumerate DATA chunks for sampling - metadata/system chunks
	// don't have file inodes so LOGICAL_INO won't find anything
	chunks, err := EnumerateDataChunks(fsFile)
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

	var session *PebbleSession
	if resume && store != nil && store.Has(fsPath) {
		session, err = store.Open(fsPath)
		if err != nil {
			session, _, err = store.OpenOrCreate(fsPath, totalSize)
			if err != nil {
				fs.Close()
				fsFile.Close()
				return nil, fmt.Errorf("create session: %w", err)
			}
		}
		// Always update total size to current chunk total in case filesystem changed
		if session.TotalSize() != totalSize {
			slog.Info("updating session total size",
				"old", session.TotalSize(),
				"new", totalSize,
			)
			session.SetTotalSize(totalSize)
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
		return nil, fmt.Errorf("store is required for PebbleSampler")
	}

	s := &PebbleSampler{
		fsPath:  fsPath,
		fs:      fs,
		fsFile:  fsFile,
		store:   store,
		session: session,
		chunks:  chunks,
	}

	for i := range s.recentPaths {
		s.recentPaths[i].Store("")
	}

	s.refreshRootPaths()

	// Log chunk statistics (only data chunks are enumerated for sampling)
	slog.Info("data chunk statistics for sampling",
		"dataChunks", len(chunks.Chunks),
		"dataSize", chunks.TotalSize,
	)

	return s, nil
}

// Session returns the underlying Pebble session.
func (s *PebbleSampler) Session() *PebbleSession {
	return s.session
}

func (s *PebbleSampler) refreshRootPaths() {
	subvols, err := s.fs.ListSubvolumes(nil)
	if err != nil {
		return
	}

	s.rootPaths.Store(uint64(5), "")

	for _, sv := range subvols {
		s.rootPaths.Store(sv.RootID, sv.Path)
	}
}

func (s *PebbleSampler) getRootPath(rootID uint64) string {
	if v, ok := s.rootPaths.Load(rootID); ok {
		return v.(string)
	}

	s.refreshRootPaths()

	if v, ok := s.rootPaths.Load(rootID); ok {
		return v.(string)
	}

	return ""
}

// Close closes the sampler.
func (s *PebbleSampler) Close() error {
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
func (s *PebbleSampler) Start(ctx context.Context) (bool, error) {
	if s.running.Load() {
		return false, fmt.Errorf("sampler already running")
	}

	resumed := s.session.SampleCount() > 0

	// Use chunks' totalSize (current filesystem state) rather than session's potentially stale value
	totalSize := s.chunks.TotalSize
	if totalSize == 0 {
		return false, fmt.Errorf("filesystem has no allocated chunks")
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel
	s.running.Store(true)
	s.lastSampleTime = time.Now()
	s.lastSampleCount = s.session.SampleCount()

	s.session.StartRun()

	go s.sampleLoop(ctx, totalSize)

	return resumed, nil
}

// Stop stops the sampler.
func (s *PebbleSampler) Stop() {
	if !s.running.Load() {
		return
	}

	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
	}
	s.running.Store(false)

	// Workers flush their batches on context cancel, then session flush persists
	s.session.StopRun()
	s.session.Flush()
}

// IsRunning returns whether the sampler is running.
func (s *PebbleSampler) IsRunning() bool {
	return s.running.Load()
}

// CurrentPath returns the most recent path.
func (s *PebbleSampler) CurrentPath() string {
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
func (s *PebbleSampler) RecentPaths(n int) []string {
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

func (s *PebbleSampler) addRecentPath(path string) {
	idx := s.pathIndex.Add(1) % recentPathsSize
	s.recentPaths[idx].Store(path)
}

// SamplesPerSecond returns the current sampling rate.
func (s *PebbleSampler) SamplesPerSecond() float64 {
	return float64(s.samplesPerSec.Load())
}

// Clear resets the session data.
func (s *PebbleSampler) Clear() error {
	s.Stop()

	// Delete from store
	if s.store != nil {
		s.store.Delete(s.fsPath)
	}

	// Create fresh session
	if s.store != nil {
		session, _, err := s.store.OpenOrCreate(s.fsPath, s.chunks.TotalSize)
		if err != nil {
			return err
		}
		s.session = session
	}

	// Reset stats
	s.lastSampleCount = 0
	s.lastSampleTime = time.Now()
	s.samplesPerSec.Store(0)

	// Clear recent paths
	for i := range s.recentPaths {
		s.recentPaths[i].Store("")
	}
	s.pathIndex.Store(0)

	return nil
}

func (s *PebbleSampler) sampleLoop(ctx context.Context, totalSize uint64) {
	statsTicker := time.NewTicker(time.Second)
	defer statsTicker.Stop()

	// Flush ticker - flush every 5 seconds (accumulator handles batching)
	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	// Start worker goroutines - each adds directly to session accumulator
	numWorkers := 8
	var wg sync.WaitGroup
	workerCtx, workerCancel := context.WithCancel(ctx)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			s.sampleWorkerDirect(workerCtx, rng, totalSize)
		}(i)
	}

	defer func() {
		workerCancel()
		wg.Wait()
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
		case <-flushTicker.C:
			// Periodic flush to disk
			s.session.FlushAccumulator()
		}
	}
}

// sampleWorkerDirect adds samples directly to session accumulator for immediate query visibility
func (s *PebbleSampler) sampleWorkerDirect(ctx context.Context, rng *rand.Rand, totalSize uint64) {
	// Small local batch to reduce lock contention
	batch := make([]SampleRecord, 0, 32)

	for {
		select {
		case <-ctx.Done():
			// Flush remaining
			if len(batch) > 0 {
				s.session.AddSampleBatch(batch)
			}
			return
		default:
			start := time.Now()
			pos := uint64(rng.Int63n(int64(totalSize)))
			logicalAddr := s.chunks.SamplePosition(pos)

			path, sampleType := s.resolveLogicalAddress(logicalAddr)
			s.addRecentPath(path)

			duration := time.Since(start)

			batch = append(batch, SampleRecord{
				Path:     path,
				Type:     sampleType,
				Offset:   Offset{Logical: logicalAddr},
				Duration: duration,
			})

			// Flush small batch frequently for live visibility
			if len(batch) >= 32 {
				s.session.AddSampleBatch(batch)
				batch = make([]SampleRecord, 0, 32)
			}
		}
	}
}

func (s *PebbleSampler) resolveLogicalAddress(logicalAddr uint64) (string, SampleType) {
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

func (s *PebbleSampler) logicalIno(logical uint64) ([]InodeResult, error) {
	return logicalInoImpl(s.fsFile, logical)
}

func (s *PebbleSampler) inodeLookup(treeID, objectID uint64) (string, error) {
	return inodeLookupImpl(s.fsFile, treeID, objectID)
}

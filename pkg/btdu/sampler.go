package btdu

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dennwc/btrfs"
	"github.com/dennwc/ioctl"
)

// Sampler performs disk usage sampling on a btrfs filesystem.
// It randomly samples logical blocks and resolves them to file paths
// using the BTRFS_IOC_LOGICAL_INO and BTRFS_IOC_INO_LOOKUP ioctls.
// Each Sampler has exactly one Session for its lifetime.
type Sampler struct {
	fsPath  string
	fs      *btrfs.FS
	fsFile  *os.File // File handle for ioctls
	Session *Session // Always valid, created at construction
	store   *Store
	chunks  *ChunkList // Allocated chunks for sampling

	// State
	running     atomic.Bool
	cancelFunc  context.CancelFunc
	recentPaths [32]atomic.Value // Ring buffer of recent paths (strings)
	pathIndex   atomic.Uint32    // Current index in ring buffer

	// Root ID to subvolume path lookup (sync.Map for concurrent access)
	rootPaths sync.Map // map[uint64]string

	// Stats
	samplesPerSec   atomic.Int64
	lastSampleCount uint64
	lastSampleTime  time.Time
}

const recentPathsSize = 32 // Size of recent paths ring buffer

// ioctl constants from btrfs
const (
	btrfsIoctlMagic         = 0x94
	btrfsInoLookupPathMax   = 4080
	firstFreeObjectID       = 256
	btrfsFirstChunkTreeObjID = 256
)

// btrfs ioctl numbers
var (
	ioctlLogicalIno = ioctl.IOWR(btrfsIoctlMagic, 36, unsafe.Sizeof(btrfsIoctlLogicalInoArgs{}))
	ioctlInoLookup  = ioctl.IOWR(btrfsIoctlMagic, 18, unsafe.Sizeof(btrfsIoctlInoLookupArgs{}))
)

// btrfsIoctlLogicalInoArgs is the args struct for BTRFS_IOC_LOGICAL_INO
type btrfsIoctlLogicalInoArgs struct {
	Logical  uint64
	Size     uint64
	Reserved [4]uint64
	Inodes   uint64 // Pointer to btrfsDataContainer
}

// btrfsIoctlInoLookupArgs is the args struct for BTRFS_IOC_INO_LOOKUP
type btrfsIoctlInoLookupArgs struct {
	TreeID   uint64
	ObjectID uint64
	Name     [btrfsInoLookupPathMax]byte
}

// btrfsDataContainer holds results from LOGICAL_INO ioctl
type btrfsDataContainer struct {
	BytesLeft    uint32
	BytesMissing uint32
	ElemCnt      uint32
	ElemMissed   uint32
	// Followed by val array
}

// InodeResult represents a single inode result from LOGICAL_INO
type InodeResult struct {
	Inum   uint64
	Offset uint64
	Root   uint64
}

// NewSampler creates a new sampler for the given filesystem path.
// If resume is true and a session exists in the store, it will be loaded.
func NewSampler(fsPath string, store *Store, resume bool) (*Sampler, error) {
	// Open the filesystem
	fs, err := btrfs.Open(fsPath, true) // read-only
	if err != nil {
		return nil, fmt.Errorf("open btrfs filesystem: %w", err)
	}

	// Open file handle for ioctls
	fsFile, err := os.OpenFile(fsPath, os.O_RDONLY, 0)
	if err != nil {
		fs.Close()
		return nil, fmt.Errorf("open fs for ioctl: %w", err)
	}

	// Enumerate chunks to get actual allocated space
	// This is what btdu does - sample only from allocated chunks, not device size
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

	// Load or create session
	var session *Session
	if resume && store != nil && store.Has(fsPath) {
		session, err = store.Load(fsPath)
		if err != nil {
			// Failed to load, create new
			session = NewSession(fsPath, totalSize)
		}
	} else {
		session = NewSession(fsPath, totalSize)
	}

	s := &Sampler{
		fsPath:  fsPath,
		fs:      fs,
		fsFile:  fsFile,
		store:   store,
		Session: session,
		chunks:  chunks,
	}

	// Initialize recent paths ring buffer
	for i := range s.recentPaths {
		s.recentPaths[i].Store("")
	}

	// Initialize root paths lookup
	s.refreshRootPaths()

	return s, nil
}

// refreshRootPaths updates the root ID to path lookup map.
func (s *Sampler) refreshRootPaths() {
	subvols, err := s.fs.ListSubvolumes(nil)
	if err != nil {
		return
	}

	// Add FS_TREE (root ID 5) as empty path
	s.rootPaths.Store(uint64(5), "")

	for _, sv := range subvols {
		s.rootPaths.Store(sv.RootID, sv.Path)
	}
}

// getRootPath returns the subvolume path for a given root ID.
func (s *Sampler) getRootPath(rootID uint64) string {
	if v, ok := s.rootPaths.Load(rootID); ok {
		return v.(string)
	}

	// Try to refresh and lookup again
	s.refreshRootPaths()

	if v, ok := s.rootPaths.Load(rootID); ok {
		return v.(string)
	}

	return ""
}

// Close closes the sampler and releases resources.
func (s *Sampler) Close() error {
	s.Stop()
	if s.fsFile != nil {
		s.fsFile.Close()
	}
	if s.fs != nil {
		return s.fs.Close()
	}
	return nil
}

// Start starts sampling. Session must already be set (created in NewSampler).
// Returns (resumed, error) where resumed indicates if the session had existing samples.
func (s *Sampler) Start(ctx context.Context) (bool, error) {
	if s.running.Load() {
		return false, fmt.Errorf("sampler already running")
	}

	// Session is always valid (created in NewSampler)
	resumed := s.Session.SampleCount > 0

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel
	s.running.Store(true)
	s.lastSampleTime = time.Now()
	s.lastSampleCount = s.Session.SampleCount

	// Mark session as running (for time tracking)
	s.Session.StartRun()

	// Start sampling goroutine
	go s.sampleLoop(ctx, s.Session.TotalSize)

	return resumed, nil
}

// Stop stops the sampler and saves the session.
func (s *Sampler) Stop() {
	if !s.running.Load() {
		return // Already stopped
	}

	if s.cancelFunc != nil {
		s.cancelFunc()
		s.cancelFunc = nil
	}
	s.running.Store(false)

	// Stop the run timer
	s.Session.StopRun()

	// Save session
	if s.store != nil {
		s.store.Save(s.Session)
	}
}

// IsRunning returns whether the sampler is currently running.
func (s *Sampler) IsRunning() bool {
	return s.running.Load()
}

// CurrentPath returns the most recent path being sampled.
func (s *Sampler) CurrentPath() string {
	idx := s.pathIndex.Load()
	if idx == 0 {
		idx = recentPathsSize - 1
	} else {
		idx--
	}
	v := s.recentPaths[idx].Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// RecentPaths returns the last N sampled paths (most recent first).
func (s *Sampler) RecentPaths(n int) []string {
	if n > recentPathsSize {
		n = recentPathsSize
	}
	result := make([]string, 0, n)
	idx := s.pathIndex.Load()

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

// addRecentPath adds a path to the ring buffer.
func (s *Sampler) addRecentPath(path string) {
	idx := s.pathIndex.Add(1) % recentPathsSize
	s.recentPaths[idx].Store(path)
}

// SamplesPerSecond returns the current sampling rate.
func (s *Sampler) SamplesPerSecond() float64 {
	return float64(s.samplesPerSec.Load())
}

// Clear resets the session data and removes it from the store.
func (s *Sampler) Clear() error {
	s.Stop()

	// Create a fresh session
	s.Session = NewSession(s.fsPath, s.Session.TotalSize)

	if s.store != nil {
		return s.store.Delete(s.fsPath)
	}
	return nil
}

// sampleLoop is the main sampling loop.
func (s *Sampler) sampleLoop(ctx context.Context, totalSize uint64) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Stats ticker
	statsTicker := time.NewTicker(time.Second)
	defer statsTicker.Stop()

	// Save ticker
	saveTicker := time.NewTicker(30 * time.Second)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTicker.C:
			// Update samples per second
			currentCount := s.Session.SampleCount

			elapsed := time.Since(s.lastSampleTime).Seconds()
			if elapsed > 0 {
				rate := float64(currentCount-s.lastSampleCount) / elapsed
				s.samplesPerSec.Store(int64(rate))
			}
			s.lastSampleCount = currentCount
			s.lastSampleTime = time.Now()
		case <-saveTicker.C:
			// Periodic save
			if s.store != nil {
				s.store.Save(s.Session)
			}
		default:
			// Sample a random block
			s.sampleOnce(rng, totalSize)
		}
	}
}

// sampleOnce samples a single random block.
func (s *Sampler) sampleOnce(rng *rand.Rand, totalSize uint64) {
	start := time.Now()

	// Pick a random position within the chunk space (0 to totalSize)
	// then map it to an actual logical address
	pos := uint64(rng.Int63n(int64(totalSize)))
	logicalAddr := s.chunks.SamplePosition(pos)

	// Try to resolve to a path using LOGICAL_INO
	path, sampleType := s.resolveLogicalAddress(logicalAddr)

	s.addRecentPath(path)

	duration := time.Since(start)

	offset := Offset{
		Physical: 0, // We don't track physical offset
		Logical:  logicalAddr,
	}

	s.Session.AddSample(path, sampleType, offset, duration)
}

// resolveLogicalAddress resolves a logical address to a file path.
// When multiple files reference the same block (snapshots/reflinks), it selects
// the most representative path (shortest path, then lexicographically smallest).
func (s *Sampler) resolveLogicalAddress(logicalAddr uint64) (string, SampleType) {
	// Use LOGICAL_INO to get inodes at this logical address
	inodes, err := s.logicalIno(logicalAddr)
	if err != nil || len(inodes) == 0 {
		// This address is not in use (free space) or metadata
		return "<free>", Unresolved
	}

	// Collect ALL paths from all inodes
	var allPaths []string
	for _, inode := range inodes {
		path, err := s.inodeLookup(inode.Root, inode.Inum)
		if err != nil {
			continue
		}

		if path == "" {
			continue
		}

		// Get the subvolume path for this root ID
		rootPath := s.getRootPath(inode.Root)

		// Build full path: /subvolume/path/to/file
		var fullPath string
		if rootPath == "" {
			// Root filesystem (FS_TREE)
			fullPath = "/" + path
		} else {
			// Under a subvolume - avoid double slashes
			if path == "" {
				fullPath = "/" + rootPath
			} else {
				fullPath = "/" + rootPath + "/" + path
			}
		}

		allPaths = append(allPaths, fullPath)
	}

	if len(allPaths) == 0 {
		// Inodes exist but no paths could be resolved - data is unreachable (orphaned/deleted)
		return "<unreachable>", Unreachable
	}

	// Select the most representative path
	// Priority: shortest path, then lexicographically smallest
	representativePath := selectRepresentativePath(allPaths)

	// Determine sample type
	sampleType := Represented
	if len(allPaths) > 1 {
		// Multiple paths reference this block - it's shared (reflink/snapshot)
		sampleType = Shared
	}

	return representativePath, sampleType
}

// selectRepresentativePath selects the most representative path from a list.
// It prefers shorter paths, and if equal length, lexicographically smaller.
// This ensures that the "original" file gets credited rather than snapshots/backups.
func selectRepresentativePath(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return paths[0]
	}

	best := paths[0]
	for _, p := range paths[1:] {
		// Shorter path wins
		if len(p) < len(best) {
			best = p
		} else if len(p) == len(best) && p < best {
			// Same length: lexicographically smaller wins
			best = p
		}
	}
	return best
}

// logicalIno calls BTRFS_IOC_LOGICAL_INO to find inodes at a logical address.
func (s *Sampler) logicalIno(logical uint64) ([]InodeResult, error) {
	return logicalInoImpl(s.fsFile, logical)
}

// inodeLookup calls BTRFS_IOC_INO_LOOKUP to resolve an inode to a path.
func (s *Sampler) inodeLookup(treeID, objectID uint64) (string, error) {
	return inodeLookupImpl(s.fsFile, treeID, objectID)
}

// logicalInoImpl is the implementation of LOGICAL_INO ioctl.
func logicalInoImpl(fsFile *os.File, logical uint64) ([]InodeResult, error) {
	bufSize := 4096
	buf := make([]byte, bufSize)

	args := btrfsIoctlLogicalInoArgs{
		Logical: logical,
		Size:    uint64(bufSize),
		Inodes:  uint64(uintptr(unsafe.Pointer(&buf[0]))),
	}

	err := ioctl.Do(fsFile, ioctlLogicalIno, &args)
	if err != nil {
		return nil, err
	}

	container := (*btrfsDataContainer)(unsafe.Pointer(&buf[0]))
	if container.ElemCnt == 0 {
		return nil, nil
	}

	resultStart := unsafe.Sizeof(btrfsDataContainer{})
	results := make([]InodeResult, 0, container.ElemCnt)

	for i := uint32(0); i < container.ElemCnt; i++ {
		offset := int(resultStart) + int(i)*24
		if offset+24 > len(buf) {
			break
		}

		inum := binary.LittleEndian.Uint64(buf[offset:])
		off := binary.LittleEndian.Uint64(buf[offset+8:])
		root := binary.LittleEndian.Uint64(buf[offset+16:])

		results = append(results, InodeResult{
			Inum:   inum,
			Offset: off,
			Root:   root,
		})
	}

	return results, nil
}

// inodeLookupImpl is the implementation of INO_LOOKUP ioctl.
func inodeLookupImpl(fsFile *os.File, treeID, objectID uint64) (string, error) {
	args := btrfsIoctlInoLookupArgs{
		TreeID:   treeID,
		ObjectID: objectID,
	}

	err := ioctl.Do(fsFile, ioctlInoLookup, &args)
	if err != nil {
		return "", err
	}

	n := 0
	for i, b := range args.Name {
		if b == 0 {
			n = i
			break
		}
	}

	return string(args.Name[:n]), nil
}

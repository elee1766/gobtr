package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"
	apiv1 "github.com/elee1766/gobtr/gen/api/v1"
	"github.com/elee1766/gobtr/pkg/btdu"
	"github.com/elee1766/gobtr/pkg/config"
	"github.com/elee1766/gobtr/pkg/db"
)

type UsageHandler struct {
	logger *slog.Logger
	db     *db.DB

	// Storage backends (one or the other based on config)
	store     *btdu.Store     // In-memory backend
	boltStore *btdu.BoltStore // BBolt backend
	useBolt   bool

	// Active samplers keyed by filesystem path
	samplers     map[string]*btdu.Sampler     // In-memory samplers
	boltSamplers map[string]*btdu.BoltSampler // BBolt samplers

	// Cached sessions for stopped samplers (only for in-memory backend)
	sessionCache map[string]*btdu.Session
	mu           sync.RWMutex
}

func NewUsageHandler(logger *slog.Logger, db *db.DB, cfg *config.Config) (*UsageHandler, error) {
	h := &UsageHandler{
		logger:       logger.With("handler", "usage"),
		db:           db,
		useBolt:      cfg.BTDUUseBolt,
		samplers:     make(map[string]*btdu.Sampler),
		boltSamplers: make(map[string]*btdu.BoltSampler),
		sessionCache: make(map[string]*btdu.Session),
	}

	if cfg.BTDUUseBolt {
		boltStore, err := btdu.NewBoltStore(cfg.BTDUStoreDir)
		if err != nil {
			return nil, fmt.Errorf("create btdu bolt store: %w", err)
		}
		h.boltStore = boltStore
		logger.Info("using BBolt backend for btdu storage")
	} else {
		store, err := btdu.NewStore(cfg.BTDUStoreDir)
		if err != nil {
			return nil, fmt.Errorf("create btdu store: %w", err)
		}
		h.store = store
		// Preload existing sessions into cache for fast initial requests
		go h.preloadSessions()
		logger.Info("using in-memory backend for btdu storage")
	}

	return h, nil
}

// preloadSessions loads all stored sessions into the cache on startup.
// Only used for in-memory backend.
func (h *UsageHandler) preloadSessions() {
	if h.store == nil {
		return
	}
	sessions, err := h.store.List()
	if err != nil {
		h.logger.Warn("failed to list stored sessions", "error", err)
		return
	}

	for _, info := range sessions {
		if info.FSPath == "" {
			continue
		}
		session, err := h.store.Load(info.FSPath)
		if err != nil {
			h.logger.Debug("failed to preload session", "fs_path", info.FSPath, "error", err)
			continue
		}
		h.mu.Lock()
		h.sessionCache[info.FSPath] = session
		h.mu.Unlock()
		h.logger.Debug("preloaded session", "fs_path", info.FSPath, "samples", session.SampleCount)
	}
}

// getSampler returns an existing in-memory sampler or creates a new one.
// Only used when useBolt is false.
func (h *UsageHandler) getSampler(fsPath string) (*btdu.Sampler, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sampler, ok := h.samplers[fsPath]; ok {
		return sampler, nil
	}

	sampler, err := btdu.NewSampler(fsPath, h.store, true) // resume=true
	if err != nil {
		return nil, err
	}

	h.samplers[fsPath] = sampler
	// Clear session cache when creating new sampler (it has fresh session)
	delete(h.sessionCache, fsPath)
	return sampler, nil
}

// getBoltSampler returns an existing BBolt sampler or creates a new one.
// Only used when useBolt is true.
func (h *UsageHandler) getBoltSampler(fsPath string) (*btdu.BoltSampler, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sampler, ok := h.boltSamplers[fsPath]; ok {
		return sampler, nil
	}

	sampler, err := btdu.NewBoltSampler(fsPath, h.boltStore, true) // resume=true
	if err != nil {
		return nil, err
	}

	h.boltSamplers[fsPath] = sampler
	return sampler, nil
}

// getSession returns a session for the filesystem path.
// Priority: active sampler > cache > disk.
// Sessions loaded from disk are cached for subsequent requests.
func (h *UsageHandler) getSession(fsPath string) *btdu.Session {
	h.mu.RLock()
	sampler, hasSampler := h.samplers[fsPath]
	cached, hasCached := h.sessionCache[fsPath]
	h.mu.RUnlock()

	// Active sampler takes priority
	if hasSampler && sampler != nil {
		return sampler.Session
	}

	// Return cached session
	if hasCached {
		return cached
	}

	// Load from disk and cache
	if h.store.Has(fsPath) {
		session, err := h.store.Load(fsPath)
		if err != nil {
			return nil
		}
		h.mu.Lock()
		h.sessionCache[fsPath] = session
		h.mu.Unlock()
		return session
	}

	return nil
}

func (h *UsageHandler) StartSampling(
	ctx context.Context,
	req *connect.Request[apiv1.StartSamplingRequest],
) (*connect.Response[apiv1.StartSamplingResponse], error) {
	h.logger.Info("start sampling", "fs_path", req.Msg.FsPath, "resume", req.Msg.Resume)

	if req.Msg.FsPath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("fs_path is required"))
	}

	if h.useBolt {
		sampler, err := h.getBoltSampler(req.Msg.FsPath)
		if err != nil {
			h.logger.Error("failed to get bolt sampler", "error", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		if !req.Msg.Resume {
			sampler.Clear()
		}

		resumed, err := sampler.Start(context.Background())
		if err != nil {
			h.logger.Error("failed to start sampling", "error", err)
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		return connect.NewResponse(&apiv1.StartSamplingResponse{
			Started:         true,
			Resumed:         resumed,
			ExistingSamples: sampler.Session().SampleCount(),
		}), nil
	}

	// In-memory backend
	sampler, err := h.getSampler(req.Msg.FsPath)
	if err != nil {
		h.logger.Error("failed to get sampler", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if !req.Msg.Resume {
		sampler.Clear()
	}

	resumed, err := sampler.Start(context.Background())
	if err != nil {
		h.logger.Error("failed to start sampling", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.StartSamplingResponse{
		Started:         true,
		Resumed:         resumed,
		ExistingSamples: sampler.Session.SampleCount,
	}), nil
}

func (h *UsageHandler) StopSampling(
	ctx context.Context,
	req *connect.Request[apiv1.StopSamplingRequest],
) (*connect.Response[apiv1.StopSamplingResponse], error) {
	h.logger.Info("stop sampling", "fs_path", req.Msg.FsPath)

	if req.Msg.FsPath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("fs_path is required"))
	}

	if h.useBolt {
		h.mu.RLock()
		sampler, ok := h.boltSamplers[req.Msg.FsPath]
		h.mu.RUnlock()

		if !ok {
			return connect.NewResponse(&apiv1.StopSamplingResponse{
				Stopped:      false,
				TotalSamples: 0,
			}), nil
		}

		sampler.Stop()

		return connect.NewResponse(&apiv1.StopSamplingResponse{
			Stopped:      true,
			TotalSamples: sampler.Session().SampleCount(),
		}), nil
	}

	h.mu.RLock()
	sampler, ok := h.samplers[req.Msg.FsPath]
	h.mu.RUnlock()

	if !ok {
		return connect.NewResponse(&apiv1.StopSamplingResponse{
			Stopped:      false,
			TotalSamples: 0,
		}), nil
	}

	sampler.Stop()

	return connect.NewResponse(&apiv1.StopSamplingResponse{
		Stopped:      true,
		TotalSamples: sampler.Session.SampleCount,
	}), nil
}

func (h *UsageHandler) GetSamplingStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetSamplingStatusRequest],
) (*connect.Response[apiv1.GetSamplingStatusResponse], error) {
	h.logger.Debug("get sampling status", "fs_path", req.Msg.FsPath)

	if req.Msg.FsPath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("fs_path is required"))
	}

	progress := &apiv1.SamplingProgress{
		IsRunning: false,
	}
	hasSession := false

	if h.useBolt {
		h.mu.RLock()
		sampler, ok := h.boltSamplers[req.Msg.FsPath]
		h.mu.RUnlock()

		if ok && sampler != nil {
			progress.IsRunning = sampler.IsRunning()
			progress.CurrentPath = sampler.CurrentPath()
			progress.SamplesPerSecond = sampler.SamplesPerSecond()
			progress.RecentPaths = sampler.RecentPaths(16)

			session := sampler.Session()
			hasSession = true
			progress.SampleCount = session.SampleCount()
			progress.TotalSize = session.TotalSize()
			progress.RunningTimeSeconds = int64(session.GetRunningTime().Seconds())
		} else if h.boltStore != nil && h.boltStore.Has(req.Msg.FsPath) {
			// Open session just to get status
			session, err := h.boltStore.Open(req.Msg.FsPath)
			if err == nil {
				hasSession = true
				progress.SampleCount = session.SampleCount()
				progress.TotalSize = session.TotalSize()
				progress.RunningTimeSeconds = int64(session.GetRunningTime().Seconds())
				session.Close()
			}
		}
	} else {
		h.mu.RLock()
		sampler, ok := h.samplers[req.Msg.FsPath]
		h.mu.RUnlock()

		if ok && sampler != nil {
			progress.IsRunning = sampler.IsRunning()
			progress.CurrentPath = sampler.CurrentPath()
			progress.SamplesPerSecond = sampler.SamplesPerSecond()
			progress.RecentPaths = sampler.RecentPaths(16)

			hasSession = true
			progress.SampleCount = sampler.Session.SampleCount
			progress.TotalSize = sampler.Session.TotalSize
			progress.RunningTimeSeconds = int64(sampler.Session.GetRunningTime().Seconds())
		} else {
			// Use cached session (loads from disk once, then cached)
			session := h.getSession(req.Msg.FsPath)
			if session != nil {
				hasSession = true
				progress.SampleCount = session.SampleCount
				progress.TotalSize = session.TotalSize
				progress.RunningTimeSeconds = int64(session.GetRunningTime().Seconds())
			}
		}
	}

	return connect.NewResponse(&apiv1.GetSamplingStatusResponse{
		Progress:   progress,
		HasSession: hasSession,
	}), nil
}

func (h *UsageHandler) ClearSampling(
	ctx context.Context,
	req *connect.Request[apiv1.ClearSamplingRequest],
) (*connect.Response[apiv1.ClearSamplingResponse], error) {
	h.logger.Info("clear sampling", "fs_path", req.Msg.FsPath)

	if req.Msg.FsPath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("fs_path is required"))
	}

	if h.useBolt {
		h.mu.Lock()
		sampler, ok := h.boltSamplers[req.Msg.FsPath]
		if ok {
			sampler.Clear()
			delete(h.boltSamplers, req.Msg.FsPath)
		}
		h.mu.Unlock()

		if !ok && h.boltStore != nil {
			h.boltStore.Delete(req.Msg.FsPath)
		}
	} else {
		h.mu.Lock()
		sampler, ok := h.samplers[req.Msg.FsPath]
		if ok {
			sampler.Clear()
			delete(h.samplers, req.Msg.FsPath)
		}
		delete(h.sessionCache, req.Msg.FsPath)
		h.mu.Unlock()

		if !ok && h.store != nil {
			h.store.Delete(req.Msg.FsPath)
		}
	}

	return connect.NewResponse(&apiv1.ClearSamplingResponse{
		Cleared: true,
	}), nil
}

func (h *UsageHandler) GetUsageTree(
	ctx context.Context,
	req *connect.Request[apiv1.GetUsageTreeRequest],
) (*connect.Response[apiv1.GetUsageTreeResponse], error) {
	h.logger.Debug("get usage tree", "fs_path", req.Msg.FsPath, "path", req.Msg.Path)

	if req.Msg.FsPath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("fs_path is required"))
	}

	path := req.Msg.Path
	if path == "" {
		path = "/"
	}

	sortBy := req.Msg.SortBy
	if sortBy == "" {
		sortBy = "size"
	}
	sortDesc := req.Msg.SortDesc
	if !req.Msg.SortDesc && req.Msg.SortBy == "" {
		sortDesc = true
	}

	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 100
	}

	if h.useBolt {
		return h.getUsageTreeBolt(req.Msg.FsPath, path, sortBy, sortDesc, limit)
	}
	return h.getUsageTreeMemory(req.Msg.FsPath, path, sortBy, sortDesc, limit)
}

// getUsageTreeBolt handles GetUsageTree for BBolt backend.
func (h *UsageHandler) getUsageTreeBolt(fsPath, path, sortBy string, sortDesc bool, limit int) (*connect.Response[apiv1.GetUsageTreeResponse], error) {
	// Get session from active sampler or open from disk
	var session *btdu.BoltSession
	var needClose bool

	h.mu.RLock()
	sampler, ok := h.boltSamplers[fsPath]
	h.mu.RUnlock()

	if ok && sampler != nil {
		session = sampler.Session()
	} else if h.boltStore != nil && h.boltStore.Has(fsPath) {
		var err error
		session, err = h.boltStore.Open(fsPath)
		if err != nil {
			return connect.NewResponse(&apiv1.GetUsageTreeResponse{
				Children:     nil,
				TotalSamples: 0,
				TotalSize:    0,
			}), nil
		}
		needClose = true
	}

	if session == nil {
		return connect.NewResponse(&apiv1.GetUsageTreeResponse{
			Children:     nil,
			TotalSamples: 0,
			TotalSize:    0,
		}), nil
	}

	if needClose {
		defer session.Close()
	}

	totalSamples := session.SampleCount()
	totalSize := session.TotalSize()

	// Get children from BBolt
	children, err := session.GetChildren(path)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Sort children
	sort.Slice(children, func(i, j int) bool {
		var cmp int
		switch sortBy {
		case "name":
			if children[i].Name < children[j].Name {
				cmp = -1
			} else if children[i].Name > children[j].Name {
				cmp = 1
			}
		default: // size or samples
			si := children[i].Stats.TotalSamples()
			sj := children[j].Stats.TotalSamples()
			if si < sj {
				cmp = -1
			} else if si > sj {
				cmp = 1
			}
		}
		if sortDesc {
			return cmp > 0
		}
		return cmp < 0
	})

	if len(children) > limit {
		children = children[:limit]
	}

	var protoChildren []*apiv1.UsageNode
	for _, c := range children {
		samples := c.Stats.TotalSamples()
		var estimatedSize uint64
		if totalSamples > 0 {
			estimatedSize = (samples * totalSize) / totalSamples
		}
		var percentage float64
		if totalSize > 0 {
			percentage = float64(estimatedSize) / float64(totalSize) * 100
		}

		// Check if has children by looking for entries with this path as prefix
		childChildren, _ := session.GetChildren(c.Path)
		hasChildren := len(childChildren) > 0

		protoChildren = append(protoChildren, &apiv1.UsageNode{
			Name:          c.Name,
			FullPath:      c.Path,
			IsDir:         hasChildren,
			Samples:       samples,
			EstimatedSize: estimatedSize,
			Percentage:    percentage,
			ChildCount:    int32(len(childChildren)),
		})
	}

	// Current node info
	var current *apiv1.UsageNode
	currentStats, err := session.GetPathStats(path)
	if err == nil && currentStats != nil {
		samples := currentStats.TotalSamples()
		var estimatedSize uint64
		if totalSamples > 0 {
			estimatedSize = (samples * totalSize) / totalSamples
		}
		// Extract name from path
		name := path
		if idx := lastIndexOf(path, '/'); idx >= 0 && idx < len(path)-1 {
			name = path[idx+1:]
		}
		if path == "/" {
			name = ""
		}
		current = &apiv1.UsageNode{
			Name:          name,
			FullPath:      path,
			IsDir:         len(children) > 0,
			Samples:       samples,
			EstimatedSize: estimatedSize,
			ChildCount:    int32(len(children)),
		}
	}

	return connect.NewResponse(&apiv1.GetUsageTreeResponse{
		Children:     protoChildren,
		Current:      current,
		TotalSamples: totalSamples,
		TotalSize:    totalSize,
	}), nil
}

// getUsageTreeMemory handles GetUsageTree for in-memory backend.
func (h *UsageHandler) getUsageTreeMemory(fsPath, path, sortBy string, sortDesc bool, limit int) (*connect.Response[apiv1.GetUsageTreeResponse], error) {
	session := h.getSession(fsPath)
	if session == nil {
		return connect.NewResponse(&apiv1.GetUsageTreeResponse{
			Children:     nil,
			TotalSamples: 0,
			TotalSize:    0,
		}), nil
	}

	node := session.Root.GetPath(path)
	if node == nil {
		node = session.Root
	}

	type childInfo struct {
		node    *btdu.PathNode
		name    string
		samples uint64
	}

	directChildren := node.DirectChildren()
	children := make([]childInfo, 0, len(directChildren))
	for name, child := range directChildren {
		children = append(children, childInfo{
			node:    child,
			name:    name,
			samples: child.Stats.TotalSamples(),
		})
	}

	sort.Slice(children, func(i, j int) bool {
		var cmp int
		switch sortBy {
		case "name":
			if children[i].name < children[j].name {
				cmp = -1
			} else if children[i].name > children[j].name {
				cmp = 1
			}
		default:
			si := children[i].samples
			sj := children[j].samples
			if si < sj {
				cmp = -1
			} else if si > sj {
				cmp = 1
			}
		}
		if sortDesc {
			return cmp > 0
		}
		return cmp < 0
	})

	if len(children) > limit {
		children = children[:limit]
	}

	var protoChildren []*apiv1.UsageNode
	totalSamples := session.SampleCount
	totalSize := session.TotalSize

	for _, c := range children {
		samples := c.node.Stats.TotalSamples()
		var estimatedSize uint64
		if totalSamples > 0 {
			estimatedSize = (samples * totalSize) / totalSamples
		}
		var percentage float64
		if totalSize > 0 {
			percentage = float64(estimatedSize) / float64(totalSize) * 100
		}

		protoChildren = append(protoChildren, &apiv1.UsageNode{
			Name:          c.name,
			FullPath:      c.node.FullPath(),
			IsDir:         c.node.ChildCount() > 0,
			Samples:       samples,
			EstimatedSize: estimatedSize,
			Percentage:    percentage,
			ChildCount:    int32(c.node.ChildCount()),
		})
	}

	var current *apiv1.UsageNode
	if node != nil {
		samples := node.Stats.TotalSamples()
		var estimatedSize uint64
		if totalSamples > 0 {
			estimatedSize = (samples * totalSize) / totalSamples
		}
		current = &apiv1.UsageNode{
			Name:          node.Name,
			FullPath:      node.FullPath(),
			IsDir:         node.ChildCount() > 0,
			Samples:       samples,
			EstimatedSize: estimatedSize,
			ChildCount:    int32(node.ChildCount()),
		}
	}

	return connect.NewResponse(&apiv1.GetUsageTreeResponse{
		Children:     protoChildren,
		Current:      current,
		TotalSamples: totalSamples,
		TotalSize:    totalSize,
	}), nil
}

func lastIndexOf(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func (h *UsageHandler) StreamSamplingProgress(
	ctx context.Context,
	req *connect.Request[apiv1.StreamSamplingProgressRequest],
	stream *connect.ServerStream[apiv1.SamplingProgress],
) error {
	h.logger.Debug("stream sampling progress", "fs_path", req.Msg.FsPath)

	if req.Msg.FsPath == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("fs_path is required"))
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			progress := &apiv1.SamplingProgress{
				IsRunning: false,
			}

			if h.useBolt {
				h.mu.RLock()
				sampler, ok := h.boltSamplers[req.Msg.FsPath]
				h.mu.RUnlock()

				if ok && sampler != nil {
					progress.IsRunning = sampler.IsRunning()
					progress.CurrentPath = sampler.CurrentPath()
					progress.SamplesPerSecond = sampler.SamplesPerSecond()
					progress.RecentPaths = sampler.RecentPaths(16)

					session := sampler.Session()
					progress.SampleCount = session.SampleCount()
					progress.TotalSize = session.TotalSize()
					progress.RunningTimeSeconds = int64(session.GetRunningTime().Seconds())
				}
			} else {
				h.mu.RLock()
				sampler, ok := h.samplers[req.Msg.FsPath]
				h.mu.RUnlock()

				if ok && sampler != nil {
					progress.IsRunning = sampler.IsRunning()
					progress.CurrentPath = sampler.CurrentPath()
					progress.SamplesPerSecond = sampler.SamplesPerSecond()
					progress.RecentPaths = sampler.RecentPaths(16)

					progress.SampleCount = sampler.Session.SampleCount
					progress.TotalSize = sampler.Session.TotalSize
					progress.RunningTimeSeconds = int64(sampler.Session.GetRunningTime().Seconds())
				}
			}

			if err := stream.Send(progress); err != nil {
				return err
			}
		}
	}
}

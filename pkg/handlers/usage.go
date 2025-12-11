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
	store  *btdu.Store

	// Active samplers keyed by filesystem path
	samplers map[string]*btdu.Sampler
	// Cached sessions for stopped samplers (loaded from disk once)
	sessionCache map[string]*btdu.Session
	mu           sync.RWMutex
}

func NewUsageHandler(logger *slog.Logger, db *db.DB, cfg *config.Config) (*UsageHandler, error) {
	store, err := btdu.NewStore(cfg.BTDUStoreDir)
	if err != nil {
		return nil, fmt.Errorf("create btdu store: %w", err)
	}

	h := &UsageHandler{
		logger:       logger.With("handler", "usage"),
		db:           db,
		store:        store,
		samplers:     make(map[string]*btdu.Sampler),
		sessionCache: make(map[string]*btdu.Session),
	}

	// Preload existing sessions into cache for fast initial requests
	go h.preloadSessions()

	return h, nil
}

// preloadSessions loads all stored sessions into the cache on startup.
func (h *UsageHandler) preloadSessions() {
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

// getSampler returns an existing sampler or creates a new one.
// New samplers always try to resume from stored session data.
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

	sampler, err := h.getSampler(req.Msg.FsPath)
	if err != nil {
		h.logger.Error("failed to get sampler", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// If client doesn't want to resume, clear existing data first
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

	h.mu.RLock()
	sampler, ok := h.samplers[req.Msg.FsPath]
	h.mu.RUnlock()

	progress := &apiv1.SamplingProgress{
		IsRunning: false,
	}

	hasSession := false

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

	h.mu.Lock()
	sampler, ok := h.samplers[req.Msg.FsPath]
	if ok {
		sampler.Clear()
		delete(h.samplers, req.Msg.FsPath)
	}
	// Always clear session cache
	delete(h.sessionCache, req.Msg.FsPath)
	h.mu.Unlock()

	// Also delete from store directly if no active sampler
	if !ok {
		h.store.Delete(req.Msg.FsPath)
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

	// Get session (from active sampler, cache, or disk - cached for performance)
	session := h.getSession(req.Msg.FsPath)
	if session == nil {
		return connect.NewResponse(&apiv1.GetUsageTreeResponse{
			Children:     nil,
			TotalSamples: 0,
			TotalSize:    0,
		}), nil
	}

	// Navigate to the requested path
	path := req.Msg.Path
	if path == "" {
		path = "/"
	}

	node := session.Root.GetPath(path)
	if node == nil {
		node = session.Root
	}

	// Get sort options
	sortBy := req.Msg.SortBy
	if sortBy == "" {
		sortBy = "size"
	}
	sortDesc := req.Msg.SortDesc
	if !req.Msg.SortDesc && req.Msg.SortBy == "" {
		sortDesc = true // Default to descending
	}

	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 100
	}

	// Build children list with cached sample counts for efficient sorting
	type childInfo struct {
		node    *btdu.PathNode
		name    string
		samples uint64 // Cached for sorting
	}

	// Get direct children (avoid Walk which can be slow)
	directChildren := node.DirectChildren()
	children := make([]childInfo, 0, len(directChildren))
	for name, child := range directChildren {
		children = append(children, childInfo{
			node:    child,
			name:    name,
			samples: child.Stats.TotalSamples(),
		})
	}

	// Sort children using cached samples
	sort.Slice(children, func(i, j int) bool {
		var cmp int
		switch sortBy {
		case "name":
			if children[i].name < children[j].name {
				cmp = -1
			} else if children[i].name > children[j].name {
				cmp = 1
			}
		case "samples":
			si := children[i].samples
			sj := children[j].samples
			if si < sj {
				cmp = -1
			} else if si > sj {
				cmp = 1
			}
		default: // size
			// Estimate size from samples
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

	// Limit
	if len(children) > limit {
		children = children[:limit]
	}

	// Convert to proto
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

	// Current node info
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
			h.mu.RLock()
			sampler, ok := h.samplers[req.Msg.FsPath]
			h.mu.RUnlock()

			progress := &apiv1.SamplingProgress{
				IsRunning: false,
			}

			if ok && sampler != nil {
				progress.IsRunning = sampler.IsRunning()
				progress.CurrentPath = sampler.CurrentPath()
				progress.SamplesPerSecond = sampler.SamplesPerSecond()
				progress.RecentPaths = sampler.RecentPaths(16)

				progress.SampleCount = sampler.Session.SampleCount
				progress.TotalSize = sampler.Session.TotalSize
				progress.RunningTimeSeconds = int64(sampler.Session.GetRunningTime().Seconds())
			}

			if err := stream.Send(progress); err != nil {
				return err
			}
		}
	}
}

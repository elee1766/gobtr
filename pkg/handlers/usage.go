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
	logger   *slog.Logger
	db       *db.DB
	store    *btdu.PebbleStore
	samplers map[string]*btdu.PebbleSampler
	mu       sync.RWMutex
}

func NewUsageHandler(logger *slog.Logger, db *db.DB, cfg *config.Config) (*UsageHandler, error) {
	store, err := btdu.NewPebbleStore(cfg.BTDUStoreDir)
	if err != nil {
		return nil, fmt.Errorf("create btdu pebble store: %w", err)
	}

	logger.Info("using PebbleDB backend for btdu storage", "dir", cfg.BTDUStoreDir)

	return &UsageHandler{
		logger:   logger.With("handler", "usage"),
		db:       db,
		store:    store,
		samplers: make(map[string]*btdu.PebbleSampler),
	}, nil
}

func (h *UsageHandler) getSampler(fsPath string) (*btdu.PebbleSampler, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if sampler, ok := h.samplers[fsPath]; ok {
		return sampler, nil
	}

	sampler, err := btdu.NewPebbleSampler(fsPath, h.store, true)
	if err != nil {
		return nil, err
	}

	h.samplers[fsPath] = sampler
	return sampler, nil
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
		TotalSamples: sampler.Session().SampleCount(),
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

	h.mu.RLock()
	sampler, ok := h.samplers[req.Msg.FsPath]
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
	} else if h.store != nil && h.store.Has(req.Msg.FsPath) {
		session, err := h.store.Open(req.Msg.FsPath)
		if err == nil {
			hasSession = true
			progress.SampleCount = session.SampleCount()
			progress.TotalSize = session.TotalSize()
			progress.RunningTimeSeconds = int64(session.GetRunningTime().Seconds())
			session.Close()
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
	h.mu.Unlock()

	if !ok && h.store != nil {
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

	// Get session from active sampler or open from disk
	var session *btdu.PebbleSession
	var needClose bool

	h.mu.RLock()
	sampler, ok := h.samplers[req.Msg.FsPath]
	h.mu.RUnlock()

	if ok && sampler != nil {
		session = sampler.Session()
	} else if h.store != nil && h.store.Has(req.Msg.FsPath) {
		var err error
		session, err = h.store.Open(req.Msg.FsPath)
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

			h.mu.RLock()
			sampler, ok := h.samplers[req.Msg.FsPath]
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

			if err := stream.Send(progress); err != nil {
				return err
			}
		}
	}
}

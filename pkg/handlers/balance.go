package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"
	apiv1 "github.com/elee1766/gobtr/gen/api/v1"
	"github.com/elee1766/gobtr/pkg/btrfs"
	"github.com/elee1766/gobtr/pkg/db"
	"github.com/elee1766/gobtr/pkg/db/queries"
)

type BalanceHandler struct {
	logger       *slog.Logger
	db           *db.DB
	btrfsManager *btrfs.Manager
}

func NewBalanceHandler(logger *slog.Logger, db *db.DB, btrfsManager *btrfs.Manager) *BalanceHandler {
	return &BalanceHandler{
		logger:       logger.With("handler", "balance"),
		db:           db,
		btrfsManager: btrfsManager,
	}
}

// balanceFlags holds the flags used for the current balance, keyed by device path
var (
	currentBalanceFlags = make(map[string]*btrfs.BalanceOptions)
	balanceFlagsMutex   sync.Mutex
)

// recordBalanceToHistory upserts a balance record
func (h *BalanceHandler) recordBalanceToHistory(devicePath string, balanceID string, status *btrfs.BalanceStatus) {
	if balanceID == "" {
		return
	}

	balanceHistory := &queries.BalanceHistory{
		BalanceID:        balanceID,
		DevicePath:       devicePath,
		StartedAt:        status.StartedAt,
		Status:           status.Status,
		ChunksConsidered: status.Considered,
		ChunksRelocated:  status.Relocated,
		SizeRelocated:    status.SizeRelocated,
		SoftErrors:       status.SoftErrors,
	}

	if balanceHistory.StartedAt.IsZero() {
		balanceHistory.StartedAt = time.Now()
	}

	// Get flags if we have them stored
	balanceFlagsMutex.Lock()
	if opts, ok := currentBalanceFlags[devicePath]; ok {
		balanceHistory.FlagData = opts.Data
		balanceHistory.FlagMetadata = opts.Metadata
		balanceHistory.FlagSystem = opts.System
		balanceHistory.FlagUsagePercent = opts.UsagePercent
		balanceHistory.FlagLimitChunks = opts.LimitChunks
		balanceHistory.FlagLimitPercent = opts.LimitPercent
		balanceHistory.FlagBackground = opts.Background
		balanceHistory.FlagDryRun = opts.DryRun
		balanceHistory.FlagForce = opts.Force
		// Clean up flags when balance is done
		if status.Status != "running" && status.Status != "starting" && status.Status != "paused" {
			delete(currentBalanceFlags, devicePath)
		}
	}
	balanceFlagsMutex.Unlock()

	if !status.FinishedAt.IsZero() {
		balanceHistory.FinishedAt = sql.NullTime{Time: status.FinishedAt, Valid: true}
	}

	if err := queries.UpsertBalance(h.db.Conn(), balanceHistory); err != nil {
		h.logger.Warn("failed to record balance to history", "error", err, "id", balanceID)
	}
}

// statusToProto converts btrfs.BalanceStatus to apiv1.BalanceProgress
func balanceStatusToProto(status *btrfs.BalanceStatus) *apiv1.BalanceProgress {
	progress := &apiv1.BalanceProgress{
		Status:          status.Status,
		IsRunning:       status.IsRunning,
		TotalChunks:     status.TotalChunks,
		Considered:      status.Considered,
		Relocated:       status.Relocated,
		Left:            status.Left,
		SizeTotal:       status.SizeTotal,
		SizeRelocated:   status.SizeRelocated,
		SoftErrors:      status.SoftErrors,
		Duration:        status.Duration,
		DurationSeconds: status.DurationSeconds,
	}

	if !status.StartedAt.IsZero() {
		progress.StartedAt = status.StartedAt.Unix()
	}
	if !status.FinishedAt.IsZero() {
		progress.FinishedAt = status.FinishedAt.Unix()
	}

	if status.TotalChunks > 0 {
		progress.ProgressPercent = float64(status.Relocated) / float64(status.TotalChunks) * 100.0
	}

	return progress
}

func (h *BalanceHandler) StartBalance(
	ctx context.Context,
	req *connect.Request[apiv1.StartBalanceRequest],
) (*connect.Response[apiv1.StartBalanceResponse], error) {
	h.logger.Info("start balance", "device", req.Msg.DevicePath,
		"filters", req.Msg.Filters, "limit_percent", req.Msg.LimitPercent,
		"background", req.Msg.Background, "dry_run", req.Msg.DryRun, "force", req.Msg.Force)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Build options
	opts := btrfs.BalanceOptions{
		LimitPercent: req.Msg.LimitPercent,
		Background:   req.Msg.Background,
		DryRun:       req.Msg.DryRun,
		Force:        req.Msg.Force,
	}

	if req.Msg.Filters != nil {
		opts.Data = req.Msg.Filters.Data
		opts.Metadata = req.Msg.Filters.Metadata
		opts.System = req.Msg.Filters.System
		opts.UsagePercent = req.Msg.Filters.UsagePercent
		opts.LimitChunks = req.Msg.Filters.LimitChunks
	}

	// Store flags for recording to history later
	balanceFlagsMutex.Lock()
	currentBalanceFlags[req.Msg.DevicePath] = &opts
	balanceFlagsMutex.Unlock()

	// Start the balance
	balanceID, err := h.btrfsManager.StartBalance(ctx, req.Msg.DevicePath, opts)
	if err != nil {
		h.logger.Error("failed to start balance", "error", err)
		// Clean up flags on error
		balanceFlagsMutex.Lock()
		delete(currentBalanceFlags, req.Msg.DevicePath)
		balanceFlagsMutex.Unlock()
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Record initial history entry
	h.recordBalanceToHistory(req.Msg.DevicePath, balanceID, &btrfs.BalanceStatus{
		Status:    "starting",
		StartedAt: time.Now(),
	})

	return connect.NewResponse(&apiv1.StartBalanceResponse{
		BalanceId: balanceID,
		Started:   true,
	}), nil
}

func (h *BalanceHandler) CancelBalance(
	ctx context.Context,
	req *connect.Request[apiv1.CancelBalanceRequest],
) (*connect.Response[apiv1.CancelBalanceResponse], error) {
	h.logger.Info("cancel balance", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Get balance ID before canceling
	balanceID := h.btrfsManager.GetActiveBalanceID(req.Msg.DevicePath)

	// Cancel the balance
	err := h.btrfsManager.CancelBalance(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to cancel balance", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Update history
	if balanceID != "" {
		h.recordBalanceToHistory(req.Msg.DevicePath, balanceID, &btrfs.BalanceStatus{
			Status:     "cancelled",
			FinishedAt: time.Now(),
		})
	}

	return connect.NewResponse(&apiv1.CancelBalanceResponse{
		Success: true,
	}), nil
}

func (h *BalanceHandler) GetBalanceStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetBalanceStatusRequest],
) (*connect.Response[apiv1.GetBalanceStatusResponse], error) {
	h.logger.Debug("get balance status", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Get current status from btrfs
	status, err := h.btrfsManager.GetBalanceStatus(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to get balance status", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto
	progress := balanceStatusToProto(status)

	// Record balance to history if running
	if status.IsRunning || status.IsPaused {
		balanceID := h.btrfsManager.GetActiveBalanceID(req.Msg.DevicePath)
		if balanceID != "" {
			h.recordBalanceToHistory(req.Msg.DevicePath, balanceID, status)
		}
	}

	return connect.NewResponse(&apiv1.GetBalanceStatusResponse{
		Progress:  progress,
		IsRunning: status.IsRunning,
	}), nil
}

func (h *BalanceHandler) ListBalanceHistory(
	ctx context.Context,
	req *connect.Request[apiv1.ListBalanceHistoryRequest],
) (*connect.Response[apiv1.ListBalanceHistoryResponse], error) {
	h.logger.Debug("list balance history", "device", req.Msg.DevicePath, "limit", req.Msg.Limit)

	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 50
	}

	// Get history from database
	history, err := queries.ListBalanceHistory(h.db.Conn(), req.Msg.DevicePath, limit)
	if err != nil {
		h.logger.Error("failed to list balance history", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto
	var entries []*apiv1.BalanceHistoryEntry
	for _, b := range history {
		entry := &apiv1.BalanceHistoryEntry{
			BalanceId:       b.BalanceID,
			DevicePath:      b.DevicePath,
			StartedAt:       b.StartedAt.Unix(),
			Status:          b.Status,
			ChunksRelocated: b.ChunksRelocated,
			SizeRelocated:   b.SizeRelocated,
			SoftErrors:      b.SoftErrors,
			Flags: &apiv1.BalanceFlags{
				Filters: &apiv1.BalanceFilters{
					Data:         b.FlagData,
					Metadata:     b.FlagMetadata,
					System:       b.FlagSystem,
					UsagePercent: b.FlagUsagePercent,
					LimitChunks:  b.FlagLimitChunks,
				},
				LimitPercent: b.FlagLimitPercent,
				Background:   b.FlagBackground,
				DryRun:       b.FlagDryRun,
				Force:        b.FlagForce,
			},
		}

		if b.FinishedAt.Valid {
			entry.FinishedAt = b.FinishedAt.Time.Unix()
		}

		entries = append(entries, entry)
	}

	return connect.NewResponse(&apiv1.ListBalanceHistoryResponse{
		Entries: entries,
	}), nil
}

// GetAllBalanceStatus gets balance status for all tracked filesystems in parallel
func (h *BalanceHandler) GetAllBalanceStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetAllBalanceStatusRequest],
) (*connect.Response[apiv1.GetAllBalanceStatusResponse], error) {
	h.logger.Debug("getting balance status for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemBalanceStatus, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, fsPath string) {
			defer wg.Done()

			result := &apiv1.FilesystemBalanceStatus{
				Path: fsPath,
			}

			status, err := h.btrfsManager.GetBalanceStatus(fsPath)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			result.Progress = balanceStatusToProto(status)
			result.IsRunning = status.IsRunning
			results[idx] = result
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.GetAllBalanceStatusResponse{
		Filesystems: results,
	}), nil
}

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

type ScrubHandler struct {
	logger       *slog.Logger
	db           *db.DB
	btrfsManager *btrfs.Manager
}

func NewScrubHandler(logger *slog.Logger, db *db.DB, btrfsManager *btrfs.Manager) *ScrubHandler {
	return &ScrubHandler{
		logger:       logger.With("handler", "scrub"),
		db:           db,
		btrfsManager: btrfsManager,
	}
}

// scrubFlags holds the flags used for the current scrub, keyed by device path
var (
	currentScrubFlags = make(map[string]*btrfs.ScrubOptions)
	scrubFlagsMutex   sync.Mutex
)

// recordScrubToHistory upserts a scrub record using the btrfs UUID
func (h *ScrubHandler) recordScrubToHistory(devicePath string, status *btrfs.ScrubStatus) {
	if status.UUID == "" {
		return
	}

	scrubHistory := &queries.ScrubHistory{
		ScrubID:             status.UUID,
		DevicePath:          devicePath,
		StartedAt:           status.StartedAt,
		Status:              status.Status,
		BytesScrubbed:       sql.NullInt64{Int64: status.BytesScrubbed, Valid: true},
		TotalBytes:          sql.NullInt64{Int64: status.TotalBytes, Valid: true},
		DataErrors:          status.DataErrors,
		TreeErrors:          status.TreeErrors,
		CorrectedErrors:     status.CorrectedErrors,
		UncorrectableErrors: status.UncorrectableErrors,
	}

	// Get flags if we have them stored
	scrubFlagsMutex.Lock()
	if opts, ok := currentScrubFlags[devicePath]; ok {
		scrubHistory.FlagReadonly = opts.Readonly
		scrubHistory.FlagLimitBytesPerSec = opts.LimitBytesPerSec
		scrubHistory.FlagForce = opts.Force
		// Clean up flags when scrub is done
		if status.Status != "running" && status.Status != "starting" {
			delete(currentScrubFlags, devicePath)
		}
	}
	scrubFlagsMutex.Unlock()

	if !status.FinishedAt.IsZero() {
		scrubHistory.FinishedAt = sql.NullTime{Time: status.FinishedAt, Valid: true}
	}

	if err := queries.UpsertScrub(h.db.Conn(), scrubHistory); err != nil {
		h.logger.Warn("failed to record scrub to history", "error", err, "uuid", status.UUID)
	}
}

// statusToProto converts btrfs.ScrubStatus to apiv1.ScrubProgress
func statusToProto(status *btrfs.ScrubStatus) *apiv1.ScrubProgress {
	progress := &apiv1.ScrubProgress{
		BytesScrubbed:        status.BytesScrubbed,
		TotalBytes:           status.TotalBytes,
		DataErrors:           status.DataErrors,
		TreeErrors:           status.TreeErrors,
		CorrectedErrors:      status.CorrectedErrors,
		UncorrectableErrors:  status.UncorrectableErrors,
		Status:               status.Status,
		DataBytesScrubbed:    status.DataBytesScrubbed,
		TreeBytesScrubbed:    status.TreeBytesScrubbed,
		DataExtentsScrubbed:  status.DataExtentsScrubbed,
		TreeExtentsScrubbed:  status.TreeExtentsScrubbed,
		ReadErrors:           status.ReadErrors,
		CsumErrors:           status.CsumErrors,
		VerifyErrors:         status.VerifyErrors,
		NoCsum:               status.NoCsum,
		CsumDiscards:         status.CsumDiscards,
		SuperErrors:          status.SuperErrors,
		MallocErrors:         status.MallocErrors,
		UnverifiedErrors:     status.UnverifiedErrors,
		LastPhysical:         status.LastPhysical,
		Duration:             status.Duration,
		DurationSeconds:      status.DurationSeconds,
		RateBytesPerSec:      status.RateBytesPerSec,
		EtaSeconds:           status.EtaSeconds,
	}

	if !status.StartedAt.IsZero() {
		progress.StartedAt = status.StartedAt.Unix()
	}
	if !status.FinishedAt.IsZero() {
		progress.FinishedAt = status.FinishedAt.Unix()
	}

	if status.TotalBytes > 0 {
		progress.ProgressPercent = float64(status.BytesScrubbed) / float64(status.TotalBytes) * 100.0
	}

	return progress
}

func (h *ScrubHandler) StartScrub(
	ctx context.Context,
	req *connect.Request[apiv1.StartScrubRequest],
) (*connect.Response[apiv1.StartScrubResponse], error) {
	h.logger.Info("start scrub", "device", req.Msg.DevicePath, "readonly", req.Msg.Readonly,
		"limit", req.Msg.LimitBytesPerSec, "force", req.Msg.Force)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Build options
	opts := btrfs.ScrubOptions{
		Readonly:         req.Msg.Readonly,
		LimitBytesPerSec: req.Msg.LimitBytesPerSec,
		Force:            req.Msg.Force,
	}

	// Store flags for recording to history later
	scrubFlagsMutex.Lock()
	currentScrubFlags[req.Msg.DevicePath] = &opts
	scrubFlagsMutex.Unlock()

	// Start the scrub with options
	scrubID, err := h.btrfsManager.StartScrubWithOptions(ctx, req.Msg.DevicePath, opts)
	if err != nil {
		h.logger.Error("failed to start scrub", "error", err)
		// Clean up flags on error
		scrubFlagsMutex.Lock()
		delete(currentScrubFlags, req.Msg.DevicePath)
		scrubFlagsMutex.Unlock()
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Scrub will be recorded to history via status polling using btrfs UUID

	return connect.NewResponse(&apiv1.StartScrubResponse{
		ScrubId: scrubID,
		Started: true,
	}), nil
}

func (h *ScrubHandler) CancelScrub(
	ctx context.Context,
	req *connect.Request[apiv1.CancelScrubRequest],
) (*connect.Response[apiv1.CancelScrubResponse], error) {
	h.logger.Info("cancel scrub", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Cancel the scrub
	err := h.btrfsManager.CancelScrub(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to cancel scrub", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Get status after cancel and record to history
	status, err := h.btrfsManager.GetScrubStatus(req.Msg.DevicePath)
	if err == nil && status.UUID != "" {
		h.recordScrubToHistory(req.Msg.DevicePath, status)
	}

	return connect.NewResponse(&apiv1.CancelScrubResponse{
		Success: true,
	}), nil
}

func (h *ScrubHandler) GetScrubStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetScrubStatusRequest],
) (*connect.Response[apiv1.GetScrubStatusResponse], error) {
	h.logger.Debug("get scrub status", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Get current status from btrfs
	status, err := h.btrfsManager.GetScrubStatus(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to get scrub status", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto
	progress := statusToProto(status)

	// Record scrub to history using btrfs UUID (upsert handles duplicates)
	if status.UUID != "" && status.Status != "never_run" {
		h.recordScrubToHistory(req.Msg.DevicePath, status)
	}

	return connect.NewResponse(&apiv1.GetScrubStatusResponse{
		Progress:  progress,
		IsRunning: status.IsRunning,
	}), nil
}

func (h *ScrubHandler) StreamScrubProgress(
	ctx context.Context,
	req *connect.Request[apiv1.StreamScrubProgressRequest],
	stream *connect.ServerStream[apiv1.ScrubProgress],
) error {
	h.logger.Debug("stream scrub progress", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Poll for status updates
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Get current status
			status, err := h.btrfsManager.GetScrubStatus(req.Msg.DevicePath)
			if err != nil {
				h.logger.Error("failed to get scrub status", "error", err)
				continue
			}

			// Send progress update
			progress := statusToProto(status)

			if err := stream.Send(progress); err != nil {
				return err
			}

			// Stop streaming if scrub is finished
			if !status.IsRunning && status.Status != "starting" {
				return nil
			}
		}
	}
}

func (h *ScrubHandler) ListScrubHistory(
	ctx context.Context,
	req *connect.Request[apiv1.ListScrubHistoryRequest],
) (*connect.Response[apiv1.ListScrubHistoryResponse], error) {
	h.logger.Debug("list scrub history", "device", req.Msg.DevicePath, "limit", req.Msg.Limit)

	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 50
	}

	// Get history from database
	history, err := queries.ListScrubHistory(h.db.Conn(), req.Msg.DevicePath, limit)
	if err != nil {
		h.logger.Error("failed to list scrub history", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto
	var entries []*apiv1.ScrubHistoryEntry
	for _, h := range history {
		entry := &apiv1.ScrubHistoryEntry{
			ScrubId:         h.ScrubID,
			DevicePath:      h.DevicePath,
			StartedAt:       h.StartedAt.Unix(),
			Status:          h.Status,
			CorrectedErrors: h.CorrectedErrors,
			Flags: &apiv1.ScrubFlags{
				Readonly:         h.FlagReadonly,
				LimitBytesPerSec: h.FlagLimitBytesPerSec,
				Force:            h.FlagForce,
			},
		}

		if h.FinishedAt.Valid {
			entry.FinishedAt = h.FinishedAt.Time.Unix()
		}

		entry.TotalErrors = h.DataErrors + h.TreeErrors + h.UncorrectableErrors

		entries = append(entries, entry)
	}

	return connect.NewResponse(&apiv1.ListScrubHistoryResponse{
		Entries: entries,
	}), nil
}

// GetAllScrubStatus gets scrub status for all tracked filesystems in parallel
func (h *ScrubHandler) GetAllScrubStatus(
	ctx context.Context,
	req *connect.Request[apiv1.GetAllScrubStatusRequest],
) (*connect.Response[apiv1.GetAllScrubStatusResponse], error) {
	h.logger.Debug("getting scrub status for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemScrubStatus, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, fsPath string) {
			defer wg.Done()

			result := &apiv1.FilesystemScrubStatus{
				Path: fsPath,
			}

			status, err := h.btrfsManager.GetScrubStatus(fsPath)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			result.Progress = statusToProto(status)
			result.IsRunning = status.IsRunning
			results[idx] = result

			// Record scrub to history using btrfs UUID
			if status.UUID != "" && status.Status != "never_run" {
				h.recordScrubToHistory(fsPath, status)
			}
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.GetAllScrubStatusResponse{
		Filesystems: results,
	}), nil
}

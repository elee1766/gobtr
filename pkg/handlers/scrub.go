package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"
	apiv1 "github.com/elee1766/gobtr/gen/api/v1"
	"github.com/elee1766/gobtr/pkg/btrfs"
	"github.com/elee1766/gobtr/pkg/db"
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

	// Start the scrub with options
	scrubID, err := h.btrfsManager.StartScrubWithOptions(ctx, req.Msg.DevicePath, opts)
	if err != nil {
		h.logger.Error("failed to start scrub", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

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

	// Get current status from btrfs status file
	status, err := h.btrfsManager.GetScrubStatus(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to get scrub status", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto
	progress := statusToProto(status)

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
	// Scrub history is now read directly from btrfs status files
	// Each filesystem only has the most recent scrub in /var/lib/btrfs/scrub.status.<UUID>
	// Return empty list - frontend should use GetScrubStatus instead
	return connect.NewResponse(&apiv1.ListScrubHistoryResponse{
		Entries: nil,
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
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.GetAllScrubStatusResponse{
		Filesystems: results,
	}), nil
}

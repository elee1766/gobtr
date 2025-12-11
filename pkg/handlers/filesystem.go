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
	"github.com/elee1766/gobtr/pkg/db/queries"
)

type FilesystemHandler struct {
	logger       *slog.Logger
	db           *db.DB
	btrfsManager *btrfs.Manager
}

func NewFilesystemHandler(logger *slog.Logger, db *db.DB, btrfsManager *btrfs.Manager) *FilesystemHandler {
	return &FilesystemHandler{
		logger:       logger.With("handler", "filesystem"),
		db:           db,
		btrfsManager: btrfsManager,
	}
}

func (h *FilesystemHandler) GetErrors(
	ctx context.Context,
	req *connect.Request[apiv1.GetErrorsRequest],
) (*connect.Response[apiv1.GetErrorsResponse], error) {
	h.logger.Debug("get errors", "limit", req.Msg.Limit, "device", req.Msg.Device)

	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 100
	}

	since := time.Time{}
	if req.Msg.Since > 0 {
		since = time.Unix(req.Msg.Since, 0)
	}

	// Get errors from database
	dbErrors, err := queries.ListErrors(h.db.Conn(), req.Msg.Device, since, limit)
	if err != nil {
		h.logger.Error("failed to list errors", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Get total count
	totalCount, err := queries.CountErrors(h.db.Conn(), req.Msg.Device, since)
	if err != nil {
		h.logger.Warn("failed to count errors", "error", err)
		totalCount = int64(len(dbErrors))
	}

	// Convert to proto format
	var errors []*apiv1.FilesystemError
	for _, dbErr := range dbErrors {
		fsError := &apiv1.FilesystemError{
			Device:    dbErr.Device,
			ErrorType: dbErr.ErrorType,
			Timestamp: dbErr.Timestamp.Unix(),
		}

		if dbErr.Message.Valid {
			fsError.Message = dbErr.Message.String
		}
		if dbErr.Inode.Valid {
			fsError.Inode = dbErr.Inode.Int64
		}
		if dbErr.Path.Valid {
			fsError.Path = dbErr.Path.String
		}

		errors = append(errors, fsError)
	}

	return connect.NewResponse(&apiv1.GetErrorsResponse{
		Errors:     errors,
		TotalCount: totalCount,
	}), nil
}

func (h *FilesystemHandler) StreamErrors(
	ctx context.Context,
	req *connect.Request[apiv1.StreamErrorsRequest],
	stream *connect.ServerStream[apiv1.FilesystemError],
) error {
	h.logger.Debug("stream errors", "device", req.Msg.Device)

	// For streaming, we'll poll for new errors periodically
	// In a real implementation, you might want to use inotify or similar
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastCheck := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Get errors since last check
			errors, err := queries.ListErrors(h.db.Conn(), req.Msg.Device, lastCheck, 100)
			if err != nil {
				h.logger.Error("failed to get new errors", "error", err)
				continue
			}

			// Stream new errors
			for _, dbErr := range errors {
				fsError := &apiv1.FilesystemError{
					Device:    dbErr.Device,
					ErrorType: dbErr.ErrorType,
					Timestamp: dbErr.Timestamp.Unix(),
				}

				if dbErr.Message.Valid {
					fsError.Message = dbErr.Message.String
				}
				if dbErr.Inode.Valid {
					fsError.Inode = dbErr.Inode.Int64
				}
				if dbErr.Path.Valid {
					fsError.Path = dbErr.Path.String
				}

				if err := stream.Send(fsError); err != nil {
					return err
				}
			}

			lastCheck = time.Now()
		}
	}
}

func (h *FilesystemHandler) GetDeviceStats(
	ctx context.Context,
	req *connect.Request[apiv1.GetDeviceStatsRequest],
) (*connect.Response[apiv1.GetDeviceStatsResponse], error) {
	h.logger.Debug("get device stats", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	// Get device stats from btrfs
	stats, err := h.btrfsManager.GetDeviceStats(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to get device stats", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto format
	var devices []*apiv1.DeviceStats
	for _, stat := range stats {
		device := &apiv1.DeviceStats{
			DevicePath:       stat.DevicePath,
			DeviceId:         stat.DeviceID,
			TotalBytes:       stat.TotalBytes,
			UsedBytes:        stat.UsedBytes,
			FreeBytes:        stat.FreeBytes,
			WriteErrors:      stat.WriteErrors,
			ReadErrors:       stat.ReadErrors,
			FlushErrors:      stat.FlushErrors,
			CorruptionErrors: stat.CorruptionErrors,
			GenerationErrors: stat.GenerationErrors,
		}
		devices = append(devices, device)
	}

	// Optionally collect and store errors in database
	if len(stats) > 0 {
		go func() {
			if err := h.btrfsManager.CollectDeviceErrors(h.db.Conn(), req.Msg.DevicePath); err != nil {
				h.logger.Warn("failed to collect device errors", "error", err)
			}
		}()
	}

	return connect.NewResponse(&apiv1.GetDeviceStatsResponse{
		Devices: devices,
	}), nil
}

// AddFilesystem adds a new filesystem to track
func (h *FilesystemHandler) AddFilesystem(
	ctx context.Context,
	req *connect.Request[apiv1.AddFilesystemRequest],
) (*connect.Response[apiv1.AddFilesystemResponse], error) {
	h.logger.Info("adding tracked filesystem", "path", req.Msg.Path, "label", req.Msg.Label, "btrbk_dir", req.Msg.BtrbkSnapshotDir)

	if req.Msg.Path == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("path is required"))
	}

	// Validate that the path is a btrfs filesystem and get its UUID
	fsInfo, err := btrfs.GetFilesystemInfo(req.Msg.Path)
	if err != nil {
		h.logger.Error("path is not a valid btrfs filesystem", "path", req.Msg.Path, "error", err)
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("path is not a valid btrfs filesystem: %w", err))
	}

	h.logger.Info("validated btrfs filesystem", "path", req.Msg.Path, "uuid", fsInfo.UUID, "num_devices", fsInfo.NumDevices)

	fs, err := h.db.AddFilesystem(fsInfo.UUID, req.Msg.Path, req.Msg.Label, req.Msg.BtrbkSnapshotDir)
	if err != nil {
		h.logger.Error("failed to add filesystem", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.AddFilesystemResponse{
		Filesystem: &apiv1.TrackedFilesystem{
			Id:               fs.ID,
			Uuid:             fs.UUID,
			Path:             fs.Path,
			Label:            fs.Label,
			BtrbkSnapshotDir: fs.BtrbkSnapshotDir,
			CreatedAt:        fs.CreatedAt,
			UpdatedAt:        fs.UpdatedAt,
		},
	}), nil
}

// RemoveFilesystem removes a tracked filesystem
func (h *FilesystemHandler) RemoveFilesystem(
	ctx context.Context,
	req *connect.Request[apiv1.RemoveFilesystemRequest],
) (*connect.Response[apiv1.RemoveFilesystemResponse], error) {
	h.logger.Info("removing tracked filesystem", "id", req.Msg.Id)

	if err := h.db.RemoveFilesystem(req.Msg.Id); err != nil {
		h.logger.Error("failed to remove filesystem", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.RemoveFilesystemResponse{
		Success: true,
	}), nil
}

// ListTrackedFilesystems lists all tracked filesystems
func (h *FilesystemHandler) ListTrackedFilesystems(
	ctx context.Context,
	req *connect.Request[apiv1.ListTrackedFilesystemsRequest],
) (*connect.Response[apiv1.ListTrackedFilesystemsResponse], error) {
	h.logger.Debug("listing tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var result []*apiv1.TrackedFilesystem
	for _, fs := range filesystems {
		result = append(result, &apiv1.TrackedFilesystem{
			Id:               fs.ID,
			Uuid:             fs.UUID,
			Path:             fs.Path,
			Label:            fs.Label,
			BtrbkSnapshotDir: fs.BtrbkSnapshotDir,
			CreatedAt:        fs.CreatedAt,
			UpdatedAt:        fs.UpdatedAt,
		})
	}

	return connect.NewResponse(&apiv1.ListTrackedFilesystemsResponse{
		Filesystems: result,
	}), nil
}

// UpdateFilesystem updates a tracked filesystem
func (h *FilesystemHandler) UpdateFilesystem(
	ctx context.Context,
	req *connect.Request[apiv1.UpdateFilesystemRequest],
) (*connect.Response[apiv1.UpdateFilesystemResponse], error) {
	h.logger.Info("updating tracked filesystem", "id", req.Msg.Id, "label", req.Msg.Label, "btrbk_dir", req.Msg.BtrbkSnapshotDir)

	if err := h.db.UpdateFilesystem(req.Msg.Id, req.Msg.Label, req.Msg.BtrbkSnapshotDir); err != nil {
		h.logger.Error("failed to update filesystem", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	fs, err := h.db.GetFilesystem(req.Msg.Id)
	if err != nil {
		h.logger.Error("failed to get filesystem", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.UpdateFilesystemResponse{
		Filesystem: &apiv1.TrackedFilesystem{
			Id:               fs.ID,
			Uuid:             fs.UUID,
			Path:             fs.Path,
			Label:            fs.Label,
			BtrbkSnapshotDir: fs.BtrbkSnapshotDir,
			CreatedAt:        fs.CreatedAt,
			UpdatedAt:        fs.UpdatedAt,
		},
	}), nil
}

// GetAllErrors gets errors for all tracked filesystems in parallel
func (h *FilesystemHandler) GetAllErrors(
	ctx context.Context,
	req *connect.Request[apiv1.GetAllErrorsRequest],
) (*connect.Response[apiv1.GetAllErrorsResponse], error) {
	h.logger.Debug("getting errors for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	limit := int(req.Msg.Limit)
	if limit <= 0 {
		limit = 100
	}

	since := time.Time{}
	if req.Msg.Since > 0 {
		since = time.Unix(req.Msg.Since, 0)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemErrors, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, fsPath string) {
			defer wg.Done()

			result := &apiv1.FilesystemErrors{
				Path: fsPath,
			}

			// Get errors from database
			dbErrors, err := queries.ListErrors(h.db.Conn(), fsPath, since, limit)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			for _, dbErr := range dbErrors {
				fsError := &apiv1.FilesystemError{
					Device:    dbErr.Device,
					ErrorType: dbErr.ErrorType,
					Timestamp: dbErr.Timestamp.Unix(),
				}
				if dbErr.Message.Valid {
					fsError.Message = dbErr.Message.String
				}
				if dbErr.Inode.Valid {
					fsError.Inode = dbErr.Inode.Int64
				}
				if dbErr.Path.Valid {
					fsError.Path = dbErr.Path.String
				}
				result.Errors = append(result.Errors, fsError)
			}

			results[idx] = result
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.GetAllErrorsResponse{
		Filesystems: results,
	}), nil
}

// GetAllDeviceStats gets device stats for all tracked filesystems in parallel
func (h *FilesystemHandler) GetAllDeviceStats(
	ctx context.Context,
	req *connect.Request[apiv1.GetAllDeviceStatsRequest],
) (*connect.Response[apiv1.GetAllDeviceStatsResponse], error) {
	h.logger.Debug("getting device stats for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemDeviceStats, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, fsPath string) {
			defer wg.Done()

			result := &apiv1.FilesystemDeviceStats{
				Path: fsPath,
			}

			stats, err := h.btrfsManager.GetDeviceStats(fsPath)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			for _, stat := range stats {
				device := &apiv1.DeviceStats{
					DevicePath:       stat.DevicePath,
					DeviceId:         stat.DeviceID,
					TotalBytes:       stat.TotalBytes,
					UsedBytes:        stat.UsedBytes,
					FreeBytes:        stat.FreeBytes,
					WriteErrors:      stat.WriteErrors,
					ReadErrors:       stat.ReadErrors,
					FlushErrors:      stat.FlushErrors,
					CorruptionErrors: stat.CorruptionErrors,
					GenerationErrors: stat.GenerationErrors,
				}
				result.Devices = append(result.Devices, device)
			}

			results[idx] = result
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.GetAllDeviceStatsResponse{
		Filesystems: results,
	}), nil
}

// GetFilesystemUsage gets filesystem usage stats for a single filesystem
func (h *FilesystemHandler) GetFilesystemUsage(
	ctx context.Context,
	req *connect.Request[apiv1.GetFilesystemUsageRequest],
) (*connect.Response[apiv1.GetFilesystemUsageResponse], error) {
	h.logger.Debug("get filesystem usage", "device", req.Msg.DevicePath)

	if req.Msg.DevicePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("device_path is required"))
	}

	usage, err := h.btrfsManager.GetFilesystemUsage(req.Msg.DevicePath)
	if err != nil {
		h.logger.Error("failed to get filesystem usage", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Convert to proto format
	protoUsage := &apiv1.FilesystemUsage{
		DeviceSize:        usage.DeviceSize,
		DeviceAllocated:   usage.DeviceAllocated,
		DeviceUnallocated: usage.DeviceUnallocated,
		DeviceSlack:       usage.DeviceSlack,
		Used:              usage.Used,
		FreeEstimated:     usage.FreeEstimated,
		FreeStatfs:        usage.FreeStatfs,
		DataRatio:         usage.DataRatio,
		MetadataRatio:     usage.MetadataRatio,
		GlobalReserve:     usage.GlobalReserve,
		GlobalReserveUsed: usage.GlobalReserveUsed,
	}

	for _, alloc := range usage.Allocations {
		protoAlloc := &apiv1.AllocationGroup{
			Type:    alloc.Type,
			Profile: alloc.Profile,
			Size:    alloc.Size,
			Used:    alloc.Used,
		}
		for _, dev := range alloc.Devices {
			protoAlloc.Devices = append(protoAlloc.Devices, &apiv1.DeviceAllocation{
				DevicePath: dev.DevicePath,
				Size:       dev.Size,
			})
		}
		protoUsage.Allocations = append(protoUsage.Allocations, protoAlloc)
	}

	return connect.NewResponse(&apiv1.GetFilesystemUsageResponse{
		Usage: protoUsage,
	}), nil
}

// GetAllFilesystemUsage gets filesystem usage stats for all tracked filesystems in parallel
func (h *FilesystemHandler) GetAllFilesystemUsage(
	ctx context.Context,
	req *connect.Request[apiv1.GetAllFilesystemUsageRequest],
) (*connect.Response[apiv1.GetAllFilesystemUsageResponse], error) {
	h.logger.Debug("getting filesystem usage for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemUsageInfo, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, fsPath string) {
			defer wg.Done()

			result := &apiv1.FilesystemUsageInfo{
				Path: fsPath,
			}

			usage, err := h.btrfsManager.GetFilesystemUsage(fsPath)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			result.Usage = &apiv1.FilesystemUsage{
				DeviceSize:        usage.DeviceSize,
				DeviceAllocated:   usage.DeviceAllocated,
				DeviceUnallocated: usage.DeviceUnallocated,
				DeviceSlack:       usage.DeviceSlack,
				Used:              usage.Used,
				FreeEstimated:     usage.FreeEstimated,
				FreeStatfs:        usage.FreeStatfs,
				DataRatio:         usage.DataRatio,
				MetadataRatio:     usage.MetadataRatio,
				GlobalReserve:     usage.GlobalReserve,
				GlobalReserveUsed: usage.GlobalReserveUsed,
			}

			for _, alloc := range usage.Allocations {
				protoAlloc := &apiv1.AllocationGroup{
					Type:    alloc.Type,
					Profile: alloc.Profile,
					Size:    alloc.Size,
					Used:    alloc.Used,
				}
				for _, dev := range alloc.Devices {
					protoAlloc.Devices = append(protoAlloc.Devices, &apiv1.DeviceAllocation{
						DevicePath: dev.DevicePath,
						Size:       dev.Size,
					})
				}
				result.Usage.Allocations = append(result.Usage.Allocations, protoAlloc)
			}

			results[idx] = result
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.GetAllFilesystemUsageResponse{
		Filesystems: results,
	}), nil
}

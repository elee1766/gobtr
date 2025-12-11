package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"connectrpc.com/connect"

	apiv1 "github.com/elee1766/gobtr/gen/api/v1"
	"github.com/elee1766/gobtr/pkg/btrfs"
	"github.com/elee1766/gobtr/pkg/db"
	"github.com/elee1766/gobtr/pkg/db/queries"
)

type SnapshotHandler struct {
	logger       *slog.Logger
	db           *db.DB
	btrfsManager *btrfs.Manager
}

func NewSnapshotHandler(logger *slog.Logger, db *db.DB, btrfsManager *btrfs.Manager) *SnapshotHandler {
	return &SnapshotHandler{
		logger:       logger.With("handler", "snapshot"),
		db:           db,
		btrfsManager: btrfsManager,
	}
}

func (h *SnapshotHandler) ListSnapshots(
	ctx context.Context,
	req *connect.Request[apiv1.ListSnapshotsRequest],
) (*connect.Response[apiv1.ListSnapshotsResponse], error) {
	h.logger.Debug("list snapshots", "subvolume_path", req.Msg.SubvolumePath)

	if req.Msg.SubvolumePath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("subvolume_path is required"))
	}

	// List subvolumes from btrfs
	subvolumes, err := h.btrfsManager.ListSubvolumes(req.Msg.SubvolumePath)
	if err != nil {
		h.logger.Error("failed to list subvolumes", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Get stored snapshots from database
	dbSnapshots, err := queries.ListSnapshots(h.db.Conn(), req.Msg.SubvolumePath)
	if err != nil {
		h.logger.Warn("failed to load snapshots from database", "error", err)
		// Continue with just btrfs data
	}

	// Create a map of stored snapshots for quick lookup
	dbMap := make(map[string]*queries.Snapshot)
	for _, snap := range dbSnapshots {
		dbMap[snap.Path] = snap
	}

	// Convert to proto format
	var snapshots []*apiv1.Snapshot
	for _, sub := range subvolumes {
		snapshot := &apiv1.Snapshot{
			Id:         fmt.Sprintf("%d", sub.ID),
			Path:       sub.Path,
			ParentUuid: sub.ParentUUID,
			IsReadonly: sub.IsReadonly,
		}

		// Check if we have database record
		if dbSnap, exists := dbMap[sub.Path]; exists {
			snapshot.CreatedAt = dbSnap.CreatedAt.Unix()
			if dbSnap.SizeBytes.Valid {
				snapshot.SizeBytes = dbSnap.SizeBytes.Int64
			}
		} else if !sub.CreatedAt.IsZero() {
			snapshot.CreatedAt = sub.CreatedAt.Unix()
		}

		snapshots = append(snapshots, snapshot)
	}

	return connect.NewResponse(&apiv1.ListSnapshotsResponse{
		Snapshots: snapshots,
	}), nil
}

func (h *SnapshotHandler) CreateSnapshot(
	ctx context.Context,
	req *connect.Request[apiv1.CreateSnapshotRequest],
) (*connect.Response[apiv1.CreateSnapshotResponse], error) {
	// Snapshot creation is disabled for safety - use btrbk or btrfs CLI directly
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("snapshot creation is disabled for safety; use btrbk or btrfs CLI directly"))
}

func (h *SnapshotHandler) DeleteSnapshot(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteSnapshotRequest],
) (*connect.Response[apiv1.DeleteSnapshotResponse], error) {
	// Snapshot deletion is disabled for safety - use btrbk or btrfs CLI directly
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("snapshot deletion is disabled for safety; use btrbk or btrfs CLI directly"))
}

// ListAllSnapshots lists snapshots for all tracked filesystems in parallel
func (h *SnapshotHandler) ListAllSnapshots(
	ctx context.Context,
	req *connect.Request[apiv1.ListAllSnapshotsRequest],
) (*connect.Response[apiv1.ListAllSnapshotsResponse], error) {
	h.logger.Debug("listing snapshots for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemSnapshots, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, fsPath string) {
			defer wg.Done()

			result := &apiv1.FilesystemSnapshots{
				Path: fsPath,
			}

			subvolumes, err := h.btrfsManager.ListSubvolumes(fsPath)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			for _, sub := range subvolumes {
				snapshot := &apiv1.Snapshot{
					Id:         fmt.Sprintf("%d", sub.ID),
					Path:       sub.Path,
					ParentUuid: sub.ParentUUID,
					IsReadonly: sub.IsReadonly,
				}

				if !sub.CreatedAt.IsZero() {
					snapshot.CreatedAt = sub.CreatedAt.Unix()
				}

				result.Snapshots = append(result.Snapshots, snapshot)
			}

			results[idx] = result
		}(i, fs.Path)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.ListAllSnapshotsResponse{
		Filesystems: results,
	}), nil
}

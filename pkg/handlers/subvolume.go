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
)

type SubvolumeHandler struct {
	logger       *slog.Logger
	db           *db.DB
	btrfsManager *btrfs.Manager
}

func NewSubvolumeHandler(logger *slog.Logger, db *db.DB, btrfsManager *btrfs.Manager) *SubvolumeHandler {
	return &SubvolumeHandler{
		logger:       logger.With("handler", "subvolume"),
		db:           db,
		btrfsManager: btrfsManager,
	}
}

func (h *SubvolumeHandler) ListSubvolumes(
	ctx context.Context,
	req *connect.Request[apiv1.ListSubvolumesRequest],
) (*connect.Response[apiv1.ListSubvolumesResponse], error) {
	h.logger.Debug("list subvolumes", "mount_path", req.Msg.MountPath)

	if req.Msg.MountPath == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("mount_path is required"))
	}

	subvolumes, err := h.btrfsManager.ListSubvolumes(req.Msg.MountPath)
	if err != nil {
		h.logger.Error("failed to list subvolumes", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var result []*apiv1.Subvolume
	for _, sub := range subvolumes {
		subvol := &apiv1.Subvolume{
			Id:         sub.ID,
			Gen:        sub.Gen,
			TopLevel:   sub.TopLevel,
			Path:       sub.Path,
			Uuid:       sub.UUID,
			ParentUuid: sub.ParentUUID,
			IsReadonly: sub.IsReadonly,
			Flags:      sub.Flags,
		}
		// Only set CreatedAt if it's a valid time (not zero)
		if !sub.CreatedAt.IsZero() {
			subvol.CreatedAt = sub.CreatedAt.Unix()
		}
		result = append(result, subvol)
	}

	return connect.NewResponse(&apiv1.ListSubvolumesResponse{
		Subvolumes: result,
	}), nil
}

// ListAllSubvolumes lists subvolumes for all tracked filesystems in parallel
func (h *SubvolumeHandler) ListAllSubvolumes(
	ctx context.Context,
	req *connect.Request[apiv1.ListAllSubvolumesRequest],
) (*connect.Response[apiv1.ListAllSubvolumesResponse], error) {
	h.logger.Debug("listing subvolumes for all tracked filesystems")

	filesystems, err := h.db.ListFilesystems()
	if err != nil {
		h.logger.Error("failed to list filesystems", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var wg sync.WaitGroup
	results := make([]*apiv1.FilesystemSubvolumes, len(filesystems))

	for i, fs := range filesystems {
		wg.Add(1)
		go func(idx int, trackedFS *db.TrackedFilesystem) {
			defer wg.Done()

			result := &apiv1.FilesystemSubvolumes{
				Path:             trackedFS.Path,
				BtrbkSnapshotDir: trackedFS.BtrbkSnapshotDir,
			}

			subvolumes, err := h.btrfsManager.ListSubvolumes(trackedFS.Path)
			if err != nil {
				result.ErrorMessage = err.Error()
				results[idx] = result
				return
			}

			for _, sub := range subvolumes {
				subvol := &apiv1.Subvolume{
					Id:         sub.ID,
					Gen:        sub.Gen,
					TopLevel:   sub.TopLevel,
					Path:       sub.Path,
					Uuid:       sub.UUID,
					ParentUuid: sub.ParentUUID,
					IsReadonly: sub.IsReadonly,
					Flags:      sub.Flags,
				}

				if !sub.CreatedAt.IsZero() {
					subvol.CreatedAt = sub.CreatedAt.Unix()
				}

				result.Subvolumes = append(result.Subvolumes, subvol)
			}

			results[idx] = result
		}(i, fs)
	}

	wg.Wait()

	return connect.NewResponse(&apiv1.ListAllSubvolumesResponse{
		Filesystems: results,
	}), nil
}

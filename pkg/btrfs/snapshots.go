package btrfs

import (
	"fmt"
	"time"
)

type SubvolumeInfo struct {
	ID         int64
	Gen        int64
	TopLevel   int64
	Path       string
	UUID       string
	ParentUUID string
	IsReadonly bool
	CreatedAt  time.Time
	Flags      uint64
}

// ListSubvolumes lists all subvolumes for a filesystem using ioctl
func (m *Manager) ListSubvolumes(mountPoint string) ([]*SubvolumeInfo, error) {
	ioctlData, err := ListSubvolumesIoctl(mountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to list subvolumes via ioctl: %w", err)
	}

	var subvolumes []*SubvolumeInfo
	for _, data := range ioctlData {
		subvol := &SubvolumeInfo{
			ID:         int64(data.ID),
			Gen:        int64(data.Generation),
			TopLevel:   int64(data.ParentID),
			Path:       data.Path,
			UUID:       data.UUIDString(),
			ParentUUID: data.ParentUUIDString(),
			IsReadonly: data.IsReadonly(),
			CreatedAt:  data.OTime,
			Flags:      data.Flags,
		}

		// Set path to "/" for top-level subvolume if not set
		if data.ID == 5 && subvol.Path == "" {
			subvol.Path = "/"
		}

		subvolumes = append(subvolumes, subvol)
	}

	return subvolumes, nil
}

// GetSubvolumeInfo gets detailed info about a specific subvolume by path using ioctl
// Returns nil if the subvolume is not found
func (m *Manager) GetSubvolumeInfo(path string) (*SubvolumeInfo, error) {
	subvolumes, err := m.ListSubvolumes(path)
	if err != nil {
		return nil, err
	}

	// Find the subvolume that matches the root (ID 5) or is at the given path
	for _, sv := range subvolumes {
		if sv.ID == 5 {
			return sv, nil
		}
	}

	// If no ID 5, return the first one (shouldn't happen with valid btrfs path)
	if len(subvolumes) > 0 {
		return subvolumes[0], nil
	}

	return nil, fmt.Errorf("no subvolume found at path: %s", path)
}

// GetSubvolumeByID gets a subvolume by its ID from a filesystem mount point
func (m *Manager) GetSubvolumeByID(mountPoint string, id uint64) (*SubvolumeInfo, error) {
	subvolumes, err := m.ListSubvolumes(mountPoint)
	if err != nil {
		return nil, err
	}

	for _, sv := range subvolumes {
		if uint64(sv.ID) == id {
			return sv, nil
		}
	}

	return nil, fmt.Errorf("subvolume ID %d not found", id)
}

// GetSubvolumeByUUID gets a subvolume by its UUID from a filesystem mount point
func (m *Manager) GetSubvolumeByUUID(mountPoint, uuid string) (*SubvolumeInfo, error) {
	subvolumes, err := m.ListSubvolumes(mountPoint)
	if err != nil {
		return nil, err
	}

	for _, sv := range subvolumes {
		if sv.UUID == uuid {
			return sv, nil
		}
	}

	return nil, fmt.Errorf("subvolume UUID %s not found", uuid)
}

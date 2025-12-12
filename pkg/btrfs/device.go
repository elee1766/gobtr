package btrfs

import (
	"fmt"
	"strconv"
)

type DeviceStats struct {
	DevicePath       string
	DeviceID         string
	TotalBytes       int64
	UsedBytes        int64
	FreeBytes        int64
	WriteErrors      int64
	ReadErrors       int64
	FlushErrors      int64
	CorruptionErrors int64
	GenerationErrors int64
}

// DeviceAllocation represents per-device allocation within an allocation group
type DeviceAllocation struct {
	DevicePath string
	Size       int64 // Bytes allocated on this device for this group
}

// AllocationGroup represents Data, Metadata, or System allocation info
type AllocationGroup struct {
	Type    string             // "Data", "Metadata", "System", "GlobalReserve"
	Profile string             // "single", "DUP", "RAID1", "RAID1C3", etc.
	Size    int64              // Total allocated for this type
	Used    int64              // Actually used
	Devices []DeviceAllocation // Per-device breakdown
}

// FilesystemUsage represents overall filesystem usage stats
type FilesystemUsage struct {
	DeviceSize        int64  // Total raw device size
	DeviceAllocated   int64  // Space allocated to chunks
	DeviceUnallocated int64  // Space not yet allocated
	DeviceSlack       int64  // Slack space
	Used              int64  // Actual data used
	FreeEstimated     int64  // Free space estimate (accounting for RAID ratio)
	FreeStatfs        int64  // Free space from statfs/df
	DataRatio         string // e.g., "2.00" for RAID1
	MetadataRatio     string // e.g., "3.00" for RAID1C3
	GlobalReserve     int64  // Global reserve size
	GlobalReserveUsed int64  // Global reserve used

	Allocations []AllocationGroup // Data/Metadata/System allocations
}

// GetDeviceStats gets device statistics for a filesystem using ioctl and sysfs
func (m *Manager) GetDeviceStats(path string) ([]*DeviceStats, error) {
	// Get filesystem info and device info in one call
	fsInfo, deviceInfos, err := GetFilesystemAndDeviceInfo(path)
	if err != nil {
		return nil, fmt.Errorf("get filesystem/device info: %w", err)
	}

	// Get error stats via sysfs
	errorStats, err := GetDeviceErrorStats(fsInfo.UUID)
	if err != nil {
		m.logger.Warn("failed to get device error stats from sysfs", "error", err)
	}

	var devices []*DeviceStats
	var totalBytes, usedBytes int64

	for _, devInfo := range deviceInfos {
		dev := &DeviceStats{
			DevicePath: devInfo.Path,
			DeviceID:   strconv.FormatUint(devInfo.DevID, 10),
			TotalBytes: int64(devInfo.TotalBytes),
			UsedBytes:  int64(devInfo.BytesUsed),
			FreeBytes:  int64(devInfo.TotalBytes - devInfo.BytesUsed),
		}

		// Merge error stats if available
		if errorStats != nil {
			if errStat, ok := errorStats[devInfo.DevID]; ok {
				dev.WriteErrors = errStat.WriteErrors
				dev.ReadErrors = errStat.ReadErrors
				dev.FlushErrors = errStat.FlushErrors
				dev.CorruptionErrors = errStat.CorruptionErrors
				dev.GenerationErrors = errStat.GenerationErrors
			}
		}

		devices = append(devices, dev)
		totalBytes += dev.TotalBytes
		usedBytes += dev.UsedBytes
	}

	// Add a "total" entry if we have multiple devices
	if len(devices) > 1 {
		devices = append([]*DeviceStats{{
			DevicePath: "total",
			DeviceID:   "0",
			TotalBytes: totalBytes,
			UsedBytes:  usedBytes,
			FreeBytes:  totalBytes - usedBytes,
		}}, devices...)
	}

	return devices, nil
}

// GetFilesystemUsage gets overall filesystem usage stats using ioctl and sysfs
func (m *Manager) GetFilesystemUsage(path string) (*FilesystemUsage, error) {
	usage := &FilesystemUsage{}

	// Get filesystem and device info in one call
	fsInfo, devices, err := GetFilesystemAndDeviceInfo(path)
	if err != nil {
		return nil, fmt.Errorf("get filesystem/device info: %w", err)
	}

	// Build a map of device ID -> path for later use
	devPathByID := make(map[uint64]string)
	for _, dev := range devices {
		devPathByID[dev.DevID] = dev.Path
	}

	// Calculate totals from devices
	for _, dev := range devices {
		usage.DeviceSize += int64(dev.TotalBytes)
		usage.DeviceAllocated += int64(dev.BytesUsed)
	}
	usage.DeviceUnallocated = usage.DeviceSize - usage.DeviceAllocated

	// Get per-device chunk allocations
	chunkAllocs, err := GetDeviceChunkAllocations(path)
	if err != nil {
		m.logger.Warn("failed to get device chunk allocations", "error", err)
	}

	// Aggregate per-device allocations by type+profile and device
	// Key: "Type:Profile" -> map[devID]size
	perDeviceByGroup := make(map[string]map[uint64]int64)
	for _, alloc := range chunkAllocs {
		key := alloc.Type + ":" + alloc.Profile
		if perDeviceByGroup[key] == nil {
			perDeviceByGroup[key] = make(map[uint64]int64)
		}
		perDeviceByGroup[key][alloc.DevID] += int64(alloc.Length)
	}

	// Get space allocation info via ioctl
	spaceInfos, err := GetSpaceInfo(path)
	if err != nil {
		m.logger.Warn("failed to get space info via ioctl", "error", err)
	} else {
		for _, space := range spaceInfos {
			ag := AllocationGroup{
				Type:    space.Type,
				Profile: space.Profile,
				Size:    int64(space.TotalBytes),
				Used:    int64(space.UsedBytes),
			}

			// Add per-device breakdown if available
			key := space.Type + ":" + space.Profile
			if devSizes, ok := perDeviceByGroup[key]; ok {
				for devID, size := range devSizes {
					ag.Devices = append(ag.Devices, DeviceAllocation{
						DevicePath: devPathByID[devID],
						Size:       size,
					})
				}
			}

			usage.Allocations = append(usage.Allocations, ag)
			usage.Used += int64(space.UsedBytes)
		}
	}

	// Get allocation info from sysfs for more details
	allocInfos, globalReserve, err := GetAllocationInfoSysfs(fsInfo.UUID)
	if err != nil {
		m.logger.Warn("failed to get allocation info from sysfs", "error", err)
	} else {
		if globalReserve != nil {
			usage.GlobalReserve = globalReserve.Size
			usage.GlobalReserveUsed = globalReserve.Reserved

			// Add GlobalReserve as an allocation entry for frontend display
			usage.Allocations = append(usage.Allocations, AllocationGroup{
				Type: "GlobalReserve",
				Size: globalReserve.Size,
				Used: globalReserve.Reserved,
			})
		}

		// Calculate ratios from sysfs data
		for _, info := range allocInfos {
			if info.DiskTotal > 0 && info.TotalBytes > 0 {
				ratio := float64(info.DiskTotal) / float64(info.TotalBytes)
				ratioStr := fmt.Sprintf("%.2f", ratio)
				switch info.Type {
				case "data":
					usage.DataRatio = ratioStr
				case "metadata":
					usage.MetadataRatio = ratioStr
				}
			}
		}
	}

	// Calculate free space estimate (accounting for RAID overhead)
	dataRatio := 1.0
	if usage.DataRatio != "" {
		if r, err := strconv.ParseFloat(usage.DataRatio, 64); err == nil && r > 0 {
			dataRatio = r
		}
	}
	usage.FreeEstimated = int64(float64(usage.DeviceUnallocated) / dataRatio)

	// FreeStatfs would require a statfs call, skip for now
	usage.FreeStatfs = usage.FreeEstimated

	return usage, nil
}

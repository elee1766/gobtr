package btrfs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const btrfsSysfsPath = "/sys/fs/btrfs"

// DeviceErrorStats contains error counters for a device
type DeviceErrorStats struct {
	DevID            uint64
	WriteErrors      int64
	ReadErrors       int64
	FlushErrors      int64
	CorruptionErrors int64
	GenerationErrors int64
}

// GetDeviceErrorStats reads device error stats from sysfs for a given filesystem UUID
func GetDeviceErrorStats(fsUUID string) (map[uint64]*DeviceErrorStats, error) {
	devinfoPath := filepath.Join(btrfsSysfsPath, fsUUID, "devinfo")

	entries, err := os.ReadDir(devinfoPath)
	if err != nil {
		return nil, fmt.Errorf("read devinfo directory: %w", err)
	}

	stats := make(map[uint64]*DeviceErrorStats)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		devID, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil {
			continue // Not a device ID directory
		}

		errorStatsPath := filepath.Join(devinfoPath, entry.Name(), "error_stats")
		devStats, err := parseErrorStatsFile(errorStatsPath)
		if err != nil {
			continue // Skip devices we can't read
		}

		devStats.DevID = devID
		stats[devID] = devStats
	}

	return stats, nil
}

func parseErrorStatsFile(path string) (*DeviceErrorStats, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stats := &DeviceErrorStats{}
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}

		val, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		switch parts[0] {
		case "write_errs":
			stats.WriteErrors = val
		case "read_errs":
			stats.ReadErrors = val
		case "flush_errs":
			stats.FlushErrors = val
		case "corruption_errs":
			stats.CorruptionErrors = val
		case "generation_errs":
			stats.GenerationErrors = val
		}
	}

	return stats, scanner.Err()
}

// AllocationInfo represents allocation data from sysfs
type AllocationInfo struct {
	Type          string // "data", "metadata", "system"
	Profile       string // from the subdirectory name
	TotalBytes    int64
	UsedBytes     int64
	DiskTotal     int64
	DiskUsed      int64
	BytesReserved int64
	BytesPinned   int64
	BytesReadonly int64
	BytesMayUse   int64
}

// GlobalReserveInfo contains global reserve data
type GlobalReserveInfo struct {
	Size     int64
	Reserved int64
}

// GetAllocationInfoSysfs reads allocation info from sysfs
func GetAllocationInfoSysfs(fsUUID string) ([]*AllocationInfo, *GlobalReserveInfo, error) {
	allocPath := filepath.Join(btrfsSysfsPath, fsUUID, "allocation")

	// Read global reserve
	globalReserve := &GlobalReserveInfo{}
	if val, err := readSysfsInt64(filepath.Join(allocPath, "global_rsv_size")); err == nil {
		globalReserve.Size = val
	}
	if val, err := readSysfsInt64(filepath.Join(allocPath, "global_rsv_reserved")); err == nil {
		globalReserve.Reserved = val
	}

	var allocations []*AllocationInfo

	for _, allocType := range []string{"data", "metadata", "system"} {
		typePath := filepath.Join(allocPath, allocType)
		if _, err := os.Stat(typePath); os.IsNotExist(err) {
			continue
		}

		info := &AllocationInfo{Type: allocType}

		// Read basic allocation values
		if val, err := readSysfsInt64(filepath.Join(typePath, "total_bytes")); err == nil {
			info.TotalBytes = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "bytes_used")); err == nil {
			info.UsedBytes = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "disk_total")); err == nil {
			info.DiskTotal = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "disk_used")); err == nil {
			info.DiskUsed = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "bytes_reserved")); err == nil {
			info.BytesReserved = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "bytes_pinned")); err == nil {
			info.BytesPinned = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "bytes_readonly")); err == nil {
			info.BytesReadonly = val
		}
		if val, err := readSysfsInt64(filepath.Join(typePath, "bytes_may_use")); err == nil {
			info.BytesMayUse = val
		}

		// Try to determine profile from subdirectories (raid1, single, etc.)
		entries, err := os.ReadDir(typePath)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					info.Profile = entry.Name()
					break
				}
			}
		}

		allocations = append(allocations, info)
	}

	return allocations, globalReserve, nil
}

func readSysfsInt64(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

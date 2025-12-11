package btrfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
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

// JSON structures for btrfs commands
type btrfsDeviceStatsJSON struct {
	DeviceStats []struct {
		Device         string `json:"device"`
		DevID          int    `json:"devid"`
		WriteIOErrs    int64  `json:"write_io_errs"`
		ReadIOErrs     int64  `json:"read_io_errs"`
		FlushIOErrs    int64  `json:"flush_io_errs"`
		CorruptionErrs int64  `json:"corruption_errs"`
		GenerationErrs int64  `json:"generation_errs"`
	} `json:"device-stats"`
}

type btrfsFilesystemDfJSON struct {
	FilesystemDf []struct {
		BgType    string `json:"bg-type"`
		BgProfile string `json:"bg-profile"`
		Total     int64  `json:"total"`
		Used      int64  `json:"used"`
	} `json:"filesystem-df"`
}

// GetDeviceStats gets device statistics for a filesystem
func (m *Manager) GetDeviceStats(path string) ([]*DeviceStats, error) {
	// Get device sizes from filesystem usage (still need text parsing for per-device sizes)
	devices, err := m.getDeviceSizes(path)
	if err != nil {
		return nil, err
	}

	// Get device error stats using JSON
	errorStats, err := m.getDeviceErrorStatsJSON(path)
	if err != nil {
		m.logger.Warn("failed to get device error stats", "error", err)
	} else {
		// Merge error stats into devices
		for _, dev := range devices {
			if stats, ok := errorStats[dev.DevicePath]; ok {
				dev.WriteErrors = stats.WriteErrors
				dev.ReadErrors = stats.ReadErrors
				dev.FlushErrors = stats.FlushErrors
				dev.CorruptionErrors = stats.CorruptionErrors
				dev.GenerationErrors = stats.GenerationErrors
			}
		}
	}

	return devices, nil
}

// getDeviceSizes gets device size info from btrfs filesystem usage -b
func (m *Manager) getDeviceSizes(path string) ([]*DeviceStats, error) {
	cmd := exec.Command("btrfs", "filesystem", "usage", "-b", path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		m.logger.Error("failed to get filesystem usage", "error", err, "output", out.String())
		return nil, fmt.Errorf("btrfs filesystem usage failed: %w", err)
	}

	return m.parseDeviceSizes(out.String()), nil
}

func (m *Manager) parseDeviceSizes(output string) []*DeviceStats {
	lines := strings.Split(output, "\n")

	// Parse overall stats from the "Overall:" section - always available
	sizeRe := regexp.MustCompile(`^\s*Device size:\s+(\d+)`)
	usedRe := regexp.MustCompile(`^\s*Used:\s+(\d+)`)
	freeRe := regexp.MustCompile(`^\s*Free \(estimated\):\s+(\d+)`)

	overall := &DeviceStats{
		DevicePath: "total",
		DeviceID:   "0",
	}

	for _, line := range lines {
		if matches := sizeRe.FindStringSubmatch(line); len(matches) == 2 {
			size, _ := strconv.ParseInt(matches[1], 10, 64)
			overall.TotalBytes = size
		} else if matches := usedRe.FindStringSubmatch(line); len(matches) == 2 {
			used, _ := strconv.ParseInt(matches[1], 10, 64)
			overall.UsedBytes = used
		} else if matches := freeRe.FindStringSubmatch(line); len(matches) == 2 {
			free, _ := strconv.ParseInt(matches[1], 10, 64)
			overall.FreeBytes = free
		}
	}

	if overall.TotalBytes > 0 {
		return []*DeviceStats{overall}
	}
	return nil
}

// getDeviceErrorStatsJSON gets device error stats using JSON output
func (m *Manager) getDeviceErrorStatsJSON(path string) (map[string]*DeviceStats, error) {
	cmd := exec.Command("btrfs", "--format", "json", "device", "stats", path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("btrfs device stats failed: %w", err)
	}

	var result btrfsDeviceStatsJSON
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("failed to parse device stats JSON: %w", err)
	}

	stats := make(map[string]*DeviceStats)
	for _, d := range result.DeviceStats {
		stats[d.Device] = &DeviceStats{
			DevicePath:       d.Device,
			DeviceID:         strconv.Itoa(d.DevID),
			WriteErrors:      d.WriteIOErrs,
			ReadErrors:       d.ReadIOErrs,
			FlushErrors:      d.FlushIOErrs,
			CorruptionErrors: d.CorruptionErrs,
			GenerationErrors: d.GenerationErrs,
		}
	}

	return stats, nil
}

// GetFilesystemUsage gets overall filesystem usage stats
func (m *Manager) GetFilesystemUsage(path string) (*FilesystemUsage, error) {
	usage := &FilesystemUsage{}

	// Get allocation info from JSON
	if err := m.getFilesystemDfJSON(path, usage); err != nil {
		m.logger.Warn("failed to get filesystem df", "error", err)
	}

	// Get overall stats from text output
	if err := m.getFilesystemUsageText(path, usage); err != nil {
		return nil, err
	}

	return usage, nil
}

// getFilesystemDfJSON gets allocation info using JSON output
func (m *Manager) getFilesystemDfJSON(path string, usage *FilesystemUsage) error {
	cmd := exec.Command("btrfs", "--format", "json", "filesystem", "df", path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("btrfs filesystem df failed: %w", err)
	}

	var result btrfsFilesystemDfJSON
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		return fmt.Errorf("failed to parse filesystem df JSON: %w", err)
	}

	for _, item := range result.FilesystemDf {
		if item.BgType == "GlobalReserve" {
			usage.GlobalReserve = item.Total
			usage.GlobalReserveUsed = item.Used
		}
		usage.Allocations = append(usage.Allocations, AllocationGroup{
			Type:    item.BgType,
			Profile: item.BgProfile,
			Size:    item.Total,
			Used:    item.Used,
		})
	}

	return nil
}

// getFilesystemUsageText gets overall stats from text output
func (m *Manager) getFilesystemUsageText(path string, usage *FilesystemUsage) error {
	cmd := exec.Command("btrfs", "filesystem", "usage", "-b", path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("btrfs filesystem usage failed: %w", err)
	}

	lines := strings.Split(out.String(), "\n")

	patterns := map[string]*int64{
		`Device size:\s+(\d+)`:        &usage.DeviceSize,
		`Device allocated:\s+(\d+)`:   &usage.DeviceAllocated,
		`Device unallocated:\s+(\d+)`: &usage.DeviceUnallocated,
		`Device slack:\s+(\d+)`:       &usage.DeviceSlack,
		`Used:\s+(\d+)`:               &usage.Used,
	}

	// Free has a more complex format: "Free (estimated):    1234    (min: 5678)"
	freeRe := regexp.MustCompile(`Free \(estimated\):\s+(\d+)`)
	freeStatfsRe := regexp.MustCompile(`Free \(statfs, df\):\s+(\d+)`)
	dataRatioRe := regexp.MustCompile(`Data ratio:\s+([\d.]+)`)
	metaRatioRe := regexp.MustCompile(`Metadata ratio:\s+([\d.]+)`)

	// Allocation section header: "Data,RAID1: Size:123, Used:456 (12.34%)"
	allocHeaderRe := regexp.MustCompile(`^(Data|Metadata|System),(\w+):\s+Size:(\d+),\s+Used:(\d+)`)
	// Unallocated section header (no profile/size/used)
	unallocHeaderRe := regexp.MustCompile(`^Unallocated:$`)
	// Device line within allocation: "   /dev/sda1    123456"
	deviceAllocRe := regexp.MustCompile(`^\s+(/\S+)\s+(\d+)`)

	// Map to store per-device allocations by type
	deviceAllocations := make(map[string][]DeviceAllocation)
	var currentAllocType string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Check for overall stats
		for pattern, target := range patterns {
			re := regexp.MustCompile(pattern)
			if matches := re.FindStringSubmatch(trimmed); len(matches) == 2 {
				val, _ := strconv.ParseInt(matches[1], 10, 64)
				*target = val
			}
		}

		if matches := freeRe.FindStringSubmatch(trimmed); len(matches) == 2 {
			val, _ := strconv.ParseInt(matches[1], 10, 64)
			usage.FreeEstimated = val
		}
		if matches := freeStatfsRe.FindStringSubmatch(trimmed); len(matches) == 2 {
			val, _ := strconv.ParseInt(matches[1], 10, 64)
			usage.FreeStatfs = val
		}
		if matches := dataRatioRe.FindStringSubmatch(trimmed); len(matches) == 2 {
			usage.DataRatio = matches[1]
		}
		if matches := metaRatioRe.FindStringSubmatch(trimmed); len(matches) == 2 {
			usage.MetadataRatio = matches[1]
		}

		// Check for allocation header
		if matches := allocHeaderRe.FindStringSubmatch(trimmed); len(matches) == 5 {
			currentAllocType = matches[1] // "Data", "Metadata", or "System"
			continue
		}

		// Check for Unallocated header
		if unallocHeaderRe.MatchString(trimmed) {
			currentAllocType = "Unallocated"
			continue
		}

		// Check for device allocation line (uses original line to preserve leading whitespace check)
		if currentAllocType != "" {
			if matches := deviceAllocRe.FindStringSubmatch(line); len(matches) == 3 {
				size, _ := strconv.ParseInt(matches[2], 10, 64)
				deviceAllocations[currentAllocType] = append(deviceAllocations[currentAllocType], DeviceAllocation{
					DevicePath: matches[1],
					Size:       size,
				})
			} else if trimmed != "" && !strings.HasPrefix(trimmed, "/") {
				// Non-empty line that's not a device path - end of this allocation section
				currentAllocType = ""
			}
		}
	}

	// Merge device allocations into the allocation groups
	for i := range usage.Allocations {
		if devices, ok := deviceAllocations[usage.Allocations[i].Type]; ok {
			usage.Allocations[i].Devices = devices
		}
	}

	// Add Unallocated as an allocation group if we have device data
	if unallocDevices, ok := deviceAllocations["Unallocated"]; ok && len(unallocDevices) > 0 {
		var totalUnalloc int64
		for _, d := range unallocDevices {
			totalUnalloc += d.Size
		}
		usage.Allocations = append(usage.Allocations, AllocationGroup{
			Type:    "Unallocated",
			Profile: "-",
			Size:    totalUnalloc,
			Used:    0,
			Devices: unallocDevices,
		})
	}

	return nil
}

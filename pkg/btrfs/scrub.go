package btrfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// ScrubStatusDir is the directory where btrfs stores scrub status files
const ScrubStatusDir = "/var/lib/btrfs"

type ScrubStatus struct {
	UUID                 string // Scrub UUID from btrfs
	IsRunning            bool
	BytesScrubbed        int64
	TotalBytes           int64
	DataBytesScrubbed    int64
	TreeBytesScrubbed    int64
	DataExtentsScrubbed  int64
	TreeExtentsScrubbed  int64
	ReadErrors           int32
	CsumErrors           int32
	VerifyErrors         int32
	NoCsum               int64
	CsumDiscards         int64
	SuperErrors          int32
	MallocErrors         int32
	UncorrectableErrors  int32
	UnverifiedErrors     int32
	CorrectedErrors      int32
	LastPhysical         int64
	Duration             string
	DurationSeconds      int64
	DataErrors           int32 // Legacy: sum of read + csum + verify
	TreeErrors           int32 // Legacy
	Status               string
	StartedAt            time.Time
	FinishedAt           time.Time
	RateBytesPerSec      int64  // Scrub rate in bytes/second
	EtaSeconds           int64  // Estimated time remaining in seconds
}

// ScrubOptions contains options for starting a scrub
type ScrubOptions struct {
	Readonly         bool
	LimitBytesPerSec int64 // 0 = unlimited
	Force            bool
}

// StartScrub starts a scrub operation on a device
func (m *Manager) StartScrub(ctx context.Context, devicePath string, readonly bool) (string, error) {
	return m.StartScrubWithOptions(ctx, devicePath, ScrubOptions{Readonly: readonly})
}

// StartScrubWithOptions starts a scrub operation on a device with options
// The scrub runs in the background (managed by btrfs kernel)
func (m *Manager) StartScrubWithOptions(ctx context.Context, devicePath string, opts ScrubOptions) (string, error) {
	// Check if already running
	status, err := m.GetScrubStatus(devicePath)
	if err == nil && status.IsRunning && !opts.Force {
		return status.UUID, fmt.Errorf("scrub already running on %s", devicePath)
	}

	scrubID := uuid.New().String()

	args := []string{"scrub", "start"}
	if opts.Readonly {
		args = append(args, "-r")
	}
	if opts.Force {
		args = append(args, "-f")
	}
	if opts.LimitBytesPerSec > 0 {
		args = append(args, "--limit", fmt.Sprintf("%d", opts.LimitBytesPerSec))
	}
	// Use -B flag to run in foreground mode, but we'll detach the process
	// so it survives if the Go process dies
	args = append(args, "-B", devicePath)

	cmd := exec.Command("btrfs", args...)

	// Detach the process: create new session so it's independent of Go process
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true, // Create new session (fully detached from parent)
	}

	// Redirect stdin/stdout/stderr to /dev/null so process is fully detached
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		m.logger.Error("scrub start failed", "device", devicePath, "error", err)
		return "", fmt.Errorf("failed to start scrub: %w", err)
	}

	// Don't wait - let it run independently
	// The process is fully detached and will continue even if Go process dies
	go cmd.Wait() // Goroutine to reap zombie when scrub eventually finishes

	m.logger.Info("scrub started", "device", devicePath, "scrub_id", scrubID, "opts", opts)
	return scrubID, nil
}

// CancelScrub cancels a running scrub
func (m *Manager) CancelScrub(devicePath string) error {
	cmd := exec.Command("btrfs", "scrub", "cancel", devicePath)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		m.logger.Warn("btrfs scrub cancel command failed", "error", err, "output", out.String())
		return fmt.Errorf("failed to cancel scrub: %w", err)
	}

	m.logger.Info("scrub canceled", "device", devicePath)
	return nil
}

// GetScrubStatus gets the current scrub status for a filesystem mount path.
// It first gets the filesystem UUID and tries to read from the status file,
// then checks if a scrub is currently running using process detection.
func (m *Manager) GetScrubStatus(devicePath string) (*ScrubStatus, error) {
	// Get filesystem UUID first
	fsInfo, err := GetFilesystemInfo(devicePath)
	if err != nil {
		return nil, fmt.Errorf("get filesystem info: %w", err)
	}

	// Read status from file
	status, err := m.GetScrubStatusByUUID(fsInfo.UUID)
	if err != nil {
		return nil, err
	}

	// Check if scrub is currently running by looking for scrub process
	// The status file alone can't tell us if a scrub is actively running
	if status.Status == "unknown" || status.Status == "" {
		// Try to detect if scrub is running by checking /proc for btrfs scrub processes
		isRunning := m.isScrubProcessRunning(devicePath)
		if isRunning {
			status.IsRunning = true
			status.Status = "running"
		}
	}

	// Get total bytes for the filesystem (for progress calculation)
	if status.TotalBytes == 0 {
		usage, err := m.GetFilesystemUsage(devicePath)
		if err == nil {
			status.TotalBytes = usage.DeviceSize
		}
	}

	// Calculate rate and ETA if running
	if status.IsRunning && status.DurationSeconds > 0 && status.BytesScrubbed > 0 {
		status.RateBytesPerSec = status.BytesScrubbed / status.DurationSeconds
		if status.RateBytesPerSec > 0 && status.TotalBytes > status.BytesScrubbed {
			status.EtaSeconds = (status.TotalBytes - status.BytesScrubbed) / status.RateBytesPerSec
		}
	}

	return status, nil
}

// isScrubProcessRunning checks if a btrfs scrub process is running for the given path
func (m *Manager) isScrubProcessRunning(devicePath string) bool {
	// Read /proc to find btrfs scrub processes
	// This is a heuristic - we look for processes with "btrfs" and "scrub" in cmdline
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Check if directory name is a PID (all digits)
		pid := entry.Name()
		isNum := true
		for _, c := range pid {
			if c < '0' || c > '9' {
				isNum = false
				break
			}
		}
		if !isNum {
			continue
		}

		// Read cmdline
		cmdline, err := os.ReadFile(filepath.Join("/proc", pid, "cmdline"))
		if err != nil {
			continue
		}

		cmdStr := string(cmdline)
		if strings.Contains(cmdStr, "btrfs") && strings.Contains(cmdStr, "scrub") {
			// Check if this scrub is for our device
			if strings.Contains(cmdStr, devicePath) {
				return true
			}
		}
	}

	return false
}

// IsScrubRunning checks if a scrub is currently running
func (m *Manager) IsScrubRunning(devicePath string) bool {
	status, err := m.GetScrubStatus(devicePath)
	if err != nil {
		return false
	}
	return status.IsRunning
}

// GetScrubStatusByUUID reads scrub status directly from the status file using the filesystem UUID.
// This is faster than running btrfs commands and provides the same information.
// The status file is located at /var/lib/btrfs/scrub.status.<UUID>
func (m *Manager) GetScrubStatusByUUID(fsUUID string) (*ScrubStatus, error) {
	statusPath := filepath.Join(ScrubStatusDir, "scrub.status."+fsUUID)

	data, err := os.ReadFile(statusPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No scrub has ever been run on this filesystem
			return &ScrubStatus{Status: "never_run"}, nil
		}
		return nil, fmt.Errorf("failed to read scrub status file: %w", err)
	}

	return m.parseScrubStatusFile(string(data))
}

// parseScrubStatusFile parses the btrfs scrub status file format.
// Format: "scrub status:1\nUUID:devid|key1:val1|key2:val2|...\n..." (one line per device)
func (m *Manager) parseScrubStatusFile(content string) (*ScrubStatus, error) {
	status := &ScrubStatus{}

	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) < 2 {
		return nil, fmt.Errorf("invalid scrub status file format")
	}

	// First line is "scrub status:1" - we can ignore it
	// Remaining lines contain data for each device in pipe-separated key:value pairs
	// Format: "UUID:devid|key1:val1|key2:val2|..."
	// We need to aggregate stats across all devices

	var allFinished, anyAborted bool
	allFinished = true

	for _, dataLine := range lines[1:] {
		if dataLine == "" {
			continue
		}

		// Split on pipes to get key:value pairs
		pairs := strings.Split(dataLine, "|")

		// Helper to get int64 value from pairs
		values := make(map[string]string)
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				values[kv[0]] = kv[1]
			}
		}

		parseInt64 := func(key string) int64 {
			if v, ok := values[key]; ok {
				val, _ := strconv.ParseInt(v, 10, 64)
				return val
			}
			return 0
		}

		parseInt32 := func(key string) int32 {
			return int32(parseInt64(key))
		}

		// Aggregate values across devices
		status.DataExtentsScrubbed += parseInt64("data_extents_scrubbed")
		status.TreeExtentsScrubbed += parseInt64("tree_extents_scrubbed")
		status.DataBytesScrubbed += parseInt64("data_bytes_scrubbed")
		status.TreeBytesScrubbed += parseInt64("tree_bytes_scrubbed")

		status.ReadErrors += parseInt32("read_errors")
		status.CsumErrors += parseInt32("csum_errors")
		status.VerifyErrors += parseInt32("verify_errors")
		status.NoCsum += parseInt64("no_csum")
		status.CsumDiscards += parseInt64("csum_discards")
		status.SuperErrors += parseInt32("super_errors")
		status.MallocErrors += parseInt32("malloc_errors")
		status.UncorrectableErrors += parseInt32("uncorrectable_errors")
		status.CorrectedErrors += parseInt32("corrected_errors")

		// Use max last_physical across devices
		lastPhys := parseInt64("last_physical")
		if lastPhys > status.LastPhysical {
			status.LastPhysical = lastPhys
		}

		// Use the same start time (should be same for all devices)
		tStart := parseInt64("t_start")
		if tStart > 0 && status.StartedAt.IsZero() {
			status.StartedAt = time.Unix(tStart, 0)
		}

		// Use max duration (slowest device determines total time)
		duration := parseInt64("duration")
		if duration > status.DurationSeconds {
			status.DurationSeconds = duration
		}

		// Track finished/canceled status across devices
		canceled := parseInt64("canceled")
		finished := parseInt64("finished")

		if canceled == 1 {
			anyAborted = true
		}
		if finished != 1 {
			allFinished = false
		}
	}

	// Calculate totals
	status.BytesScrubbed = status.DataBytesScrubbed + status.TreeBytesScrubbed
	status.DataErrors = status.ReadErrors + status.CsumErrors + status.VerifyErrors

	// Format duration string
	if status.DurationSeconds > 0 {
		hours := status.DurationSeconds / 3600
		mins := (status.DurationSeconds % 3600) / 60
		secs := status.DurationSeconds % 60
		status.Duration = fmt.Sprintf("%d:%02d:%02d", hours, mins, secs)
	}

	// Determine overall status
	if anyAborted && !allFinished {
		status.Status = "aborted"
		status.IsRunning = false
	} else if allFinished {
		status.Status = "finished"
		status.IsRunning = false
		if !status.StartedAt.IsZero() && status.DurationSeconds > 0 {
			status.FinishedAt = status.StartedAt.Add(time.Duration(status.DurationSeconds) * time.Second)
		}
	} else {
		// Some devices not finished - could be running or interrupted
		status.Status = "unknown"
		status.IsRunning = false // Conservative default, will be updated by process check
	}

	return status, nil
}

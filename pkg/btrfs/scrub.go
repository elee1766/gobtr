package btrfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

// GetScrubStatus gets the current scrub status
func (m *Manager) GetScrubStatus(devicePath string) (*ScrubStatus, error) {
	// Get raw stats with -R flag
	cmd := exec.Command("btrfs", "scrub", "status", "-R", devicePath)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		m.logger.Error("failed to get scrub status", "error", err, "output", out.String())
		return nil, fmt.Errorf("btrfs scrub status failed: %w", err)
	}

	status, err := m.parseScrubStatus(out.String(), devicePath)
	if err != nil {
		return nil, err
	}

	// Get human-readable output for TotalBytes (only shown without -R)
	cmd2 := exec.Command("btrfs", "scrub", "status", devicePath)
	var out2 bytes.Buffer
	cmd2.Stdout = &out2
	cmd2.Stderr = &out2

	if err := cmd2.Run(); err == nil {
		m.parseHumanScrubStatus(out2.String(), status)
	}

	return status, nil
}

func (m *Manager) parseScrubStatus(output, devicePath string) (*ScrubStatus, error) {
	status := &ScrubStatus{}

	if strings.Contains(output, "no stats available") || strings.Contains(output, "never run") {
		status.Status = "never_run"
		return status, nil
	}

	// Parse UUID
	uuidRe := regexp.MustCompile(`UUID:\s+([0-9a-f-]+)`)
	if matches := uuidRe.FindStringSubmatch(output); len(matches) == 2 {
		status.UUID = matches[1]
	}

	// Check status
	statusRe := regexp.MustCompile(`Status:\s+(\w+)`)
	if matches := statusRe.FindStringSubmatch(output); len(matches) == 2 {
		status.Status = strings.ToLower(matches[1])
		status.IsRunning = status.Status == "running"
	} else {
		// Fallback to text-based detection
		if strings.Contains(output, "running") {
			status.IsRunning = true
			status.Status = "running"
		} else if strings.Contains(output, "finished") {
			status.Status = "finished"
		} else if strings.Contains(output, "aborted") {
			status.Status = "aborted"
		} else {
			status.Status = "unknown"
		}
	}

	// Helper to parse int64 values
	parseInt64 := func(pattern string) int64 {
		re := regexp.MustCompile(pattern + `:\s+(\d+)`)
		if matches := re.FindStringSubmatch(output); len(matches) == 2 {
			val, _ := strconv.ParseInt(matches[1], 10, 64)
			return val
		}
		return 0
	}

	// Helper to parse int32 values
	parseInt32 := func(pattern string) int32 {
		return int32(parseInt64(pattern))
	}

	// Parse all statistics
	status.DataBytesScrubbed = parseInt64("data_bytes_scrubbed")
	status.TreeBytesScrubbed = parseInt64("tree_bytes_scrubbed")
	status.DataExtentsScrubbed = parseInt64("data_extents_scrubbed")
	status.TreeExtentsScrubbed = parseInt64("tree_extents_scrubbed")
	status.ReadErrors = parseInt32("read_errors")
	status.CsumErrors = parseInt32("csum_errors")
	status.VerifyErrors = parseInt32("verify_errors")
	status.NoCsum = parseInt64("no_csum")
	status.CsumDiscards = parseInt64("csum_discards")
	status.SuperErrors = parseInt32("super_errors")
	status.MallocErrors = parseInt32("malloc_errors")
	status.UncorrectableErrors = parseInt32("uncorrectable_errors")
	status.UnverifiedErrors = parseInt32("unverified_errors")
	status.CorrectedErrors = parseInt32("corrected_errors")
	status.LastPhysical = parseInt64("last_physical")

	// Calculate total bytes scrubbed
	status.BytesScrubbed = status.DataBytesScrubbed + status.TreeBytesScrubbed

	// Calculate data errors (for backwards compatibility)
	status.DataErrors = status.ReadErrors + status.CsumErrors + status.VerifyErrors

	// Parse duration (format: "0:02:03" for H:MM:SS)
	durationRe := regexp.MustCompile(`Duration:\s+(\d+):(\d+):(\d+)`)
	if matches := durationRe.FindStringSubmatch(output); len(matches) == 4 {
		hours, _ := strconv.ParseInt(matches[1], 10, 64)
		mins, _ := strconv.ParseInt(matches[2], 10, 64)
		secs, _ := strconv.ParseInt(matches[3], 10, 64)
		status.DurationSeconds = hours*3600 + mins*60 + secs
		status.Duration = fmt.Sprintf("%d:%02d:%02d", hours, mins, secs)
	}

	// Parse start timestamp (format: "Mon Aug 18 21:02:02 2025")
	startRe := regexp.MustCompile(`Scrub started:\s+(.+)`)
	if matches := startRe.FindStringSubmatch(output); len(matches) == 2 {
		timeStr := strings.TrimSpace(matches[1])
		// Try multiple formats
		formats := []string{
			"Mon Jan 2 15:04:05 2006",
			"Mon Jan 02 15:04:05 2006",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, timeStr); err == nil {
				status.StartedAt = t
				break
			}
		}
	}

	// Calculate finished time from start + duration
	if !status.StartedAt.IsZero() && status.DurationSeconds > 0 && status.Status != "running" {
		status.FinishedAt = status.StartedAt.Add(time.Duration(status.DurationSeconds) * time.Second)
	}

	return status, nil
}

// parseHumanScrubStatus extracts info from human-readable output (without -R)
func (m *Manager) parseHumanScrubStatus(output string, status *ScrubStatus) {
	// Helper to parse human-readable byte values like "2.59TiB" or "320.43MiB/s"
	parseHumanBytes := func(value string, unit string) int64 {
		val, _ := strconv.ParseFloat(value, 64)
		multiplier := float64(1)
		switch unit {
		case "KiB", "KB":
			multiplier = 1024
		case "MiB", "MB":
			multiplier = 1024 * 1024
		case "GiB", "GB":
			multiplier = 1024 * 1024 * 1024
		case "TiB", "TB":
			multiplier = 1024 * 1024 * 1024 * 1024
		case "PiB", "PB":
			multiplier = 1024 * 1024 * 1024 * 1024 * 1024
		}
		return int64(val * multiplier)
	}

	// Parse "Total to scrub: 2.59TiB"
	totalRe := regexp.MustCompile(`Total to scrub:\s+([\d.]+)([KMGTP]i?B)`)
	if matches := totalRe.FindStringSubmatch(output); len(matches) == 3 {
		status.TotalBytes = parseHumanBytes(matches[1], matches[2])
	}

	// Parse "Rate: 320.43MiB/s"
	rateRe := regexp.MustCompile(`Rate:\s+([\d.]+)([KMGTP]i?B)/s`)
	if matches := rateRe.FindStringSubmatch(output); len(matches) == 3 {
		status.RateBytesPerSec = parseHumanBytes(matches[1], matches[2])
	}

	// Parse "ETA: 0:12:34" (H:MM:SS)
	etaRe := regexp.MustCompile(`ETA:\s+(\d+):(\d+):(\d+)`)
	if matches := etaRe.FindStringSubmatch(output); len(matches) == 4 {
		hours, _ := strconv.ParseInt(matches[1], 10, 64)
		mins, _ := strconv.ParseInt(matches[2], 10, 64)
		secs, _ := strconv.ParseInt(matches[3], 10, 64)
		status.EtaSeconds = hours*3600 + mins*60 + secs
	}
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
// Format: "scrub status:1\nUUID:devid|key1:val1|key2:val2|..."
func (m *Manager) parseScrubStatusFile(content string) (*ScrubStatus, error) {
	status := &ScrubStatus{}

	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) < 2 {
		return nil, fmt.Errorf("invalid scrub status file format")
	}

	// First line is "scrub status:1" - we can ignore it
	// Second line contains all the data in pipe-separated key:value pairs
	// Format: "UUID:devid|key1:val1|key2:val2|..."
	dataLine := lines[1]

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

	// Parse all the values
	status.DataExtentsScrubbed = parseInt64("data_extents_scrubbed")
	status.TreeExtentsScrubbed = parseInt64("tree_extents_scrubbed")
	status.DataBytesScrubbed = parseInt64("data_bytes_scrubbed")
	status.TreeBytesScrubbed = parseInt64("tree_bytes_scrubbed")
	status.BytesScrubbed = status.DataBytesScrubbed + status.TreeBytesScrubbed

	status.ReadErrors = parseInt32("read_errors")
	status.CsumErrors = parseInt32("csum_errors")
	status.VerifyErrors = parseInt32("verify_errors")
	status.NoCsum = parseInt64("no_csum")
	status.CsumDiscards = parseInt64("csum_discards")
	status.SuperErrors = parseInt32("super_errors")
	status.MallocErrors = parseInt32("malloc_errors")
	status.UncorrectableErrors = parseInt32("uncorrectable_errors")
	status.CorrectedErrors = parseInt32("corrected_errors")
	status.LastPhysical = parseInt64("last_physical")

	// Calculate legacy error sum
	status.DataErrors = status.ReadErrors + status.CsumErrors + status.VerifyErrors

	// Parse timestamps
	tStart := parseInt64("t_start")
	if tStart > 0 {
		status.StartedAt = time.Unix(tStart, 0)
	}

	// Duration is in seconds
	status.DurationSeconds = parseInt64("duration")
	if status.DurationSeconds > 0 {
		hours := status.DurationSeconds / 3600
		mins := (status.DurationSeconds % 3600) / 60
		secs := status.DurationSeconds % 60
		status.Duration = fmt.Sprintf("%d:%02d:%02d", hours, mins, secs)
	}

	// Determine status from canceled and finished flags
	canceled := parseInt64("canceled")
	finished := parseInt64("finished")

	if canceled == 1 && finished == 0 {
		status.Status = "aborted"
		status.IsRunning = false
	} else if finished == 1 {
		status.Status = "finished"
		status.IsRunning = false
		if !status.StartedAt.IsZero() && status.DurationSeconds > 0 {
			status.FinishedAt = status.StartedAt.Add(time.Duration(status.DurationSeconds) * time.Second)
		}
	} else if canceled == 0 && finished == 0 {
		// Neither canceled nor finished - could be running or interrupted
		// The file alone can't tell us if it's currently running
		// We'd need to check for a running scrub process
		status.Status = "unknown"
		status.IsRunning = false // Conservative default
	} else {
		status.Status = "finished"
		status.IsRunning = false
	}

	return status, nil
}

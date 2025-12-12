package fragmap

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/dennwc/btrfs"
)

// Scanner reads fragmentation data from a btrfs filesystem
type Scanner struct {
	fsPath string
	file   *os.File
}

// NewScanner creates a new fragmap scanner for the given filesystem path
func NewScanner(fsPath string) (*Scanner, error) {
	f, err := os.OpenFile(fsPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open filesystem: %w", err)
	}

	return &Scanner{
		fsPath: fsPath,
		file:   f,
	}, nil
}

// Close closes the scanner
func (s *Scanner) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Scan performs a full scan of the filesystem and returns the fragmentation map
func (s *Scanner) Scan() (*FragMap, error) {
	totalStart := time.Now()
	fm := &FragMap{
		DeviceExtents: make(map[uint64][]DeviceExtent),
	}

	// Get devices
	start := time.Now()
	devices, err := s.scanDevices()
	if err != nil {
		return nil, fmt.Errorf("scan devices: %w", err)
	}
	fm.Devices = devices
	slog.Debug("fragmap scan timing", "phase", "scanDevices", "duration", time.Since(start), "count", len(devices))

	// Calculate total size
	for _, dev := range devices {
		fm.TotalSize += dev.TotalSize
	}

	// Get chunks
	start = time.Now()
	chunks, err := s.scanChunks()
	if err != nil {
		return nil, fmt.Errorf("scan chunks: %w", err)
	}
	fm.Chunks = chunks
	slog.Debug("fragmap scan timing", "phase", "scanChunks", "duration", time.Since(start), "count", len(chunks))

	// Get device extents
	start = time.Now()
	for _, dev := range devices {
		devStart := time.Now()
		extents, err := s.scanDeviceExtents(dev.ID)
		if err != nil {
			return nil, fmt.Errorf("scan device %d extents: %w", dev.ID, err)
		}
		fm.DeviceExtents[dev.ID] = extents
		slog.Debug("fragmap scan timing", "phase", "scanDeviceExtents", "deviceID", dev.ID, "duration", time.Since(devStart), "count", len(extents))
	}
	slog.Debug("fragmap scan timing", "phase", "allDeviceExtents", "duration", time.Since(start))

	slog.Debug("fragmap scan timing", "phase", "total", "duration", time.Since(totalStart))
	return fm, nil
}

// scanDevices scans for all devices in the filesystem
func (s *Scanner) scanDevices() ([]Device, error) {
	// Search the chunk tree for device items
	// Device items are stored with objectid = device id, type = DEV_ITEM_KEY
	start := time.Now()
	results, err := TreeSearch(s.file, ChunkTreeObjectID, 1, ^uint64(0), DevItemKey, DevItemKey, 0, ^uint64(0))
	if err != nil {
		return nil, err
	}
	slog.Debug("fragmap scan timing", "phase", "scanDevices.TreeSearch", "duration", time.Since(start))

	// Open btrfs handle to get device info including path
	start = time.Now()
	fs, err := btrfs.Open(s.fsPath, true)
	if err != nil {
		return nil, fmt.Errorf("open btrfs: %w", err)
	}
	defer fs.Close()
	slog.Debug("fragmap scan timing", "phase", "scanDevices.btrfs.Open", "duration", time.Since(start))

	var devices []Device
	start = time.Now()
	for _, r := range results {
		if r.Header.Type != DevItemKey {
			continue
		}

		dev, err := ParseDevItem(r.Data)
		if err != nil {
			continue
		}

		// Get device path from btrfs
		devInfoStart := time.Now()
		devInfo, err := fs.GetDevInfo(dev.ID)
		if err == nil {
			dev.Path = devInfo.Path
		}
		slog.Debug("fragmap scan timing", "phase", "scanDevices.GetDevInfo", "deviceID", dev.ID, "duration", time.Since(devInfoStart))

		devices = append(devices, *dev)
	}
	slog.Debug("fragmap scan timing", "phase", "scanDevices.parseAndGetPaths", "duration", time.Since(start))

	return devices, nil
}

// scanChunks scans for all chunks in the filesystem
func (s *Scanner) scanChunks() ([]Chunk, error) {
	// Search the chunk tree for chunk items
	// Use FirstChunkTreeObjectID as min, but max should be unlimited to get all chunks
	start := time.Now()
	results, err := TreeSearch(s.file, ChunkTreeObjectID, FirstChunkTreeObjectID, ^uint64(0), ChunkItemKey, ChunkItemKey, 0, ^uint64(0))
	if err != nil {
		return nil, err
	}
	slog.Debug("fragmap scan timing", "phase", "scanChunks.TreeSearch", "duration", time.Since(start), "results", len(results))

	var chunks []Chunk
	for _, r := range results {
		if r.Header.Type != ChunkItemKey {
			continue
		}

		chunk, err := ParseChunk(r.Data)
		if err != nil {
			continue
		}

		// The logical offset is stored in the search result header offset
		chunk.LogicalOffset = r.Header.Offset
		chunks = append(chunks, *chunk)
	}

	// Sort by logical offset
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].LogicalOffset < chunks[j].LogicalOffset
	})

	// Fetch block group usage and merge into chunks
	start = time.Now()
	blockGroups, err := s.scanBlockGroups()
	if err != nil {
		// Non-fatal - we can still return chunks without usage data
		return chunks, nil
	}
	slog.Debug("fragmap scan timing", "phase", "scanChunks.scanBlockGroups", "duration", time.Since(start), "count", len(blockGroups))

	// Create a map for quick lookup
	bgMap := make(map[uint64]*BlockGroupItem)
	for i := range blockGroups {
		bgMap[blockGroups[i].LogicalOffset] = &blockGroups[i]
	}

	// Merge usage data into chunks
	for i := range chunks {
		if bg, ok := bgMap[chunks[i].LogicalOffset]; ok {
			chunks[i].Used = bg.Used
		}
	}

	return chunks, nil
}

// scanBlockGroups scans for all block groups to get usage information
// It does a single tree search for all BLOCK_GROUP_ITEM entries in the extent tree
func (s *Scanner) scanBlockGroups() ([]BlockGroupItem, error) {
	// Single query to get ALL block group items from the extent tree
	// Block groups use objectid = logical offset, type = BLOCK_GROUP_ITEM_KEY
	// We search for all objectids (0 to max) with type = BlockGroupItemKey
	results, err := TreeSearch(s.file, ExtentTreeObjectID,
		0, ^uint64(0),
		BlockGroupItemKey, BlockGroupItemKey,
		0, ^uint64(0))
	if err != nil {
		return nil, err
	}

	var blockGroups []BlockGroupItem
	for _, r := range results {
		if r.Header.Type != BlockGroupItemKey {
			continue
		}

		bg, err := ParseBlockGroupItem(r.Data)
		if err != nil {
			continue
		}

		bg.LogicalOffset = r.Header.ObjectID
		bg.Length = r.Header.Offset
		blockGroups = append(blockGroups, *bg)
	}

	return blockGroups, nil
}

// scanDeviceExtents scans for all extents on a specific device
func (s *Scanner) scanDeviceExtents(deviceID uint64) ([]DeviceExtent, error) {
	// Search the device tree for device extent items
	results, err := TreeSearch(s.file, DevTreeObjectID, deviceID, deviceID, DevExtentKey, DevExtentKey, 0, ^uint64(0))
	if err != nil {
		return nil, err
	}

	var extents []DeviceExtent
	for _, r := range results {
		if r.Header.Type != DevExtentKey {
			continue
		}

		ext, err := ParseDevExtent(r.Data)
		if err != nil {
			continue
		}

		ext.DeviceID = r.Header.ObjectID
		ext.PhysicalOffset = r.Header.Offset
		extents = append(extents, *ext)
	}

	// Sort by physical offset
	sort.Slice(extents, func(i, j int) bool {
		return extents[i].PhysicalOffset < extents[j].PhysicalOffset
	})

	return extents, nil
}

// BuildDeviceBlockMap builds a block map for a specific device
// This shows the physical layout including free space gaps
func (fm *FragMap) BuildDeviceBlockMap(deviceID uint64) (*DeviceBlockMap, error) {
	// Find the device
	var device *Device
	for i := range fm.Devices {
		if fm.Devices[i].ID == deviceID {
			device = &fm.Devices[i]
			break
		}
	}
	if device == nil {
		return nil, fmt.Errorf("device %d not found", deviceID)
	}

	extents := fm.DeviceExtents[deviceID]
	if extents == nil {
		return nil, fmt.Errorf("no extents for device %d", deviceID)
	}

	// Build a map from chunk offset to chunk info
	chunkMap := make(map[uint64]*Chunk)
	for i := range fm.Chunks {
		chunkMap[fm.Chunks[i].LogicalOffset] = &fm.Chunks[i]
	}

	blockMap := &DeviceBlockMap{
		DeviceID:  deviceID,
		TotalSize: device.TotalSize,
		Entries:   make([]BlockMapEntry, 0),
	}

	// Build entries, including gaps (free space)
	var lastEnd uint64 = 0
	for _, ext := range extents {
		// Add free space gap if there is one
		if ext.PhysicalOffset > lastEnd {
			blockMap.Entries = append(blockMap.Entries, BlockMapEntry{
				Offset:    lastEnd,
				Length:    ext.PhysicalOffset - lastEnd,
				Allocated: false,
			})
		}

		// Add the extent
		entry := BlockMapEntry{
			Offset:      ext.PhysicalOffset,
			Length:      ext.Length,
			Allocated:   true,
			ChunkOffset: ext.ChunkOffset,
		}

		// Look up chunk info to get type, profile, and usage
		if chunk, ok := chunkMap[ext.ChunkOffset]; ok {
			entry.Type = chunk.Type
			entry.Profile = chunk.Profile
			entry.ChunkUsed = chunk.Used
			entry.ChunkLength = chunk.Length
		}

		blockMap.Entries = append(blockMap.Entries, entry)
		lastEnd = ext.PhysicalOffset + ext.Length
	}

	// Add trailing free space
	if lastEnd < device.TotalSize {
		blockMap.Entries = append(blockMap.Entries, BlockMapEntry{
			Offset:    lastEnd,
			Length:    device.TotalSize - lastEnd,
			Allocated: false,
		})
	}

	return blockMap, nil
}

// FragmentationStats calculates fragmentation statistics for a device
type FragmentationStats struct {
	TotalSize      uint64
	AllocatedSize  uint64
	FreeSize       uint64
	DataSize       uint64
	MetadataSize   uint64
	SystemSize     uint64
	NumExtents     int
	NumFreeRegions int
	LargestFree    uint64
	SmallestFree   uint64
	AvgExtentSize  uint64
	AvgFreeSize    uint64
}

// CalculateStats calculates fragmentation statistics for a device block map
func (bm *DeviceBlockMap) CalculateStats() FragmentationStats {
	stats := FragmentationStats{
		TotalSize:    bm.TotalSize,
		SmallestFree: ^uint64(0),
	}

	for _, entry := range bm.Entries {
		if entry.Allocated {
			stats.AllocatedSize += entry.Length
			stats.NumExtents++

			switch {
			case entry.Type&BlockTypeData != 0:
				stats.DataSize += entry.Length
			case entry.Type&BlockTypeMetadata != 0:
				stats.MetadataSize += entry.Length
			case entry.Type&BlockTypeSystem != 0:
				stats.SystemSize += entry.Length
			}
		} else {
			stats.FreeSize += entry.Length
			stats.NumFreeRegions++

			if entry.Length > stats.LargestFree {
				stats.LargestFree = entry.Length
			}
			if entry.Length < stats.SmallestFree {
				stats.SmallestFree = entry.Length
			}
		}
	}

	if stats.NumExtents > 0 {
		stats.AvgExtentSize = stats.AllocatedSize / uint64(stats.NumExtents)
	}
	if stats.NumFreeRegions > 0 {
		stats.AvgFreeSize = stats.FreeSize / uint64(stats.NumFreeRegions)
	}
	if stats.SmallestFree == ^uint64(0) {
		stats.SmallestFree = 0
	}

	return stats
}

// HeatMapData generates data suitable for a 2D heat map visualization
// Resolution determines how many blocks to divide the device into
func (bm *DeviceBlockMap) HeatMapData(resolution int) []HeatMapCell {
	if resolution <= 0 {
		resolution = 256
	}

	blockSize := bm.TotalSize / uint64(resolution)
	if blockSize == 0 {
		blockSize = 1
	}

	cells := make([]HeatMapCell, resolution)
	for i := range cells {
		cells[i].Index = i
		cells[i].StartOffset = uint64(i) * blockSize
		cells[i].EndOffset = uint64(i+1) * blockSize
	}

	// Fill in cell data based on entries
	for _, entry := range bm.Entries {
		startCell := int(entry.Offset / blockSize)
		endCell := int((entry.Offset + entry.Length) / blockSize)

		if startCell >= resolution {
			startCell = resolution - 1
		}
		if endCell >= resolution {
			endCell = resolution - 1
		}

		for c := startCell; c <= endCell; c++ {
			// Calculate how much of this entry overlaps with this cell
			cellStart := uint64(c) * blockSize
			cellEnd := uint64(c+1) * blockSize

			overlapStart := entry.Offset
			if overlapStart < cellStart {
				overlapStart = cellStart
			}
			overlapEnd := entry.Offset + entry.Length
			if overlapEnd > cellEnd {
				overlapEnd = cellEnd
			}

			if overlapEnd > overlapStart {
				overlapLen := overlapEnd - overlapStart
				if entry.Allocated {
					cells[c].AllocatedBytes += overlapLen
					cells[c].ExtentCount++
					if entry.Type&BlockTypeData != 0 {
						cells[c].DataBytes += overlapLen
					} else if entry.Type&BlockTypeMetadata != 0 {
						cells[c].MetadataBytes += overlapLen
					} else if entry.Type&BlockTypeSystem != 0 {
						cells[c].SystemBytes += overlapLen
					}
				} else {
					cells[c].FreeBytes += overlapLen
				}
			}
		}
	}

	// Calculate utilization percentage for each cell
	for i := range cells {
		cellSize := cells[i].EndOffset - cells[i].StartOffset
		if cellSize > 0 {
			cells[i].Utilization = float64(cells[i].AllocatedBytes) / float64(cellSize)
		}
	}

	return cells
}

// HeatMapCell represents a single cell in the heat map
type HeatMapCell struct {
	Index          int
	StartOffset    uint64
	EndOffset      uint64
	AllocatedBytes uint64
	FreeBytes      uint64
	DataBytes      uint64
	MetadataBytes  uint64
	SystemBytes    uint64
	ExtentCount    int
	Utilization    float64 // 0.0 to 1.0
}

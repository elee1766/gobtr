package handlers

import (
	"context"
	"log/slog"
	"sync"

	"connectrpc.com/connect"
	apiv1 "github.com/elee1766/gobtr/gen/api/v1"
	"github.com/elee1766/gobtr/pkg/fragmap"
)

type FragMapHandler struct {
	logger *slog.Logger
}

func NewFragMapHandler(logger *slog.Logger) *FragMapHandler {
	return &FragMapHandler{
		logger: logger.With("handler", "fragmap"),
	}
}

func (h *FragMapHandler) GetFragMap(ctx context.Context, req *connect.Request[apiv1.GetFragMapRequest]) (*connect.Response[apiv1.GetFragMapResponse], error) {
	scanner, err := fragmap.NewScanner(req.Msg.FsPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer scanner.Close()

	fm, err := scanner.Scan()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetFragMapResponse{
		TotalSize: fm.TotalSize,
		Devices:   make([]*apiv1.Device, len(fm.Devices)),
		Chunks:    make([]*apiv1.Chunk, len(fm.Chunks)),
	}

	// Convert devices
	for i, dev := range fm.Devices {
		resp.Devices[i] = &apiv1.Device{
			Id:        dev.ID,
			Uuid:      dev.UUID[:],
			TotalSize: dev.TotalSize,
			Path:      dev.Path,
		}
	}

	// Convert chunks
	for i, chunk := range fm.Chunks {
		stripes := make([]*apiv1.Stripe, len(chunk.Stripes))
		for j, s := range chunk.Stripes {
			stripes[j] = &apiv1.Stripe{
				DeviceId: s.DeviceID,
				Offset:   s.Offset,
			}
		}
		resp.Chunks[i] = &apiv1.Chunk{
			LogicalOffset: chunk.LogicalOffset,
			Length:        chunk.Length,
			Type:          uint64(chunk.Type),
			Profile:       uint64(chunk.Profile),
			Stripes:       stripes,
			Used:          chunk.Used,
		}
	}

	// Convert device extents (flattened)
	for devID, extents := range fm.DeviceExtents {
		for _, ext := range extents {
			resp.DeviceExtents = append(resp.DeviceExtents, &apiv1.DeviceExtent{
				DeviceId:       devID,
				PhysicalOffset: ext.PhysicalOffset,
				Length:         ext.Length,
				ChunkOffset:    ext.ChunkOffset,
			})
		}
	}

	return connect.NewResponse(resp), nil
}

func (h *FragMapHandler) GetDeviceBlockMap(ctx context.Context, req *connect.Request[apiv1.GetDeviceBlockMapRequest]) (*connect.Response[apiv1.GetDeviceBlockMapResponse], error) {
	scanner, err := fragmap.NewScanner(req.Msg.FsPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer scanner.Close()

	fm, err := scanner.Scan()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	blockMap, err := fm.BuildDeviceBlockMap(req.Msg.DeviceId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	resp := &apiv1.GetDeviceBlockMapResponse{
		DeviceId:  blockMap.DeviceID,
		TotalSize: blockMap.TotalSize,
		Entries:   make([]*apiv1.BlockMapEntry, len(blockMap.Entries)),
	}

	for i, entry := range blockMap.Entries {
		resp.Entries[i] = &apiv1.BlockMapEntry{
			Offset:      entry.Offset,
			Length:      entry.Length,
			Type:        uint64(entry.Type),
			Profile:     uint64(entry.Profile),
			Allocated:   entry.Allocated,
			ChunkOffset: entry.ChunkOffset,
			ChunkUsed:   entry.ChunkUsed,
			ChunkLength: entry.ChunkLength,
		}
	}

	return connect.NewResponse(resp), nil
}

func (h *FragMapHandler) GetDeviceBlockMaps(ctx context.Context, req *connect.Request[apiv1.GetDeviceBlockMapsRequest]) (*connect.Response[apiv1.GetDeviceBlockMapsResponse], error) {
	scanner, err := fragmap.NewScanner(req.Msg.FsPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer scanner.Close()

	fm, err := scanner.Scan()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Build device lookup map
	deviceMap := make(map[uint64]fragmap.Device)
	for _, dev := range fm.Devices {
		deviceMap[dev.ID] = dev
	}

	// Determine which devices to fetch
	deviceIDs := req.Msg.DeviceIds
	if len(deviceIDs) == 0 {
		// If no device IDs specified, fetch all
		for _, dev := range fm.Devices {
			deviceIDs = append(deviceIDs, dev.ID)
		}
	}

	// Process devices in parallel
	type result struct {
		index int
		data  *apiv1.DeviceBlockMap
	}
	results := make(chan result, len(deviceIDs))
	var wg sync.WaitGroup

	for idx, devID := range deviceIDs {
		dev, ok := deviceMap[devID]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(idx int, devID uint64, dev fragmap.Device) {
			defer wg.Done()

			blockMap, err := fm.BuildDeviceBlockMap(devID)
			if err != nil {
				return
			}

			stats := blockMap.CalculateStats()

			entries := make([]*apiv1.BlockMapEntry, len(blockMap.Entries))
			for i, entry := range blockMap.Entries {
				entries[i] = &apiv1.BlockMapEntry{
					Offset:      entry.Offset,
					Length:      entry.Length,
					Type:        uint64(entry.Type),
					Profile:     uint64(entry.Profile),
					Allocated:   entry.Allocated,
					ChunkOffset: entry.ChunkOffset,
					ChunkUsed:   entry.ChunkUsed,
					ChunkLength: entry.ChunkLength,
				}
			}

			results <- result{
				index: idx,
				data: &apiv1.DeviceBlockMap{
					Device: &apiv1.Device{
						Id:        dev.ID,
						Uuid:      dev.UUID[:],
						TotalSize: dev.TotalSize,
						Path:      dev.Path,
					},
					TotalSize: blockMap.TotalSize,
					Entries:   entries,
					Stats: &apiv1.FragStats{
						DeviceId:       devID,
						TotalSize:      stats.TotalSize,
						AllocatedSize:  stats.AllocatedSize,
						FreeSize:       stats.FreeSize,
						DataSize:       stats.DataSize,
						MetadataSize:   stats.MetadataSize,
						SystemSize:     stats.SystemSize,
						NumExtents:     int32(stats.NumExtents),
						NumFreeRegions: int32(stats.NumFreeRegions),
						LargestFree:    stats.LargestFree,
						SmallestFree:   stats.SmallestFree,
						AvgExtentSize:  stats.AvgExtentSize,
						AvgFreeSize:    stats.AvgFreeSize,
					},
				},
			}
		}(idx, devID, dev)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results and maintain order
	collected := make([]*apiv1.DeviceBlockMap, len(deviceIDs))
	for r := range results {
		collected[r.index] = r.data
	}

	// Build final response, filtering out nil entries
	resp := &apiv1.GetDeviceBlockMapsResponse{
		Maps: make([]*apiv1.DeviceBlockMap, 0, len(deviceIDs)),
	}
	for _, m := range collected {
		if m != nil {
			resp.Maps = append(resp.Maps, m)
		}
	}

	return connect.NewResponse(resp), nil
}

func (h *FragMapHandler) GetHeatMap(ctx context.Context, req *connect.Request[apiv1.GetHeatMapRequest]) (*connect.Response[apiv1.GetHeatMapResponse], error) {
	scanner, err := fragmap.NewScanner(req.Msg.FsPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer scanner.Close()

	fm, err := scanner.Scan()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	blockMap, err := fm.BuildDeviceBlockMap(req.Msg.DeviceId)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	resolution := int(req.Msg.Resolution)
	if resolution <= 0 {
		resolution = 256
	}

	cells := blockMap.HeatMapData(resolution)

	resp := &apiv1.GetHeatMapResponse{
		DeviceId:   blockMap.DeviceID,
		TotalSize:  blockMap.TotalSize,
		Resolution: int32(resolution),
		Cells:      make([]*apiv1.HeatMapCell, len(cells)),
	}

	for i, cell := range cells {
		resp.Cells[i] = &apiv1.HeatMapCell{
			Index:          int32(cell.Index),
			StartOffset:    cell.StartOffset,
			EndOffset:      cell.EndOffset,
			AllocatedBytes: cell.AllocatedBytes,
			FreeBytes:      cell.FreeBytes,
			DataBytes:      cell.DataBytes,
			MetadataBytes:  cell.MetadataBytes,
			SystemBytes:    cell.SystemBytes,
			ExtentCount:    int32(cell.ExtentCount),
			Utilization:    cell.Utilization,
		}
	}

	return connect.NewResponse(resp), nil
}

func (h *FragMapHandler) GetFragStats(ctx context.Context, req *connect.Request[apiv1.GetFragStatsRequest]) (*connect.Response[apiv1.GetFragStatsResponse], error) {
	scanner, err := fragmap.NewScanner(req.Msg.FsPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer scanner.Close()

	fm, err := scanner.Scan()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &apiv1.GetFragStatsResponse{
		Stats: make([]*apiv1.FragStats, 0),
	}

	// If device_id is 0, return stats for all devices
	deviceIDs := []uint64{}
	if req.Msg.DeviceId == 0 {
		for _, dev := range fm.Devices {
			deviceIDs = append(deviceIDs, dev.ID)
		}
	} else {
		deviceIDs = append(deviceIDs, req.Msg.DeviceId)
	}

	for _, devID := range deviceIDs {
		blockMap, err := fm.BuildDeviceBlockMap(devID)
		if err != nil {
			continue
		}

		stats := blockMap.CalculateStats()
		resp.Stats = append(resp.Stats, &apiv1.FragStats{
			DeviceId:       devID,
			TotalSize:      stats.TotalSize,
			AllocatedSize:  stats.AllocatedSize,
			FreeSize:       stats.FreeSize,
			DataSize:       stats.DataSize,
			MetadataSize:   stats.MetadataSize,
			SystemSize:     stats.SystemSize,
			NumExtents:     int32(stats.NumExtents),
			NumFreeRegions: int32(stats.NumFreeRegions),
			LargestFree:    stats.LargestFree,
			SmallestFree:   stats.SmallestFree,
			AvgExtentSize:  stats.AvgExtentSize,
			AvgFreeSize:    stats.AvgFreeSize,
		})
	}

	return connect.NewResponse(resp), nil
}

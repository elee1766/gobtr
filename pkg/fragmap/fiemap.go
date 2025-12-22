package fragmap

import (
	"fmt"
	"os"
	"sort"
	"syscall"
	"unsafe"
)

// FIEMAP ioctl constants
const (
	FS_IOC_FIEMAP = 0xc020660b // ioctl number for FIEMAP

	FIEMAP_FLAG_SYNC  = 0x00000001
	FIEMAP_FLAG_XATTR = 0x00000002

	FIEMAP_EXTENT_LAST           = 0x00000001
	FIEMAP_EXTENT_UNKNOWN        = 0x00000002
	FIEMAP_EXTENT_DELALLOC       = 0x00000004
	FIEMAP_EXTENT_ENCODED        = 0x00000008
	FIEMAP_EXTENT_DATA_ENCRYPTED = 0x00000080
	FIEMAP_EXTENT_NOT_ALIGNED    = 0x00000100
	FIEMAP_EXTENT_DATA_INLINE    = 0x00000200
	FIEMAP_EXTENT_DATA_TAIL      = 0x00000400
	FIEMAP_EXTENT_UNWRITTEN      = 0x00000800
	FIEMAP_EXTENT_MERGED         = 0x00001000
	FIEMAP_EXTENT_SHARED         = 0x00002000
)

// fiemapExtent is the kernel's fiemap_extent structure
type fiemapExtent struct {
	Logical    uint64 // Logical offset in bytes for the start of the extent
	Physical   uint64 // Physical offset in bytes for the start of the extent
	Length     uint64 // Length in bytes for the extent
	Reserved64 [2]uint64
	Flags      uint32 // FIEMAP_EXTENT_* flags
	Reserved   [3]uint32
}

// fiemap is the kernel's fiemap structure
type fiemap struct {
	Start         uint64 // Logical offset (in bytes) where to start mapping
	Length        uint64 // Logical length of mapping (in bytes)
	Flags         uint32 // FIEMAP_FLAG_* flags for request
	MappedExtents uint32 // Number of extents that were mapped
	ExtentCount   uint32 // Size of extent buffer (request)
	Reserved      uint32
}

// FileExtent represents a single extent of a file
type FileExtent struct {
	LogicalOffset  uint64 // Offset within the file
	PhysicalOffset uint64 // Physical offset on device
	Length         uint64 // Length in bytes
	Flags          uint32
	IsShared       bool // FIEMAP_EXTENT_SHARED
	IsInline       bool // FIEMAP_EXTENT_DATA_INLINE
	IsDelalloc     bool // FIEMAP_EXTENT_DELALLOC (not yet written)
}

// FileFragInfo contains fragmentation information for a single file
type FileFragInfo struct {
	Path        string
	Size        int64
	Extents     []FileExtent
	ExtentCount int

	// Calculated metrics
	DoF                   float64 // Degree of Fragmentation (actual/ideal extents)
	FragmentationPct      float64 // Percentage of fragmentation points vs potential
	OutOfOrderPct         float64 // Percentage of extents that are out of physical order
	BackwardsFragments    int     // Number of backwards jumps in physical layout
	FragmentationPoints   int     // Number of discontinuities
	IdealExtents          int     // Minimum extents needed for contiguous storage
	ContiguousExtentBytes int64   // Total bytes in physically contiguous runs
}

// GetFileExtents retrieves all extents for a file using FIEMAP
func GetFileExtents(path string) ([]FileExtent, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	fileSize := stat.Size()

	if fileSize == 0 {
		return nil, 0, nil
	}

	var extents []FileExtent
	start := uint64(0)
	length := uint64(fileSize)

	for {
		// Allocate buffer for extents (get up to 256 at a time)
		const maxExtents = 256
		bufSize := int(unsafe.Sizeof(fiemap{})) + maxExtents*int(unsafe.Sizeof(fiemapExtent{}))
		buf := make([]byte, bufSize)

		// Set up fiemap request
		fm := (*fiemap)(unsafe.Pointer(&buf[0]))
		fm.Start = start
		fm.Length = length
		fm.Flags = FIEMAP_FLAG_SYNC
		fm.ExtentCount = maxExtents

		// Call ioctl
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), FS_IOC_FIEMAP, uintptr(unsafe.Pointer(fm)))
		if errno != 0 {
			return nil, fileSize, fmt.Errorf("FIEMAP ioctl failed: %w", errno)
		}

		if fm.MappedExtents == 0 {
			break
		}

		// Parse extents
		extentPtr := unsafe.Pointer(&buf[unsafe.Sizeof(fiemap{})])
		for i := uint32(0); i < fm.MappedExtents; i++ {
			ext := (*fiemapExtent)(unsafe.Pointer(uintptr(extentPtr) + uintptr(i)*unsafe.Sizeof(fiemapExtent{})))
			extents = append(extents, FileExtent{
				LogicalOffset:  ext.Logical,
				PhysicalOffset: ext.Physical,
				Length:         ext.Length,
				Flags:          ext.Flags,
				IsShared:       ext.Flags&FIEMAP_EXTENT_SHARED != 0,
				IsInline:       ext.Flags&FIEMAP_EXTENT_DATA_INLINE != 0,
				IsDelalloc:     ext.Flags&FIEMAP_EXTENT_DELALLOC != 0,
			})

			// Check if this is the last extent
			if ext.Flags&FIEMAP_EXTENT_LAST != 0 {
				return extents, fileSize, nil
			}
		}

		// Move to next batch
		lastExt := extents[len(extents)-1]
		start = lastExt.LogicalOffset + lastExt.Length
		if start >= uint64(fileSize) {
			break
		}
		length = uint64(fileSize) - start
	}

	return extents, fileSize, nil
}

// AnalyzeFileFragmentation calculates fragmentation metrics for a file
func AnalyzeFileFragmentation(path string) (*FileFragInfo, error) {
	extents, fileSize, err := GetFileExtents(path)
	if err != nil {
		return nil, err
	}

	info := &FileFragInfo{
		Path:        path,
		Size:        fileSize,
		Extents:     extents,
		ExtentCount: len(extents),
	}

	if len(extents) == 0 || fileSize == 0 {
		info.DoF = 1.0 // Empty file has perfect fragmentation
		return info, nil
	}

	// Calculate ideal extents
	// For btrfs, max extent size is 128MB (compressed) or 1GB (uncompressed)
	// We'll use 128MB as a conservative estimate
	const maxExtentSize = 128 * 1024 * 1024
	info.IdealExtents = int((fileSize + maxExtentSize - 1) / maxExtentSize)
	if info.IdealExtents < 1 {
		info.IdealExtents = 1
	}

	// DoF = actual extents / ideal extents
	info.DoF = float64(len(extents)) / float64(info.IdealExtents)

	// Calculate fragmentation points (discontinuities in logical or physical space)
	// and out-of-order metrics
	if len(extents) > 1 {
		potentialFragPoints := len(extents) - 1
		actualFragPoints := 0
		backwardsFragments := 0
		contiguousBytes := int64(extents[0].Length)

		for i := 1; i < len(extents); i++ {
			prev := extents[i-1]
			curr := extents[i]

			// Check if physically contiguous
			prevPhysEnd := prev.PhysicalOffset + prev.Length
			isContiguous := curr.PhysicalOffset == prevPhysEnd

			if !isContiguous {
				actualFragPoints++

				// Check if out of order (physical goes backwards)
				if curr.PhysicalOffset < prev.PhysicalOffset {
					backwardsFragments++
				}
			} else {
				contiguousBytes += int64(curr.Length)
			}
		}

		info.FragmentationPoints = actualFragPoints
		info.BackwardsFragments = backwardsFragments
		info.ContiguousExtentBytes = contiguousBytes

		if potentialFragPoints > 0 {
			info.FragmentationPct = float64(actualFragPoints) / float64(potentialFragPoints) * 100.0
			info.OutOfOrderPct = float64(backwardsFragments) / float64(actualFragPoints) * 100.0
			if actualFragPoints == 0 {
				info.OutOfOrderPct = 0
			}
		}
	} else {
		// Single extent = no fragmentation
		info.FragmentationPct = 0
		info.OutOfOrderPct = 0
		info.ContiguousExtentBytes = fileSize
	}

	return info, nil
}

// AggregateFragStats holds aggregate fragmentation statistics
type AggregateFragStats struct {
	TotalFiles       int
	TotalExtents     int
	TotalBytes       int64
	FragmentedFiles  int     // Files with DoF > 1.0
	AvgDoF           float64 // Average Degree of Fragmentation
	AvgFragPct       float64 // Average Fragmentation Percentage
	AvgOutOfOrderPct float64 // Average Out-of-Order Percentage
	MaxDoF           float64 // Maximum DoF seen
	MaxExtents       int     // Maximum extents in a single file

	// Distribution
	DoFHistogram map[string]int // Buckets: "1", "1-2", "2-5", "5-10", "10+"
}

// AggregateFileFragmentation aggregates fragmentation stats for multiple files
func AggregateFileFragmentation(files []*FileFragInfo) *AggregateFragStats {
	stats := &AggregateFragStats{
		DoFHistogram: map[string]int{
			"1":    0,
			"1-2":  0,
			"2-5":  0,
			"5-10": 0,
			"10+":  0,
		},
	}

	if len(files) == 0 {
		return stats
	}

	var totalDoF, totalFragPct, totalOutOfOrder float64
	var filesWithFragPoints int

	for _, f := range files {
		stats.TotalFiles++
		stats.TotalExtents += f.ExtentCount
		stats.TotalBytes += f.Size
		totalDoF += f.DoF

		if f.DoF > 1.0 {
			stats.FragmentedFiles++
		}

		if f.FragmentationPoints > 0 {
			totalFragPct += f.FragmentationPct
			totalOutOfOrder += f.OutOfOrderPct
			filesWithFragPoints++
		}

		if f.DoF > stats.MaxDoF {
			stats.MaxDoF = f.DoF
		}
		if f.ExtentCount > stats.MaxExtents {
			stats.MaxExtents = f.ExtentCount
		}

		// Histogram
		switch {
		case f.DoF <= 1.0:
			stats.DoFHistogram["1"]++
		case f.DoF <= 2.0:
			stats.DoFHistogram["1-2"]++
		case f.DoF <= 5.0:
			stats.DoFHistogram["2-5"]++
		case f.DoF <= 10.0:
			stats.DoFHistogram["5-10"]++
		default:
			stats.DoFHistogram["10+"]++
		}
	}

	stats.AvgDoF = totalDoF / float64(stats.TotalFiles)
	if filesWithFragPoints > 0 {
		stats.AvgFragPct = totalFragPct / float64(filesWithFragPoints)
		stats.AvgOutOfOrderPct = totalOutOfOrder / float64(filesWithFragPoints)
	}

	return stats
}

// SortFilesByDoF sorts files by Degree of Fragmentation (descending)
func SortFilesByDoF(files []*FileFragInfo) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].DoF > files[j].DoF
	})
}

// SortFilesByExtents sorts files by extent count (descending)
func SortFilesByExtents(files []*FileFragInfo) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].ExtentCount > files[j].ExtentCount
	})
}

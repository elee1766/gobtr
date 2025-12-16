package btdu

import (
	"encoding/binary"
	"math"
	"strings"
	"time"
)

// SampleType represents the type of sample measurement.
type SampleType int

const (
	// Represented means this path was chosen to represent the space.
	// When multiple paths share an extent, one is picked as representative.
	Represented SampleType = iota
	// Exclusive means space used only by this path (no sharing).
	Exclusive
	// Shared means total space including all references (visible size).
	Shared
	// Unresolved means free space or metadata (no data extents at address).
	Unresolved
	// Unreachable means data exists but no file references it (orphaned/deleted).
	Unreachable
	// NumSampleTypes is the count of sample types.
	NumSampleTypes
)

func (s SampleType) String() string {
	switch s {
	case Represented:
		return "represented"
	case Exclusive:
		return "exclusive"
	case Shared:
		return "shared"
	case Unresolved:
		return "unresolved"
	case Unreachable:
		return "unreachable"
	default:
		return "unknown"
	}
}

// Offset represents a physical offset on disk (for example samples).
type Offset struct {
	Physical uint64
	Logical  uint64
}

// SampleData holds statistics for a single sample type at a path.
type SampleData struct {
	Samples  uint64      // Count of samples hitting this path
	Duration time.Duration // Total time spent resolving samples
	Offsets  [3]Offset   // Last 3 sample offsets seen (examples)
}

// AddSample adds a sample to the data.
func (d *SampleData) AddSample(offset Offset, duration time.Duration) {
	d.Samples++
	d.Duration += duration
	// Shift offsets and add new one at the end
	d.Offsets[0] = d.Offsets[1]
	d.Offsets[1] = d.Offsets[2]
	d.Offsets[2] = offset
}

// PathStats holds all sample statistics for a path node.
type PathStats struct {
	Data [NumSampleTypes]SampleData

	// For distributed/shared extent tracking
	DistributedSamples  float64
	DistributedDuration float64
}

// AddSample adds a sample of the given type.
func (s *PathStats) AddSample(sampleType SampleType, offset Offset, duration time.Duration) {
	s.Data[sampleType].AddSample(offset, duration)
}

// TotalSamples returns the total sample count across all types.
func (s *PathStats) TotalSamples() uint64 {
	var total uint64
	for _, d := range s.Data {
		total += d.Samples
	}
	return total
}

// SampleRecord represents a single sample measurement.
type SampleRecord struct {
	Path     string
	Type     SampleType
	Offset   Offset
	Duration time.Duration
}

// InodeResult represents the result of a logical-to-inode lookup.
type InodeResult struct {
	Inum   uint64
	Offset uint64
	Root   uint64
}

// ChildInfo contains information about a child path node.
type ChildInfo struct {
	Name  string
	Path  string
	Stats PathStats
}

// RankedPath represents a path with its sample count and estimated size.
type RankedPath struct {
	Path    string
	Samples uint64
	Size    uint64
}

// recentPathsSize is the number of recent paths to track.
const recentPathsSize = 32

// statsEncodedSize is the size of the encoded PathStats structure.
const statsEncodedSize = int(NumSampleTypes)*8*2 + 16 // samples + durations + distributed

// Encoding helpers for binary serialization

func putUint64(buf []byte, v uint64) {
	binary.LittleEndian.PutUint64(buf, v)
}

func getUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func putInt64(buf []byte, v int64) {
	binary.LittleEndian.PutUint64(buf, uint64(v))
}

func getInt64(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf))
}

func putFloat64(buf []byte, v float64) {
	binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
}

func getFloat64(buf []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(buf))
}

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return buf
}

func decodeUint64(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}

func decodeInt64(data []byte) int64 {
	if len(data) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data))
}

func encodeTime(t time.Time) []byte {
	return encodeInt64(t.UnixNano())
}

func decodeTime(data []byte) time.Time {
	nano := decodeInt64(data)
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

// String helpers

func joinPath(segments []string) string {
	return strings.Join(segments, "/")
}

func hasPrefix(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

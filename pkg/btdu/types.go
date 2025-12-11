package btdu

import "time"

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

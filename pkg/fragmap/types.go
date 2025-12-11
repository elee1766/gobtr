package fragmap

// BlockType represents the type of data in a block/chunk
type BlockType uint64

const (
	BlockTypeData     BlockType = 1 << 0
	BlockTypeSystem   BlockType = 1 << 1
	BlockTypeMetadata BlockType = 1 << 2
)

// BlockProfile represents the RAID profile of a chunk
type BlockProfile uint64

const (
	ProfileSingle BlockProfile = 0
	ProfileRAID0  BlockProfile = 1 << 3
	ProfileRAID1  BlockProfile = 1 << 4
	ProfileDUP    BlockProfile = 1 << 5
	ProfileRAID10 BlockProfile = 1 << 6
	ProfileRAID5  BlockProfile = 1 << 7
	ProfileRAID6  BlockProfile = 1 << 8
	ProfileRAID1C3 BlockProfile = 1 << 9
	ProfileRAID1C4 BlockProfile = 1 << 10
)

// Chunk represents a btrfs chunk (logical allocation unit)
type Chunk struct {
	// Logical address in the filesystem
	LogicalOffset uint64
	// Size of the chunk
	Length uint64
	// Type flags (data, metadata, system)
	Type BlockType
	// RAID profile
	Profile BlockProfile
	// Stripes - physical locations on devices
	Stripes []Stripe
	// Used bytes within this chunk (from block group)
	Used uint64
}

// Stripe represents a physical location of chunk data on a device
type Stripe struct {
	DeviceID uint64
	Offset   uint64
}

// DeviceExtent represents a physical extent on a device
type DeviceExtent struct {
	// Device ID
	DeviceID uint64
	// Physical offset on the device
	PhysicalOffset uint64
	// Length of the extent
	Length uint64
	// Logical chunk offset this maps to
	ChunkOffset uint64
}

// Device represents a btrfs device
type Device struct {
	ID        uint64
	UUID      [16]byte
	TotalSize uint64
	Path      string
}

// FragMap represents the complete fragmentation map of a filesystem
type FragMap struct {
	// Total filesystem size (logical)
	TotalSize uint64
	// Devices in the filesystem
	Devices []Device
	// All chunks (logical allocations)
	Chunks []Chunk
	// Device extents (physical allocations per device)
	DeviceExtents map[uint64][]DeviceExtent // keyed by device ID
}

// BlockMapEntry represents a single entry in the block map visualization
type BlockMapEntry struct {
	// Physical offset on device
	Offset uint64
	// Length of this region
	Length uint64
	// Type of data (or 0 for free)
	Type BlockType
	// RAID profile
	Profile BlockProfile
	// Whether this is allocated or free
	Allocated bool
	// Logical chunk offset (if allocated)
	ChunkOffset uint64
	// Used bytes within this chunk (only for allocated entries)
	ChunkUsed uint64
	// Total chunk length (for calculating utilization)
	ChunkLength uint64
}

// DeviceBlockMap represents the physical layout of a single device
type DeviceBlockMap struct {
	DeviceID  uint64
	TotalSize uint64
	Entries   []BlockMapEntry
}

// TypeName returns a human-readable name for the block type
func (t BlockType) TypeName() string {
	switch {
	case t&BlockTypeData != 0:
		return "data"
	case t&BlockTypeMetadata != 0:
		return "metadata"
	case t&BlockTypeSystem != 0:
		return "system"
	default:
		return "unknown"
	}
}

// ProfileName returns a human-readable name for the RAID profile
func (p BlockProfile) ProfileName() string {
	switch p {
	case ProfileSingle:
		return "single"
	case ProfileRAID0:
		return "raid0"
	case ProfileRAID1:
		return "raid1"
	case ProfileDUP:
		return "dup"
	case ProfileRAID10:
		return "raid10"
	case ProfileRAID5:
		return "raid5"
	case ProfileRAID6:
		return "raid6"
	case ProfileRAID1C3:
		return "raid1c3"
	case ProfileRAID1C4:
		return "raid1c4"
	default:
		return "unknown"
	}
}

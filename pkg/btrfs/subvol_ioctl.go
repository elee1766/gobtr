package btrfs

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"
	"unsafe"

	"github.com/dennwc/ioctl"
)

// btrfs ioctl magic number
const btrfsIoctlMagic = 0x94

// Tree IDs
const (
	RootTreeObjectID  = 1
	DevTreeObjectID   = 4
	ChunkTreeObjectID = 3
)

// Item key types
const (
	RootItemKey    = 132
	RootBackrefKey = 144
	DevExtentKey   = 204
	ChunkItemKey   = 228
)

// Special object IDs
const (
	FirstFreeObjectID = 256
)

// Root flags
const (
	RootSubvolReadonly = 1 << 0
)

// Search key structure size
const searchKeySize = 104

// Buffer size for search results
const searchBufSize = 4096 - searchKeySize

// btrfsIoctlSearchKey is the search parameters
type btrfsIoctlSearchKey struct {
	TreeID      uint64
	MinObjectID uint64
	MaxObjectID uint64
	MinOffset   uint64
	MaxOffset   uint64
	MinTransID  uint64
	MaxTransID  uint64
	MinType     uint32
	MaxType     uint32
	NrItems     uint32
	_unused     uint32
	_unused1    uint64
	_unused2    uint64
	_unused3    uint64
	_unused4    uint64
}

// btrfsIoctlSearchArgs is the full search ioctl args
type btrfsIoctlSearchArgs struct {
	Key btrfsIoctlSearchKey
	Buf [searchBufSize]byte
}

// btrfsSearchHeader is the header for each search result item
type btrfsSearchHeader struct {
	TransID  uint64
	ObjectID uint64
	Offset   uint64
	Type     uint32
	Len      uint32
}

// SearchResult holds a single search result
type SearchResult struct {
	Header btrfsSearchHeader
	Data   []byte
}

var ioctlTreeSearch = ioctl.IOWR(btrfsIoctlMagic, 17, unsafe.Sizeof(btrfsIoctlSearchArgs{}))

// btrfsIoctlFsInfoArgs is the structure for BTRFS_IOC_FS_INFO
type btrfsIoctlFsInfoArgs struct {
	MaxID          uint64
	NumDevices     uint64
	FSID           [16]byte
	NodeSize       uint32
	SectorSize     uint32
	CloneAlignment uint32
	CsumType       uint16
	CsumSize       uint16
	Flags          uint64
	Generation     uint64
	MetadataUUID   [16]byte
	Reserved       [944]byte
}

var ioctlFsInfo = ioctl.IOR(btrfsIoctlMagic, 31, unsafe.Sizeof(btrfsIoctlFsInfoArgs{}))

// BTRFS_DEVICE_PATH_NAME_MAX from kernel headers
const devicePathNameMax = 1024

// btrfsIoctlDevInfoArgs for BTRFS_IOC_DEV_INFO
type btrfsIoctlDevInfoArgs struct {
	DevID      uint64
	UUID       [16]byte
	BytesUsed  uint64
	TotalBytes uint64
	FSID       [16]byte
	Unused     [377]uint64
	Path       [devicePathNameMax]byte
}

var ioctlDevInfo = ioctl.IOWR(btrfsIoctlMagic, 30, unsafe.Sizeof(btrfsIoctlDevInfoArgs{}))

// btrfsIoctlSpaceInfo for BTRFS_IOC_SPACE_INFO results
type btrfsIoctlSpaceInfo struct {
	Flags      uint64
	TotalBytes uint64
	UsedBytes  uint64
}

// btrfsIoctlSpaceArgs for BTRFS_IOC_SPACE_INFO
type btrfsIoctlSpaceArgs struct {
	SpaceSlots  uint64
	TotalSpaces uint64
}

var ioctlSpaceInfo = ioctl.IOWR(btrfsIoctlMagic, 20, unsafe.Sizeof(btrfsIoctlSpaceArgs{}))

// Block group type flags
const (
	BlockGroupData       = 1 << 0
	BlockGroupSystem     = 1 << 1
	BlockGroupMetadata   = 1 << 2
	BlockGroupRaid0      = 1 << 3
	BlockGroupRaid1      = 1 << 4
	BlockGroupDup        = 1 << 5
	BlockGroupRaid10     = 1 << 6
	BlockGroupRaid5      = 1 << 7
	BlockGroupRaid6      = 1 << 8
	BlockGroupRaid1C3    = 1 << 9
	BlockGroupRaid1C4    = 1 << 10
	SpaceInfoGlobalRsv   = 1 << 49 // BTRFS_SPACE_INFO_GLOBAL_RSV
)

// DeviceInfoIoctl contains device info from ioctl
type DeviceInfoIoctl struct {
	DevID      uint64
	UUID       string
	BytesUsed  uint64
	TotalBytes uint64
	Path       string
}

// SpaceInfoIoctl contains space allocation info from ioctl
type SpaceInfoIoctl struct {
	Type       string // "Data", "Metadata", "System", "unknown"
	Profile    string // "single", "DUP", "RAID1", etc.
	TotalBytes uint64
	UsedBytes  uint64
}

// GetDeviceInfo gets device info via BTRFS_IOC_DEV_INFO
func GetDeviceInfo(path string, devID uint64) (*DeviceInfoIoctl, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	var args btrfsIoctlDevInfoArgs
	args.DevID = devID

	if err := ioctl.Do(f, ioctlDevInfo, &args); err != nil {
		return nil, fmt.Errorf("DEV_INFO ioctl: %w", err)
	}

	// Find null terminator in path
	pathLen := 0
	for i, b := range args.Path {
		if b == 0 {
			pathLen = i
			break
		}
	}

	return &DeviceInfoIoctl{
		DevID:      args.DevID,
		UUID:       formatUUID(args.UUID),
		BytesUsed:  args.BytesUsed,
		TotalBytes: args.TotalBytes,
		Path:       string(args.Path[:pathLen]),
	}, nil
}

// GetAllDeviceInfo gets info for all devices in a filesystem
func GetAllDeviceInfo(path string) ([]*DeviceInfoIoctl, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	return getAllDeviceInfoFromFile(f)
}

// GetFilesystemAndDeviceInfo gets both filesystem info and device info in a single file open
func GetFilesystemAndDeviceInfo(path string) (*FilesystemInfo, []*DeviceInfoIoctl, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	// Get filesystem info
	var fsArgs btrfsIoctlFsInfoArgs
	if err := ioctl.Do(f, ioctlFsInfo, &fsArgs); err != nil {
		return nil, nil, fmt.Errorf("FS_INFO ioctl: %w", err)
	}

	fsInfo := &FilesystemInfo{
		UUID:       formatUUID(fsArgs.FSID),
		NumDevices: fsArgs.NumDevices,
		NodeSize:   fsArgs.NodeSize,
		SectorSize: fsArgs.SectorSize,
		Generation: fsArgs.Generation,
	}
	if !isZeroUUID(fsArgs.MetadataUUID) && fsArgs.MetadataUUID != fsArgs.FSID {
		fsInfo.MetadataUUID = formatUUID(fsArgs.MetadataUUID)
	}

	// Get device info using the same file handle and fsArgs
	var devices []*DeviceInfoIoctl
	for devID := uint64(1); devID <= fsArgs.MaxID; devID++ {
		var args btrfsIoctlDevInfoArgs
		args.DevID = devID

		if err := ioctl.Do(f, ioctlDevInfo, &args); err != nil {
			continue // Device ID doesn't exist
		}

		pathLen := 0
		for i, b := range args.Path {
			if b == 0 {
				pathLen = i
				break
			}
		}

		devices = append(devices, &DeviceInfoIoctl{
			DevID:      args.DevID,
			UUID:       formatUUID(args.UUID),
			BytesUsed:  args.BytesUsed,
			TotalBytes: args.TotalBytes,
			Path:       string(args.Path[:pathLen]),
		})

		if uint64(len(devices)) >= fsArgs.NumDevices {
			break
		}
	}

	return fsInfo, devices, nil
}

// getAllDeviceInfoFromFile gets device info using an already-open file handle
func getAllDeviceInfoFromFile(f *os.File) ([]*DeviceInfoIoctl, error) {
	// Get filesystem info to know max device ID and count
	var fsInfoArgs btrfsIoctlFsInfoArgs
	if err := ioctl.Do(f, ioctlFsInfo, &fsInfoArgs); err != nil {
		return nil, fmt.Errorf("FS_INFO ioctl: %w", err)
	}

	var devices []*DeviceInfoIoctl

	for devID := uint64(1); devID <= fsInfoArgs.MaxID; devID++ {
		var args btrfsIoctlDevInfoArgs
		args.DevID = devID

		if err := ioctl.Do(f, ioctlDevInfo, &args); err != nil {
			continue // Device ID doesn't exist
		}

		// Find null terminator in path
		pathLen := 0
		for i, b := range args.Path {
			if b == 0 {
				pathLen = i
				break
			}
		}

		devices = append(devices, &DeviceInfoIoctl{
			DevID:      args.DevID,
			UUID:       formatUUID(args.UUID),
			BytesUsed:  args.BytesUsed,
			TotalBytes: args.TotalBytes,
			Path:       string(args.Path[:pathLen]),
		})

		if uint64(len(devices)) >= fsInfoArgs.NumDevices {
			break
		}
	}

	return devices, nil
}

// GetSpaceInfo gets space allocation info via BTRFS_IOC_SPACE_INFO
func GetSpaceInfo(path string) ([]*SpaceInfoIoctl, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	// First call to get number of spaces
	var args btrfsIoctlSpaceArgs
	if err := ioctl.Do(f, ioctlSpaceInfo, &args); err != nil {
		return nil, fmt.Errorf("SPACE_INFO ioctl (count): %w", err)
	}

	if args.TotalSpaces == 0 {
		return nil, nil
	}

	// Allocate buffer for full results
	// The ioctl returns btrfsIoctlSpaceArgs followed by TotalSpaces * btrfsIoctlSpaceInfo
	bufSize := 16 + args.TotalSpaces*24 // 16 bytes header + 24 bytes per space info
	buf := make([]byte, bufSize)

	// Set space_slots to request all spaces
	binary.LittleEndian.PutUint64(buf[0:8], args.TotalSpaces)

	// Make the ioctl call with raw buffer
	if err := ioctl.Ioctl(f, ioctlSpaceInfo, uintptr(unsafe.Pointer(&buf[0]))); err != nil {
		return nil, fmt.Errorf("SPACE_INFO ioctl (data): %w", err)
	}

	// Parse results
	totalSpaces := binary.LittleEndian.Uint64(buf[8:16])
	var spaces []*SpaceInfoIoctl

	for i := uint64(0); i < totalSpaces; i++ {
		offset := 16 + i*24
		flags := binary.LittleEndian.Uint64(buf[offset : offset+8])
		total := binary.LittleEndian.Uint64(buf[offset+8 : offset+16])
		used := binary.LittleEndian.Uint64(buf[offset+16 : offset+24])

		spaces = append(spaces, &SpaceInfoIoctl{
			Type:       getBlockGroupType(flags),
			Profile:    getBlockGroupProfile(flags),
			TotalBytes: total,
			UsedBytes:  used,
		})
	}

	return spaces, nil
}

// DeviceChunkAllocation represents a chunk allocation on a specific device
type DeviceChunkAllocation struct {
	DevID      uint64
	ChunkStart uint64 // Logical address of chunk
	Length     uint64 // Size on this device
	Type       string // Data/Metadata/System
	Profile    string // single/dup/raid1/etc
}

// GetDeviceChunkAllocations gets per-device chunk allocations by searching the device tree
func GetDeviceChunkAllocations(path string) ([]*DeviceChunkAllocation, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	// First, get chunk info from the chunk tree to map chunk_start -> flags
	chunkFlags := make(map[uint64]uint64)
	chunkResults, err := treeSearch(f, ChunkTreeObjectID, 256, ^uint64(0), ChunkItemKey, ChunkItemKey, 0, ^uint64(0))
	if err == nil {
		for _, res := range chunkResults {
			if res.Header.Type == ChunkItemKey && len(res.Data) >= 32 {
				// btrfs_chunk structure layout:
				// offset 0: length (8 bytes)
				// offset 8: owner (8 bytes)
				// offset 16: stripe_len (8 bytes)
				// offset 24: type/flags (8 bytes) - block group flags
				chunkStart := res.Header.Offset
				flags := binary.LittleEndian.Uint64(res.Data[24:32])
				chunkFlags[chunkStart] = flags
			}
		}
	}

	// Search the device tree for dev extents
	// Object ID is the device ID, type is DEV_EXTENT_KEY (204), offset is physical offset on device
	results, err := treeSearch(f, DevTreeObjectID, 1, ^uint64(0), DevExtentKey, DevExtentKey, 0, ^uint64(0))
	if err != nil {
		return nil, fmt.Errorf("tree search for dev extents: %w", err)
	}

	var allocs []*DeviceChunkAllocation
	for _, res := range results {
		if res.Header.Type != DevExtentKey {
			continue
		}

		// btrfs_dev_extent structure:
		// chunk_tree: u64 (0-8)
		// chunk_objectid: u64 (8-16)
		// chunk_offset: u64 (16-24) - this is the logical chunk start
		// length: u64 (24-32)
		if len(res.Data) < 32 {
			continue
		}

		chunkStart := binary.LittleEndian.Uint64(res.Data[16:24])
		length := binary.LittleEndian.Uint64(res.Data[24:32])
		devID := res.Header.ObjectID

		// Get chunk type from the chunk flags map
		flags := chunkFlags[chunkStart]

		allocs = append(allocs, &DeviceChunkAllocation{
			DevID:      devID,
			ChunkStart: chunkStart,
			Length:     length,
			Type:       getBlockGroupType(flags),
			Profile:    getBlockGroupProfile(flags),
		})
	}

	return allocs, nil
}

func getBlockGroupType(flags uint64) string {
	if flags&SpaceInfoGlobalRsv != 0 {
		return "GlobalReserve"
	}
	if flags&BlockGroupData != 0 {
		return "Data"
	}
	if flags&BlockGroupMetadata != 0 {
		return "Metadata"
	}
	if flags&BlockGroupSystem != 0 {
		return "System"
	}
	return "unknown"
}

// btrfsIoctlBalanceArgs for BTRFS_IOC_BALANCE_PROGRESS
type btrfsIoctlBalanceArgs struct {
	Flags uint64
	State uint64
	Data  btrfsBalanceArgs
	Meta  btrfsBalanceArgs
	Sys   btrfsBalanceArgs
}

type btrfsBalanceArgs struct {
	Profiles    uint64
	Usage       uint64
	UsageMin    uint32
	UsageMax    uint32
	Devid       uint64
	PStart      uint64
	PEnd        uint64
	VStart      uint64
	VEnd        uint64
	Target      uint64
	Flags       uint64
	Limit       uint64
	LimitMin    uint32
	LimitMax    uint32
	Stripes     uint32
	StripesMin  uint32
	StripesMax  uint32
	Unused      [6]uint64
}

// Balance state flags
const (
	BalanceStateRunning = 1 << 0
	BalanceStatePauseReq = 1 << 1
	BalanceStateCancelReq = 1 << 2
)

var ioctlBalanceProgress = ioctl.IOWR(btrfsIoctlMagic, 34, unsafe.Sizeof(btrfsIoctlBalanceArgs{}))

// BalanceProgressIoctl contains balance progress from ioctl
type BalanceProgressIoctl struct {
	IsRunning bool
	IsPaused  bool
	State     uint64
}

// GetBalanceProgress gets balance progress via BTRFS_IOC_BALANCE_PROGRESS
func GetBalanceProgress(path string) (*BalanceProgressIoctl, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	var args btrfsIoctlBalanceArgs
	if err := ioctl.Do(f, ioctlBalanceProgress, &args); err != nil {
		// ENOTCONN means no balance running
		return &BalanceProgressIoctl{
			IsRunning: false,
			IsPaused:  false,
		}, nil
	}

	return &BalanceProgressIoctl{
		IsRunning: args.State&BalanceStateRunning != 0,
		IsPaused:  args.State&BalanceStatePauseReq != 0,
		State:     args.State,
	}, nil
}

func getBlockGroupProfile(flags uint64) string {
	switch {
	case flags&BlockGroupRaid1C4 != 0:
		return "RAID1C4"
	case flags&BlockGroupRaid1C3 != 0:
		return "RAID1C3"
	case flags&BlockGroupRaid6 != 0:
		return "RAID6"
	case flags&BlockGroupRaid5 != 0:
		return "RAID5"
	case flags&BlockGroupRaid10 != 0:
		return "RAID10"
	case flags&BlockGroupRaid1 != 0:
		return "RAID1"
	case flags&BlockGroupRaid0 != 0:
		return "RAID0"
	case flags&BlockGroupDup != 0:
		return "DUP"
	default:
		return "single"
	}
}

// FilesystemInfo contains basic filesystem info from ioctl
type FilesystemInfo struct {
	UUID         string
	MetadataUUID string
	NumDevices   uint64
	NodeSize     uint32
	SectorSize   uint32
	Generation   uint64
}

// GetFilesystemInfo gets filesystem info via ioctl - returns an error if path is not a btrfs filesystem
func GetFilesystemInfo(path string) (*FilesystemInfo, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open path: %w", err)
	}
	defer f.Close()

	var args btrfsIoctlFsInfoArgs
	if err := ioctl.Do(f, ioctlFsInfo, &args); err != nil {
		return nil, fmt.Errorf("not a btrfs filesystem: %w", err)
	}

	info := &FilesystemInfo{
		UUID:       formatUUID(args.FSID),
		NumDevices: args.NumDevices,
		NodeSize:   args.NodeSize,
		SectorSize: args.SectorSize,
		Generation: args.Generation,
	}

	// Only include metadata UUID if it's different from FSID
	if !isZeroUUID(args.MetadataUUID) && args.MetadataUUID != args.FSID {
		info.MetadataUUID = formatUUID(args.MetadataUUID)
	}

	return info, nil
}

// SubvolumeIoctl contains subvolume info fetched via ioctl
type SubvolumeIoctl struct {
	ID           uint64
	ParentID     uint64 // From the key offset field
	Generation   uint64
	Flags        uint64
	UUID         [16]byte
	ParentUUID   [16]byte
	ReceivedUUID [16]byte
	CTransID     uint64 // Last modification transaction
	OTransID     uint64 // Creation transaction
	STransID     uint64 // Send transaction
	RTransID     uint64 // Receive transaction
	CTime        time.Time
	OTime        time.Time // Creation time
	STime        time.Time
	RTime        time.Time
	Path         string // Resolved path relative to filesystem root
}

// IsReadonly returns true if the subvolume is read-only
func (s *SubvolumeIoctl) IsReadonly() bool {
	return s.Flags&RootSubvolReadonly != 0
}

// UUIDString returns the UUID as a string
func (s *SubvolumeIoctl) UUIDString() string {
	return formatUUID(s.UUID)
}

// ParentUUIDString returns the parent UUID as a string, or empty if not set
func (s *SubvolumeIoctl) ParentUUIDString() string {
	if isZeroUUID(s.ParentUUID) {
		return ""
	}
	return formatUUID(s.ParentUUID)
}

// ReceivedUUIDString returns the received UUID as a string, or empty if not set
func (s *SubvolumeIoctl) ReceivedUUIDString() string {
	if isZeroUUID(s.ReceivedUUID) {
		return ""
	}
	return formatUUID(s.ReceivedUUID)
}

func isZeroUUID(uuid [16]byte) bool {
	for _, b := range uuid {
		if b != 0 {
			return false
		}
	}
	return true
}

func formatUUID(uuid [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(uuid[0:4]),
		binary.BigEndian.Uint16(uuid[4:6]),
		binary.BigEndian.Uint16(uuid[6:8]),
		binary.BigEndian.Uint16(uuid[8:10]),
		uuid[10:16])
}

// ListSubvolumesIoctl lists all subvolumes using the tree search ioctl
func ListSubvolumesIoctl(fsPath string) ([]SubvolumeIoctl, error) {
	f, err := os.OpenFile(fsPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open filesystem: %w", err)
	}
	defer f.Close()

	subvolumes, err := listSubvolumesFromFile(f)
	if err != nil {
		return nil, err
	}

	// Resolve paths for each subvolume using ROOT_BACKREF entries
	pathMap, err := getSubvolumePaths(f)
	if err != nil {
		// Paths are optional, continue without them
		return subvolumes, nil
	}

	for i := range subvolumes {
		if path, ok := pathMap[subvolumes[i].ID]; ok {
			subvolumes[i].Path = path
		}
	}

	return subvolumes, nil
}

// getSubvolumePaths builds a map of subvolume ID to path using ROOT_BACKREF entries
func getSubvolumePaths(f *os.File) (map[uint64]string, error) {
	// Search for ROOT_BACKREF entries which contain the name and parent info
	results, err := treeSearch(f, RootTreeObjectID, FirstFreeObjectID, ^uint64(0), RootBackrefKey, RootBackrefKey, 0, ^uint64(0))
	if err != nil {
		return nil, fmt.Errorf("tree search for backrefs: %w", err)
	}

	// Build parent->name mapping
	type backref struct {
		parentID uint64
		name     string
	}
	backrefs := make(map[uint64]backref)

	for _, r := range results {
		if r.Header.Type != RootBackrefKey || len(r.Data) < 12 {
			continue
		}

		// ROOT_BACKREF structure:
		// dirid (8 bytes) - directory inode in parent subvolume
		// sequence (8 bytes)
		// name_len (2 bytes)
		// name (variable)
		nameLen := binary.LittleEndian.Uint16(r.Data[16:18])
		if len(r.Data) < 18+int(nameLen) {
			continue
		}
		name := string(r.Data[18 : 18+nameLen])

		backrefs[r.Header.ObjectID] = backref{
			parentID: r.Header.Offset,
			name:     name,
		}
	}

	// Build full paths by walking up the tree
	pathMap := make(map[uint64]string)
	pathMap[5] = "/" // Top-level subvolume

	// Helper to resolve full path
	var resolvePath func(id uint64, visited map[uint64]bool) string
	resolvePath = func(id uint64, visited map[uint64]bool) string {
		if id == 5 {
			return ""
		}
		if path, ok := pathMap[id]; ok {
			return path
		}
		if visited[id] {
			return "" // Cycle detection
		}
		visited[id] = true

		br, ok := backrefs[id]
		if !ok {
			return ""
		}

		parentPath := resolvePath(br.parentID, visited)
		if parentPath == "" && br.parentID != 5 {
			return br.name
		}
		if parentPath == "" {
			return br.name
		}
		return parentPath + "/" + br.name
	}

	for id := range backrefs {
		visited := make(map[uint64]bool)
		pathMap[id] = resolvePath(id, visited)
	}

	return pathMap, nil
}

func listSubvolumesFromFile(f *os.File) ([]SubvolumeIoctl, error) {
	// Search the root tree for all ROOT_ITEM entries
	// Subvolume IDs start at 256 (5 is the FS_TREE root, 256+ are user subvolumes)
	results, err := treeSearch(f, RootTreeObjectID, 5, ^uint64(0), RootItemKey, RootItemKey, 0, ^uint64(0))
	if err != nil {
		return nil, fmt.Errorf("tree search: %w", err)
	}

	var subvolumes []SubvolumeIoctl
	for _, r := range results {
		if r.Header.Type != RootItemKey {
			continue
		}

		subvol, err := parseRootItem(r.Header.ObjectID, r.Header.Offset, r.Data)
		if err != nil {
			continue // Skip malformed entries
		}

		subvolumes = append(subvolumes, *subvol)
	}

	return subvolumes, nil
}

// parseRootItem parses a ROOT_ITEM from the raw data
// Structure offsets based on btrfs on-disk format:
// 0-159: inode_item (160 bytes)
// 160: generation (8)
// 168: root_dirid (8)
// 176: bytenr (8)
// 184: byte_limit (8)
// 192: bytes_used (8)
// 200: last_snapshot (8)
// 208: flags (8)
// 216: refs (4)
// 220: drop_progress (17)
// 237: drop_level (1)
// 238: level (1)
// 239: generation_v2 (8) - only in newer format
// 247: uuid (16)
// 263: parent_uuid (16)
// 279: received_uuid (16)
// 295: ctransid (8)
// 303: otransid (8)
// 311: stransid (8)
// 319: rtransid (8)
// 327: ctime (12)
// 339: otime (12)
// 351: stime (12)
// 363: rtime (12)
func parseRootItem(objectID, offset uint64, data []byte) (*SubvolumeIoctl, error) {
	// Minimum size check - old format was smaller
	if len(data) < 239 {
		return nil, fmt.Errorf("root item too small: %d bytes", len(data))
	}

	subvol := &SubvolumeIoctl{
		ID:         objectID,
		ParentID:   offset,
		Generation: binary.LittleEndian.Uint64(data[160:168]),
		Flags:      binary.LittleEndian.Uint64(data[208:216]),
	}

	// Check if we have the extended format with UUIDs and times
	if len(data) >= 375 {
		copy(subvol.UUID[:], data[247:263])
		copy(subvol.ParentUUID[:], data[263:279])
		copy(subvol.ReceivedUUID[:], data[279:295])

		subvol.CTransID = binary.LittleEndian.Uint64(data[295:303])
		subvol.OTransID = binary.LittleEndian.Uint64(data[303:311])
		subvol.STransID = binary.LittleEndian.Uint64(data[311:319])
		subvol.RTransID = binary.LittleEndian.Uint64(data[319:327])

		subvol.CTime = parseTimespec(data[327:339])
		subvol.OTime = parseTimespec(data[339:351])
		subvol.STime = parseTimespec(data[351:363])
		subvol.RTime = parseTimespec(data[363:375])
	}

	return subvol, nil
}

// parseTimespec parses a btrfs_timespec (12 bytes: 8 byte seconds + 4 byte nsec)
func parseTimespec(data []byte) time.Time {
	if len(data) < 12 {
		return time.Time{}
	}
	sec := int64(binary.LittleEndian.Uint64(data[0:8]))
	nsec := int64(binary.LittleEndian.Uint32(data[8:12]))

	// Check for zero/invalid times
	if sec <= 0 {
		return time.Time{}
	}

	return time.Unix(sec, nsec)
}

// treeSearch performs a tree search ioctl
func treeSearch(f *os.File, treeID uint64, minObjID, maxObjID uint64, minType, maxType uint32, minOffset, maxOffset uint64) ([]SearchResult, error) {
	var results []SearchResult

	args := btrfsIoctlSearchArgs{
		Key: btrfsIoctlSearchKey{
			TreeID:      treeID,
			MinObjectID: minObjID,
			MaxObjectID: maxObjID,
			MinOffset:   minOffset,
			MaxOffset:   maxOffset,
			MinTransID:  0,
			MaxTransID:  ^uint64(0),
			MinType:     minType,
			MaxType:     maxType,
			NrItems:     4096,
		},
	}

	for {
		err := ioctl.Do(f, ioctlTreeSearch, &args)
		if err != nil {
			return nil, fmt.Errorf("tree search ioctl: %w", err)
		}

		if args.Key.NrItems == 0 {
			break
		}

		// Parse results from buffer
		offset := 0
		var lastHdr btrfsSearchHeader
		gotItems := false
		for i := uint32(0); i < args.Key.NrItems; i++ {
			if offset+int(unsafe.Sizeof(btrfsSearchHeader{})) > len(args.Buf) {
				break
			}

			// Read header
			hdr := btrfsSearchHeader{
				TransID:  binary.LittleEndian.Uint64(args.Buf[offset:]),
				ObjectID: binary.LittleEndian.Uint64(args.Buf[offset+8:]),
				Offset:   binary.LittleEndian.Uint64(args.Buf[offset+16:]),
				Type:     binary.LittleEndian.Uint32(args.Buf[offset+24:]),
				Len:      binary.LittleEndian.Uint32(args.Buf[offset+28:]),
			}
			offset += 32 // sizeof header

			// Read item data
			if offset+int(hdr.Len) > len(args.Buf) {
				break
			}

			// Only copy data for matching types
			if hdr.Type >= minType && hdr.Type <= maxType {
				data := make([]byte, hdr.Len)
				copy(data, args.Buf[offset:offset+int(hdr.Len)])
				results = append(results, SearchResult{
					Header: hdr,
					Data:   data,
				})
			}
			offset += int(hdr.Len)

			lastHdr = hdr
			gotItems = true
		}

		if !gotItems {
			break
		}

		// Update search key for next iteration
		if lastHdr.Offset == ^uint64(0) {
			if lastHdr.Type == maxType {
				if lastHdr.ObjectID == maxObjID {
					break
				}
				args.Key.MinObjectID = lastHdr.ObjectID + 1
				args.Key.MinType = minType
			} else {
				args.Key.MinType = lastHdr.Type + 1
			}
			args.Key.MinOffset = 0
		} else {
			args.Key.MinObjectID = lastHdr.ObjectID
			args.Key.MinType = lastHdr.Type
			args.Key.MinOffset = lastHdr.Offset + 1
		}
		args.Key.NrItems = 4096
	}

	return results, nil
}

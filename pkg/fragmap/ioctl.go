package fragmap

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"time"
	"unsafe"

	"github.com/dennwc/ioctl"
)

// btrfs ioctl magic number
const btrfsIoctlMagic = 0x94

// Tree IDs
const (
	ExtentTreeObjectID = 2
	ChunkTreeObjectID  = 3
	DevTreeObjectID    = 4
)

// Item key types
const (
	DevItemKey       = 216
	ChunkItemKey     = 228
	DevExtentKey     = 204
	BlockGroupItemKey = 192
)

// First chunk tree object ID
const FirstChunkTreeObjectID = 256

// Search key structure size
const searchKeySize = 104 // sizeof(btrfs_ioctl_search_key)

// Buffer size for search results (4096 - search_key size)
const searchBufSize = 4096 - searchKeySize

// btrfsIoctlSearchKey is the search parameters
type btrfsIoctlSearchKey struct {
	TreeID       uint64
	MinObjectID  uint64
	MaxObjectID  uint64
	MinOffset    uint64
	MaxOffset    uint64
	MinTransID   uint64
	MaxTransID   uint64
	MinType      uint32
	MaxType      uint32
	NrItems      uint32
	_unused      uint32
	_unused1     uint64
	_unused2     uint64
	_unused3     uint64
	_unused4     uint64
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

// btrfsChunk is the on-disk chunk structure
type btrfsChunk struct {
	Length     uint64
	Owner      uint64
	StripeLen  uint64
	Type       uint64
	IOAlign    uint32
	IOWidth    uint32
	SectorSize uint32
	NumStripes uint16
	SubStripes uint16
	// Followed by btrfsStripe array
}

// btrfsStripe is the on-disk stripe structure
type btrfsStripe struct {
	DevID  uint64
	Offset uint64
	UUID   [16]byte
}

// btrfsDevExtent is the on-disk device extent structure
type btrfsDevExtent struct {
	ChunkTree      uint64
	ChunkObjectID  uint64
	ChunkOffset    uint64
	Length         uint64
	ChunkTreeUUID  [16]byte
}

// btrfsDevItem is the on-disk device item structure
type btrfsDevItem struct {
	DevID        uint64
	TotalBytes   uint64
	BytesUsed    uint64
	IOAlign      uint32
	IOWidth      uint32
	SectorSize   uint32
	Type         uint64
	Generation   uint64
	StartOffset  uint64
	DevGroup     uint32
	SeekSpeed    uint8
	Bandwidth    uint8
	UUID         [16]byte
	FSID         [16]byte
}

var ioctlTreeSearch = ioctl.IOWR(btrfsIoctlMagic, 17, unsafe.Sizeof(btrfsIoctlSearchArgs{}))

// TreeSearch performs a tree search ioctl
func TreeSearch(f *os.File, treeID uint64, minObjID, maxObjID uint64, minType, maxType uint32, minOffset, maxOffset uint64) ([]SearchResult, error) {
	var results []SearchResult
	ioctlCount := 0
	totalIoctlTime := time.Duration(0)

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
		ioctlStart := time.Now()
		err := ioctl.Do(f, ioctlTreeSearch, &args)
		totalIoctlTime += time.Since(ioctlStart)
		ioctlCount++
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

			// Only copy data and append for results that match the requested type range
			// This filters out unwanted items when searching across objectids
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

		// If we didn't get any items, we're done
		if !gotItems {
			break
		}

		// Update search key for next iteration based on last item processed
		// Handle offset overflow by incrementing type/objectid if needed
		if lastHdr.Offset == ^uint64(0) {
			// Offset would overflow, move to next type
			if lastHdr.Type == maxType {
				// Type maxed out, move to next object
				if lastHdr.ObjectID == maxObjID {
					// All done
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

	slog.Debug("TreeSearch stats", "treeID", treeID, "minType", minType, "ioctlCount", ioctlCount, "ioctlTime", totalIoctlTime, "results", len(results))
	return results, nil
}

// SearchResult holds a single search result
type SearchResult struct {
	Header btrfsSearchHeader
	Data   []byte
}

// ParseChunk parses a chunk item from search result data
func ParseChunk(data []byte) (*Chunk, error) {
	if len(data) < 48 { // minimum chunk size without stripes
		return nil, fmt.Errorf("chunk data too short: %d bytes", len(data))
	}

	ch := &btrfsChunk{
		Length:     binary.LittleEndian.Uint64(data[0:]),
		Owner:      binary.LittleEndian.Uint64(data[8:]),
		StripeLen:  binary.LittleEndian.Uint64(data[16:]),
		Type:       binary.LittleEndian.Uint64(data[24:]),
		IOAlign:    binary.LittleEndian.Uint32(data[32:]),
		IOWidth:    binary.LittleEndian.Uint32(data[36:]),
		SectorSize: binary.LittleEndian.Uint32(data[40:]),
		NumStripes: binary.LittleEndian.Uint16(data[44:]),
		SubStripes: binary.LittleEndian.Uint16(data[46:]),
	}

	chunk := &Chunk{
		Length:  ch.Length,
		Type:    BlockType(ch.Type & 0x7), // data/metadata/system bits
		Profile: BlockProfile(ch.Type & ^uint64(0x7)), // RAID profile bits
		Stripes: make([]Stripe, ch.NumStripes),
	}

	// Parse stripes
	stripeOffset := 48
	stripeSize := 32 // sizeof btrfsStripe
	for i := uint16(0); i < ch.NumStripes; i++ {
		off := stripeOffset + int(i)*stripeSize
		if off+stripeSize > len(data) {
			break
		}
		chunk.Stripes[i] = Stripe{
			DeviceID: binary.LittleEndian.Uint64(data[off:]),
			Offset:   binary.LittleEndian.Uint64(data[off+8:]),
		}
	}

	return chunk, nil
}

// ParseDevExtent parses a device extent item from search result data
func ParseDevExtent(data []byte) (*DeviceExtent, error) {
	if len(data) < 48 {
		return nil, fmt.Errorf("dev extent data too short: %d bytes", len(data))
	}

	return &DeviceExtent{
		ChunkOffset: binary.LittleEndian.Uint64(data[16:]),
		Length:      binary.LittleEndian.Uint64(data[24:]),
	}, nil
}

// ParseDevItem parses a device item from search result data
func ParseDevItem(data []byte) (*Device, error) {
	if len(data) < 98 {
		return nil, fmt.Errorf("dev item data too short: %d bytes", len(data))
	}

	dev := &Device{
		ID:        binary.LittleEndian.Uint64(data[0:]),
		TotalSize: binary.LittleEndian.Uint64(data[8:]),
	}
	copy(dev.UUID[:], data[66:82])

	return dev, nil
}

// BlockGroupItem represents a block group's usage information
type BlockGroupItem struct {
	LogicalOffset uint64 // From the search key
	Length        uint64 // From the search key (offset field)
	Used          uint64 // Bytes used within this block group
	Flags         uint64 // Type flags (same as chunk type)
}

// ParseBlockGroupItem parses a block group item from search result data
func ParseBlockGroupItem(data []byte) (*BlockGroupItem, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("block group item data too short: %d bytes", len(data))
	}

	return &BlockGroupItem{
		Used:  binary.LittleEndian.Uint64(data[0:]),
		// chunk_objectid at offset 8 (always 256, not needed)
		Flags: binary.LittleEndian.Uint64(data[16:]),
	}, nil
}

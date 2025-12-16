package btdu

import (
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/dennwc/ioctl"
)

// Chunk represents an allocated chunk in the filesystem.
type Chunk struct {
	LogicalOffset uint64
	Length        uint64
	Type          uint64 // BTRFS_BLOCK_GROUP_* flags
}

// ChunkList holds all chunks and provides sampling support.
type ChunkList struct {
	Chunks    []Chunk
	TotalSize uint64 // Sum of all chunk lengths
}

// btrfs tree search constants
const (
	btrfsIoctlMagic             = 0x94
	btrfsChunkTreeObjectID      = 3
	btrfsFirstChunkTreeObjectID = 256
	btrfsChunkItemKey           = 228
	btrfsSearchArgsBufSize      = 4096 - 104 // 4096 - sizeof(search_key)
)

// ioctl number for TREE_SEARCH
var ioctlTreeSearch = ioctl.IOWR(btrfsIoctlMagic, 17, unsafe.Sizeof(btrfsIoctlSearchArgs{}))

// btrfsIoctlSearchKey matches struct btrfs_ioctl_search_key
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
	_            uint32 // padding
	_            uint64 // unused1
	_            uint64 // unused2
	_            uint64 // unused3
	_            uint64 // unused4
}

// btrfsIoctlSearchArgs matches struct btrfs_ioctl_search_args
type btrfsIoctlSearchArgs struct {
	Key btrfsIoctlSearchKey
	Buf [btrfsSearchArgsBufSize]byte
}

// btrfsIoctlSearchHeader matches struct btrfs_ioctl_search_header
type btrfsIoctlSearchHeader struct {
	TransID  uint64
	ObjectID uint64
	Offset   uint64
	Type     uint32
	Len      uint32
}

// btrfsChunk matches the start of struct btrfs_chunk (we only need type and length)
type btrfsChunk struct {
	Length        uint64
	Owner         uint64
	StripeLen     uint64
	Type          uint64
	IOAlign       uint32
	IOWidth       uint32
	SectorSize    uint32
	NumStripes    uint16
	SubStripes    uint16
	// Followed by btrfs_stripe array
}

// Block group type constants
const (
	btrfsBlockGroupData     = 1 << 0
	btrfsBlockGroupSystem   = 1 << 1
	btrfsBlockGroupMetadata = 1 << 2
)

// EnumerateChunks reads all chunks from the filesystem's chunk tree.
// If dataOnly is true, only DATA chunks are included (for sampling).
func EnumerateChunks(fsFile *os.File) (*ChunkList, error) {
	return enumerateChunksFiltered(fsFile, false)
}

// EnumerateDataChunks reads only DATA chunks from the filesystem's chunk tree.
func EnumerateDataChunks(fsFile *os.File) (*ChunkList, error) {
	return enumerateChunksFiltered(fsFile, true)
}

func enumerateChunksFiltered(fsFile *os.File, dataOnly bool) (*ChunkList, error) {
	chunks := &ChunkList{}

	args := btrfsIoctlSearchArgs{}
	args.Key.TreeID = btrfsChunkTreeObjectID
	args.Key.MinObjectID = btrfsFirstChunkTreeObjectID
	args.Key.MaxObjectID = btrfsFirstChunkTreeObjectID
	args.Key.MinType = btrfsChunkItemKey
	args.Key.MaxType = btrfsChunkItemKey
	args.Key.MinOffset = 0
	args.Key.MaxOffset = ^uint64(0)
	args.Key.MinTransID = 0
	args.Key.MaxTransID = ^uint64(0)
	args.Key.NrItems = 4096

	for {
		err := ioctl.Do(fsFile, ioctlTreeSearch, &args)
		if err != nil {
			return nil, fmt.Errorf("tree search ioctl: %w", err)
		}

		if args.Key.NrItems == 0 {
			break
		}

		offset := 0
		for i := uint32(0); i < args.Key.NrItems; i++ {
			if offset+int(unsafe.Sizeof(btrfsIoctlSearchHeader{})) > len(args.Buf) {
				break
			}

			// Parse header
			header := btrfsIoctlSearchHeader{
				TransID:  binary.LittleEndian.Uint64(args.Buf[offset:]),
				ObjectID: binary.LittleEndian.Uint64(args.Buf[offset+8:]),
				Offset:   binary.LittleEndian.Uint64(args.Buf[offset+16:]),
				Type:     binary.LittleEndian.Uint32(args.Buf[offset+24:]),
				Len:      binary.LittleEndian.Uint32(args.Buf[offset+28:]),
			}
			offset += int(unsafe.Sizeof(btrfsIoctlSearchHeader{}))

			if header.Type == btrfsChunkItemKey {
				// Parse chunk data
				if offset+int(unsafe.Sizeof(btrfsChunk{})) <= len(args.Buf) {
					chunk := btrfsChunk{
						Length: binary.LittleEndian.Uint64(args.Buf[offset:]),
						Type:   binary.LittleEndian.Uint64(args.Buf[offset+24:]),
					}

					// Filter to data chunks only if requested
					if !dataOnly || (chunk.Type&btrfsBlockGroupData != 0) {
						chunks.Chunks = append(chunks.Chunks, Chunk{
							LogicalOffset: header.Offset,
							Length:        chunk.Length,
							Type:          chunk.Type,
						})
						chunks.TotalSize += chunk.Length
					}
				}
			}

			offset += int(header.Len)

			// Update search key for next iteration
			args.Key.MinOffset = header.Offset + 1
		}

		// Reset for next search
		args.Key.NrItems = 4096
	}

	return chunks, nil
}

// SamplePosition maps a random position (0 to TotalSize) to a logical address.
func (cl *ChunkList) SamplePosition(pos uint64) uint64 {
	var accumulated uint64
	for _, chunk := range cl.Chunks {
		if accumulated+chunk.Length > pos {
			// Position falls within this chunk
			offsetInChunk := pos - accumulated
			return chunk.LogicalOffset + offsetInChunk
		}
		accumulated += chunk.Length
	}
	// Shouldn't happen if pos < TotalSize
	if len(cl.Chunks) > 0 {
		last := cl.Chunks[len(cl.Chunks)-1]
		return last.LogicalOffset + last.Length - 1
	}
	return 0
}

// IsDataChunk returns true if the given logical address is in a data chunk.
func (cl *ChunkList) IsDataChunk(logicalAddr uint64) bool {
	for _, chunk := range cl.Chunks {
		if logicalAddr >= chunk.LogicalOffset && logicalAddr < chunk.LogicalOffset+chunk.Length {
			return chunk.Type&btrfsBlockGroupData != 0
		}
	}
	return false
}

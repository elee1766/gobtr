package btdu

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"unsafe"

	"github.com/dennwc/ioctl"
)

// ioctl numbers for BTRFS operations
var (
	ioctlLogicalIno = ioctl.IOWR(btrfsIoctlMagic, 36, unsafe.Sizeof(btrfsIoctlLogicalInoArgs{}))
	ioctlInoLookup  = ioctl.IOWR(btrfsIoctlMagic, 18, unsafe.Sizeof(btrfsIoctlInoLookupArgs{}))
)

// btrfsIoctlLogicalInoArgs matches struct btrfs_ioctl_logical_ino_args
type btrfsIoctlLogicalInoArgs struct {
	Logical   uint64
	Size      uint64
	Reserved  [3]uint64
	Flags     uint64
	Inodes    uint64
}

// btrfsIoctlInoLookupArgs matches struct btrfs_ioctl_ino_lookup_args
type btrfsIoctlInoLookupArgs struct {
	TreeID   uint64
	ObjectID uint64
	Name     [4080]byte
}

const (
	logicalInoArgsSize = 4096
)

// btrfsDataContainer matches struct btrfs_data_container
type btrfsDataContainer struct {
	BytesLeft    uint32
	BytesMissing uint32
	ElemCnt      uint32
	ElemMissed   uint32
	// followed by val[] array
}

// logicalInoImpl performs LOGICAL_INO ioctl to find inodes for a logical address.
func logicalInoImpl(f *os.File, logical uint64) ([]InodeResult, error) {
	// Result buffer - needs to be separate from args struct
	resultBufSize := logicalInoArgsSize - 56
	resultBuf := make([]byte, resultBufSize)

	// Set up args struct
	args := btrfsIoctlLogicalInoArgs{
		Logical: logical,
		Size:    uint64(resultBufSize),
		Flags:   0,
		Inodes:  uint64(uintptr(unsafe.Pointer(&resultBuf[0]))),
	}

	err := ioctl.Do(f, ioctlLogicalIno, &args)
	if err != nil {
		return nil, fmt.Errorf("logical_ino ioctl: %w", err)
	}

	// Parse btrfs_data_container header
	elemCnt := binary.LittleEndian.Uint32(resultBuf[8:])
	if elemCnt == 0 {
		return nil, nil
	}

	// Each result is 3 uint64s: inum, offset, root
	// Results start at offset 16 (after the header)
	var results []InodeResult
	offset := 16
	for i := uint32(0); i < elemCnt && offset+24 <= len(resultBuf); i++ {
		results = append(results, InodeResult{
			Inum:   binary.LittleEndian.Uint64(resultBuf[offset:]),
			Offset: binary.LittleEndian.Uint64(resultBuf[offset+8:]),
			Root:   binary.LittleEndian.Uint64(resultBuf[offset+16:]),
		})
		offset += 24
	}

	return results, nil
}

// inodeLookupImpl performs INO_LOOKUP ioctl to resolve inode to path.
func inodeLookupImpl(f *os.File, treeID, objectID uint64) (string, error) {
	args := btrfsIoctlInoLookupArgs{
		TreeID:   treeID,
		ObjectID: objectID,
	}

	err := ioctl.Do(f, ioctlInoLookup, &args)
	if err != nil {
		return "", fmt.Errorf("ino_lookup ioctl: %w", err)
	}

	// Find null terminator
	var pathLen int
	for pathLen = 0; pathLen < len(args.Name) && args.Name[pathLen] != 0; pathLen++ {
	}

	return string(args.Name[:pathLen]), nil
}

// selectRepresentativePath picks the "best" path from a list of paths
// that all reference the same data. Prefers shorter paths and stable ordering.
func selectRepresentativePath(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return paths[0]
	}

	// Sort for stable ordering, preferring shorter paths
	sorted := make([]string, len(paths))
	copy(sorted, paths)
	sort.Slice(sorted, func(i, j int) bool {
		if len(sorted[i]) != len(sorted[j]) {
			return len(sorted[i]) < len(sorted[j])
		}
		return sorted[i] < sorted[j]
	})

	return sorted[0]
}

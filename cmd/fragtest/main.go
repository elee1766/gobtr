package main

import (
	"fmt"
	"log"

	"github.com/elee1766/gobtr/pkg/fragmap"
)

func main() {
	fsPath := "/mnt/btrfs"

	fmt.Println("Creating scanner for", fsPath)
	scanner, err := fragmap.NewScanner(fsPath)
	if err != nil {
		log.Fatalf("Failed to create scanner: %v", err)
	}
	defer scanner.Close()

	fmt.Println("Scanning...")
	fm, err := scanner.Scan()
	if err != nil {
		log.Fatalf("Failed to scan: %v", err)
	}

	fmt.Printf("Total size: %d\n", fm.TotalSize)
	fmt.Printf("Devices: %d\n", len(fm.Devices))
	for _, dev := range fm.Devices {
		fmt.Printf("  Device %d: %d bytes\n", dev.ID, dev.TotalSize)
	}
	fmt.Printf("Chunks: %d\n", len(fm.Chunks))
	for i, chunk := range fm.Chunks {
		if i < 5 {
			fmt.Printf("  Chunk %d: offset=%d len=%d type=%d\n", i, chunk.LogicalOffset, chunk.Length, chunk.Type)
		}
	}
}

package btdu

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFsPathToFilename(t *testing.T) {
	tests := []struct {
		fsPath   string
		expected string
	}{
		{"/", "root.gob"},
		{"/mnt/data", "mnt-data.gob"},
		{"/home/user", "home-user.gob"},
		{"/mnt/my-drive", "mnt-my-drive.gob"},
		{"/var/lib/docker", "var-lib-docker.gob"},
	}

	for _, tt := range tests {
		t.Run(tt.fsPath, func(t *testing.T) {
			got := fsPathToFilename(tt.fsPath)
			if got != tt.expected {
				t.Errorf("fsPathToFilename(%q) = %q, want %q", tt.fsPath, got, tt.expected)
			}
		})
	}
}

func TestFilenameToFSPath(t *testing.T) {
	tests := []struct {
		filename string
		expected string
	}{
		{"root.gob", "/"},
		{"mnt-data.gob", "/mnt/data"},
		{"home-user.gob", "/home/user"},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := filenameToFSPath(tt.filename)
			if got != tt.expected {
				t.Errorf("filenameToFSPath(%q) = %q, want %q", tt.filename, got, tt.expected)
			}
		})
	}
}

func TestLongPathHashing(t *testing.T) {
	longPath := "/mnt/very/long/path/that/exceeds/the/maximum/allowed/filename/length/on/most/filesystems/and/needs/truncation"
	filename := fsPathToFilename(longPath)

	if len(filename) > 80 { // 64 + .gob + some margin
		t.Errorf("filename too long: %d chars", len(filename))
	}

	// Ensure uniqueness - different long paths should produce different filenames
	longPath2 := "/mnt/very/long/path/that/exceeds/the/maximum/allowed/filename/length/on/most/filesystems/and/needs/different"
	filename2 := fsPathToFilename(longPath2)

	if filename == filename2 {
		t.Error("different long paths produced same filename")
	}
}

func TestStore(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Initially no session
	if store.Has("/mnt/data") {
		t.Error("expected no session initially")
	}

	// Create and save a session
	session := NewSession("/mnt/data", 1000000)
	session.AddSample("/home/user/file.txt", Represented, Offset{}, time.Millisecond)

	if err := store.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Now it should exist
	if !store.Has("/mnt/data") {
		t.Error("expected session to exist after save")
	}

	// Load it back
	loaded, err := store.Load("/mnt/data")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.FSPath != "/mnt/data" {
		t.Errorf("FSPath mismatch: got %s", loaded.FSPath)
	}
	if loaded.SampleCount != 1 {
		t.Errorf("SampleCount mismatch: got %d", loaded.SampleCount)
	}

	// Delete it
	if err := store.Delete("/mnt/data"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if store.Has("/mnt/data") {
		t.Error("expected session to be deleted")
	}
}

func TestStoreLoadOrCreate(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// First call creates new
	s1, resumed, err := store.LoadOrCreate("/mnt/data", 1000000)
	if err != nil {
		t.Fatalf("LoadOrCreate failed: %v", err)
	}
	if resumed {
		t.Error("expected new session")
	}

	// Add data and save
	s1.AddSample("/test", Represented, Offset{}, time.Millisecond)
	if err := store.Save(s1); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Second call resumes
	s2, resumed, err := store.LoadOrCreate("/mnt/data", 1000000)
	if err != nil {
		t.Fatalf("LoadOrCreate failed: %v", err)
	}
	if !resumed {
		t.Error("expected resumed session")
	}
	if s2.SampleCount != 1 {
		t.Errorf("expected 1 sample, got %d", s2.SampleCount)
	}
}

func TestStoreList(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Create sessions for multiple filesystems
	fsPaths := []string{"/mnt/data", "/home", "/var/lib"}
	for _, fsPath := range fsPaths {
		session := NewSession(fsPath, 1000000)
		if err := store.Save(session); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	// List them
	list, err := store.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(list) != len(fsPaths) {
		t.Errorf("expected %d sessions, got %d", len(fsPaths), len(list))
	}

	// Verify all fs paths are present
	found := make(map[string]bool)
	for _, info := range list {
		found[info.FSPath] = true
	}
	for _, fsPath := range fsPaths {
		if !found[fsPath] {
			t.Errorf("missing session for %s", fsPath)
		}
	}
}

func TestStorePath(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	path := store.Path("/mnt/data")
	expected := filepath.Join(tmpDir, "mnt-data.gob")
	if path != expected {
		t.Errorf("Path() = %q, want %q", path, expected)
	}
}

func TestStoreMultipleFilesystems(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	// Create sessions for multiple filesystems with different data
	s1 := NewSession("/mnt/ssd", 500_000_000_000)    // 500GB
	s2 := NewSession("/mnt/hdd", 4_000_000_000_000)  // 4TB
	s3 := NewSession("/mnt/nvme", 2_000_000_000_000) // 2TB

	s1.AddSample("/data/file1.txt", Represented, Offset{}, time.Millisecond)
	s2.AddSample("/backup/archive.tar", Exclusive, Offset{}, time.Millisecond)
	s2.AddSample("/backup/archive2.tar", Exclusive, Offset{}, time.Millisecond)
	s3.AddSample("/vm/disk.qcow2", Shared, Offset{}, time.Millisecond)

	for _, s := range []*Session{s1, s2, s3} {
		if err := store.Save(s); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	// Load and verify each independently
	loaded1, err := store.Load("/mnt/ssd")
	if err != nil {
		t.Fatalf("Load /mnt/ssd failed: %v", err)
	}
	if loaded1.TotalSize != 500_000_000_000 {
		t.Errorf("ssd TotalSize wrong")
	}
	if loaded1.SampleCount != 1 {
		t.Errorf("ssd SampleCount wrong: %d", loaded1.SampleCount)
	}

	loaded2, err := store.Load("/mnt/hdd")
	if err != nil {
		t.Fatalf("Load /mnt/hdd failed: %v", err)
	}
	if loaded2.SampleCount != 2 {
		t.Errorf("hdd SampleCount wrong: %d", loaded2.SampleCount)
	}
}

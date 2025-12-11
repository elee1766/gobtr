package btdu

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewSession(t *testing.T) {
	s := NewSession("/mnt/data", 64*1024*1024*1024*1024) // 64TB

	if s.FSPath != "/mnt/data" {
		t.Errorf("expected FSPath /mnt/data, got %s", s.FSPath)
	}
	if s.TotalSize != 64*1024*1024*1024*1024 {
		t.Errorf("expected TotalSize 64TB, got %d", s.TotalSize)
	}
	if s.Root == nil {
		t.Error("expected Root to be non-nil")
	}
}

func TestAddSample(t *testing.T) {
	s := NewSession("/mnt/data", 1000000)

	// Add samples to a path
	s.AddSample("/home/user/file.txt", Represented, Offset{Physical: 100, Logical: 200}, time.Millisecond)
	s.AddSample("/home/user/file.txt", Represented, Offset{Physical: 101, Logical: 201}, time.Millisecond)
	s.AddSample("/home/user/other.txt", Exclusive, Offset{Physical: 300, Logical: 400}, time.Millisecond)

	if s.SampleCount != 3 {
		t.Errorf("expected 3 samples, got %d", s.SampleCount)
	}

	// Check the node exists
	node := s.GetPath("/home/user/file.txt")
	if node == nil {
		t.Fatal("expected node to exist")
	}
	if node.Stats.Data[Represented].Samples != 2 {
		t.Errorf("expected 2 represented samples, got %d", node.Stats.Data[Represented].Samples)
	}

	// Check aggregation to parent
	parent := s.GetPath("/home/user")
	if parent == nil {
		t.Fatal("expected parent node to exist")
	}
	// Parent should have 3 samples total (2 represented + 1 exclusive)
	if parent.Stats.Data[Represented].Samples != 2 {
		t.Errorf("expected parent to have 2 represented samples, got %d", parent.Stats.Data[Represented].Samples)
	}
	if parent.Stats.Data[Exclusive].Samples != 1 {
		t.Errorf("expected parent to have 1 exclusive sample, got %d", parent.Stats.Data[Exclusive].Samples)
	}
}

func TestSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	sessionFile := filepath.Join(tmpDir, "test-session.gob")

	// Create and populate session
	s := NewSession("/mnt/data", 1000000)
	s.AddSample("/home/user/file.txt", Represented, Offset{Physical: 100}, time.Millisecond)
	s.AddSample("/home/user/file.txt", Exclusive, Offset{Physical: 200}, time.Millisecond*2)
	s.AddSample("/var/log/syslog", Shared, Offset{Physical: 300}, time.Millisecond*3)

	// Save
	if err := s.Save(sessionFile); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(sessionFile); os.IsNotExist(err) {
		t.Fatal("session file not created")
	}

	// Load
	loaded, err := Load(sessionFile)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify metadata
	if loaded.FSPath != s.FSPath {
		t.Errorf("FSPath mismatch: got %s, want %s", loaded.FSPath, s.FSPath)
	}
	if loaded.TotalSize != s.TotalSize {
		t.Errorf("TotalSize mismatch: got %d, want %d", loaded.TotalSize, s.TotalSize)
	}
	if loaded.SampleCount != s.SampleCount {
		t.Errorf("SampleCount mismatch: got %d, want %d", loaded.SampleCount, s.SampleCount)
	}

	// Verify data
	node := loaded.GetPath("/home/user/file.txt")
	if node == nil {
		t.Fatal("expected node to exist after load")
	}
	if node.Stats.Data[Represented].Samples != 1 {
		t.Errorf("expected 1 represented sample, got %d", node.Stats.Data[Represented].Samples)
	}
	if node.Stats.Data[Exclusive].Samples != 1 {
		t.Errorf("expected 1 exclusive sample, got %d", node.Stats.Data[Exclusive].Samples)
	}
}

func TestLoadOrCreate(t *testing.T) {
	tmpDir := t.TempDir()
	sessionFile := filepath.Join(tmpDir, "session.gob")

	// First call should create new
	s1, resumed, err := LoadOrCreate(sessionFile, "/mnt/data", 1000000)
	if err != nil {
		t.Fatalf("LoadOrCreate failed: %v", err)
	}
	if resumed {
		t.Error("expected new session, got resumed")
	}

	// Add some data and save
	s1.AddSample("/test/path", Represented, Offset{}, time.Millisecond)
	if err := s1.Save(sessionFile); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Second call should resume
	s2, resumed, err := LoadOrCreate(sessionFile, "/mnt/data", 1000000)
	if err != nil {
		t.Fatalf("LoadOrCreate failed: %v", err)
	}
	if !resumed {
		t.Error("expected resumed session, got new")
	}
	if s2.SampleCount != 1 {
		t.Errorf("expected 1 sample after resume, got %d", s2.SampleCount)
	}
}

func TestTopPaths(t *testing.T) {
	s := NewSession("/mnt/data", 1000000)

	// Add samples with varying counts
	for i := 0; i < 10; i++ {
		s.AddSample("/high/count/file.txt", Represented, Offset{}, time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		s.AddSample("/medium/count/file.txt", Represented, Offset{}, time.Millisecond)
	}
	s.AddSample("/low/count/file.txt", Represented, Offset{}, time.Millisecond)

	top := s.TopPaths(Represented, 2)
	if len(top) != 2 {
		t.Fatalf("expected 2 top paths, got %d", len(top))
	}
	if top[0].Path != "/high/count/file.txt" {
		t.Errorf("expected /high/count/file.txt first, got %s", top[0].Path)
	}
	if top[0].Samples != 10 {
		t.Errorf("expected 10 samples, got %d", top[0].Samples)
	}
}

func TestPathNode_FullPath(t *testing.T) {
	root := NewRootNode()
	node := root.GetOrCreatePath("/home/user/documents/file.txt")

	if node.FullPath() != "/home/user/documents/file.txt" {
		t.Errorf("expected /home/user/documents/file.txt, got %s", node.FullPath())
	}

	if root.FullPath() != "/" {
		t.Errorf("expected /, got %s", root.FullPath())
	}
}

func TestStats(t *testing.T) {
	s := NewSession("/mnt/data", 1000000)
	s.AddSample("/a/b/c", Represented, Offset{}, time.Millisecond)
	s.AddSample("/a/b/d", Represented, Offset{}, time.Millisecond)
	s.AddSample("/x/y", Represented, Offset{}, time.Millisecond)

	stats := s.Stats()
	if stats.SampleCount != 3 {
		t.Errorf("expected 3 samples, got %d", stats.SampleCount)
	}
	// Nodes: root, a, b, c, d, x, y = 7
	if stats.UniquePathCount != 7 {
		t.Errorf("expected 7 unique paths, got %d", stats.UniquePathCount)
	}
	// Depth: /a/b/c = 3
	if stats.MaxDepth != 3 {
		t.Errorf("expected max depth 3, got %d", stats.MaxDepth)
	}
}

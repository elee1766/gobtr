package btdu

import "time"

// SessionInterface defines the common interface for session implementations.
// This allows both in-memory Session and disk-backed BoltSession to be used interchangeably.
type SessionInterface interface {
	// AddSample adds a sample to the session
	AddSample(path string, sampleType SampleType, offset Offset, duration time.Duration) error

	// GetPathStats returns stats for a specific path
	GetPathStats(path string) (*PathStats, error)

	// GetChildren returns direct children of a path
	GetChildren(parentPath string) ([]ChildInfo, error)

	// StartRun marks the beginning of sampling
	StartRun()

	// StopRun marks the end of sampling
	StopRun()

	// Flush persists any pending changes
	Flush() error

	// Close closes the session
	Close() error

	// Metadata accessors
	FSPath() string
	TotalSize() uint64
	SampleCount() uint64
	GetRunningTime() time.Duration
	StartedAt() time.Time
	LastUpdated() time.Time
	PathCount() int

	// TopPaths returns top paths by sample count
	TopPaths(sampleType SampleType, n int) ([]RankedPath, error)
}

// Ensure both implementations satisfy the interface
var _ SessionInterface = (*BoltSession)(nil)
var _ SessionInterface = (*MemorySession)(nil)

// MemorySession wraps the original Session to implement SessionInterface.
// This allows the old in-memory implementation to be used with the new interface.
type MemorySession struct {
	*Session
}

// NewMemorySession creates a new in-memory session wrapper.
func NewMemorySession(fsPath string, totalSize uint64) *MemorySession {
	return &MemorySession{
		Session: NewSession(fsPath, totalSize),
	}
}

// WrapSession wraps an existing Session in the interface.
func WrapSession(s *Session) *MemorySession {
	return &MemorySession{Session: s}
}

// AddSample implements SessionInterface.
func (m *MemorySession) AddSample(path string, sampleType SampleType, offset Offset, duration time.Duration) error {
	m.Session.AddSample(path, sampleType, offset, duration)
	return nil
}

// GetPathStats implements SessionInterface.
func (m *MemorySession) GetPathStats(path string) (*PathStats, error) {
	node := m.Session.GetPath(path)
	if node == nil {
		return &PathStats{}, nil
	}
	return &node.Stats, nil
}

// GetChildren implements SessionInterface.
func (m *MemorySession) GetChildren(parentPath string) ([]ChildInfo, error) {
	if parentPath == "" {
		parentPath = "/"
	}

	node := m.Session.Root.GetPath(parentPath)
	if node == nil {
		return nil, nil
	}

	children := node.DirectChildren()
	result := make([]ChildInfo, 0, len(children))
	for name, child := range children {
		result = append(result, ChildInfo{
			Name:  name,
			Path:  child.FullPath(),
			Stats: child.Stats,
		})
	}
	return result, nil
}

// StartRun implements SessionInterface.
func (m *MemorySession) StartRun() {
	m.Session.StartRun()
}

// StopRun implements SessionInterface.
func (m *MemorySession) StopRun() {
	m.Session.StopRun()
}

// Flush implements SessionInterface.
func (m *MemorySession) Flush() error {
	// In-memory session doesn't need explicit flush
	return nil
}

// Close implements SessionInterface.
func (m *MemorySession) Close() error {
	// In-memory session doesn't need close
	return nil
}

// FSPath implements SessionInterface.
func (m *MemorySession) FSPath() string {
	return m.Session.FSPath
}

// TotalSize implements SessionInterface.
func (m *MemorySession) TotalSize() uint64 {
	return m.Session.TotalSize
}

// SampleCount implements SessionInterface.
func (m *MemorySession) SampleCount() uint64 {
	return m.Session.SampleCount
}

// GetRunningTime implements SessionInterface.
func (m *MemorySession) GetRunningTime() time.Duration {
	return m.Session.GetRunningTime()
}

// StartedAt implements SessionInterface.
func (m *MemorySession) StartedAt() time.Time {
	return m.Session.StartedAt
}

// LastUpdated implements SessionInterface.
func (m *MemorySession) LastUpdated() time.Time {
	return m.Session.LastUpdated
}

// PathCount implements SessionInterface.
func (m *MemorySession) PathCount() int {
	var count int
	m.Session.Root.Walk(func(node *PathNode, depth int) bool {
		count++
		return true
	})
	return count
}

// TopPaths implements SessionInterface.
func (m *MemorySession) TopPaths(sampleType SampleType, n int) ([]RankedPath, error) {
	return m.Session.TopPaths(sampleType, n), nil
}

// GetRootNode returns the root PathNode for legacy code that needs direct access.
// This is only available on MemorySession.
func (m *MemorySession) GetRootNode() *PathNode {
	return m.Session.Root
}

// GetNode returns a PathNode for the given path.
// This is only available on MemorySession.
func (m *MemorySession) GetNode(path string) *PathNode {
	return m.Session.Root.GetPath(path)
}

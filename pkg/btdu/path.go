package btdu

import (
	"strings"
	"sync"
	"time"
)

// PathNode represents a node in the path trie.
// Each node corresponds to a directory or file segment.
type PathNode struct {
	Name     string              // Segment name (e.g., "home", "user", "file.txt")
	Parent   *PathNode           // Parent node (nil for root)
	Children map[string]*PathNode // Child nodes keyed by name
	Stats    PathStats           // Sample statistics for this node

	mu sync.RWMutex // Protects concurrent access
}

// NewPathNode creates a new path node.
func NewPathNode(name string, parent *PathNode) *PathNode {
	return &PathNode{
		Name:     name,
		Parent:   parent,
		Children: make(map[string]*PathNode),
	}
}

// NewRootNode creates a new root node for the trie.
func NewRootNode() *PathNode {
	return NewPathNode("", nil)
}

// GetOrCreateChild returns an existing child or creates a new one.
func (n *PathNode) GetOrCreateChild(name string) *PathNode {
	n.mu.Lock()
	defer n.mu.Unlock()

	if child, ok := n.Children[name]; ok {
		return child
	}

	child := NewPathNode(name, n)
	n.Children[name] = child
	return child
}

// GetChild returns a child by name, or nil if not found.
func (n *PathNode) GetChild(name string) *PathNode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Children[name]
}

// GetOrCreatePath returns the node for the given path, creating nodes as needed.
// Path should be absolute (e.g., "/home/user/file.txt").
func (n *PathNode) GetOrCreatePath(path string) *PathNode {
	segments := splitPath(path)
	current := n
	for _, seg := range segments {
		current = current.GetOrCreateChild(seg)
	}
	return current
}

// GetPath returns the node for the given path, or nil if not found.
func (n *PathNode) GetPath(path string) *PathNode {
	segments := splitPath(path)
	current := n
	for _, seg := range segments {
		current = current.GetChild(seg)
		if current == nil {
			return nil
		}
	}
	return current
}

// AddSample adds a sample to this node and propagates to ancestors.
func (n *PathNode) AddSample(sampleType SampleType, offset Offset, duration time.Duration) {
	n.mu.Lock()
	n.Stats.AddSample(sampleType, offset, duration)
	n.mu.Unlock()

	// Propagate to parent (aggregation)
	if n.Parent != nil {
		n.Parent.AddSample(sampleType, offset, duration)
	}
}

// AddDistributedSample adds a distributed sample share to this node and ancestors.
func (n *PathNode) AddDistributedSample(sampleShare, durationShare float64) {
	n.mu.Lock()
	n.Stats.DistributedSamples += sampleShare
	n.Stats.DistributedDuration += durationShare
	n.mu.Unlock()

	if n.Parent != nil {
		n.Parent.AddDistributedSample(sampleShare, durationShare)
	}
}

// FullPath returns the full path from root to this node.
func (n *PathNode) FullPath() string {
	if n.Parent == nil {
		return "/"
	}

	var parts []string
	for node := n; node != nil && node.Parent != nil; node = node.Parent {
		parts = append(parts, node.Name)
	}

	// Reverse
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}

	return "/" + strings.Join(parts, "/")
}

// IsLeaf returns true if this node has no children.
func (n *PathNode) IsLeaf() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.Children) == 0
}

// ChildCount returns the number of children.
func (n *PathNode) ChildCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.Children)
}

// DirectChildren returns a copy of direct children (thread-safe).
func (n *PathNode) DirectChildren() map[string]*PathNode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make(map[string]*PathNode, len(n.Children))
	for name, child := range n.Children {
		result[name] = child
	}
	return result
}

// Walk traverses the trie depth-first, calling fn for each node.
func (n *PathNode) Walk(fn func(node *PathNode, depth int) bool) {
	n.walk(fn, 0)
}

func (n *PathNode) walk(fn func(node *PathNode, depth int) bool, depth int) {
	if !fn(n, depth) {
		return
	}

	n.mu.RLock()
	children := make([]*PathNode, 0, len(n.Children))
	for _, child := range n.Children {
		children = append(children, child)
	}
	n.mu.RUnlock()

	for _, child := range children {
		child.walk(fn, depth+1)
	}
}

// Delete removes this node from its parent.
func (n *PathNode) Delete() {
	if n.Parent == nil {
		return
	}

	n.Parent.mu.Lock()
	delete(n.Parent.Children, n.Name)
	n.Parent.mu.Unlock()
}

// splitPath splits a path into segments, handling edge cases.
func splitPath(path string) []string {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// SerializedNode is the serialization-friendly version of PathNode.
// Used for gob encoding (which doesn't handle cyclic pointers well).
type SerializedNode struct {
	Name     string
	Stats    PathStats
	Children []SerializedNode
}

// Serialize converts the trie to a serialization-friendly format.
func (n *PathNode) Serialize() SerializedNode {
	n.mu.RLock()
	defer n.mu.RUnlock()

	sn := SerializedNode{
		Name:     n.Name,
		Stats:    n.Stats,
		Children: make([]SerializedNode, 0, len(n.Children)),
	}

	for _, child := range n.Children {
		sn.Children = append(sn.Children, child.Serialize())
	}

	return sn
}

// Deserialize rebuilds the trie from a serialized format.
func (sn *SerializedNode) Deserialize(parent *PathNode) *PathNode {
	node := &PathNode{
		Name:     sn.Name,
		Parent:   parent,
		Stats:    sn.Stats,
		Children: make(map[string]*PathNode, len(sn.Children)),
	}

	for i := range sn.Children {
		child := sn.Children[i].Deserialize(node)
		node.Children[child.Name] = child
	}

	return node
}

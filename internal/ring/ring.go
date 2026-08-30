// Package ring implements EdgeMesh's consistent hash ring.
//
// The ring maps cache keys to the edge nodes that own them. It is implemented
// here rather than pulled from a library because ownership stability under
// membership change is one of the properties this project exists to
// demonstrate: adding an Nth node should move roughly 1/N of the keyspace,
// where modulo hashing would move nearly all of it.
//
// # Design
//
// Each physical node is projected onto the ring at VirtualNodes positions
// derived from hashing "<node-id>#<replica-index>". A key's owner is the first
// virtual node clockwise from the key's hash; additional replicas are the next
// distinct *physical* nodes clockwise from there.
//
// # Concurrency
//
// A Ring is immutable once built. Membership changes produce an entirely new
// Ring which is published through an atomic pointer swap (see Holder). Readers
// on the request hot path therefore never take a lock and never observe a
// half-updated ring.
package ring

import (
	"sort"
	"strconv"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// DefaultVirtualNodes is the per-node replica count on the ring. 128 keeps the
// standard deviation of load across a small cluster within a few percent while
// keeping ring construction cheap (nodes * 128 hashes).
const DefaultVirtualNodes = 128

// Node is a ring member. Only ID participates in hashing; the remaining fields
// travel with the node so callers can dial it without a second lookup.
type Node struct {
	ID          string
	PeerAddress string
	Region      string
	Zone        string
	// Weight scales the node's share of the ring. Zero and one are equivalent.
	// Weighted placement is a P1 feature; V1 deployments leave this at 1.
	Weight uint32
}

// vnode is one virtual node position on the ring.
type vnode struct {
	hash uint64
	node int32 // index into Ring.nodes
}

// Ring is an immutable ownership snapshot for a fixed membership set.
type Ring struct {
	nodes []Node
	// index maps node ID to its position in nodes.
	index map[string]int32
	// vnodes is sorted by hash ascending. Binary search over this slice is the
	// entire lookup cost.
	vnodes []vnode
	// virtualNodes is the per-node replica count used to build this ring.
	virtualNodes int
	// version is the membership version this ring was built from. It is carried
	// so that observability can report which config produced an ownership
	// decision; it does not affect hashing.
	version uint64
}

// New builds a ring over nodes. Nodes are deduplicated by ID and the result is
// independent of the input order: two rings built from the same set of node IDs
// always produce identical ownership.
//
// virtualNodes <= 0 uses DefaultVirtualNodes.
func New(nodes []Node, virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	// Deduplicate by ID (last write wins for metadata) and sort by ID so ring
	// construction is deterministic regardless of membership event ordering.
	dedup := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		if n.ID == "" {
			continue
		}
		dedup[n.ID] = n
	}
	ids := make([]string, 0, len(dedup))
	for id := range dedup {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	r := &Ring{
		nodes:        make([]Node, 0, len(ids)),
		index:        make(map[string]int32, len(ids)),
		virtualNodes: virtualNodes,
	}
	total := 0
	for _, id := range ids {
		n := dedup[id]
		reps := virtualNodes
		if n.Weight > 1 {
			reps = virtualNodes * int(n.Weight)
		}
		total += reps
	}
	r.vnodes = make([]vnode, 0, total)

	for _, id := range ids {
		n := dedup[id]
		idx := int32(len(r.nodes))
		r.nodes = append(r.nodes, n)
		r.index[id] = idx

		reps := virtualNodes
		if n.Weight > 1 {
			reps = virtualNodes * int(n.Weight)
		}
		for i := 0; i < reps; i++ {
			r.vnodes = append(r.vnodes, vnode{hash: hashVirtual(id, i), node: idx})
		}
	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		if r.vnodes[i].hash != r.vnodes[j].hash {
			return r.vnodes[i].hash < r.vnodes[j].hash
		}
		// Deterministic tie-break. Collisions are astronomically unlikely with
		// a 64-bit hash but the ordering must not depend on sort stability.
		return r.nodes[r.vnodes[i].node].ID < r.nodes[r.vnodes[j].node].ID
	})
	return r
}

// WithVersion returns a copy of r tagged with a membership version. The ring
// contents are shared, not copied: rings are immutable.
func (r *Ring) WithVersion(v uint64) *Ring {
	if r == nil {
		return nil
	}
	c := *r
	c.version = v
	return &c
}

// Version reports the membership version this ring was built from.
func (r *Ring) Version() uint64 {
	if r == nil {
		return 0
	}
	return r.version
}

// hashVirtual derives a virtual node position. The separator keeps
// ("node1", 23) distinct from ("node12", 3).
func hashVirtual(nodeID string, replica int) uint64 {
	var buf [64]byte
	b := append(buf[:0], nodeID...)
	b = append(b, '#')
	b = strconv.AppendInt(b, int64(replica), 10)
	return xxhash.Sum64(b)
}

// HashKey is the ring's key hash. Exported so tests and benchmarks can build
// synthetic distributions using exactly the production hash.
func HashKey(key string) uint64 { return xxhash.Sum64String(key) }

// Len reports the number of physical nodes on the ring.
func (r *Ring) Len() int {
	if r == nil {
		return 0
	}
	return len(r.nodes)
}

// Empty reports whether the ring has no members. An empty ring owns nothing and
// every lookup fails, which callers must treat as "no peer path available"
// rather than as an error.
func (r *Ring) Empty() bool { return r.Len() == 0 }

// Nodes returns a copy of the membership. Callers must not assume ordering
// beyond it being sorted by node ID.
func (r *Ring) Nodes() []Node {
	if r == nil {
		return nil
	}
	out := make([]Node, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Node looks up a member by ID.
func (r *Ring) Node(id string) (Node, bool) {
	if r == nil {
		return Node{}, false
	}
	i, ok := r.index[id]
	if !ok {
		return Node{}, false
	}
	return r.nodes[i], true
}

// Owner returns the primary owner of key. It reports false only when the ring
// is empty.
func (r *Ring) Owner(key string) (Node, bool) {
	if r == nil || len(r.vnodes) == 0 {
		return Node{}, false
	}
	return r.nodes[r.vnodes[r.search(HashKey(key))].node], true
}

// Owners returns up to n distinct physical nodes for key, starting at the
// primary owner and walking clockwise. The result is shorter than n when the
// ring has fewer than n members; it never contains duplicates.
//
// The returned slice is freshly allocated and owned by the caller.
func (r *Ring) Owners(key string, n int) []Node {
	if r == nil || len(r.vnodes) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	out := make([]Node, 0, n)
	// seen is indexed by physical node position. For the cluster sizes EdgeMesh
	// targets this is far cheaper than a map allocation on the hot path.
	seen := make([]bool, len(r.nodes))

	start := r.search(HashKey(key))
	for i := 0; i < len(r.vnodes) && len(out) < n; i++ {
		vi := start + i
		if vi >= len(r.vnodes) {
			vi -= len(r.vnodes)
		}
		p := r.vnodes[vi].node
		if seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, r.nodes[p])
	}
	return out
}

// IsOwner reports whether nodeID is among the first n owners of key. This is
// the check an edge performs to decide whether it should store a replica.
func (r *Ring) IsOwner(key, nodeID string, n int) bool {
	for _, o := range r.Owners(key, n) {
		if o.ID == nodeID {
			return true
		}
	}
	return false
}

// search returns the index of the first vnode at or clockwise of h, wrapping to
// zero past the end of the ring.
func (r *Ring) search(h uint64) int {
	i := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].hash >= h })
	if i == len(r.vnodes) {
		return 0
	}
	return i
}

// VirtualNodes reports the per-node replica count used to build the ring.
func (r *Ring) VirtualNodes() int {
	if r == nil {
		return 0
	}
	return r.virtualNodes
}

// Holder publishes ring snapshots to hot-path readers.
//
// Load is a single atomic pointer read: request handling never blocks on a ring
// update, and an in-flight request always sees one coherent ring rather than a
// ring being mutated underneath it.
type Holder struct {
	v atomic.Pointer[Ring]
}

// NewHolder returns a Holder seeded with an empty ring so Load never returns
// nil.
func NewHolder() *Holder {
	h := &Holder{}
	h.v.Store(New(nil, DefaultVirtualNodes))
	return h
}

// Store publishes a new ring. A nil ring is ignored: losing membership must not
// be able to nil out the hot path.
func (h *Holder) Store(r *Ring) {
	if r == nil {
		return
	}
	h.v.Store(r)
}

// Load returns the current ring. It never returns nil.
func (h *Holder) Load() *Ring { return h.v.Load() }

package l1

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

// TinyLFU is a frequency-aware admission policy.
//
// Plain LRU admits every candidate, which makes it vulnerable to scans: a burst
// of one-hit keys evicts a working set that will be needed again. TinyLFU
// instead asks "has this candidate been requested more often than the key it
// would displace?" and rejects the candidate when the answer is no.
//
// Frequency is estimated with a Count-Min Sketch: fixed memory, no per-key
// allocation, and a bounded overestimate. Counters are 4 bits and are halved
// once the sample count reaches a threshold, so the estimate tracks a recent
// window rather than all history. This is the "aging"/reset step that keeps
// TinyLFU responsive to a shifting working set.
//
// This is TinyLFU's admission filter only. The main cache remains LRU (see
// Cache), which is the W-TinyLFU shape without the segmented window; it is
// enough to compare admission behaviour, which is what the benchmark measures.
type TinyLFU struct {
	mu sync.Mutex
	// counters holds 4-bit counters packed two per byte.
	counters []byte
	// mask selects a counter index; len(counters)*2 is a power of two.
	mask uint64
	// samples counts Touch calls since the last aging pass.
	samples int
	// resetAt triggers aging.
	resetAt int
	// doorkeeper is a bloom-like bit set that absorbs the very long tail of
	// single-access keys, so the sketch is not polluted by keys seen once.
	doorkeeper  []uint64
	doorMask    uint64
	doorCount   int
	doorResetAt int
}

const (
	// countersPerByte is fixed by the 4-bit counter width.
	countersPerByte = 2
	// maxCount is the saturation value of a 4-bit counter.
	maxCount = 15
	// hashRounds is the Count-Min Sketch depth. Four independent positions keep
	// the overestimate small at a modest memory cost.
	hashRounds = 4
)

// NewTinyLFU builds an admission policy sized for an expected number of
// resident keys. counters is rounded up to a power of two and is typically
// ~10x the resident key count, which keeps sketch error low.
func NewTinyLFU(expectedKeys int) *TinyLFU {
	if expectedKeys < 128 {
		expectedKeys = 128
	}
	n := nextPow2(uint64(expectedKeys) * 10)
	doorBits := nextPow2(uint64(expectedKeys) * 8)

	t := &TinyLFU{
		counters:    make([]byte, n/countersPerByte),
		mask:        n - 1,
		resetAt:     int(n / 2),
		doorkeeper:  make([]uint64, doorBits/64),
		doorMask:    doorBits - 1,
		doorResetAt: int(doorBits / 4),
	}
	return t
}

func nextPow2(v uint64) uint64 {
	if v < 2 {
		return 2
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}

// Name identifies the policy.
func (t *TinyLFU) Name() string { return "tinylfu" }

// Touch records an access.
//
// The first sighting of a key only sets its doorkeeper bit; the sketch is
// incremented from the second sighting onwards. That is what keeps a scan of
// unique keys from inflating the sketch and starving the real working set.
func (t *TinyLFU) Touch(k string) {
	h := xxhash.Sum64String(k)

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.doorTestAndSet(h) {
		t.doorCount++
		if t.doorCount >= t.doorResetAt {
			t.clearDoorkeeper()
		}
		return
	}
	for i := 0; i < hashRounds; i++ {
		t.increment(sketchIndex(h, i) & t.mask)
	}
	t.samples++
	if t.samples >= t.resetAt {
		t.age()
	}
}

// Admit reports whether candidate should displace victim.
//
// A candidate with no victim (the shard has room) is always admitted. A
// candidate is otherwise admitted only when its estimated frequency is at least
// the victim's: ties go to the candidate so a cold cache still fills.
func (t *TinyLFU) Admit(candidate, victim string) bool {
	if victim == "" {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.estimate(candidate) >= t.estimate(victim)
}

// Estimate reports the sketch's frequency estimate for k. Exported for the
// policy benchmark and for tests.
func (t *TinyLFU) Estimate(k string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.estimate(k)
}

// estimate returns the minimum across the sketch's rows, which is the
// Count-Min Sketch's bounded overestimate. The caller must hold t.mu.
func (t *TinyLFU) estimate(k string) int {
	h := xxhash.Sum64String(k)
	min := maxCount + 1
	for i := 0; i < hashRounds; i++ {
		if v := t.counter(sketchIndex(h, i) & t.mask); v < min {
			min = v
		}
	}
	if min > maxCount {
		return 0
	}
	return min
}

// sketchIndex derives the i-th independent position from one 64-bit hash by
// mixing in a per-row constant. This avoids hashing the key four times.
func sketchIndex(h uint64, round int) uint64 {
	const prime = 0x9e3779b97f4a7c15
	return h ^ (uint64(round+1) * prime)
}

func (t *TinyLFU) counter(i uint64) int {
	b := t.counters[i/countersPerByte]
	if i%countersPerByte == 0 {
		return int(b & 0x0f)
	}
	return int(b >> 4)
}

func (t *TinyLFU) increment(i uint64) {
	idx := i / countersPerByte
	b := t.counters[idx]
	if i%countersPerByte == 0 {
		if v := b & 0x0f; v < maxCount {
			t.counters[idx] = (b & 0xf0) | (v + 1)
		}
		return
	}
	if v := b >> 4; v < maxCount {
		t.counters[idx] = (b & 0x0f) | ((v + 1) << 4)
	}
}

// age halves every counter. Halving rather than clearing preserves the relative
// ordering of hot keys while letting cold keys decay out.
func (t *TinyLFU) age() {
	for i, b := range t.counters {
		// Shift both nibbles right by one in a single operation.
		t.counters[i] = (b >> 1) & 0x77
	}
	t.samples /= 2
}

// doorTestAndSet reports whether h was already present, setting its bits.
func (t *TinyLFU) doorTestAndSet(h uint64) bool {
	seen := true
	for i := 0; i < 2; i++ {
		bit := sketchIndex(h, i+hashRounds) & t.doorMask
		word, off := bit/64, bit%64
		mask := uint64(1) << off
		if t.doorkeeper[word]&mask == 0 {
			seen = false
			t.doorkeeper[word] |= mask
		}
	}
	return seen
}

func (t *TinyLFU) clearDoorkeeper() {
	for i := range t.doorkeeper {
		t.doorkeeper[i] = 0
	}
	t.doorCount = 0
}

// Package storage persists Raft state that must survive a process restart.
//
// # What must be durable, and why
//
// Raft's safety argument depends on three things surviving a crash:
//
//   - currentTerm and votedFor, written *before* a vote is granted. A node that
//     forgets it voted could vote twice in one term, electing two leaders.
//   - the log, written before an entry is acknowledged to the leader. A follower
//     that acknowledges an entry and then forgets it lets the leader believe an
//     entry is replicated when it is not.
//   - the newest snapshot plus its last included index and term, so a restarted
//     node can restore its state machine without replaying from index 1.
//
// # Why bbolt
//
// The consensus algorithm is implemented here; the *storage primitive* is not
// the portfolio-defining part, and hand-rolling a crash-safe on-disk B-tree
// would add risk without adding signal. bbolt gives a single-file, transactional,
// fsync-on-commit store, which is exactly the durability contract Raft needs.
package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	raftlog "github.com/sheehanlloyd/edgemesh/internal/raft/log"
)

var (
	bucketMeta     = []byte("meta")
	bucketLog      = []byte("log")
	bucketSnapshot = []byte("snapshot")

	keyCurrentTerm       = []byte("current_term")
	keyVotedFor          = []byte("voted_for")
	keyLastIncludedIndex = []byte("last_included_index")
	keyLastIncludedTerm  = []byte("last_included_term")
	keySnapshotData      = []byte("data")
	keyCommitIndex       = []byte("commit_index")
)

// PersistentState is everything recovered from disk at startup.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Entries     []raftlog.Entry
	// LastIncludedIndex/Term describe the newest durable snapshot.
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	SnapshotData      []byte
	// CommitIndex is a recovery hint, not a safety-critical value: Raft can
	// always recompute the commit index from the leader, but restoring it
	// avoids re-applying entries the state machine has already seen.
	CommitIndex uint64
}

// Store is the durable Raft store.
type Store struct {
	db   *bolt.DB
	path string
}

// Options configures a Store.
type Options struct {
	// Dir holds the database file. It is created if missing.
	Dir string
	// NoSync disables fsync on commit. It is a *test-only* accelerator: with
	// it, a crash can lose acknowledged entries and Raft's guarantees no longer
	// hold. It is never set by the control-plane binary.
	NoSync bool
	// Timeout bounds waiting for the file lock, which surfaces "another process
	// already owns this data directory" as a clear error rather than a hang.
	Timeout time.Duration
}

// Open creates or opens the store.
func Open(o Options) (*Store, error) {
	if o.Dir == "" {
		return nil, errs.New(errs.ClassValidation, "raft storage: data directory is required")
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if err := os.MkdirAll(o.Dir, 0o750); err != nil {
		return nil, errs.Wrap(errs.ClassStorage, err, "create raft data directory %q", o.Dir)
	}
	path := filepath.Join(o.Dir, "raft.db")

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: o.Timeout, NoSync: o.NoSync})
	if err != nil {
		return nil, errs.Wrap(errs.ClassStorage, err, "open raft store %q", path)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketLog, bucketSnapshot} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, errs.Wrap(errs.ClassStorage, err, "initialize raft store buckets")
	}
	return &Store{db: db, path: path}, nil
}

// Path reports the database file location.
func (s *Store) Path() string { return s.path }

// Close releases the database.
func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return errs.Wrap(errs.ClassStorage, err, "close raft store")
	}
	return nil
}

// Load recovers persistent state.
//
// A store that has never been written returns a zero-valued state with no
// error: a fresh node starting at term 0 with an empty log is the normal
// bootstrap case, not a failure.
func (s *Store) Load() (PersistentState, error) {
	var st PersistentState
	err := s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		st.CurrentTerm = getUint64(meta, keyCurrentTerm)
		st.CommitIndex = getUint64(meta, keyCommitIndex)
		if v := meta.Get(keyVotedFor); v != nil {
			st.VotedFor = string(v)
		}

		snap := tx.Bucket(bucketSnapshot)
		st.LastIncludedIndex = getUint64(snap, keyLastIncludedIndex)
		st.LastIncludedTerm = getUint64(snap, keyLastIncludedTerm)
		if d := snap.Get(keySnapshotData); d != nil {
			// bbolt values are only valid inside the transaction, so copy.
			st.SnapshotData = append([]byte(nil), d...)
		}

		logBucket := tx.Bucket(bucketLog)
		return logBucket.ForEach(func(k, v []byte) error {
			var pb edgemeshv1.LogEntry
			if err := proto.Unmarshal(v, &pb); err != nil {
				return errs.Wrap(errs.ClassStorage, err, "decode log entry at key %x", k)
			}
			st.Entries = append(st.Entries, raftlog.FromProto(&pb))
			return nil
		})
	})
	if err != nil {
		return PersistentState{}, errs.Wrap(errs.ClassStorage, err, "load raft state from %q", s.path)
	}
	return st, nil
}

// SaveTermVote durably records the term and vote.
//
// This must complete before a RequestVote response is sent. Responding first
// and persisting later is the classic Raft implementation bug: a crash in that
// window lets the node vote again for a different candidate in the same term.
func (s *Store) SaveTermVote(term uint64, votedFor string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if err := b.Put(keyCurrentTerm, encodeUint64(term)); err != nil {
			return err
		}
		return b.Put(keyVotedFor, []byte(votedFor))
	})
	return errs.Wrap(errs.ClassStorage, err, "persist term %d and vote %q", term, votedFor)
}

// SaveCommitIndex records the commit index as a restart hint.
func (s *Store) SaveCommitIndex(index uint64) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(keyCommitIndex, encodeUint64(index))
	})
	return errs.Wrap(errs.ClassStorage, err, "persist commit index %d", index)
}

// Append durably stores entries.
//
// All entries land in one transaction so a crash cannot leave a partial batch
// on disk, which would violate the log's contiguity invariant.
func (s *Store) Append(entries []raftlog.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		for _, e := range entries {
			data, err := proto.Marshal(e.ToProto())
			if err != nil {
				return errs.Wrap(errs.ClassStorage, err, "encode log entry %d", e.Index)
			}
			if err := b.Put(encodeUint64(e.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
	return errs.Wrap(errs.ClassStorage, err, "persist %d log entries", len(entries))
}

// TruncateSuffix durably removes every entry from index onwards.
func (s *Store) TruncateSuffix(from uint64) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		// Seek positions at the first key >= from; deleting through the end
		// removes exactly the conflicting suffix.
		for k, _ := c.Seek(encodeUint64(from)); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	return errs.Wrap(errs.ClassStorage, err, "truncate log from index %d", from)
}

// TruncatePrefix removes every entry up to and including index.
//
// It is only called after a snapshot covering that index is durable. Doing it
// in the other order would create a window where a crash loses both the
// entries and the snapshot meant to replace them.
func (s *Store) TruncatePrefix(through uint64) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)
		c := b.Cursor()
		limit := encodeUint64(through)
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if compareKeys(k, limit) > 0 {
				break
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	return errs.Wrap(errs.ClassStorage, err, "compact log through index %d", through)
}

// Snapshot is a durable state-machine snapshot.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// SaveSnapshot atomically writes a snapshot and its metadata.
//
// Atomicity matters: a snapshot whose data and index disagree would restore the
// state machine to one point while claiming it covers another, silently losing
// or double-applying entries.
func (s *Store) SaveSnapshot(snap Snapshot) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSnapshot)
		if err := b.Put(keySnapshotData, snap.Data); err != nil {
			return err
		}
		if err := b.Put(keyLastIncludedIndex, encodeUint64(snap.LastIncludedIndex)); err != nil {
			return err
		}
		return b.Put(keyLastIncludedTerm, encodeUint64(snap.LastIncludedTerm))
	})
	return errs.Wrap(errs.ClassStorage, err,
		"persist snapshot through index %d", snap.LastIncludedIndex)
}

// LoadSnapshot reads the newest snapshot. It reports ok=false when none exists.
func (s *Store) LoadSnapshot() (Snapshot, bool, error) {
	var snap Snapshot
	var ok bool
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSnapshot)
		d := b.Get(keySnapshotData)
		if d == nil {
			return nil
		}
		ok = true
		snap.Data = append([]byte(nil), d...)
		snap.LastIncludedIndex = getUint64(b, keyLastIncludedIndex)
		snap.LastIncludedTerm = getUint64(b, keyLastIncludedTerm)
		return nil
	})
	if err != nil {
		return Snapshot{}, false, errs.Wrap(errs.ClassStorage, err, "load snapshot")
	}
	return snap, ok, nil
}

// Sync flushes pending writes. It is a no-op unless NoSync was set.
func (s *Store) Sync() error {
	return errs.Wrap(errs.ClassStorage, s.db.Sync(), "sync raft store")
}

// encodeUint64 renders a value big-endian so bbolt's byte-ordered keys sort
// numerically. Little-endian keys would make a cursor scan visit index 256
// before index 2.
func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func getUint64(b *bolt.Bucket, key []byte) uint64 {
	v := b.Get(key)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func compareKeys(a, b []byte) int {
	av, bv := binary.BigEndian.Uint64(a), binary.BigEndian.Uint64(b)
	switch {
	case av < bv:
		return -1
	case av > bv:
		return 1
	default:
		return 0
	}
}

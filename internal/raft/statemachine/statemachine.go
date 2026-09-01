// Package statemachine implements EdgeMesh's replicated configuration store.
//
// # Determinism
//
// Every replica must reach the same state from the same log. Apply therefore
// never reads the wall clock, never calls the network, and never consumes
// randomness. Anything time-dependent (a route's updated_at) is stamped by the
// *leader* into the command before it is proposed, so every replica applies the
// identical value.
//
// This is why the timestamps live in the command rather than being generated in
// Apply: a clock read inside Apply would make two replicas' state diverge by
// exactly the difference between their clocks, and that divergence would then
// be served to clients as different configuration.
package statemachine

import (
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
	"github.com/sheehanlloyd/edgemesh/internal/origin"
	"github.com/sheehanlloyd/edgemesh/internal/pb"
	"github.com/sheehanlloyd/edgemesh/internal/routing"
)

// CommandType identifies a state-machine mutation.
type CommandType int32

const (
	CommandUnspecified       CommandType = 0
	CommandCreateRoute       CommandType = 1
	CommandUpdateRoute       CommandType = 2
	CommandDeleteRoute       CommandType = 3
	CommandCreateOriginPool  CommandType = 4
	CommandUpdateOriginPool  CommandType = 5
	CommandDeleteOriginPool  CommandType = 6
	CommandSetGlobalSettings CommandType = 7
	CommandPurgeCache        CommandType = 8
)

// String renders a command type for logs and metrics.
func (c CommandType) String() string {
	switch c {
	case CommandCreateRoute:
		return "CreateRoute"
	case CommandUpdateRoute:
		return "UpdateRoute"
	case CommandDeleteRoute:
		return "DeleteRoute"
	case CommandCreateOriginPool:
		return "CreateOriginPool"
	case CommandUpdateOriginPool:
		return "UpdateOriginPool"
	case CommandDeleteOriginPool:
		return "DeleteOriginPool"
	case CommandSetGlobalSettings:
		return "SetGlobalSettings"
	case CommandPurgeCache:
		return "PurgeCache"
	default:
		return "Unspecified"
	}
}

// Command is one replicated mutation.
//
// It is encoded with protobuf's deterministic marshaling so that the same
// command produces identical bytes on every node, which keeps log comparison
// and checksums meaningful.
type Command struct {
	Type CommandType
	// TimestampUnixMs is stamped by the leader before proposing. Apply uses it
	// instead of reading a clock so replicas stay identical.
	TimestampUnixMs int64
	Route           *edgemeshv1.Route
	OriginPool      *edgemeshv1.OriginPool
	Settings        *edgemeshv1.GlobalSettings
	Purge           *edgemeshv1.PurgeDirective
	ID              string
	// ExpectedVersion enables optimistic concurrency. Zero disables the check.
	ExpectedVersion uint64
}

// Encode serializes a command for the Raft log.
//
// Payloads are marshaled deterministically so that the same logical command
// produces identical bytes on every node. Identical bytes are what make log
// comparison, checksums, and snapshot equality meaningful.
func (c *Command) Encode() ([]byte, error) {
	enc := newEncoder()
	enc.int32(int32(c.Type))
	enc.int64(c.TimestampUnixMs)
	enc.string(c.ID)
	enc.uint64(c.ExpectedVersion)

	// A nil payload encodes as a zero-length field, which Decode reads back as
	// absent. Order is fixed and must match Decode exactly.
	if err := encodePayload(enc, c.Route); err != nil {
		return nil, err
	}
	if err := encodePayload(enc, c.OriginPool); err != nil {
		return nil, err
	}
	if err := encodePayload(enc, c.Settings); err != nil {
		return nil, err
	}
	if err := encodePayload(enc, c.Purge); err != nil {
		return nil, err
	}
	return enc.result(), nil
}

// encodePayload writes an optional protobuf payload as a length-prefixed field.
// The type parameter keeps the nil check on the concrete pointer: a typed nil
// stored in a proto.Message interface is not equal to nil and would marshal to
// an empty message instead of being skipped.
func encodePayload[T proto.Message](e *encoder, m T) error {
	if isNil(m) {
		e.bytes(nil)
		return nil
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return errs.Wrap(errs.ClassProtocol, err, "encode command payload")
	}
	e.bytes(b)
	return nil
}

// isNil reports whether a protobuf pointer is nil without reflection.
func isNil[T proto.Message](m T) bool {
	switch v := any(m).(type) {
	case *edgemeshv1.Route:
		return v == nil
	case *edgemeshv1.OriginPool:
		return v == nil
	case *edgemeshv1.GlobalSettings:
		return v == nil
	case *edgemeshv1.PurgeDirective:
		return v == nil
	default:
		return false
	}
}

// Decode parses a command from the Raft log.
func Decode(data []byte) (*Command, error) {
	d := newDecoder(data)
	c := &Command{}

	t, err := d.int32()
	if err != nil {
		return nil, err
	}
	c.Type = CommandType(t)

	if c.TimestampUnixMs, err = d.int64(); err != nil {
		return nil, err
	}
	if c.ID, err = d.string(); err != nil {
		return nil, err
	}
	if c.ExpectedVersion, err = d.uint64(); err != nil {
		return nil, err
	}

	routeB, err := d.bytes()
	if err != nil {
		return nil, err
	}
	poolB, err := d.bytes()
	if err != nil {
		return nil, err
	}
	settingsB, err := d.bytes()
	if err != nil {
		return nil, err
	}
	purgeB, err := d.bytes()
	if err != nil {
		return nil, err
	}

	if len(routeB) > 0 {
		c.Route = &edgemeshv1.Route{}
		if err := proto.Unmarshal(routeB, c.Route); err != nil {
			return nil, errs.Wrap(errs.ClassProtocol, err, "decode route payload")
		}
	}
	if len(poolB) > 0 {
		c.OriginPool = &edgemeshv1.OriginPool{}
		if err := proto.Unmarshal(poolB, c.OriginPool); err != nil {
			return nil, errs.Wrap(errs.ClassProtocol, err, "decode origin pool payload")
		}
	}
	if len(settingsB) > 0 {
		c.Settings = &edgemeshv1.GlobalSettings{}
		if err := proto.Unmarshal(settingsB, c.Settings); err != nil {
			return nil, errs.Wrap(errs.ClassProtocol, err, "decode settings payload")
		}
	}
	if len(purgeB) > 0 {
		c.Purge = &edgemeshv1.PurgeDirective{}
		if err := proto.Unmarshal(purgeB, c.Purge); err != nil {
			return nil, errs.Wrap(errs.ClassProtocol, err, "decode purge payload")
		}
	}
	return c, nil
}

// Result is what a committed command returns to the admin API.
type Result struct {
	ConfigVersion uint64
	Route         *edgemeshv1.Route
	OriginPool    *edgemeshv1.OriginPool
	Settings      *edgemeshv1.GlobalSettings
	Purge         *edgemeshv1.PurgeDirective
}

// maxRecentPurges bounds the purge history carried in a snapshot. Edges only
// need enough history to reconcile a brief disconnect; a full history would
// grow the snapshot without bound.
const maxRecentPurges = 256

// StateMachine holds the replicated configuration.
//
// Apply is called only from the Raft node's apply loop, but reads come from
// admin API handlers and config-stream goroutines, so the mutex protects
// readers against a concurrent apply.
type StateMachine struct {
	mu sync.RWMutex

	routes   map[string]*edgemeshv1.Route
	pools    map[string]*edgemeshv1.OriginPool
	settings *edgemeshv1.GlobalSettings
	purges   []*edgemeshv1.PurgeDirective

	configVersion uint64
	lastApplied   uint64
}

// New returns an empty state machine with default global settings.
func New() *StateMachine {
	return &StateMachine{
		routes: make(map[string]*edgemeshv1.Route),
		pools:  make(map[string]*edgemeshv1.OriginPool),
		settings: &edgemeshv1.GlobalSettings{
			ReplicationFactor: 2,
			RingVirtualNodes:  128,
			MaxObjectBytes:    8 << 20,
		},
	}
}

// Apply executes a committed command at the given log index.
//
// A command that fails validation still advances lastApplied: the entry is
// committed and every replica must treat it identically. Rejecting it on one
// node and accepting it on another is precisely the divergence Raft exists to
// prevent. Validation therefore happens on the *admin API* before proposal, and
// anything reaching Apply that still fails is recorded as an error result while
// the log position advances.
func (s *StateMachine) Apply(index uint64, c *Command) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index <= s.lastApplied {
		// A replayed entry after a restart. Applying it twice would double
		// increment the config version, so it is skipped idempotently.
		return Result{ConfigVersion: s.configVersion}, nil
	}
	s.lastApplied = index

	res, err := s.applyLocked(c)
	if err != nil {
		return Result{ConfigVersion: s.configVersion}, err
	}
	return res, nil
}

func (s *StateMachine) applyLocked(c *Command) (Result, error) {
	switch c.Type {
	case CommandCreateRoute, CommandUpdateRoute:
		return s.applyRoute(c)
	case CommandDeleteRoute:
		return s.applyDeleteRoute(c)
	case CommandCreateOriginPool, CommandUpdateOriginPool:
		return s.applyPool(c)
	case CommandDeleteOriginPool:
		return s.applyDeletePool(c)
	case CommandSetGlobalSettings:
		return s.applySettings(c)
	case CommandPurgeCache:
		return s.applyPurge(c)
	default:
		return Result{}, errs.New(errs.ClassProtocol, "unknown command type %d", c.Type)
	}
}

func (s *StateMachine) applyRoute(c *Command) (Result, error) {
	if c.Route == nil {
		return Result{}, errs.New(errs.ClassValidation, "%s requires a route", c.Type)
	}
	r := pb.Clone(c.Route)

	existing, exists := s.routes[r.GetId()]
	if c.Type == CommandCreateRoute && exists {
		return Result{}, errs.New(errs.ClassConflict, "route %q already exists", r.GetId())
	}
	if c.Type == CommandUpdateRoute && !exists {
		return Result{}, errs.New(errs.ClassNotFound, "route %q does not exist", r.GetId())
	}
	if c.ExpectedVersion != 0 && exists && existing.GetVersion() != c.ExpectedVersion {
		return Result{}, errs.New(errs.ClassConflict,
			"route %q is at version %d, not the expected %d",
			r.GetId(), existing.GetVersion(), c.ExpectedVersion)
	}
	// Cross-route validation must run against the *resulting* set, since a
	// route that is valid alone can still collide with an existing one.
	if err := s.validateRouteSetWith(r); err != nil {
		return Result{}, err
	}

	if exists {
		r.CreatedAtUnixMs = existing.GetCreatedAtUnixMs()
	} else {
		r.CreatedAtUnixMs = c.TimestampUnixMs
	}
	r.UpdatedAtUnixMs = c.TimestampUnixMs
	s.configVersion++
	r.Version = s.configVersion
	s.routes[r.GetId()] = r

	return Result{ConfigVersion: s.configVersion, Route: pb.Clone(r)}, nil
}

// validateRouteSetWith checks that adding or replacing candidate keeps the whole
// route set valid.
func (s *StateMachine) validateRouteSetWith(candidate *edgemeshv1.Route) error {
	set := make([]*edgemeshv1.Route, 0, len(s.routes)+1)
	for id, r := range s.routes {
		if id == candidate.GetId() {
			continue
		}
		set = append(set, r)
	}
	set = append(set, candidate)
	// Sorting makes the error message deterministic across replicas.
	sort.Slice(set, func(i, j int) bool { return set[i].GetId() < set[j].GetId() })
	return routing.ValidateSet(set)
}

func (s *StateMachine) applyDeleteRoute(c *Command) (Result, error) {
	if c.ID == "" {
		return Result{}, errs.New(errs.ClassValidation, "DeleteRoute requires an id")
	}
	existing, ok := s.routes[c.ID]
	if !ok {
		return Result{}, errs.New(errs.ClassNotFound, "route %q does not exist", c.ID)
	}
	if c.ExpectedVersion != 0 && existing.GetVersion() != c.ExpectedVersion {
		return Result{}, errs.New(errs.ClassConflict,
			"route %q is at version %d, not the expected %d", c.ID, existing.GetVersion(), c.ExpectedVersion)
	}
	delete(s.routes, c.ID)
	s.configVersion++
	return Result{ConfigVersion: s.configVersion}, nil
}

func (s *StateMachine) applyPool(c *Command) (Result, error) {
	if c.OriginPool == nil {
		return Result{}, errs.New(errs.ClassValidation, "%s requires an origin pool", c.Type)
	}
	p := pb.Clone(c.OriginPool)

	existing, exists := s.pools[p.GetId()]
	if c.Type == CommandCreateOriginPool && exists {
		return Result{}, errs.New(errs.ClassConflict, "origin pool %q already exists", p.GetId())
	}
	if c.Type == CommandUpdateOriginPool && !exists {
		return Result{}, errs.New(errs.ClassNotFound, "origin pool %q does not exist", p.GetId())
	}
	if c.ExpectedVersion != 0 && exists && existing.GetVersion() != c.ExpectedVersion {
		return Result{}, errs.New(errs.ClassConflict,
			"origin pool %q is at version %d, not the expected %d",
			p.GetId(), existing.GetVersion(), c.ExpectedVersion)
	}
	if err := origin.ValidatePool(p); err != nil {
		return Result{}, err
	}

	if exists {
		p.CreatedAtUnixMs = existing.GetCreatedAtUnixMs()
	} else {
		p.CreatedAtUnixMs = c.TimestampUnixMs
	}
	p.UpdatedAtUnixMs = c.TimestampUnixMs
	s.configVersion++
	p.Version = s.configVersion
	s.pools[p.GetId()] = p

	return Result{ConfigVersion: s.configVersion, OriginPool: pb.Clone(p)}, nil
}

func (s *StateMachine) applyDeletePool(c *Command) (Result, error) {
	if c.ID == "" {
		return Result{}, errs.New(errs.ClassValidation, "DeleteOriginPool requires an id")
	}
	if _, ok := s.pools[c.ID]; !ok {
		return Result{}, errs.New(errs.ClassNotFound, "origin pool %q does not exist", c.ID)
	}
	// A pool still referenced by a route must not be deleted: doing so would
	// leave routes pointing at nothing and turn every matching request into a
	// 503 with no way to see why from the route alone.
	for _, r := range s.routes {
		if r.GetOriginPoolId() == c.ID {
			return Result{}, errs.New(errs.ClassConflict,
				"origin pool %q is still referenced by route %q", c.ID, r.GetId())
		}
	}
	delete(s.pools, c.ID)
	s.configVersion++
	return Result{ConfigVersion: s.configVersion}, nil
}

func (s *StateMachine) applySettings(c *Command) (Result, error) {
	if c.Settings == nil {
		return Result{}, errs.New(errs.ClassValidation, "SetGlobalSettings requires settings")
	}
	set := pb.Clone(c.Settings)
	if set.GetReplicationFactor() < 1 {
		return Result{}, errs.New(errs.ClassValidation, "replication_factor must be at least 1")
	}
	if v := set.GetRingVirtualNodes(); v < 1 || v > 4096 {
		return Result{}, errs.New(errs.ClassValidation,
			"ring_virtual_nodes must be within [1,4096], got %d", v)
	}
	if set.GetMaxObjectBytes() == 0 {
		return Result{}, errs.New(errs.ClassValidation, "max_object_bytes must be positive")
	}
	s.settings = set
	s.configVersion++
	return Result{ConfigVersion: s.configVersion, Settings: pb.Clone(set)}, nil
}

func (s *StateMachine) applyPurge(c *Command) (Result, error) {
	if c.Purge == nil {
		return Result{}, errs.New(errs.ClassValidation, "PurgeCache requires a directive")
	}
	p := pb.Clone(c.Purge)
	switch p.GetScope() {
	case edgemeshv1.PurgeScope_PURGE_SCOPE_KEY:
		if p.GetCacheKey() == "" {
			return Result{}, errs.New(errs.ClassValidation, "a key purge requires cache_key")
		}
	case edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE:
		if p.GetRouteId() == "" {
			return Result{}, errs.New(errs.ClassValidation, "a route purge requires route_id")
		}
	case edgemeshv1.PurgeScope_PURGE_SCOPE_ALL:
	default:
		return Result{}, errs.New(errs.ClassValidation, "unknown purge scope %v", p.GetScope())
	}

	s.configVersion++
	p.Version = s.configVersion
	p.IssuedAtUnixMs = c.TimestampUnixMs

	s.purges = append(s.purges, p)
	if len(s.purges) > maxRecentPurges {
		s.purges = append(s.purges[:0], s.purges[len(s.purges)-maxRecentPurges:]...)
	}
	return Result{ConfigVersion: s.configVersion, Purge: pb.Clone(p)}, nil
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// ConfigVersion reports the current configuration version.
func (s *StateMachine) ConfigVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configVersion
}

// LastApplied reports the highest log index applied.
func (s *StateMachine) LastApplied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}

// Route returns a copy of one route.
func (s *StateMachine) Route(id string) (*edgemeshv1.Route, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.routes[id]
	if !ok {
		return nil, false
	}
	return pb.Clone(r), true
}

// Routes returns every route, sorted by ID for a stable API response.
func (s *StateMachine) Routes() []*edgemeshv1.Route {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.routesLocked()
}

func (s *StateMachine) routesLocked() []*edgemeshv1.Route {
	out := make([]*edgemeshv1.Route, 0, len(s.routes))
	for _, r := range s.routes {
		out = append(out, pb.Clone(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	return out
}

// OriginPool returns a copy of one pool.
func (s *StateMachine) OriginPool(id string) (*edgemeshv1.OriginPool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pools[id]
	if !ok {
		return nil, false
	}
	return pb.Clone(p), true
}

// OriginPools returns every pool, sorted by ID.
func (s *StateMachine) OriginPools() []*edgemeshv1.OriginPool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.poolsLocked()
}

func (s *StateMachine) poolsLocked() []*edgemeshv1.OriginPool {
	out := make([]*edgemeshv1.OriginPool, 0, len(s.pools))
	for _, p := range s.pools {
		out = append(out, pb.Clone(p))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	return out
}

// Settings returns a copy of the global settings.
func (s *StateMachine) Settings() *edgemeshv1.GlobalSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return pb.Clone(s.settings)
}

// PurgesSince returns purge directives newer than version.
//
// This is how a briefly disconnected edge catches up on invalidations without
// requesting a whole snapshot.
func (s *StateMachine) PurgesSince(version uint64) []*edgemeshv1.PurgeDirective {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*edgemeshv1.PurgeDirective
	for _, p := range s.purges {
		if p.GetVersion() > version {
			out = append(out, pb.Clone(p))
		}
	}
	return out
}

// Snapshot renders the full configuration.
func (s *StateMachine) Snapshot() *edgemeshv1.ConfigSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *StateMachine) snapshotLocked() *edgemeshv1.ConfigSnapshot {
	purges := make([]*edgemeshv1.PurgeDirective, 0, len(s.purges))
	for _, p := range s.purges {
		purges = append(purges, pb.Clone(p))
	}
	return &edgemeshv1.ConfigSnapshot{
		Routes:        s.routesLocked(),
		OriginPools:   s.poolsLocked(),
		Settings:      pb.Clone(s.settings),
		RecentPurges:  purges,
		ConfigVersion: s.configVersion,
	}
}

// ---------------------------------------------------------------------------
// Snapshot persistence
// ---------------------------------------------------------------------------

// Serialize renders the state machine for a Raft snapshot.
//
// Deterministic marshaling means two replicas snapshotting the same state
// produce byte-identical output, which makes snapshots comparable and any
// divergence immediately visible.
func (s *StateMachine) Serialize() ([]byte, error) {
	s.mu.RLock()
	snap := s.snapshotLocked()
	lastApplied := s.lastApplied
	s.mu.RUnlock()

	// last_applied rides along inside the membership version field slot is not
	// available, so it is encoded alongside the snapshot in a small envelope.
	enc := newEncoder()
	enc.uint64(lastApplied)
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(snap)
	if err != nil {
		return nil, errs.Wrap(errs.ClassStorage, err, "serialize state machine")
	}
	enc.bytes(b)
	return enc.result(), nil
}

// Restore replaces the state machine's contents from a snapshot.
func (s *StateMachine) Restore(data []byte) error {
	d := newDecoder(data)
	lastApplied, err := d.uint64()
	if err != nil {
		return errs.Wrap(errs.ClassStorage, err, "decode snapshot envelope")
	}
	body, err := d.bytes()
	if err != nil {
		return errs.Wrap(errs.ClassStorage, err, "decode snapshot body")
	}

	var snap edgemeshv1.ConfigSnapshot
	if err := proto.Unmarshal(body, &snap); err != nil {
		return errs.Wrap(errs.ClassStorage, err, "decode configuration snapshot")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.routes = make(map[string]*edgemeshv1.Route, len(snap.GetRoutes()))
	for _, r := range snap.GetRoutes() {
		s.routes[r.GetId()] = r
	}
	s.pools = make(map[string]*edgemeshv1.OriginPool, len(snap.GetOriginPools()))
	for _, p := range snap.GetOriginPools() {
		s.pools[p.GetId()] = p
	}
	if snap.GetSettings() != nil {
		s.settings = snap.GetSettings()
	}
	s.purges = snap.GetRecentPurges()
	s.configVersion = snap.GetConfigVersion()
	s.lastApplied = lastApplied
	return nil
}

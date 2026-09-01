package statemachine

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	edgemeshv1 "github.com/sheehanlloyd/edgemesh/api/gen/edgemesh/v1"
	"github.com/sheehanlloyd/edgemesh/internal/errs"
)

const ts = int64(1_700_000_000_000)

func routeCmd(t CommandType, id string) *Command {
	return &Command{
		Type:            t,
		TimestampUnixMs: ts,
		Route: &edgemeshv1.Route{
			Id: id, Hostname: id + ".local", PathPrefix: "/",
			OriginPoolId: "pool-1", Enabled: true,
		},
	}
}

func poolCmd(t CommandType, id string) *Command {
	return &Command{
		Type:            t,
		TimestampUnixMs: ts,
		OriginPool: &edgemeshv1.OriginPool{
			Id: id,
			Origins: []*edgemeshv1.Origin{
				{Id: "o1", Scheme: "http", Host: "origin-1", Port: 8080, Weight: 1},
			},
		},
	}
}

func apply(t *testing.T, sm *StateMachine, index uint64, c *Command) Result {
	t.Helper()
	res, err := sm.Apply(index, c)
	if err != nil {
		t.Fatalf("Apply(%d, %s): %v", index, c.Type, err)
	}
	return res
}

func TestCreateAndReadRoute(t *testing.T) {
	sm := New()
	res := apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))
	if res.ConfigVersion != 1 {
		t.Fatalf("config version = %d, want 1", res.ConfigVersion)
	}
	if res.Route.GetVersion() != 1 {
		t.Fatalf("route version = %d, want 1", res.Route.GetVersion())
	}
	if res.Route.GetCreatedAtUnixMs() != ts || res.Route.GetUpdatedAtUnixMs() != ts {
		t.Fatal("timestamps must come from the command, not a clock read")
	}
	got, ok := sm.Route("r1")
	if !ok || got.GetHostname() != "r1.local" {
		t.Fatalf("Route lookup = %+v ok=%v", got, ok)
	}
}

// Reads must return copies: a caller mutating the result would silently corrupt
// replicated state that other replicas do not share.
func TestReadsReturnCopies(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))

	got, _ := sm.Route("r1")
	got.Hostname = "mutated.local"
	again, _ := sm.Route("r1")
	if again.GetHostname() != "r1.local" {
		t.Fatal("mutating a read result corrupted state machine contents")
	}

	list := sm.Routes()
	list[0].Enabled = false
	fresh, _ := sm.Route("r1")
	if !fresh.GetEnabled() {
		t.Fatal("mutating a list result corrupted state machine contents")
	}
}

// The command carries the timestamp precisely so that two replicas applying the
// same log produce byte-identical state.
func TestApplyIsDeterministicAcrossReplicas(t *testing.T) {
	cmds := []*Command{
		routeCmd(CommandCreateRoute, "a"),
		poolCmd(CommandCreateOriginPool, "pool-1"),
		routeCmd(CommandCreateRoute, "b"),
		{Type: CommandSetGlobalSettings, TimestampUnixMs: ts,
			Settings: &edgemeshv1.GlobalSettings{ReplicationFactor: 3, RingVirtualNodes: 256, MaxObjectBytes: 1 << 20}},
		{Type: CommandPurgeCache, TimestampUnixMs: ts,
			Purge: &edgemeshv1.PurgeDirective{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE, RouteId: "a"}},
	}

	run := func() []byte {
		sm := New()
		for i, c := range cmds {
			apply(t, sm, uint64(i+1), c)
		}
		b, err := sm.Serialize()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, b := run(), run()
	if string(a) != string(b) {
		t.Fatal("two replicas applying the same log produced different state")
	}
}

func TestApplyIsIdempotentOnReplay(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))
	v := sm.ConfigVersion()

	// Replaying an already-applied index after a restart must not double-apply.
	res, err := sm.Apply(1, routeCmd(CommandCreateRoute, "r1"))
	if err != nil {
		t.Fatalf("replay returned an error: %v", err)
	}
	if sm.ConfigVersion() != v || res.ConfigVersion != v {
		t.Fatalf("replay advanced the config version from %d to %d", v, sm.ConfigVersion())
	}
	if sm.LastApplied() != 1 {
		t.Fatalf("last applied = %d, want 1", sm.LastApplied())
	}
}

func TestCreateRejectsDuplicateAndUpdateRejectsMissing(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))

	_, err := sm.Apply(2, routeCmd(CommandCreateRoute, "r1"))
	if !errs.IsClass(err, errs.ClassConflict) {
		t.Fatalf("duplicate create error class = %q, want conflict", errs.ClassOf(err))
	}
	_, err = sm.Apply(3, routeCmd(CommandUpdateRoute, "missing"))
	if !errs.IsClass(err, errs.ClassNotFound) {
		t.Fatalf("update of a missing route error class = %q, want not_found", errs.ClassOf(err))
	}
}

func TestUpdatePreservesCreationTimestamp(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))

	upd := routeCmd(CommandUpdateRoute, "r1")
	upd.TimestampUnixMs = ts + 5000
	upd.Route.PathPrefix = "/api"
	res := apply(t, sm, 2, upd)

	if res.Route.GetCreatedAtUnixMs() != ts {
		t.Fatalf("created_at was overwritten: %d", res.Route.GetCreatedAtUnixMs())
	}
	if res.Route.GetUpdatedAtUnixMs() != ts+5000 {
		t.Fatalf("updated_at not advanced: %d", res.Route.GetUpdatedAtUnixMs())
	}
	if res.Route.GetVersion() <= 1 {
		t.Fatal("update did not advance the route version")
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	sm := New()
	res := apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))

	stale := routeCmd(CommandUpdateRoute, "r1")
	stale.ExpectedVersion = res.Route.GetVersion() + 99
	if _, err := sm.Apply(2, stale); !errs.IsClass(err, errs.ClassConflict) {
		t.Fatalf("stale version error class = %q, want conflict", errs.ClassOf(err))
	}

	fresh := routeCmd(CommandUpdateRoute, "r1")
	fresh.ExpectedVersion = res.Route.GetVersion()
	if _, err := sm.Apply(3, fresh); err != nil {
		t.Fatalf("matching version rejected: %v", err)
	}
}

// A route that is valid alone can still collide with an existing one, so
// cross-route validation must run against the resulting set.
func TestDuplicateHostPathIsRejected(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))

	clash := &Command{
		Type: CommandCreateRoute, TimestampUnixMs: ts,
		Route: &edgemeshv1.Route{
			Id: "r2", Hostname: "r1.local", PathPrefix: "/",
			OriginPoolId: "pool-1", Enabled: true,
		},
	}
	if _, err := sm.Apply(2, clash); !errs.IsClass(err, errs.ClassValidation) {
		t.Fatalf("colliding route error class = %q, want validation", errs.ClassOf(err))
	}
	// The rejected route must not be present.
	if _, ok := sm.Route("r2"); ok {
		t.Fatal("a rejected route was stored")
	}
}

// Updating a route in place must not report a conflict against itself.
func TestUpdateDoesNotCollideWithItself(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))
	if _, err := sm.Apply(2, routeCmd(CommandUpdateRoute, "r1")); err != nil {
		t.Fatalf("a route collided with its own previous version: %v", err)
	}
}

func TestDeleteRoute(t *testing.T) {
	sm := New()
	apply(t, sm, 1, routeCmd(CommandCreateRoute, "r1"))
	apply(t, sm, 2, &Command{Type: CommandDeleteRoute, TimestampUnixMs: ts, ID: "r1"})

	if _, ok := sm.Route("r1"); ok {
		t.Fatal("route survived deletion")
	}
	if _, err := sm.Apply(3, &Command{Type: CommandDeleteRoute, TimestampUnixMs: ts, ID: "r1"}); !errs.IsClass(err, errs.ClassNotFound) {
		t.Fatal("deleting a missing route must report not_found")
	}
}

func TestOriginPoolLifecycle(t *testing.T) {
	sm := New()
	res := apply(t, sm, 1, poolCmd(CommandCreateOriginPool, "pool-1"))
	if res.OriginPool.GetVersion() == 0 {
		t.Fatal("pool version not assigned")
	}
	if _, ok := sm.OriginPool("pool-1"); !ok {
		t.Fatal("pool not readable")
	}
	if len(sm.OriginPools()) != 1 {
		t.Fatal("pool list is wrong")
	}
	// An invalid pool is rejected.
	bad := poolCmd(CommandCreateOriginPool, "pool-2")
	bad.OriginPool.Origins[0].Scheme = "file"
	if _, err := sm.Apply(2, bad); err == nil {
		t.Fatal("an invalid pool must be rejected")
	}
}

// Deleting a referenced pool would leave routes pointing at nothing.
func TestDeletingReferencedPoolIsRejected(t *testing.T) {
	sm := New()
	apply(t, sm, 1, poolCmd(CommandCreateOriginPool, "pool-1"))
	apply(t, sm, 2, routeCmd(CommandCreateRoute, "r1"))

	_, err := sm.Apply(3, &Command{Type: CommandDeleteOriginPool, TimestampUnixMs: ts, ID: "pool-1"})
	if !errs.IsClass(err, errs.ClassConflict) {
		t.Fatalf("error class = %q, want conflict", errs.ClassOf(err))
	}

	// Once the route is gone the pool may be deleted.
	apply(t, sm, 4, &Command{Type: CommandDeleteRoute, TimestampUnixMs: ts, ID: "r1"})
	if _, err := sm.Apply(5, &Command{Type: CommandDeleteOriginPool, TimestampUnixMs: ts, ID: "pool-1"}); err != nil {
		t.Fatalf("deleting an unreferenced pool failed: %v", err)
	}
}

func TestGlobalSettingsValidation(t *testing.T) {
	sm := New()
	def := sm.Settings()
	if def.GetReplicationFactor() != 2 || def.GetRingVirtualNodes() != 128 {
		t.Fatalf("unexpected defaults: %+v", def)
	}
	bad := []*edgemeshv1.GlobalSettings{
		{ReplicationFactor: 0, RingVirtualNodes: 128, MaxObjectBytes: 1},
		{ReplicationFactor: 2, RingVirtualNodes: 0, MaxObjectBytes: 1},
		{ReplicationFactor: 2, RingVirtualNodes: 99999, MaxObjectBytes: 1},
		{ReplicationFactor: 2, RingVirtualNodes: 128, MaxObjectBytes: 0},
	}
	for i, s := range bad {
		if _, err := sm.Apply(uint64(i+1), &Command{Type: CommandSetGlobalSettings, TimestampUnixMs: ts, Settings: s}); err == nil {
			t.Errorf("invalid settings %d were accepted: %+v", i, s)
		}
	}
	good := &edgemeshv1.GlobalSettings{ReplicationFactor: 3, RingVirtualNodes: 256, MaxObjectBytes: 1 << 20}
	if _, err := sm.Apply(10, &Command{Type: CommandSetGlobalSettings, TimestampUnixMs: ts, Settings: good}); err != nil {
		t.Fatalf("valid settings rejected: %v", err)
	}
	if sm.Settings().GetRingVirtualNodes() != 256 {
		t.Fatal("settings not applied")
	}
}

func TestPurgeVersioningAndHistory(t *testing.T) {
	sm := New()
	res := apply(t, sm, 1, &Command{
		Type: CommandPurgeCache, TimestampUnixMs: ts,
		Purge: &edgemeshv1.PurgeDirective{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE, RouteId: "r1"},
	})
	if res.Purge.GetVersion() == 0 || res.Purge.GetIssuedAtUnixMs() != ts {
		t.Fatalf("purge not versioned or stamped: %+v", res.Purge)
	}

	// A disconnected edge catches up on only the purges it has not seen.
	apply(t, sm, 2, &Command{
		Type: CommandPurgeCache, TimestampUnixMs: ts,
		Purge: &edgemeshv1.PurgeDirective{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_KEY, CacheKey: "k"},
	})
	since := sm.PurgesSince(res.Purge.GetVersion())
	if len(since) != 1 || since[0].GetCacheKey() != "k" {
		t.Fatalf("PurgesSince returned %d directives: %+v", len(since), since)
	}
	if len(sm.PurgesSince(0)) != 2 {
		t.Fatal("full purge history is wrong")
	}
}

func TestPurgeValidation(t *testing.T) {
	sm := New()
	cases := []*edgemeshv1.PurgeDirective{
		{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_KEY},   // no key
		{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ROUTE}, // no route
		{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_UNSPECIFIED},
	}
	for i, p := range cases {
		if _, err := sm.Apply(uint64(i+1), &Command{Type: CommandPurgeCache, TimestampUnixMs: ts, Purge: p}); err == nil {
			t.Errorf("invalid purge %d was accepted", i)
		}
	}
	if _, err := sm.Apply(10, &Command{
		Type: CommandPurgeCache, TimestampUnixMs: ts,
		Purge: &edgemeshv1.PurgeDirective{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ALL},
	}); err != nil {
		t.Fatalf("a purge-all directive was rejected: %v", err)
	}
}

// The purge history must be bounded or a snapshot grows without limit.
func TestPurgeHistoryIsBounded(t *testing.T) {
	sm := New()
	for i := 0; i < maxRecentPurges*2; i++ {
		apply(t, sm, uint64(i+1), &Command{
			Type: CommandPurgeCache, TimestampUnixMs: ts,
			Purge: &edgemeshv1.PurgeDirective{
				Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_KEY, CacheKey: fmt.Sprintf("k%d", i)},
		})
	}
	if n := len(sm.PurgesSince(0)); n > maxRecentPurges {
		t.Fatalf("purge history grew to %d, above the %d bound", n, maxRecentPurges)
	}
}

func TestSerializeRestoreRoundTrip(t *testing.T) {
	sm := New()
	apply(t, sm, 1, poolCmd(CommandCreateOriginPool, "pool-1"))
	apply(t, sm, 2, routeCmd(CommandCreateRoute, "r1"))
	apply(t, sm, 3, routeCmd(CommandCreateRoute, "r2"))
	apply(t, sm, 4, &Command{
		Type: CommandPurgeCache, TimestampUnixMs: ts,
		Purge: &edgemeshv1.PurgeDirective{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ALL},
	})

	data, err := sm.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	restored := New()
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if restored.ConfigVersion() != sm.ConfigVersion() {
		t.Fatalf("config version %d != %d", restored.ConfigVersion(), sm.ConfigVersion())
	}
	if restored.LastApplied() != sm.LastApplied() {
		t.Fatalf("last applied %d != %d", restored.LastApplied(), sm.LastApplied())
	}
	if len(restored.Routes()) != 2 || len(restored.OriginPools()) != 1 {
		t.Fatal("contents lost in the round trip")
	}
	// Re-serializing must produce identical bytes.
	again, err := restored.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(data) {
		t.Fatal("serialize is not stable across a restore")
	}
}

func TestRestoreRejectsCorruptData(t *testing.T) {
	sm := New()
	for _, bad := range [][]byte{
		nil,
		{1, 2, 3},
		append([]byte{0, 0, 0, 0, 0, 0, 0, 1}, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
	} {
		if err := sm.Restore(bad); err == nil {
			t.Errorf("corrupt snapshot %v was accepted", bad)
		}
	}
}

func TestCommandCodecRoundTrip(t *testing.T) {
	cases := []*Command{
		routeCmd(CommandCreateRoute, "r1"),
		poolCmd(CommandUpdateOriginPool, "pool-1"),
		{Type: CommandDeleteRoute, TimestampUnixMs: ts, ID: "r1", ExpectedVersion: 7},
		{Type: CommandSetGlobalSettings, TimestampUnixMs: ts,
			Settings: &edgemeshv1.GlobalSettings{ReplicationFactor: 2, RingVirtualNodes: 128, MaxObjectBytes: 1}},
		{Type: CommandPurgeCache, TimestampUnixMs: ts,
			Purge: &edgemeshv1.PurgeDirective{Scope: edgemeshv1.PurgeScope_PURGE_SCOPE_ALL}},
	}
	for _, c := range cases {
		t.Run(c.Type.String(), func(t *testing.T) {
			data, err := c.Encode()
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != c.Type || got.TimestampUnixMs != c.TimestampUnixMs ||
				got.ID != c.ID || got.ExpectedVersion != c.ExpectedVersion {
				t.Fatalf("scalar fields lost: %+v vs %+v", got, c)
			}
			if c.Route != nil && !proto.Equal(got.Route, c.Route) {
				t.Fatal("route payload lost")
			}
			if c.OriginPool != nil && !proto.Equal(got.OriginPool, c.OriginPool) {
				t.Fatal("pool payload lost")
			}
			if c.Settings != nil && !proto.Equal(got.Settings, c.Settings) {
				t.Fatal("settings payload lost")
			}
			if c.Purge != nil && !proto.Equal(got.Purge, c.Purge) {
				t.Fatal("purge payload lost")
			}
			// Absent payloads must decode back as nil, not as empty messages.
			if c.Route == nil && got.Route != nil {
				t.Fatal("an absent route decoded as a present empty message")
			}
			// Encoding must be deterministic.
			again, err := c.Encode()
			if err != nil {
				t.Fatal(err)
			}
			if string(again) != string(data) {
				t.Fatal("encoding is not deterministic")
			}
		})
	}
}

func TestUnknownCommandTypeIsRejected(t *testing.T) {
	sm := New()
	if _, err := sm.Apply(1, &Command{Type: CommandType(999), TimestampUnixMs: ts}); !errs.IsClass(err, errs.ClassProtocol) {
		t.Fatalf("error class = %q, want protocol", errs.ClassOf(err))
	}
}

// A truncated or hostile log record must produce an error, never a panic.
func FuzzDecodeCommand(f *testing.F) {
	seed, _ := routeCmd(CommandCreateRoute, "r1").Encode()
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := Decode(data)
		if err != nil {
			return
		}
		// Anything that decodes must survive a re-encode without panicking.
		if _, err := cmd.Encode(); err != nil {
			return
		}
		// And must be safely appliable.
		sm := New()
		_, _ = sm.Apply(1, cmd)
	})
}

func FuzzRestore(f *testing.F) {
	sm := New()
	_, _ = sm.Apply(1, routeCmd(CommandCreateRoute, "r1"))
	seed, _ := sm.Serialize()
	f.Add(seed)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		target := New()
		if err := target.Restore(data); err != nil {
			return
		}
		// A successful restore must leave a usable state machine.
		_ = target.Routes()
		_ = target.OriginPools()
		_ = target.Settings()
		if _, err := target.Serialize(); err != nil {
			t.Fatalf("a restored state machine failed to serialize: %v", err)
		}
	})
}

func BenchmarkApplyCreateRoute(b *testing.B) {
	sm := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sm.Apply(uint64(i+1), routeCmd(CommandCreateRoute, fmt.Sprintf("r%d", i)))
	}
}

func BenchmarkSerialize(b *testing.B) {
	sm := New()
	for i := 0; i < 500; i++ {
		_, _ = sm.Apply(uint64(i+1), routeCmd(CommandCreateRoute, fmt.Sprintf("r%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sm.Serialize(); err != nil {
			b.Fatal(err)
		}
	}
}

package main

import (
	"flag"
	"io"
	"reflect"
	"testing"
	"time"
)

// Global flags must work before the subcommand as well as after it, because
// `edgemeshctl --server X status` is what a user naturally types.
func TestHoistGlobalFlags(t *testing.T) {
	cases := []struct {
		name        string
		args        []string
		wantRest    []string
		wantHoisted []string
	}{
		{
			name:        "no flags",
			args:        []string{"status"},
			wantRest:    []string{"status"},
			wantHoisted: nil,
		},
		{
			name:        "server before the subcommand",
			args:        []string{"--server", "http://cp-1:7100", "status"},
			wantRest:    []string{"status"},
			wantHoisted: []string{"--server", "http://cp-1:7100"},
		},
		{
			name:        "several flags before the subcommand",
			args:        []string{"--server", "cp-1:7100", "--json", "routes", "list"},
			wantRest:    []string{"routes", "list"},
			wantHoisted: []string{"--server", "cp-1:7100", "--json"},
		},
		{
			name:        "equals form",
			args:        []string{"--server=cp-1:7100", "status"},
			wantRest:    []string{"status"},
			wantHoisted: []string{"--server=cp-1:7100"},
		},
		{
			name:        "single dash form",
			args:        []string{"-server", "cp-1:7100", "status"},
			wantRest:    []string{"status"},
			wantHoisted: []string{"-server", "cp-1:7100"},
		},
		{
			// An unknown leading flag is left in place so the subcommand's own
			// FlagSet reports it, rather than being silently relocated.
			name:        "unknown flag is left alone",
			args:        []string{"--nonsense", "status"},
			wantRest:    []string{"--nonsense", "status"},
			wantHoisted: nil,
		},
		{
			// A value-taking flag with no value must reach the FlagSet so the
			// error message comes from there.
			name:        "dangling value flag",
			args:        []string{"--server"},
			wantRest:    []string{"--server"},
			wantHoisted: nil,
		},
		{
			name:        "flags after the subcommand are untouched",
			args:        []string{"status", "--server", "cp-1:7100"},
			wantRest:    []string{"status", "--server", "cp-1:7100"},
			wantHoisted: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rest, hoisted := hoistGlobalFlags(c.args)
			if !reflect.DeepEqual(rest, c.wantRest) {
				t.Errorf("rest = %v, want %v", rest, c.wantRest)
			}
			if !reflect.DeepEqual(hoisted, c.wantHoisted) {
				t.Errorf("hoisted = %v, want %v", hoisted, c.wantHoisted)
			}
		})
	}
}

// normalizeArgs must deliver leading global flags to every subcommand,
// including single-word ones.
//
// This is the regression test for the bug that made `edgemeshctl --server X
// status` ignore --server entirely: hoisting was correct, but the caller
// re-appended the hoisted flags only when more than one argument remained, so
// `status` and `nodes` silently ran against the default endpoint.
func TestNormalizeArgsDeliversGlobalFlagsToEverySubcommand(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "single-word subcommand keeps the server flag",
			args: []string{"--server", "http://cp-2:7102", "status"},
			want: []string{"status", "--server", "http://cp-2:7102"},
		},
		{
			name: "single-word subcommand keeps the token flag",
			args: []string{"--token", "secret", "nodes"},
			want: []string{"nodes", "--token", "secret"},
		},
		{
			name: "single-word subcommand keeps a boolean flag",
			args: []string{"--json", "status"},
			want: []string{"status", "--json"},
		},
		{
			name: "several flags reach a single-word subcommand",
			args: []string{"--server", "cp-2:7102", "--json", "status"},
			want: []string{"status", "--server", "cp-2:7102", "--json"},
		},
		{
			name: "multi-word subcommand still works",
			args: []string{"--server", "cp-2:7102", "routes", "list"},
			want: []string{"routes", "list", "--server", "cp-2:7102"},
		},
		{
			name: "trailing flags are left where they are",
			args: []string{"status", "--server", "cp-2:7102"},
			want: []string{"status", "--server", "cp-2:7102"},
		},
		{
			name: "no subcommand yields nothing to run",
			args: []string{"--server", "cp-2:7102"},
			want: []string{},
		},
		{
			name: "an unknown leading flag is preserved for the FlagSet",
			args: []string{"--nonsense", "status"},
			want: []string{"--nonsense", "status"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalizeArgs(c.args)
			if len(got) != len(c.want) {
				t.Fatalf("normalizeArgs(%v) = %v, want %v", c.args, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("normalizeArgs(%v) = %v, want %v", c.args, got, c.want)
				}
			}
		})
	}
}

// Every global flag must actually be parseable by a subcommand's FlagSet once
// normalizeArgs has moved it. A flag that is hoisted but not registered would
// turn into a usage error instead of taking effect.
func TestGlobalFlagsAreRegisteredOnSubcommandFlagSets(t *testing.T) {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c := commonFlags(fs)
	if err := fs.Parse([]string{"--server", "http://cp-2:7102", "--token", "s3cret", "--json", "--timeout", "3s"}); err != nil {
		t.Fatalf("parsing hoisted global flags failed: %v", err)
	}
	c.finish()

	if len(c.servers) != 1 || c.servers[0] != "http://cp-2:7102" {
		t.Errorf("servers = %v, want [http://cp-2:7102]", c.servers)
	}
	if c.token != "s3cret" {
		t.Errorf("token = %q, want %q", c.token, "s3cret")
	}
	if !c.json {
		t.Error("--json did not take effect")
	}
	if c.timeout != 3*time.Second {
		t.Errorf("timeout = %v, want 3s", c.timeout)
	}
}

// The command router must not treat a hoisted flag as a subcommand.
func TestRunRejectsUnknownCommand(t *testing.T) {
	if err := run([]string{"not-a-command"}); err == nil {
		t.Fatal("an unknown command must produce an error")
	}
	// Help and version never need a cluster.
	if err := run([]string{"--help"}); err != nil {
		t.Fatalf("--help returned an error: %v", err)
	}
	if err := run([]string{"version"}); err != nil {
		t.Fatalf("version returned an error: %v", err)
	}
}

// Cache occupancy spans several orders of magnitude across a fleet, so the
// nodes table renders it in the largest unit that stays readable.
func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0B"},
		{1, "1B"},
		{1023, "1023B"},
		{1024, "1.0KiB"},
		{1536, "1.5KiB"},
		{1024 * 1024, "1.0MiB"},
		{1024 * 1024 * 1024, "1.0GiB"},
		{1024 * 1024 * 1024 * 1024, "1.0TiB"},
		// Beyond TiB the exponent is clamped rather than indexing past the
		// unit string, which would panic.
		{1024 * 1024 * 1024 * 1024 * 1024, "1024.0TiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.in); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

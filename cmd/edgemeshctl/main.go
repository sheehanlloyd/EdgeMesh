// Command edgemeshctl is EdgeMesh's admin CLI.
//
// It exists so control-plane operations are visible and scriptable without a
// bespoke UI, and so the demo can show configuration changes propagating in
// real time. It handles leader redirection transparently: a user should never
// need to know which control node currently leads.
package main

import (
	"fmt"
	"os"
	"strings"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "edgemeshctl: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// Global flags are accepted before the subcommand as well as after it.
	// `edgemeshctl --server X status` is what a user naturally types, and
	// rejecting it with a usage dump is a poor first impression.
	args = normalizeArgs(args)

	if len(args) == 0 {
		usage()
		return nil
	}
	switch args[0] {
	case "-h", "--help", "help":
		usage()
		return nil
	case "version", "--version":
		fmt.Println("edgemeshctl", version)
		return nil
	case "status":
		return cmdStatus(args[1:])
	case "raft":
		return cmdRaft(args[1:])
	case "nodes":
		return cmdNodes(args[1:])
	case "routes":
		return cmdRoutes(args[1:])
	case "origins":
		return cmdOrigins(args[1:])
	case "settings":
		return cmdSettings(args[1:])
	case "cache":
		return cmdCache(args[1:])
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// normalizeArgs moves global flags that precede the subcommand to the end of
// the argument list, where each subcommand's own FlagSet parses them. That
// keeps one definition of every flag instead of a second parser for the
// leading position.
//
// Every subcommand gets them, including single-word ones such as `status` and
// `nodes`. An earlier version re-appended only when more than one argument
// remained, which silently dropped `--server` from `edgemeshctl --server X
// status`: the CLI fell back to the default endpoint and reported *that* node
// as unreachable, and a dropped `--token` surfaced as an unexplained 401.
func normalizeArgs(args []string) []string {
	rest, leading := hoistGlobalFlags(args)
	if len(rest) == 0 || len(leading) == 0 {
		return rest
	}
	return append(rest, leading...)
}

// globalFlags are the flags every subcommand accepts. Only these may appear
// before the subcommand; anything else is left in place so a genuine typo still
// produces a clear error rather than being silently relocated.
var globalFlags = map[string]bool{
	"--server": true, "-server": true,
	"--token": true, "-token": true,
	"--timeout": true, "-timeout": true,
	"--json": true, "-json": true,
}

// hoistGlobalFlags splits leading global flags from the command arguments.
//
// It returns the remaining arguments and the flags that were hoisted, so the
// caller can re-append them after the subcommand.
func hoistGlobalFlags(args []string) (rest, hoisted []string) {
	i := 0
	for i < len(args) {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			break
		}
		// A --flag=value form carries its own value.
		if name, _, found := strings.Cut(arg, "="); found {
			if !globalFlags[name] {
				break
			}
			hoisted = append(hoisted, arg)
			i++
			continue
		}
		if !globalFlags[arg] {
			break
		}
		// Boolean flags take no value.
		if arg == "--json" || arg == "-json" {
			hoisted = append(hoisted, arg)
			i++
			continue
		}
		if i+1 >= len(args) {
			// A value-taking flag with no value: leave it for the subcommand's
			// FlagSet to report properly.
			break
		}
		hoisted = append(hoisted, arg, args[i+1])
		i += 2
	}
	return args[i:], hoisted
}

func usage() {
	fmt.Fprint(os.Stderr, strings.TrimLeft(`
edgemeshctl - EdgeMesh control-plane CLI

Usage:
  edgemeshctl <command> [subcommand] [flags]

Commands:
  status                        cluster and leader summary
  raft status                   raft role, term, commit index, replication lag
  nodes                         registered edge nodes and liveness

  routes list                   list routes
  routes get <id>               show one route
  routes apply -f <file.yaml>   create or update a route
  routes delete <id>            delete a route

  origins list                  list origin pools
  origins get <id>              show one origin pool
  origins apply -f <file.yaml>  create or update an origin pool
  origins delete <id>           delete an origin pool

  settings get                  show global settings
  settings apply -f <file.yaml> update global settings

  cache purge --route <id>      purge every object for a route
  cache purge --key <key>       purge one cache key
  cache purge --all             purge everything

Global flags:
  --server <addr>    control-plane admin address (repeatable; default $EDGEMESH_SERVER
                     or http://127.0.0.1:7101)
  --token <token>    admin bearer token (default $EDGEMESH_TOKEN)
  --json             emit raw JSON instead of a table
  --timeout <dur>    request timeout (default 10s)

Environment:
  EDGEMESH_SERVER    comma-separated admin addresses
  EDGEMESH_TOKEN     admin bearer token

The CLI follows leader redirection automatically: point it at any control node.
`, "\n"))
}

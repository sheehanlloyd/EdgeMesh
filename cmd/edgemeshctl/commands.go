package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// newFlagSet parses a subcommand's arguments with the common flags attached.
//
// The FlagSet itself is not returned: every caller discarded it, and handing
// back a parser that has already been used invites a second Parse on it.
func newFlagSet(name string, args []string) (*client, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	c := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	c.finish()
	return c, nil
}

func cmdStatus(args []string) error {
	c, err := newFlagSet("status", args)
	if err != nil {
		return err
	}
	data, err := c.do("GET", "/v1/status", nil)
	if err != nil {
		return err
	}
	if c.json {
		return printJSON(data)
	}

	var s struct {
		NodeID        string `json:"node_id"`
		Version       string `json:"version"`
		Role          string `json:"role"`
		Term          uint64 `json:"term"`
		LeaderID      string `json:"leader_id"`
		IsLeader      bool   `json:"is_leader"`
		ConfigVersion uint64 `json:"config_version"`
		Routes        int    `json:"routes"`
		OriginPools   int    `json:"origin_pools"`
		EdgeNodes     int    `json:"edge_nodes"`
		CommitIndex   uint64 `json:"commit_index"`
		LastApplied   uint64 `json:"last_applied"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return printJSON(data)
	}

	w := table()
	fmt.Fprintf(w, "NODE\t%s\n", s.NodeID)
	fmt.Fprintf(w, "VERSION\t%s\n", s.Version)
	fmt.Fprintf(w, "ROLE\t%s\n", s.Role)
	fmt.Fprintf(w, "TERM\t%d\n", s.Term)
	fmt.Fprintf(w, "LEADER\t%s\n", s.LeaderID)
	fmt.Fprintf(w, "CONFIG VERSION\t%d\n", s.ConfigVersion)
	fmt.Fprintf(w, "ROUTES\t%d\n", s.Routes)
	fmt.Fprintf(w, "ORIGIN POOLS\t%d\n", s.OriginPools)
	fmt.Fprintf(w, "EDGE NODES\t%d\n", s.EdgeNodes)
	fmt.Fprintf(w, "COMMIT INDEX\t%d\n", s.CommitIndex)
	fmt.Fprintf(w, "LAST APPLIED\t%d\n", s.LastApplied)
	return w.Flush()
}

func cmdRaft(args []string) error {
	if len(args) > 0 && args[0] == "status" {
		args = args[1:]
	}
	c, err := newFlagSet("raft", args)
	if err != nil {
		return err
	}
	data, err := c.do("GET", "/v1/raft", nil)
	if err != nil {
		return err
	}
	if c.json {
		return printJSON(data)
	}

	var s struct {
		NodeID        string            `json:"node_id"`
		Role          string            `json:"role"`
		Term          uint64            `json:"term"`
		LeaderID      string            `json:"leader_id"`
		CommitIndex   uint64            `json:"commit_index"`
		LastApplied   uint64            `json:"last_applied"`
		LastLogIndex  uint64            `json:"last_log_index"`
		SnapshotIndex uint64            `json:"snapshot_index"`
		LogEntries    int               `json:"log_entries"`
		Peers         map[string]uint64 `json:"peer_match_index"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return printJSON(data)
	}

	w := table()
	fmt.Fprintf(w, "NODE\t%s\n", s.NodeID)
	fmt.Fprintf(w, "ROLE\t%s\n", s.Role)
	fmt.Fprintf(w, "TERM\t%d\n", s.Term)
	fmt.Fprintf(w, "LEADER\t%s\n", s.LeaderID)
	fmt.Fprintf(w, "COMMIT INDEX\t%d\n", s.CommitIndex)
	fmt.Fprintf(w, "LAST APPLIED\t%d\n", s.LastApplied)
	fmt.Fprintf(w, "LAST LOG INDEX\t%d\n", s.LastLogIndex)
	fmt.Fprintf(w, "SNAPSHOT INDEX\t%d\n", s.SnapshotIndex)
	fmt.Fprintf(w, "LOG ENTRIES\t%d\n", s.LogEntries)
	if err := w.Flush(); err != nil {
		return err
	}
	if len(s.Peers) > 0 {
		fmt.Println()
		pw := table()
		fmt.Fprintln(pw, "PEER\tMATCH INDEX\tLAG")
		for id, match := range s.Peers {
			lag := uint64(0)
			if s.LastLogIndex > match {
				lag = s.LastLogIndex - match
			}
			fmt.Fprintf(pw, "%s\t%d\t%d\n", id, match, lag)
		}
		return pw.Flush()
	}
	return nil
}

func cmdNodes(args []string) error {
	c, err := newFlagSet("nodes", args)
	if err != nil {
		return err
	}
	data, err := c.do("GET", "/v1/nodes", nil)
	if err != nil {
		return err
	}
	if c.json {
		return printJSON(data)
	}

	var resp struct {
		Nodes []struct {
			ID                   string      `json:"id"`
			Region               string      `json:"region"`
			Zone                 string      `json:"zone"`
			PeerAddress          string      `json:"peer_address"`
			PublicAddress        string      `json:"public_address"`
			State                string      `json:"state"`
			LastSeenUnixMs       protoInt64  `json:"last_seen_unix_ms"`
			ConfigVersionApplied protoUint64 `json:"config_version_applied"`
			Version              string      `json:"version"`
			Stats                *struct {
				InflightRequests  protoUint64 `json:"inflight_requests"`
				CacheObjects      protoUint64 `json:"cache_objects"`
				CacheBytes        protoUint64 `json:"cache_bytes"`
				RequestsPerSecond float64     `json:"requests_per_second"`
			} `json:"stats"`
		} `json:"nodes"`
		MembershipVersion protoUint64 `json:"membership_version"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return printJSON(data)
	}
	if len(resp.Nodes) == 0 {
		fmt.Println("no edge nodes are registered")
		return nil
	}

	w := table()
	fmt.Fprintln(w, "NODE\tSTATE\tREGION\tPEER ADDRESS\tCONFIG\tOBJECTS\tCACHE\tRPS\tLAST SEEN")
	for _, n := range resp.Nodes {
		last := "never"
		if n.LastSeenUnixMs > 0 {
			last = time.Since(time.UnixMilli(n.LastSeenUnixMs.Int64())).Truncate(time.Millisecond).String() + " ago"
		}
		// A node that has not heartbeated since this leader took over has no
		// reading yet. "-" says that, where a zero would claim an empty cache.
		objects, size, rps := "-", "-", "-"
		if n.Stats != nil {
			objects = strconv.FormatUint(n.Stats.CacheObjects.Uint64(), 10)
			size = humanBytes(n.Stats.CacheBytes.Uint64())
			rps = strconv.FormatFloat(n.Stats.RequestsPerSecond, 'f', 1, 64)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			n.ID, shortState(n.State), n.Region, n.PeerAddress,
			n.ConfigVersionApplied.Uint64(), objects, size, rps, last)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Printf("\nmembership version %d\n", resp.MembershipVersion.Uint64())
	return nil
}

func cmdRoutes(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("routes requires a subcommand: list, get, apply, delete")
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "list":
		c, err := newFlagSet("routes list", rest)
		if err != nil {
			return err
		}
		data, err := c.do("GET", "/v1/routes", nil)
		if err != nil {
			return err
		}
		if c.json {
			return printJSON(data)
		}
		var resp struct {
			Routes []struct {
				ID           string      `json:"id"`
				Hostname     string      `json:"hostname"`
				PathPrefix   string      `json:"path_prefix"`
				OriginPoolID string      `json:"origin_pool_id"`
				Enabled      bool        `json:"enabled"`
				Version      protoUint64 `json:"version"`
				CachePolicy  struct {
					Enabled           bool   `json:"enabled"`
					DefaultTTLSeconds uint32 `json:"default_ttl_seconds"`
				} `json:"cache_policy"`
			} `json:"routes"`
			ConfigVersion uint64 `json:"config_version"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return printJSON(data)
		}
		if len(resp.Routes) == 0 {
			fmt.Println("no routes are configured")
			return nil
		}
		w := table()
		fmt.Fprintln(w, "ROUTE\tHOSTNAME\tPREFIX\tPOOL\tENABLED\tCACHE\tVERSION")
		for _, r := range resp.Routes {
			cacheDesc := "off"
			if r.CachePolicy.Enabled {
				cacheDesc = fmt.Sprintf("%ds", r.CachePolicy.DefaultTTLSeconds)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%t\t%s\t%d\n",
				r.ID, r.Hostname, r.PathPrefix, r.OriginPoolID, r.Enabled, cacheDesc, r.Version.Uint64())
		}
		if err := w.Flush(); err != nil {
			return err
		}
		fmt.Printf("\nconfig version %d\n", resp.ConfigVersion)
		return nil

	case "get":
		if len(rest) == 0 {
			return fmt.Errorf("routes get requires a route id")
		}
		id, flags := rest[0], rest[1:]
		c, err := newFlagSet("routes get", flags)
		if err != nil {
			return err
		}
		data, err := c.do("GET", "/v1/routes/"+id, nil)
		if err != nil {
			return err
		}
		return printJSON(data)

	case "apply":
		return applyDocument(rest, "routes apply", "/v1/routes", "id")

	case "delete":
		if len(rest) == 0 {
			return fmt.Errorf("routes delete requires a route id")
		}
		id, flags := rest[0], rest[1:]
		c, err := newFlagSet("routes delete", flags)
		if err != nil {
			return err
		}
		data, err := c.do("DELETE", "/v1/routes/"+id, nil)
		if err != nil {
			return err
		}
		return printJSON(data)

	default:
		return fmt.Errorf("unknown routes subcommand %q", sub)
	}
}

func cmdOrigins(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("origins requires a subcommand: list, get, apply, delete")
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "list":
		c, err := newFlagSet("origins list", rest)
		if err != nil {
			return err
		}
		data, err := c.do("GET", "/v1/origin-pools", nil)
		if err != nil {
			return err
		}
		if c.json {
			return printJSON(data)
		}
		var resp struct {
			OriginPools []struct {
				ID      string      `json:"id"`
				Version protoUint64 `json:"version"`
				Origins []struct {
					ID     string `json:"id"`
					Scheme string `json:"scheme"`
					Host   string `json:"host"`
					Port   uint32 `json:"port"`
					Weight uint32 `json:"weight"`
				} `json:"origins"`
				LoadBalancing string `json:"load_balancing"`
			} `json:"origin_pools"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return printJSON(data)
		}
		if len(resp.OriginPools) == 0 {
			fmt.Println("no origin pools are configured")
			return nil
		}
		w := table()
		fmt.Fprintln(w, "POOL\tORIGINS\tLOAD BALANCING\tVERSION")
		for _, p := range resp.OriginPools {
			targets := make([]string, 0, len(p.Origins))
			for _, o := range p.Origins {
				targets = append(targets, fmt.Sprintf("%s://%s:%d", o.Scheme, o.Host, o.Port))
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\n",
				p.ID, strings.Join(targets, ","), shortLB(p.LoadBalancing), p.Version.Uint64())
		}
		return w.Flush()

	case "get":
		if len(rest) == 0 {
			return fmt.Errorf("origins get requires a pool id")
		}
		id, flags := rest[0], rest[1:]
		c, err := newFlagSet("origins get", flags)
		if err != nil {
			return err
		}
		data, err := c.do("GET", "/v1/origin-pools/"+id, nil)
		if err != nil {
			return err
		}
		return printJSON(data)

	case "apply":
		return applyDocument(rest, "origins apply", "/v1/origin-pools", "id")

	case "delete":
		if len(rest) == 0 {
			return fmt.Errorf("origins delete requires a pool id")
		}
		id, flags := rest[0], rest[1:]
		c, err := newFlagSet("origins delete", flags)
		if err != nil {
			return err
		}
		data, err := c.do("DELETE", "/v1/origin-pools/"+id, nil)
		if err != nil {
			return err
		}
		return printJSON(data)

	default:
		return fmt.Errorf("unknown origins subcommand %q", sub)
	}
}

func cmdSettings(args []string) error {
	if len(args) == 0 {
		args = []string{"get"}
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "get":
		c, err := newFlagSet("settings get", rest)
		if err != nil {
			return err
		}
		data, err := c.do("GET", "/v1/settings", nil)
		if err != nil {
			return err
		}
		return printJSON(data)

	case "apply":
		fs := flag.NewFlagSet("settings apply", flag.ContinueOnError)
		file := fs.String("f", "", "path to a YAML or JSON settings document")
		c := commonFlags(fs)
		if err := fs.Parse(rest); err != nil {
			return err
		}
		c.finish()
		if *file == "" {
			return fmt.Errorf("settings apply requires -f <file>")
		}
		doc, err := loadDocument(*file)
		if err != nil {
			return err
		}
		data, err := c.do("PUT", "/v1/settings", doc)
		if err != nil {
			return err
		}
		return printJSON(data)

	default:
		return fmt.Errorf("unknown settings subcommand %q", sub)
	}
}

func cmdCache(args []string) error {
	if len(args) == 0 || args[0] != "purge" {
		return fmt.Errorf("cache requires the purge subcommand")
	}
	fs := flag.NewFlagSet("cache purge", flag.ContinueOnError)
	route := fs.String("route", "", "purge every object belonging to this route")
	cacheKey := fs.String("key", "", "purge one exact cache key")
	all := fs.Bool("all", false, "purge every cached object on every edge")
	c := commonFlags(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	c.finish()

	// Exactly one scope must be chosen: an ambiguous purge could silently do
	// far more than the operator intended.
	chosen := 0
	body := map[string]any{}
	switch {
	case *all:
		chosen++
		body["scope"] = "all"
	}
	if *route != "" {
		chosen++
		body["scope"] = "route"
		body["route_id"] = *route
	}
	if *cacheKey != "" {
		chosen++
		body["scope"] = "key"
		body["cache_key"] = *cacheKey
	}
	if chosen != 1 {
		return fmt.Errorf("specify exactly one of --route, --key, or --all")
	}

	data, err := c.do("POST", "/v1/cache/purge", body)
	if err != nil {
		return err
	}
	return printJSON(data)
}

// applyDocument implements the shared create-or-update flow.
//
// It chooses POST or PUT from whether the object already exists, so an operator
// can run the same `apply -f` command repeatedly and get idempotent behaviour
// rather than a 409 on the second run.
func applyDocument(args []string, name, basePath, idField string) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	file := fs.String("f", "", "path to a YAML or JSON document")
	c := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c.finish()

	if *file == "" {
		return fmt.Errorf("%s requires -f <file>", name)
	}
	doc, err := loadDocument(*file)
	if err != nil {
		return err
	}
	id, _ := doc[idField].(string)
	if id == "" {
		return fmt.Errorf("%q must contain a %q field", *file, idField)
	}

	exists := false
	if _, err := c.do("GET", basePath+"/"+id, nil); err == nil {
		exists = true
	}

	method, path := "POST", basePath
	if exists {
		method, path = "PUT", basePath+"/"+id
	}
	data, err := c.do(method, path, doc)
	if err != nil {
		return err
	}
	if c.json {
		return printJSON(data)
	}
	action := "created"
	if exists {
		action = "updated"
	}
	fmt.Printf("%s %s\n", action, id)
	return printJSON(data)
}

func table() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
}

// humanBytes renders a byte count in the largest unit that keeps it readable.
//
// Cache occupancy spans several orders of magnitude across a fleet, and a
// column of raw byte counts is a column nobody reads.
func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatUint(n, 10) + "B"
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit && exp < 3; v /= unit {
		div *= unit
		exp++
	}
	return strconv.FormatFloat(float64(n)/float64(div), 'f', 1, 64) + string("KMGT"[exp]) + "iB"
}

// shortState trims the protobuf enum prefix for display.
func shortState(s string) string {
	return strings.ToLower(strings.TrimPrefix(s, "NODE_STATE_"))
}

func shortLB(s string) string {
	return strings.ToLower(strings.TrimPrefix(s, "LOAD_BALANCING_"))
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// client talks to the admin API, following leader redirection.
type client struct {
	servers []string
	token   string
	timeout time.Duration
	json    bool
	http    *http.Client
	// resolve normalizes the server list after flag parsing, since flag values
	// are not available until Parse has run.
	resolve func()
}

// commonFlags registers the flags every subcommand accepts.
func commonFlags(fs *flag.FlagSet) *client {
	c := &client{}
	var servers string
	fs.StringVar(&servers, "server", os.Getenv("EDGEMESH_SERVER"), "control-plane admin address(es), comma separated")
	fs.StringVar(&c.token, "token", os.Getenv("EDGEMESH_TOKEN"), "admin bearer token")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "request timeout")
	fs.BoolVar(&c.json, "json", false, "emit raw JSON")

	// The slice is resolved after parsing, so this closure is stashed on the
	// client and invoked by parse.
	c.resolve = func() {
		if servers == "" {
			servers = "http://127.0.0.1:7101"
		}
		for _, s := range strings.Split(servers, ",") {
			if s = strings.TrimSpace(s); s != "" {
				if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
					s = "http://" + s
				}
				c.servers = append(c.servers, strings.TrimRight(s, "/"))
			}
		}
	}
	return c
}

func (c *client) finish() {
	if c.resolve != nil {
		c.resolve()
	}
	c.http = &http.Client{Timeout: c.timeout}
}

// errorBody mirrors the admin API's error response.
type errorBody struct {
	Error         string `json:"error"`
	Message       string `json:"message"`
	LeaderID      string `json:"leader_id"`
	LeaderAddress string `json:"leader_address"`
}

// do performs a request, following a leader hint when a follower answers.
//
// Following the hint is what lets a user point the CLI at any control node.
// Without it every operator would need to discover the leader before every
// write, which is exactly the friction the leader hint exists to remove.
func (c *client) do(method, path string, body any) ([]byte, error) {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}
	}

	// Each configured server is tried, plus one extra attempt for a leader
	// hint. The bound stops a hint loop between two nodes that disagree.
	attempts := len(c.servers) + 2
	target := c.servers[0]
	var lastErr error

	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		req, err := http.NewRequestWithContext(ctx, method, target+path, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return nil, err
		}
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.http.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			// This server is unreachable; try the next configured one.
			target = c.servers[(i+1)%len(c.servers)]
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		_ = resp.Body.Close()
		cancel()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}

		var e errorBody
		_ = json.Unmarshal(data, &e)

		if resp.StatusCode == http.StatusServiceUnavailable && e.LeaderAddress != "" {
			next := e.LeaderAddress
			if !strings.HasPrefix(next, "http://") && !strings.HasPrefix(next, "https://") {
				next = "http://" + next
			}
			if next == target {
				// The leader hint points back here, which means the cluster has
				// no stable leader right now.
				return nil, fmt.Errorf("no stable leader: %s", e.Message)
			}
			target = strings.TrimRight(next, "/")
			continue
		}
		if e.Message != "" {
			return nil, fmt.Errorf("%s (%s)", e.Message, e.Error)
		}
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if lastErr != nil {
		return nil, fmt.Errorf("no control node reachable: %w", lastErr)
	}
	return nil, fmt.Errorf("no control node answered after %d attempts", attempts)
}

// printJSON writes indented JSON.
func printJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		// Not JSON: print it as-is rather than failing. A plain-text response
		// is a valid thing for a server to send, and refusing to display it
		// would be less useful than showing it unformatted.
		fmt.Println(strings.TrimSpace(string(data)))
		return nil //nolint:nilerr // deliberate: fall back to raw output
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// loadDocument reads a YAML or JSON document into a generic map.
//
// YAML is accepted because it is what an operator writes by hand; it is
// converted to JSON because that is what the admin API speaks.
func loadDocument(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}
	if len(doc) == 0 {
		return nil, fmt.Errorf("%q is empty", path)
	}
	return doc, nil
}

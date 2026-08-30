package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// newGoCollector exposes goroutine counts, GC behaviour, and heap statistics.
// The Grafana runtime row and the goroutine-leak checks both read from it.
func newGoCollector() prometheus.Collector { return collectors.NewGoCollector() }

// newProcessCollector exposes CPU, resident memory, and file descriptors.
func newProcessCollector() prometheus.Collector {
	return collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})
}

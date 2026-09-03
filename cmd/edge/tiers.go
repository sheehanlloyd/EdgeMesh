package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/sheehanlloyd/edgemesh/internal/cache"
	"github.com/sheehanlloyd/edgemesh/internal/cache/l1"
	"github.com/sheehanlloyd/edgemesh/internal/config"
	"github.com/sheehanlloyd/edgemesh/internal/observability"
)

// newTier builds one cache tier with the configured admission policy.
//
// Both tiers use the same implementation and differ only in budget: L1 is the
// node's local fast path, L2 is the node's share of the distributed cache.
func newTier(cfg *config.EdgeConfig, maxBytes uint64, tier cache.Tier, m *observability.Metrics) (*l1.Cache, error) {
	var admitter l1.Admitter
	if cfg.Cache.Policy == "tinylfu" {
		// Sizing the sketch from the byte budget and a nominal object size
		// keeps its memory proportional to what the tier can actually hold.
		const nominalObjectBytes = 8 << 10
		expected := int(maxBytes / nominalObjectBytes)
		admitter = l1.NewTinyLFU(expected)
	}
	return l1.New(l1.Options{
		MaxBytes:        int64(maxBytes),
		Shards:          cfg.Cache.Shards,
		MaxObjectBytes:  int64(cfg.Cache.MaxObjectBytes),
		CleanupInterval: cfg.Cache.CleanupInterval,
		Admitter:        admitter,
		OnEvict: func(_ *cache.Object, reason cache.EvictionReason) {
			m.CacheEvictions.WithLabelValues(string(tier), string(reason)).Inc()
		},
	})
}

// closeTier stops a tier's sweeper.
func closeTier(c *l1.Cache, tier cache.Tier, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		log.Warn("cache tier did not shut down cleanly",
			slog.String("tier", string(tier)), slog.String("error", err.Error()))
	}
}

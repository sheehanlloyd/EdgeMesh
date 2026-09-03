package integration

import (
	"io"
	"log/slog"
)

// quietLogger discards the cluster's chatter. Raise the level here when
// debugging a specific integration failure.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

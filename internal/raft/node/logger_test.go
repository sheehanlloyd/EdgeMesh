package node

import (
	"io"
	"log/slog"
	"testing"
)

// testLogger discards Raft's chatter by default. Raise the level here when
// debugging a specific consensus failure.
func testLogger(_ *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

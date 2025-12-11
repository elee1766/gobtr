package btrfs

import (
	"log/slog"

	"go.uber.org/fx"
)

// Package btrfs will provide functionality for:
// - Reading filesystem errors
// - Running scrubs
// - Managing snapshots (btrbk alternative)
// - Device statistics and monitoring

var Module = fx.Module("btrfs",
	fx.Provide(New),
)

type Manager struct {
	logger *slog.Logger
}

func New(logger *slog.Logger) *Manager {
	return &Manager{
		logger: logger.With("component", "btrfs"),
	}
}

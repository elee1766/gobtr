package main

import (
	"log/slog"
	"os"

	"github.com/elee1766/gobtr/pkg/api"
	"github.com/elee1766/gobtr/pkg/btrfs"
	"github.com/elee1766/gobtr/pkg/config"
	"github.com/elee1766/gobtr/pkg/db"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func main() {
	app := fx.New(
		fx.Provide(
			provideLogger,
			config.New,
		),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log}
		}),
		db.Module,
		btrfs.Module,
		api.Module,
	)

	app.Run()
}

func provideLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	return slog.New(handler)
}

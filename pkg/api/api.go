package api

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"

	"github.com/elee1766/gobtr/gen/api/v1/apiv1connect"
	"github.com/elee1766/gobtr/pkg/config"
	"github.com/elee1766/gobtr/pkg/handlers"
	"go.uber.org/fx"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// EmbeddedFS can be set to an embed.FS to serve static files from embedded assets.
// If nil, files will be served from the StaticDir on disk.
var EmbeddedFS fs.FS

// StaticDir is the directory to serve static files from when EmbeddedFS is nil.
var StaticDir = "web/dist"

var Module = fx.Module("api",
	fx.Provide(
		NewServer,
		handlers.NewHealthHandler,
		handlers.NewSnapshotHandler,
		handlers.NewFilesystemHandler,
		handlers.NewScrubHandler,
		handlers.NewBalanceHandler,
		handlers.NewSubvolumeHandler,
		handlers.NewUsageHandler,
		handlers.NewFragMapHandler,
	),
	fx.Invoke(registerHooks),
)

type Server struct {
	http   *http.Server
	logger *slog.Logger
}

type HandlerParams struct {
	fx.In

	Health     *handlers.HealthHandler
	Snapshot   *handlers.SnapshotHandler
	Filesystem *handlers.FilesystemHandler
	Scrub      *handlers.ScrubHandler
	Balance    *handlers.BalanceHandler
	Subvolume  *handlers.SubvolumeHandler
	Usage      *handlers.UsageHandler
	FragMap    *handlers.FragMapHandler
}

type ServerParams struct {
	fx.In

	Config   *config.Config
	Logger   *slog.Logger
	Handlers HandlerParams
}

func NewServer(p ServerParams) *Server {
	logger := p.Logger.With("component", "api")
	h := p.Handlers

	mux := http.NewServeMux()

	// Register all Connect handlers
	register := func(path string, handler http.Handler) {
		mux.Handle(path, handler)
	}
	register(apiv1connect.NewHealthServiceHandler(h.Health))
	register(apiv1connect.NewSnapshotServiceHandler(h.Snapshot))
	register(apiv1connect.NewFilesystemServiceHandler(h.Filesystem))
	register(apiv1connect.NewScrubServiceHandler(h.Scrub))
	register(apiv1connect.NewBalanceServiceHandler(h.Balance))
	register(apiv1connect.NewSubvolumeServiceHandler(h.Subvolume))
	register(apiv1connect.NewUsageServiceHandler(h.Usage))
	register(apiv1connect.NewFragMapServiceHandler(h.FragMap))

	// Register pprof handlers for profiling
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	logger.Info("pprof endpoints enabled at /debug/pprof/")

	// Serve static files with SPA fallback
	if EmbeddedFS != nil {
		logger.Info("serving frontend from embedded filesystem")
		mux.Handle("/", spaHandlerFS(EmbeddedFS))
	} else {
		logger.Info("serving frontend from disk", "dir", StaticDir)
		mux.Handle("/", spaHandlerDir(StaticDir))
	}

	// Use h2c for HTTP/2 without TLS
	h2cHandler := h2c.NewHandler(mux, &http2.Server{})

	return &Server{
		http: &http.Server{
			Addr:    p.Config.APIAddress,
			Handler: h2cHandler,
		},
		logger: logger,
	}
}

// spaHandlerDir serves static files from a directory with SPA fallback.
func spaHandlerDir(dir string) http.Handler {
	fileServer := http.FileServer(http.Dir(dir))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Try to serve the file directly
		fullPath := filepath.Join(dir, path)
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			fileServer.ServeHTTP(w, r)
			return
		}

		// For paths that look like files (have extension), return 404
		if strings.Contains(filepath.Base(path), ".") {
			http.NotFound(w, r)
			return
		}

		// For SPA routes, serve index.html
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}

// spaHandlerFS serves static files from an fs.FS with SPA fallback.
func spaHandlerFS(fsys fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		// Try to open the file to check if it exists
		f, err := fsys.Open(path)
		if err == nil {
			defer f.Close()
			stat, err := f.Stat()
			if err == nil && !stat.IsDir() {
				http.ServeFileFS(w, r, fsys, path)
				return
			}
		}

		// For paths that look like files (have extension), return 404
		if strings.Contains(filepath.Base(path), ".") {
			http.NotFound(w, r)
			return
		}

		// For SPA routes, serve index.html directly
		http.ServeFileFS(w, r, fsys, "index.html")
	})
}

func registerHooks(lc fx.Lifecycle, s *Server) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				s.logger.Info("starting api server", "address", s.http.Addr)
				if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					s.logger.Error("api server error", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.logger.Info("stopping api server")
			return s.http.Shutdown(ctx)
		},
	})
}

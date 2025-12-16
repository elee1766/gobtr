# dev information

The dev server runs on port 8147.

**IMPORTANT**: The server is managed by the user. Do NOT start, stop, or restart the server yourself. If a restart is needed after code changes, ask the user to restart it.

# project structure

- `cmd/gobtr/` - Go backend entrypoint
- `pkg/` - Go backend packages (handlers, btrfs, fragmap, etc.)
- `proto/` - Protobuf definitions for the API
- `gen/` - Generated protobuf Go code
- `web/` - SolidJS frontend
  - `web/src/pages/` - Page components
  - `web/src/components/` - Reusable components
  - `web/src/api/` - API client (connect-rpc)

# Makefile targets

- `make build` - Build web frontend + Go server + set capabilities (default)
- `make web` - Build only the web frontend (SolidJS)
- `make server` - Build only the Go server binary
- `make setcap` - Set Linux capabilities for btrfs access (requires sudo)
- `make embed` - Build production binary with embedded frontend (single file)
- `make generate` - Regenerate protobuf code (Go + TypeScript)
- `make clean` - Remove build artifacts
- `make run` - Build and run the server
- `make dev` - Run vite dev server for frontend (backend must run separately)

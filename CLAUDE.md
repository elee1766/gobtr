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

Build with `make build`. Run `make generate` to regenerate protobuf code.

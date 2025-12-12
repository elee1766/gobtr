.PHONY: all build web server clean run generate dev setcap install embed

all: build

# Build both web and server (development - serves from disk)
build: web server setcap

# Build web frontend (Solid + Vite)
web:
	@echo "Building web frontend..."
	cd web && npm run build
	@echo "Web frontend built: web/dist/"

# Build server binary (development - serves from disk)
server:
	@echo "Building server binary..."
	@mkdir -p bin
	go build -o bin/gobtr ./cmd/gobtr
	@echo "Server binary built: bin/gobtr"

# Build server with embedded frontend (production - single binary)
embed: web
	@echo "Building server with embedded frontend..."
	@mkdir -p bin
	@rm -rf cmd/gobtr/dist
	@cp -r web/dist cmd/gobtr/dist
	go build -tags embed -o bin/gobtr ./cmd/gobtr
	@rm -rf cmd/gobtr/dist
	@echo "Server binary built with embedded assets: bin/gobtr"

# Set capabilities for btrfs access (requires sudo)
setcap: server
	@echo "Setting capabilities on bin/gobtr..."
	sudo setcap 'cap_sys_admin,cap_dac_read_search+ep' bin/gobtr
	@echo "Capabilities set. Verify with: getcap bin/gobtr"

# Build and set capabilities
install: build setcap

# Generate protobuf code (Go + TypeScript)
generate:
	@echo "Generating Go protobuf code..."
	cd proto && buf generate
	@echo "Generating TypeScript protobuf code..."
	cd web && npm run generate
	@echo "Code generation complete"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin web/dist
	@echo "Clean complete"

# Run the server (builds if needed)
run: build
	./bin/gobtr

# Development mode - run vite dev server (requires backend running separately)
dev:
	cd web && npm run dev

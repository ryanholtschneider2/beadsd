.PHONY: build install clean run

# Build the daemon
build:
	go build -o bin/beadsd ./cmd/beadsd

# Install to $GOPATH/bin
install:
	go install ./cmd/beadsd

# Clean build artifacts
clean:
	rm -rf bin/

# Run the daemon (for development)
run:
	go run ./cmd/beadsd --workspace=$(PWD)/..

# Run with verbose logging
run-verbose:
	go run ./cmd/beadsd --workspace=$(PWD)/.. --poll-interval=10s

# Download dependencies
deps:
	go mod tidy
	go mod download

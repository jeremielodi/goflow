# syntax=docker/dockerfile:1
# Dockerfile
FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app
COPY . .

# Dependencies are vendored (./vendor, committed to the repo via `go mod
# vendor`) so this build never needs network access — this host has hit
# severe, unrelated Docker network flakiness reaching proxy.golang.org
# mid-build, and vendoring makes the build fully hermetic instead of
# fighting that per-attempt. Regenerate with `go mod vendor` after
# changing go.mod.
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -a -installsuffix cgo -o goflow .

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy the binary
COPY --from=builder /app/goflow .

# Copy .env file
COPY .env .env

# Expose port
EXPOSE 8080

# Run the application
CMD ["./goflow"]
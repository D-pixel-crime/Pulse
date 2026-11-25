# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS builder

WORKDIR /src

# Download dependencies first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy application source and embedded SQL schema
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pulse ./cmd/pulse

FROM alpine:3

# Install CA certificates for outbound HTTPS health checks and tzdata for timezone handling
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S pulse \
    && adduser -S -G pulse pulse

WORKDIR /app

COPY --from=builder /out/pulse /app/pulse

USER pulse

EXPOSE 8080

ENV APP_ADDR=0.0.0.0:8080

ENTRYPOINT ["/app/pulse"]

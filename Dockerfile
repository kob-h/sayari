# Multi-stage build. One image contains all three binaries; the compose file
# selects which one each service runs via `command`. This keeps the image small
# and the build cached across services.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache dependencies first.
COPY go.mod go.sum ./
RUN go mod download

# Build all three binaries.
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api        ./cmd/api && \
    CGO_ENABLED=0 go build -o /out/extractor  ./cmd/extractor && \
    CGO_ENABLED=0 go build -o /out/classifier ./cmd/classifier

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/api /out/extractor /out/classifier /usr/local/bin/
USER app
# Default command; each service overrides it in docker-compose.yml. Using CMD
# (not ENTRYPOINT) so `command: ["extractor"]` replaces the binary rather than
# being appended as an argument to it.
CMD ["api"]

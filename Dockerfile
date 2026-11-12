ARG GO_VERSION_TAG=1.26-alpine
FROM golang:${GO_VERSION_TAG} AS builder

ARG CMD
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY gen/ gen/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -o /app/server ./cmd/${CMD}
RUN go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.39

FROM golang:${GO_VERSION_TAG}
COPY --from=builder /app/server /app/server
COPY --from=builder /go/bin/grpc-health-probe /grpc-health-probe
ENTRYPOINT ["/app/server"]

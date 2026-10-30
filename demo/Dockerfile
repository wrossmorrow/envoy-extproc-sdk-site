ARG GO_VERSION_TAG=1.26-alpine

FROM golang:${GO_VERSION_TAG} AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /app/demo

RUN go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.39

FROM golang:${GO_VERSION_TAG}
COPY --from=builder /app/demo /app/demo
COPY --from=builder /go/bin/grpc-health-probe /grpc-health-probe
EXPOSE 50051
EXPOSE 9090
ENTRYPOINT [ "/app/demo" ]

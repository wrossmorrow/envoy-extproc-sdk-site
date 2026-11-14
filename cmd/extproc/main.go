package main

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	sdk "github.com/wrossmorrow/envoy-extproc-sdk-go"
	extproc "github.com/wrossmorrow/envoy-extproc-sdk-site/internal/extproc"
)

const (
	defaultControllerAddr = "controller:9093"

	// demoMetricsPort is the port the demo serves its own Prometheus metrics
	// endpoint on (Go runtime goroutines) in addition to the SDK metrics endpoint.
	demoMetricsPort = 9096
)

func main() {
	sopts := sdk.NewDefaultServerOptions()
	sopts.ExtProcPort = 50051
	sopts.MetricsHTTPPort = 9090
	sopts.TerminationGracePeriodSeconds = 10

	proc := extproc.NewRequestProcessor(defaultControllerAddr)

	sdk.MustServe(sopts, proc, slog.Default())

	startDemoMetrics(demoMetricsPort, slog.Default())
}

// startDemoMetrics starts a Prometheus metrics server for the demo that exposes
// the Go runtime collector (goroutines) in addition to the metrics exposed by the
// extproc SDK server started inside MustServe.
func startDemoMetrics(port int, logger *slog.Logger) {
	registry := prometheus.NewRegistry()
	if err := registry.Register(prometheus.NewGoCollector()); err != nil {
		logger.Error("failed to register go runtime collector", slog.String("error", err.Error()))
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("Started demo metrics HTTP server", slog.Int("port", port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start demo metrics HTTP server", slog.String("error", err.Error()))
		}
	}()
}

package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
	"google.golang.org/grpc"
	hpb "google.golang.org/grpc/health/grpc_health_v1"

	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
	controller "github.com/wrossmorrow/envoy-extproc-sdk-site/internal/controller"
)

const (
	listenAddr = "0.0.0.0:9092"
	grpcAddr   = "0.0.0.0:9093"
)

// newMux builds the http.Handler for the demo controller service: the
// human-facing Connect API, healthz, and Prometheus metrics. The interactive
// demo static frontend is served by cmd/web, and Envoy fronts all browser and
// demo traffic on a single origin.
func newMux(sessCtrl *controller.SessionController) (*http.ServeMux, string) {
	mux := http.NewServeMux()
	controllerPath, controllerHandler := v1connect.NewDemoControllerServiceHandler(&controller.DemoControllerHandler{Controller: sessCtrl})
	mux.Handle(controllerPath, controllerHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handler := promhttp.HandlerFor(controller.MetricsRegistry(), promhttp.HandlerOpts{EnableOpenMetrics: true})
		handler.ServeHTTP(w, r)
	})
	return mux, controllerPath
}

func main() {
	sessCtrl := controller.NewSessionController()
	mux, _ := newMux(sessCtrl)
	srv := &http.Server{Addr: listenAddr, Handler: mux}

	// gRPC server hosting the demo controller (human-facing) and demo processor
	// (internal channel) services, sharing the same session controller.
	gRPCServer := grpc.NewServer()
	hpb.RegisterHealthServer(gRPCServer, extproc.NewReadyHealthServer())
	v1.RegisterDemoControllerServiceServer(gRPCServer, &controller.DemoControllerGRPCServer{Controller: sessCtrl})
	v1.RegisterDemoProcessorServiceServer(gRPCServer, &controller.DemoProcessorGRPCServer{Controller: sessCtrl})
	gRPCListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("controller gRPC server error", "error", err.Error())
		return
	}
	go func() {
		if err := gRPCServer.Serve(gRPCListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("controller gRPC server error", "error", err.Error())
		}
	}()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("controller shutdown error", "error", err.Error())
		}
		gRPCServer.Stop()
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("controller server error", "error", err.Error())
	}
}

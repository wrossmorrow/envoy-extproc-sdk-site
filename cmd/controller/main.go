package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
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

// envoyAddr is the upstream Envoy instance relaying envoy-bound demo
// requests proxied through the demo web frontend.
var envoyAddr = envOr("ENVOY_ADDR", "localhost:8081")

// envOr returns the value of the environment variable named key, or fallback if unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newMux builds the http.Handler for the demo controller service: the
// human-facing Connect API and an envoy reverse proxy for envoy-bound demo
// requests. The interactive demo static frontend is served by the standalone
// web asset server (cmd/web), not here. Browser-facing endpoints answer CORS
// preflights so the standalone web origin can drive the controller cross-origin.
func newMux(sessCtrl *controller.SessionController) (*http.ServeMux, string) {
	mux := http.NewServeMux()
	controllerPath, controllerHandler := v1connect.NewDemoControllerServiceHandler(&controller.DemoControllerHandler{Controller: sessCtrl})
	mux.Handle(controllerPath, corsHandler(controllerHandler))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handler := promhttp.HandlerFor(controller.MetricsRegistry(), promhttp.HandlerOpts{EnableOpenMetrics: true})
		handler.ServeHTTP(w, r)
	})
	envoyProxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = envoyAddr
			req.URL.Path = "/"
		},
	}
	mux.Handle("/envoy/", corsHandler(envoyProxy))
	return mux, controllerPath
}

// writeCorsPreflight answers an CORS preflight (OPTIONS) for the controller's
// browser-facing endpoints with the headers the interactive demo frontend needs
// to send cross-origin requests (Connect streaming, envoy-bound demo requests
// with custom headers and JSON content types).
func writeCorsPreflight(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Accept, Accept-Encoding, Content-Type, Connect-Protocol-Version, X-Http-Method-Override, X-Request-Id, X-Demo-Session-Id, X-Demo-Target-Lang, Authorization")
	h.Set("Access-Control-Max-Age", "86400")
	w.WriteHeader(http.StatusNoContent)
}

// corsHandler wraps a browser-facing controller handler: it answers CORS
// preflights (OPTIONS) and attaches the CORS headers to actual responses, so
// the standalone web origin can drive the controller cross-origin.
func corsHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCorsPreflight(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
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

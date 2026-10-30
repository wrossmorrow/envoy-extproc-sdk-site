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

	"google.golang.org/grpc"

	"github.com/wrossmorrow/envoy-extproc-sdk-controller/controller"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1/v1connect"
)

const (
	listenAddr = "0.0.0.0:9092"
	grpcAddr   = "0.0.0.0:9093"
)

func main() {
	handler := &controller.DemoControllerHandler{Controller: controller.NewSessionController()}
	path, h := v1connect.NewDemoControllerHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := &http.Server{Addr: listenAddr, Handler: mux}

	// gRPC server hosting the demo controller (human-facing) and demo processor
	// (internal channel) services.
	gRPCServer := grpc.NewServer()
	v1.RegisterDemoControllerServer(gRPCServer, &controller.DemoControllerGRPCServer{Controller: controller.NewSessionController()})
	v1.RegisterDemoProcessorServer(gRPCServer, &controller.DemoProcessorGRPCServer{Controller: controller.NewSessionController()})
	gRPCListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("controller gRPC server error", "error", err.Error())
		return
	}
	go func() {
		if err := gRPCServer.Serve(gRPCListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

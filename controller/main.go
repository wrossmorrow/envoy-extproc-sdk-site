package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wrossmorrow/envoy-extproc-sdk-controller/controller"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1/v1connect"
)

func main() {
	handler := &controller.DemoControllerHandler{Controller: controller.NewSessionController()}
	path, h := v1connect.NewDemoControllerHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := &http.Server{Addr: "127.0.0.1:9092", Handler: mux}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("controller shutdown error", "error", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("controller server error", "error", err)
	}
}

// Command web serves the static assets of the interactive envoy extproc demo
// frontend. It is a standalone static-asset server: it embeds the frontend files
// from web/static and serves them at the root path, nothing else. The
// human-facing controller Connect API and the envoy reverse proxy for
// envoy-bound demo requests are served by the controller service; the browser
// reaches them through the controller-base and envoy-base meta tags declared in
// index.html.
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

	"github.com/wrossmorrow/envoy-extproc-sdk-site/web/static"
)

const (
	listenAddr = "0.0.0.0:8000"
)

func main() {
	mux := newMux()
	srv := &http.Server{Addr: listenAddr, Handler: mux}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("web shutdown error", "error", err.Error())
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("web server error", "error", err.Error())
	}
}

// newMux builds the http.Handler for the standalone web asset server: the
// embedded demo frontend static assets.
func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static.FS)))
	return mux
}

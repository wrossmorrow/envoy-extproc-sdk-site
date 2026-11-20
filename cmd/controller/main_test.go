package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	connect "connectrpc.com/connect"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
	controller "github.com/wrossmorrow/envoy-extproc-sdk-site/internal/controller"
)

// TestHandlerServesHealthAndRejectsUnmountedPaths asserts that the controller
// serves /healthz and returns 404 for unmounted paths (static assets live in
// cmd/web and demo requests go directly to Envoy).
func TestHandlerServesHealthAndRejectsUnmountedPaths(t *testing.T) {
	mux, _ := newMux(controller.NewSessionController())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	for _, missing := range []string{"/envoy/", "/missing-asset.txt"} {
		r, err := http.Get(srv.URL + missing)
		if err != nil {
			t.Fatalf("GET %s: %v", missing, err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want %d", missing, r.StatusCode, http.StatusNotFound)
		}
	}
}

// TestHandlerMountsControllerConnect asserts that the human-facing Connect API is
// mounted on the controller HTTP handler and answers a CreateSession
// server-streaming call.
func TestHandlerMountsControllerConnect(t *testing.T) {
	mux, _ := newMux(controller.NewSessionController())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := v1connect.NewDemoControllerServiceClient(&http.Client{}, srv.URL)
	ctx := context.Background()
	stream, err := client.CreateSession(ctx, &connect.Request[v1.CreateSessionRequest]{})
	if err != nil {
		t.Fatalf("create session via mounted handler: %v", err)
	}
	defer func() { _ = stream.Close() }()
	if !stream.Receive() {
		if err := stream.Err(); err != nil {
			t.Fatalf("create session stream closed without updates: %v", err)
		}
		t.Fatalf("create session stream closed without updates")
	}
	msg := stream.Msg()
	if msg.GetSessionId() == "" {
		t.Fatalf("create session first update session id = %q, want non-empty", msg.GetSessionId())
	}
	if got := msg.GetPhase(); got.String() != "PHASE_UNSPECIFIED" {
		t.Fatalf("create session first update phase = %v, want PHASE_UNSPECIFIED", got)
	}
	if msg.GetDeadline() == nil {
		t.Fatalf("create session first update deadline = nil, want phase deadline")
	}
}

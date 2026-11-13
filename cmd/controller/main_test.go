package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	connect "connectrpc.com/connect"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
	controller "github.com/wrossmorrow/envoy-extproc-sdk-site/internal/controller"
)

// TestHandlerServesStaticFrontend asserts that the demo web frontend assets are
// served by the controller HTTP handler, and that unknown paths are not served.
func TestHandlerServesStaticFrontend(t *testing.T) {
	mux, _ := newMux(controller.NewSessionController())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cases := []struct {
		path       string
		wantStatus int
		wantCT     string
		wantBody   string
	}{
		{path: "/", wantStatus: http.StatusOK, wantCT: "text/html", wantBody: "<!DOCTYPE html>"},
		{path: "/app.js", wantStatus: http.StatusOK, wantCT: "text/javascript", wantBody: "connect"},
		{path: "/style.css", wantStatus: http.StatusOK, wantCT: "text/css", wantBody: "background"},
		{path: "/missing-asset.txt", wantStatus: http.StatusNotFound, wantCT: "", wantBody: ""},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatalf("GET %s: %v", tc.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("GET %s status = %d, want %d", tc.path, resp.StatusCode, tc.wantStatus)
			}
			gotCT := resp.Header.Get("Content-Type")
			if !strings.HasPrefix(gotCT, tc.wantCT) {
				t.Fatalf("GET %s Content-Type = %q, want prefix %q", tc.path, gotCT, tc.wantCT)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("GET %s body read: %v", tc.path, err)
			}
			if !strings.Contains(string(body), tc.wantBody) {
				t.Fatalf("GET %s body = %q, want it to contain %q", tc.path, string(body), tc.wantBody)
			}
		})
	}
}

// TestHandlerMountsControllerConnect asserts that the human-facing Connect API is
// mounted on the same handler as the static frontend and answers a CreateSession
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
	if got := msg.GetPhase(); got.String() != "PHASE_NOT_STARTED" {
		t.Fatalf("create session first update phase = %v, want PHASE_NOT_STARTED", got)
	}
	if msg.GetDeadline() == nil {
		t.Fatalf("create session first update deadline = nil, want phase deadline")
	}
}

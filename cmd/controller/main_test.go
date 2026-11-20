package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	connect "connectrpc.com/connect"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
	controller "github.com/wrossmorrow/envoy-extproc-sdk-site/internal/controller"
)

// TestHandlerAnswersCorsPreflight asserts that the controller answers CORS
// preflights (OPTIONS) for the browser-facing controller and envoy-bound demo
// endpoints, and that unknown paths are not served now that the static frontend
// lives in the standalone web asset server.
func TestHandlerAnswersCorsPreflight(t *testing.T) {
	mux, controllerPath := newMux(controller.NewSessionController())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	preflights := []string{
		controllerPath,
		"/envoy/",
	}
	for _, target := range preflights {
		t.Run("OPTIONS "+target, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodOptions, srv.URL+target, nil)
			if err != nil {
				t.Fatalf("new OPTIONS request %s: %v", target, err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("OPTIONS %s: %v", target, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("OPTIONS %s status = %d, want %d", target, resp.StatusCode, http.StatusNoContent)
			}
			if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
				t.Fatalf("OPTIONS %s Access-Control-Allow-Origin = %q, want %q", target, got, "*")
			}
		})
	}

	resp, err := http.Get(srv.URL + "/missing-asset.txt")
	if err != nil {
		t.Fatalf("GET /missing-asset.txt: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /missing-asset.txt status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

// TestCorsHandlerAnswersPreflightAndActualResponse asserts that the controller's
// CORS handler answers preflights and attaches the CORS headers to actual
// responses, so the standalone web origin can drive the controller cross-origin.
func TestCorsHandlerAnswersPreflightAndActualResponse(t *testing.T) {
	handler := corsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))

	preflight := httptest.NewRequest(http.MethodOptions, "/controller.v1.DemoControllerService/SubmitDecision", nil)
	preflight.Header.Set("Access-Control-Request-Method", http.MethodPost)
	preflight.Header.Set("Access-Control-Request-Headers", "content-type")
	preflightRec := httptest.NewRecorder()
	handler.ServeHTTP(preflightRec, preflight)
	if preflightRec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want %d", preflightRec.Code, http.StatusNoContent)
	}
	if got := preflightRec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("preflight Access-Control-Allow-Origin = %q, want %q", got, "*")
	}

	actual := httptest.NewRequest(http.MethodPost, "/controller.v1.DemoControllerService/SubmitDecision", strings.NewReader(`{}`))
	actual.Header.Set("Content-Type", "application/connect+json")
	actual.Header.Set("Connect-Protocol-Version", "1")
	actualRec := httptest.NewRecorder()
	handler.ServeHTTP(actualRec, actual)
	if actualRec.Code != http.StatusOK {
		t.Fatalf("actual status = %d, want %d", actualRec.Code, http.StatusOK)
	}
	if got := actualRec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("actual Access-Control-Allow-Origin = %q, want %q", got, "*")
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

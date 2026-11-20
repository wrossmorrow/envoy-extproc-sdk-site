package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMuxServesStaticFrontend asserts that the standalone web asset server
// serves the demo web frontend assets and that unknown paths are not served.
func TestMuxServesStaticFrontend(t *testing.T) {
	srv := httptest.NewServer(newMux())
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

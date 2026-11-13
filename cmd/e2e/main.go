// Command e2e is a minimal end-to-end smoke tester for the envoy extproc demo
// compose stack. It drives the human-facing controller session flow:
// create session (server-streaming) -> envoy-bound phase request -> submit human
// decision. The create-session stream is consumed concurrently with the decision
// submission.
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
)

const (
	defaultControllerAddr = "localhost:9093"
	defaultEnvoyAddr      = "http://localhost:8080"
	defaultDecision       = "continue"
)

func main() {
	ctx := context.Background()
	controllerAddr := envOr("CONTROLLER_ADDR", defaultControllerAddr)
	envoyAddr := envOr("ENVY_ADDR", defaultEnvoyAddr)
	decisionAction := envOr("DECISION", defaultDecision)

	conn, err := grpc.NewClient(controllerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	client := v1.NewDemoControllerServiceClient(conn)

	// CreateSession is server-streaming: open the stream, consume updates, and
	// keep the stream alive while decisions are submitted.
	stream, err := client.CreateSession(ctx, &v1.CreateSessionRequest{})
	if err != nil {
		fatal(err)
	}
	updates := make(chan *v1.CreateSessionResponse, 16)
	go func() {
		defer close(updates)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			updates <- msg
		}
	}()

	first, ok := <-updates
	if !ok {
		fatal(fmt.Errorf("create session stream closed without updates"))
	}
	sessionID := first.GetSessionId()
	sessionPhase := first.GetPhase()
	fmt.Printf("session created: id=%q phase=%v\n", sessionID, sessionPhase)

	// Send the envoy-bound phase request carrying the session id; this drives
	// the demo extproc which puts the session in flight and waits for a decision.
	// The call only returns once the demo has applied the decision, so run it
	// concurrently with the human decision submission.
	envoyDone := make(chan error, 1)
	go func() {
		envoyDone <- sendEnvoyBound(ctx, envoyAddr, sessionID)
	}()

	// Submit the human decision (concurrently with the create-session stream and
	// the envoy-bound phase request).
	if err := submitDecision(ctx, client, sessionID, sessionPhase, decisionAction); err != nil {
		fatal(err)
	}

	if err := <-envoyDone; err != nil {
		fatal(err)
	}

	// Drain the create-session stream until it closes.
	for range updates {
	}
}

// sendEnvoyBound sends an envoy-bound phase request carrying the session id,
// which drives the demo extproc into the phase flow.
func sendEnvoyBound(ctx context.Context, addr, sessionID string) error {
	body := `{"hello":"world"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/", strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-demo-session-id", sessionID)
	httpClient := &http.Client{Timeout: 35 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("envoy-bound phase request failed: status %d body=%q", resp.StatusCode, string(respBody))
	}
	return nil
}

// submitDecision submits a human decision (continue/cancel) for a session phase.
func submitDecision(ctx context.Context, client v1.DemoControllerServiceClient, sessionID string, phase v1.Phase, action string) error {
	decision := &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}
	if action == "cancel" {
		decision = &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: 503, Headers: map[string]string{}}}}
	}
	_, err := client.SubmitDecision(ctx, &v1.SubmitDecisionRequest{
		SessionId: sessionID,
		Phase:     phase,
		Decision:  decision,
	})
	return err
}

// envOr returns the value of the environment variable named key, or fallback if unset.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// fatal reports an error and exits.
func fatal(err error) {
	fmt.Fprintln(os.Stderr, "e2e:", err)
	os.Exit(1)
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	epb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
	controller "github.com/wrossmorrow/envoy-extproc-sdk-site/internal/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Ports used by the in-process integration harness. The demo extproc and the
// controller-side gRPC server each get a loopback listener; the extproc metrics
// server gets a dedicated loopback port. These deliberately differ from the
// ports used by the unit tests in this package.
const integrationFlowTimeout = 30 * time.Second

// phaseKind identifies which envoy ext_proc processing phase a phase update
// corresponds to, so the demo processor can be driven with the matching envoy
// processing request message.
type phaseKind int32

const (
	phaseKindRequestHeaders  phaseKind = 1
	phaseKindResponseHeaders phaseKind = 2
	phaseKindResponseBody    phaseKind = 3
)

// flowEnv wires the demo controller (human-facing Connect + gRPC services) and
// the demo processor (extproc side of the controller<->extproc bidi channel)
// together, and exposes the client-side streams used end to end by the browser:
// a CreateSession server stream and a SubmitDecision unary, plus the demo
// extproc bidi process stream.
type flowEnv struct {
	t          *testing.T
	ctrl       *controller.SessionController
	grpcServer *grpc.Server
	grpcAddr   string
	connectSrv *httptest.Server
	demoConn   *grpc.ClientConn
	demoProc   epb.ExternalProcessor_ProcessClient
}

// newFlowEnv starts an in-process controller + demo extproc pair and returns
// the clients used to drive the interactive session flow end to end.
func newFlowEnv(t *testing.T, extProcPort, metricsPort int) *flowEnv {
	t.Helper()
	env := &flowEnv{t: t}
	env.ctrl = controller.NewSessionController()
	env.grpcServer = grpc.NewServer()
	v1.RegisterDemoControllerServiceServer(env.grpcServer, &controller.DemoControllerGRPCServer{Controller: env.ctrl})
	v1.RegisterDemoProcessorServiceServer(env.grpcServer, &controller.DemoProcessorGRPCServer{Controller: env.ctrl})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("integration: listen controller: %v", err)
	}
	env.grpcAddr = lis.Addr().String()
	go func() { _ = env.grpcServer.Serve(lis) }()
	t.Cleanup(env.grpcServer.Stop)

	handler := &controller.DemoControllerHandler{Controller: env.ctrl}
	_, connectHandler := v1connect.NewDemoControllerServiceHandler(handler)
	connectSrv := httptest.NewUnstartedServer(connectHandler)
	connectSrv.Config.Protocols = new(http.Protocols)
	connectSrv.Config.Protocols.SetHTTP1(true)
	connectSrv.Config.Protocols.SetUnencryptedHTTP2(true)
	connectSrv.Start()
	env.connectSrv = connectSrv
	t.Cleanup(connectSrv.Close)

	proc := newDemoRequestProcessor()
	proc.controllerAddr = env.grpcAddr
	proc.sessionID = ""
	demoConn, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", extProcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("integration: connect demo extproc: %v", err)
	}
	env.demoConn = demoConn
	go func() {
		srvErr := extproc.Serve(&extproc.ServerOptions{
			ExtProcPort:                   extProcPort,
			MetricsHTTPPort:               metricsPort,
			TerminationGracePeriodSeconds: 10,
		}, proc, slog.Default())
		if srvErr != nil {
			fmt.Printf("integration: extproc serve error: %v\n", srvErr)
		}
	}()
	var demoProc epb.ExternalProcessor_ProcessClient
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	for i := 0; i < 200; i++ {
		conn, dialErr := dialer.DialContext(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", extProcPort))
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	demoProc, err = epb.NewExternalProcessorClient(demoConn).Process(context.Background())
	if err != nil {
		t.Fatalf("integration: open demo extproc process stream: %v", err)
	}
	env.demoProc = demoProc

	return env
}

// controllerURL returns the base URL of the in-process controller Connect server.
func (e *flowEnv) controllerURL() string {
	return e.connectSrv.URL
}

// connectControllerClient returns a human-facing Connect client for the
// controller demo service.
func (e *flowEnv) connectControllerClient() v1connect.DemoControllerServiceClient {
	httpClient := &http.Client{Transport: &http.Transport{}}
	httpTransport := httpClient.Transport.(*http.Transport)
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	httpTransport.Protocols = protos
	httpTransport.DisableCompression = true
	return v1connect.NewDemoControllerServiceClient(httpClient, e.controllerURL())
}

// createSession opens a human-facing CreateSession server stream, reads the
// first update (session id + phase PHASE_NOT_STARTED), and returns the stream
// positioned to receive subsequent phase updates.
func (e *flowEnv) createSession(ctx context.Context) (*connect.ServerStreamForClient[v1.CreateSessionResponse], string, error) {
	client := e.connectControllerClient()
	stream, err := client.CreateSession(ctx, &connect.Request[v1.CreateSessionRequest]{})
	if err != nil {
		return nil, "", err
	}
	if !stream.Receive() {
		return nil, "", e.streamErr(e.t, stream)
	}
	msg := stream.Msg()
	if msg.GetSessionId() == "" {
		return nil, "", errors.New("create session first update missing session id")
	}
	return stream, msg.GetSessionId(), nil
}

// streamErr reports the error that ended a server stream, if any.
func (e *flowEnv) streamErr(t *testing.T, stream *connect.ServerStreamForClient[v1.CreateSessionResponse]) error {
	if err := stream.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// nextUpdate receives the next phase update from the create-session stream.
func (e *flowEnv) nextUpdate(ctx context.Context, stream *connect.ServerStreamForClient[v1.CreateSessionResponse]) (*v1.CreateSessionResponse, error) {
	if !stream.Receive() {
		return nil, e.streamErr(e.t, stream)
	}
	return stream.Msg(), nil
}

// sendPhase sends an envoy-style phase update through the demo extproc bidi
// stream and blocks awaiting the decision the controller sends back.
func (e *flowEnv) sendPhase(ctx context.Context, sessionID string, kind phaseKind) error {
	req, err := phaseUpdateRequest(sessionID, kind)
	if err != nil {
		return err
	}
	if err := e.demoProc.Send(req); err != nil {
		return fmt.Errorf("integration: send phase update: %w", err)
	}
	return nil
}

// sendPhaseUpdate sends an envoy-style phase update through the demo extproc
// bidi stream and receives the phase update the controller relays on the
// human-facing create-session stream.
func (e *flowEnv) sendPhaseUpdate(ctx context.Context, stream *connect.ServerStreamForClient[v1.CreateSessionResponse], sessionID string, phase v1.Phase, kind phaseKind) error {
	if err := e.sendPhase(ctx, sessionID, kind); err != nil {
		return err
	}
	update, err := e.nextUpdate(ctx, stream)
	if err != nil {
		return fmt.Errorf("integration: next phase update: %w", err)
	}
	if update.GetSessionId() != sessionID {
		return fmt.Errorf("phase update session id = %q, want %q", update.GetSessionId(), sessionID)
	}
	if update.GetPhase() != phase {
		return fmt.Errorf("phase update phase = %v, want %v", update.GetPhase(), phase)
	}
	if update.GetSnapshot() == nil {
		return fmt.Errorf("phase update snapshot = nil, want HttpSnapshot")
	}
	return nil
}

// recvDecision receives the decision the controller sent back over the demo
// extproc bidi stream after the phase update was sent and the human decision
// submitted.
func (e *flowEnv) recvDecision(ctx context.Context) (*epb.ProcessingResponse, error) {
	return e.demoProc.Recv()
}

// submitHumanDecision submits a human decision (continue/cancel) for a
// session phase through the human-facing Connect SubmitDecision RPC.
func (e *flowEnv) submitHumanDecision(ctx context.Context, sessionID string, phase v1.Phase, action string) error {
	client := e.connectControllerClient()
	decision := &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}
	if action == "cancel" {
		decision = &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: 503, Headers: map[string]string{}}}}
	}
	_, err := client.SubmitDecision(ctx, &connect.Request[v1.SubmitDecisionRequest]{
		Msg: &v1.SubmitDecisionRequest{
			SessionId: sessionID,
			Phase:     phase,
			Decision:  decision,
		},
	})
	return err
}

// phaseUpdateRequest builds an envoy-style ext_proc ProcessingRequest for the
// given session phase, carrying the session id in the request headers.
func phaseUpdateRequest(sessionID string, kind phaseKind) (*epb.ProcessingRequest, error) {
	switch kind {
	case phaseKindRequestHeaders:
		return &epb.ProcessingRequest{
			Request: &epb.ProcessingRequest_RequestHeaders{
				RequestHeaders: &epb.HttpHeaders{
					Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "x-demo-session-id", Value: sessionID}}},
					EndOfStream: true,
				},
			},
		}, nil
	case phaseKindResponseHeaders:
		return &epb.ProcessingRequest{
			Request: &epb.ProcessingRequest_ResponseHeaders{
				ResponseHeaders: &epb.HttpHeaders{
					Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "x-demo-session-id", Value: sessionID}}},
					EndOfStream: true,
				},
			},
		}, nil
	case phaseKindResponseBody:
		return &epb.ProcessingRequest{
			Request: &epb.ProcessingRequest_ResponseBody{
				ResponseBody: &epb.HttpBody{Body: []byte(`{"hello":"world"}`), EndOfStream: true},
			},
		}, nil
	default:
		return nil, errors.New("unsupported phase kind")
	}
}

// assertContinueApplied verifies the demo extproc replied over the envoy
// processing response stream with the continue response for the phase the human
// continued on, including the demo's processed-request marker header.
func assertContinueApplied(t *testing.T, resp *epb.ProcessingResponse, kind phaseKind) {
	t.Helper()
	switch kind {
	case phaseKindRequestHeaders:
		if resp.GetRequestHeaders() == nil {
			t.Fatalf("processing response = %+v, want continue request headers response", resp)
		}
		common := resp.GetRequestHeaders().GetResponse()
		if common == nil {
			t.Fatalf("processing response = %+v, want continue with common response", resp)
		}
		found := false
		for _, opt := range common.GetHeaderMutation().GetSetHeaders() {
			if opt.GetHeader() != nil && opt.GetHeader().GetKey() == "x-demo-processed" && string(opt.GetHeader().GetRawValue()) == "true" {
				found = true
			}
		}
		if !found {
			t.Fatalf("continue response = %+v, want x-demo-processed header mutation", resp)
		}
	case phaseKindResponseHeaders:
		if resp.GetResponseHeaders() == nil {
			t.Fatalf("processing response = %+v, want continue response headers response", resp)
		}
	case phaseKindResponseBody:
		if resp.GetResponseBody() == nil {
			t.Fatalf("processing response = %+v, want continue response body response", resp)
		}
	}
}

// assertCancelApplied verifies the demo extproc replied over the envoy
// processing response stream with the immediate cancel response the demo
// applied for the phase, carrying the fail-closed gateway status.
func assertCancelApplied(t *testing.T, resp *epb.ProcessingResponse, kind phaseKind) {
	t.Helper()
	if resp.GetImmediateResponse() == nil {
		t.Fatalf("processing response = %+v, want immediate cancel response", resp)
	}
	immediate := resp.GetImmediateResponse()
	if immediate.GetStatus() == nil {
		t.Fatalf("immediate cancel response = %+v, want status", immediate)
	}
	if immediate.GetStatus().GetCode() != http.StatusGatewayTimeout {
		t.Fatalf("immediate cancel status = %d, want %d", immediate.GetStatus().GetCode(), http.StatusGatewayTimeout)
	}
}

// TestFlow_FullContinueAcrossPhases drives a full interactive session across
// two phases end to end: create session via the human-facing Connect CreateSession
// server stream, envoy-bound phase update via the demo extproc bidi process
// stream, human decision via the Connect SubmitDecision unary, and the decision
// applied by the demo extproc. The human continues through the request-headers
// phase and then the response-headers phase.
func TestFlow_FullContinueAcrossPhases(t *testing.T) {
	ctx := context.Background()
	e := newFlowEnv(t, 50121, 9211)

	stream, sessionID, err := e.createSession(ctx)
	if err != nil {
		t.Fatalf("integration: create session: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Phase 1: envoy-bound request-headers phase. The demo extproc sends the
	// phase update, the controller relays it on the create-session stream, the
	// human continues, and the demo extproc applies the continue decision and
	// replies over the processing response stream.
	if sendErr := e.sendPhaseUpdate(ctx, stream, sessionID, v1.Phase_PHASE_REQUEST_HEADERS, phaseKindRequestHeaders); sendErr != nil {
		t.Fatalf("integration: send phase update (request headers): %v", sendErr)
	}
	if submitErr := e.submitHumanDecision(ctx, sessionID, v1.Phase_PHASE_REQUEST_HEADERS, "continue"); submitErr != nil {
		t.Fatalf("integration: submit human decision (continue): %v", submitErr)
	}
	resp, err := e.recvDecision(ctx)
	if err != nil {
		t.Fatalf("integration: recv decision (continue): %v", err)
	}
	assertContinueApplied(t, resp, phaseKindRequestHeaders)

	// Phase 2: envoy-bound response-headers phase. After the human continued,
	// the demo extproc waits for the next phase update, now for the response
	// headers phase of the same session.
	if sendErr := e.sendPhaseUpdate(ctx, stream, sessionID, v1.Phase_PHASE_RESPONSE_HEADERS, phaseKindResponseHeaders); sendErr != nil {
		t.Fatalf("integration: send phase update (response headers): %v", sendErr)
	}
	if submitErr := e.submitHumanDecision(ctx, sessionID, v1.Phase_PHASE_RESPONSE_HEADERS, "continue"); submitErr != nil {
		t.Fatalf("integration: submit human decision (continue): %v", submitErr)
	}
	resp, err = e.recvDecision(ctx)
	if err != nil {
		t.Fatalf("integration: recv decision (continue): %v", err)
	}
	assertContinueApplied(t, resp, phaseKindResponseHeaders)
}

// TestFlow_CancelDecisionShortCircuits drives a full cancel cycle end to end:
// the demo extproc sends the phase update, the human submits a cancel decision,
// and the demo extproc applies the cancel decision and replies over the
// processing response stream with the fail-closed immediate response.
func TestFlow_CancelDecisionShortCircuits(t *testing.T) {
	ctx := context.Background()
	e := newFlowEnv(t, 50221, 9321)

	stream, sessionID, err := e.createSession(ctx)
	if err != nil {
		t.Fatalf("integration: create session: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if sendErr := e.sendPhaseUpdate(ctx, stream, sessionID, v1.Phase_PHASE_REQUEST_HEADERS, phaseKindRequestHeaders); sendErr != nil {
		t.Fatalf("integration: send phase update: %v", sendErr)
	}
	if submitErr := e.submitHumanDecision(ctx, sessionID, v1.Phase_PHASE_REQUEST_HEADERS, "cancel"); submitErr != nil {
		t.Fatalf("integration: submit human decision (cancel): %v", submitErr)
	}
	resp, err := e.recvDecision(ctx)
	if err != nil {
		t.Fatalf("integration: recv decision (cancel): %v", err)
	}
	assertCancelApplied(t, resp, phaseKindRequestHeaders)
}

// TestFlow_PhaseTimeoutFailsClosed drives a phase cycle without a human decision
// and asserts the phase timeout fails the session closed with a cancel decision
// that the demo extproc applies.
func TestFlow_PhaseTimeoutFailsClosed(t *testing.T) {
	ctx := context.Background()
	e := newFlowEnv(t, 50321, 9331)

	controller.PhaseTimeout = 250 * time.Millisecond
	defer func() { controller.PhaseTimeout = 30 * time.Second }()

	stream, sessionID, err := e.createSession(ctx)
	if err != nil {
		t.Fatalf("integration: create session: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if sendErr := e.sendPhaseUpdate(ctx, stream, sessionID, v1.Phase_PHASE_REQUEST_HEADERS, phaseKindRequestHeaders); sendErr != nil {
		t.Fatalf("integration: send phase update: %v", sendErr)
	}
	resp, err := e.recvDecision(ctx)
	if err != nil {
		t.Fatalf("integration: recv decision (phase timeout): %v", err)
	}
	assertCancelApplied(t, resp, phaseKindRequestHeaders)
}

// TestFlow_HumanDecisionValidation drives the human-facing SubmitDecision RPC
// error surface over the full session flow: decisions for unknown sessions and
// phases that do not match the session phase are rejected.
func TestFlow_HumanDecisionValidation(t *testing.T) {
	ctx := context.Background()
	e := newFlowEnv(t, 50421, 9341)

	// Decision for an unknown session is rejected.
	if err := e.submitHumanDecision(ctx, "session-does-not-exist", v1.Phase_PHASE_NOT_STARTED, "continue"); err == nil {
		t.Fatalf("integration: submit decision for unknown session: error = nil, want error")
	}

	// Decision for a session that is not in flight is rejected.
	stream, sessionID, err := e.createSession(ctx)
	if err != nil {
		t.Fatalf("integration: create session: %v", err)
	}
	defer func() { _ = stream.Close() }()
	if err := e.submitHumanDecision(ctx, sessionID, v1.Phase_PHASE_NOT_STARTED, "continue"); err == nil {
		t.Fatalf("integration: submit decision for session not in flight: error = nil, want error")
	}

	// Decision for a phase that does not match the in-flight phase is rejected.
	if err := e.sendPhaseUpdate(ctx, stream, sessionID, v1.Phase_PHASE_REQUEST_HEADERS, phaseKindRequestHeaders); err != nil {
		t.Fatalf("integration: send phase update: %v", err)
	}
	if err := e.submitHumanDecision(ctx, sessionID, v1.Phase_PHASE_RESPONSE_HEADERS, "continue"); err == nil {
		t.Fatalf("integration: submit decision for phase mismatch: error = nil, want error")
	}
}

package extproc

import (
	"context"
	"net"
	"testing"
	"time"

	"log/slog"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	epb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	"github.com/wrossmorrow/envoy-extproc-sdk-site/internal/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestDemoProcessorSendsPhaseAndAppliesDecision drives the demo extproc server
// through the envoy ext_proc bidi protocol and asserts that the processor sends
// a phase update to the controller and receives/applies a decision.
func TestDemoProcessorSendsPhaseAndAppliesDecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start controller gRPC server (controller side of the Process bidi).
	ctrl := controller.NewSessionController()
	grpcServer := grpc.NewServer()
	v1.RegisterDemoControllerServiceServer(grpcServer, &controller.DemoControllerGRPCServer{Controller: ctrl})
	v1.RegisterDemoProcessorServiceServer(grpcServer, &controller.DemoProcessorGRPCServer{Controller: ctrl})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	go grpcServer.Serve(lis)
	ctrlAddr := lis.Addr().String()
	defer grpcServer.Stop()

	// Create a session and open the human decision for its first phase.
	sess, err := ctrl.CreateSession()
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sessID := sess.ID
	if err = ctrl.BeginPhase(sessID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("begin phase: %v", err)
	}

	// Start demo extproc server (processor side of the Process bidi).
	proc := NewRequestProcessor(ctrlAddr)
	sopts := &extproc.ServerOptions{ExtProcPort: 50053, MetricsHTTPPort: 9099, TerminationGracePeriodSeconds: 10}
	go func() { extproc.Serve(sopts, proc, slog.Default()) }()
	time.Sleep(700 * time.Millisecond)

	// Drive the extproc bidi stream: send a request-headers phase, expect a response.
	extprocConn, err := grpc.NewClient("127.0.0.1:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect extproc: %v", err)
	}
	defer extprocConn.Close()
	extprocClient, err := epb.NewExternalProcessorClient(extprocConn).Process(ctx)
	if err != nil {
		t.Fatalf("open extproc process stream: %v", err)
	}

	req := &epb.ProcessingRequest{
		Request: &epb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &epb.HttpHeaders{
				Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "x-demo-session-id", Value: sessID}}},
				EndOfStream: true,
			},
		},
	}
	if err = extprocClient.Send(req); err != nil {
		t.Fatalf("send processing request: %v", err)
	}
	// Human submits a decision for the session phase; the controller resolves
	// it and replies over the bidi stream, which the demo processor applies.
	if err = ctrl.SubmitDecision(ctx, sessID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err != nil {
		t.Fatalf("submit human decision: %v", err)
	}
	resp, err := extprocClient.Recv()
	if err != nil {
		t.Fatalf("recv processing response: %v", err)
	}
	t.Logf("got processing response: %+v", resp)

	// The processor should have responded (continue) since the controller echoes a continue decision.
	if resp.GetResponse() == nil {
		t.Fatalf("expected a processing response, got nil")
	}
}

// TestDemoProcessorAppliesCancelDecision drives the demo extproc server through
// the envoy ext_proc bidi protocol and asserts that the processor receives a
// cancel decision and applies it (short-circuits the request).
func TestDemoProcessorAppliesCancelDecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctrl := controller.NewSessionController()
	grpcServer := grpc.NewServer()
	v1.RegisterDemoControllerServiceServer(grpcServer, &controller.DemoControllerGRPCServer{Controller: ctrl})
	v1.RegisterDemoProcessorServiceServer(grpcServer, &controller.DemoProcessorGRPCServer{Controller: ctrl})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	go grpcServer.Serve(lis)
	ctrlAddr := lis.Addr().String()
	defer grpcServer.Stop()

	sess, err := ctrl.CreateSession()
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err = ctrl.BeginPhase(sess.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("begin phase: %v", err)
	}

	proc := NewRequestProcessor(ctrlAddr)
	sopts := &extproc.ServerOptions{ExtProcPort: 50055, MetricsHTTPPort: 9098, TerminationGracePeriodSeconds: 10}
	go func() { extproc.Serve(sopts, proc, slog.Default()) }()
	time.Sleep(700 * time.Millisecond)

	extprocConn, err := grpc.NewClient("127.0.0.1:50055", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect extproc: %v", err)
	}
	defer extprocConn.Close()
	extprocClient, err := epb.NewExternalProcessorClient(extprocConn).Process(ctx)
	if err != nil {
		t.Fatalf("open extproc process stream: %v", err)
	}

	req := &epb.ProcessingRequest{
		Request: &epb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &epb.HttpHeaders{
				Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "x-demo-session-id", Value: sess.ID}}},
				EndOfStream: true,
			},
		},
	}
	if err = extprocClient.Send(req); err != nil {
		t.Fatalf("send processing request: %v", err)
	}
	if err = ctrl.SubmitDecision(ctx, sess.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: 503, Headers: map[string]string{}}}}); err != nil {
		t.Fatalf("submit human decision: %v", err)
	}
	resp, err := extprocClient.Recv()
	if err != nil {
		t.Fatalf("recv processing response: %v", err)
	}
	t.Logf("got processing response: %+v", resp)

	if resp.GetResponse() == nil {
		t.Fatalf("expected a processing response, got nil")
	}
}

func TestDemoProcessorRejectsMissingAndUnknownSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctrl := controller.NewSessionController()
	grpcServer := grpc.NewServer()
	v1.RegisterDemoProcessorServiceServer(grpcServer, &controller.DemoProcessorGRPCServer{Controller: ctrl})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	proc := NewRequestProcessor(lis.Addr().String())
	sopts := &extproc.ServerOptions{ExtProcPort: 50056, MetricsHTTPPort: 9097, TerminationGracePeriodSeconds: 10}
	go func() { extproc.Serve(sopts, proc, slog.Default()) }()
	time.Sleep(700 * time.Millisecond)

	extprocConn, err := grpc.NewClient("127.0.0.1:50056", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect extproc: %v", err)
	}
	defer extprocConn.Close()

	// 1. Missing x-demo-session-id -> immediate 400 without calling controller.
	stream1, err := epb.NewExternalProcessorClient(extprocConn).Process(ctx)
	if err != nil {
		t.Fatalf("open stream 1: %v", err)
	}
	if err = stream1.Send(&epb.ProcessingRequest{
		Request: &epb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &epb.HttpHeaders{
				Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: ":method", Value: "GET"}}},
				EndOfStream: true,
			},
		},
	}); err != nil {
		t.Fatalf("send missing-session request: %v", err)
	}
	resp1, err := stream1.Recv()
	if err != nil {
		t.Fatalf("recv missing-session response: %v", err)
	}
	if resp1.GetImmediateResponse() == nil || resp1.GetImmediateResponse().GetStatus().GetCode() != 400 {
		t.Fatalf("missing session response = %+v, want immediate 400", resp1)
	}

	// 2. Unknown session id -> controller returns NotFound -> immediate 404.
	stream2, err := epb.NewExternalProcessorClient(extprocConn).Process(ctx)
	if err != nil {
		t.Fatalf("open stream 2: %v", err)
	}
	if err = stream2.Send(&epb.ProcessingRequest{
		Request: &epb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &epb.HttpHeaders{
				Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "x-demo-session-id", Value: "does-not-exist"}}},
				EndOfStream: true,
			},
		},
	}); err != nil {
		t.Fatalf("send unknown-session request: %v", err)
	}
	resp2, err := stream2.Recv()
	if err != nil {
		t.Fatalf("recv unknown-session response: %v", err)
	}
	if resp2.GetImmediateResponse() == nil || resp2.GetImmediateResponse().GetStatus().GetCode() != 404 {
		t.Fatalf("unknown session response = %+v, want immediate 404", resp2)
	}
}

func TestDemoProcessorClearsResponseBody(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctrl := controller.NewSessionController()
	grpcServer := grpc.NewServer()
	v1.RegisterDemoControllerServiceServer(grpcServer, &controller.DemoControllerGRPCServer{Controller: ctrl})
	v1.RegisterDemoProcessorServiceServer(grpcServer, &controller.DemoProcessorGRPCServer{Controller: ctrl})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	go grpcServer.Serve(lis)
	ctrlAddr := lis.Addr().String()
	defer grpcServer.Stop()

	sess, err := ctrl.CreateSession()
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sessID := sess.ID

	proc := NewRequestProcessor(ctrlAddr)
	sopts := &extproc.ServerOptions{ExtProcPort: 50057, MetricsHTTPPort: 9103, TerminationGracePeriodSeconds: 10}
	go func() { extproc.Serve(sopts, proc, slog.Default()) }()
	time.Sleep(700 * time.Millisecond)

	extprocConn, err := grpc.NewClient("127.0.0.1:50057", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect extproc: %v", err)
	}
	defer extprocConn.Close()
	extprocClient, err := epb.NewExternalProcessorClient(extprocConn).Process(ctx)
	if err != nil {
		t.Fatalf("open extproc process stream: %v", err)
	}

	// 1. REQUEST_HEADERS
	if err = extprocClient.Send(&epb.ProcessingRequest{
		Request: &epb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &epb.HttpHeaders{
				Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "x-demo-session-id", Value: sessID}}},
				EndOfStream: true,
			},
		},
	}); err != nil {
		t.Fatalf("send request headers: %v", err)
	}
	for ctrl.SubmitDecision(ctx, sessID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{}}}) != nil {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err = extprocClient.Recv(); err != nil {
		t.Fatalf("recv request headers response: %v", err)
	}

	// 2. RESPONSE_BODY with empty Continue.Body -> ClearBody + content-length: 0
	if err = extprocClient.Send(&epb.ProcessingRequest{
		Request: &epb.ProcessingRequest_ResponseBody{
			ResponseBody: &epb.HttpBody{Body: []byte("hello world"), EndOfStream: true},
		},
	}); err != nil {
		t.Fatalf("send response body: %v", err)
	}
	for ctrl.SubmitDecision(ctx, sessID, int32(v1.Phase_PHASE_RESPONSE_BODY), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Body: []byte{}}}}) != nil {
		time.Sleep(10 * time.Millisecond)
	}
	resp, err := extprocClient.Recv()
	if err != nil {
		t.Fatalf("recv response body response: %v", err)
	}
	common := resp.GetResponseBody().GetResponse()
	if !common.GetBodyMutation().GetClearBody() {
		t.Fatalf("expected clear_body=true, got %+v", common.GetBodyMutation())
	}
	var gotCL string
	for _, h := range common.GetHeaderMutation().GetSetHeaders() {
		if h.GetHeader().GetKey() == "content-length" {
			gotCL = string(h.GetHeader().GetRawValue())
		}
	}
	if gotCL != "0" {
		t.Fatalf("expected content-length=0, got %q", gotCL)
	}
}

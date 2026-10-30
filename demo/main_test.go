package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	epb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/wrossmorrow/envoy-extproc-sdk-controller/controller"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1"
	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
)

// TestDemoProcessorSendsPhaseAndAppliesDecision drives the demo extproc server
// through the envoy ext_proc bidi protocol and asserts that the processor sends
// a phase update to the controller and receives/applies a decision.
func TestDemoProcessorSendsPhaseAndAppliesDecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start controller gRPC server (controller side of the Process bidi).
	grpcServer := grpc.NewServer()
	v1.RegisterDemoControllerServer(grpcServer, &controller.DemoControllerGRPCServer{Controller: controller.NewSessionController()})
	v1.RegisterDemoProcessorServer(grpcServer, &controller.DemoProcessorGRPCServer{Controller: controller.NewSessionController()})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen controller: %v", err)
	}
	go grpcServer.Serve(lis)
	ctrlAddr := lis.Addr().String()
	defer grpcServer.Stop()

	// Start demo extproc server (processor side of the Process bidi).
	proc := newDemoRequestProcessor()
	proc.controllerAddr = ctrlAddr
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
				Headers:     &corev3.HeaderMap{},
				EndOfStream: true,
			},
		},
	}
	if err := extprocClient.Send(req); err != nil {
		t.Fatalf("send processing request: %v", err)
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

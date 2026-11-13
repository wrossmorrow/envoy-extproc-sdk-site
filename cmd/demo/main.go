package main

import (
	"context"
	"log/slog"
	"os"

	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultControllerAddr = "controller:9093"
	sessionIDHeader       = "x-demo-session-id"
)

// demoRequestProcessor is the demo extproc processor. It drives the controller's
// internal DemoProcessorService channel over a bidi gRPC stream, sending phase updates
// and applying the decisions returned by the controller (continue/cancel).
type demoRequestProcessor struct {
	opts   *extproc.ProcessingOptions
	logger *slog.Logger

	conn      *grpc.ClientConn
	sessionID string
	stream    grpc.BidiStreamingClient[v1.ProcessRequest, v1.ProcessResponse]

	controllerAddr string
}

func newDemoRequestProcessor() *demoRequestProcessor {
	addr := os.Getenv("DEMO_CONTROLLER_ADDR")
	if addr == "" {
		addr = defaultControllerAddr
	}
	return &demoRequestProcessor{
		opts:           &extproc.ProcessingOptions{AbortOnProcessorFailure: true},
		logger:         slog.Default(),
		sessionID:      "",
		controllerAddr: addr,
	}
}

func (s *demoRequestProcessor) GetName() string {
	return "demo"
}

func (s *demoRequestProcessor) GetOptions() *extproc.ProcessingOptions {
	return s.opts
}

// controllerStream returns the controller bidi stream, opening one lazily.
func (s *demoRequestProcessor) controllerStream(ctx context.Context) (grpc.BidiStreamingClient[v1.ProcessRequest, v1.ProcessResponse], error) {
	if s.stream != nil {
		return s.stream, nil
	}
	conn, err := grpc.NewClient(s.controllerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.conn = conn
	client := v1.NewDemoProcessorServiceClient(conn)
	st, err := client.Process(ctx)
	if err != nil {
		return nil, err
	}
	s.stream = st
	return st, nil
}

// sendPhase sends a phase update carrying the phase snapshot to the controller
// for the session and waits for the controller's decision. Returns the decision
// (continue/cancel).
func (s *demoRequestProcessor) sendPhase(ctx context.Context, phase int32, sessionID string, snapshot *v1.HttpSnapshot) (*v1.ProcessResponse, error) {
	stream, err := s.controllerStream(ctx)
	if err != nil {
		return nil, err
	}
	if err = stream.Send(&v1.ProcessRequest{SessionId: sessionID, Phase: v1.Phase(phase), Snapshot: snapshot}); err != nil {
		return nil, err
	}
	dec, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	return dec, nil
}

// sessionIDFor resolves the controller session id for an incoming phase from
// the request headers, falling back to the stored session id when absent.
func (s *demoRequestProcessor) sessionIDFor(headers extproc.AllHeaders) string {
	if id, ok := headers.Get(sessionIDHeader); ok && id != "" {
		s.sessionID = id
		return id
	}
	return s.sessionID
}

const snapshotBodyLimit = 16 * 1024 // cap on bodies reported in phase snapshots

// snapshotHasHeader reports whether the snapshot already carries a header with
// the given key.
func snapshotHasHeader(headers []*v1.Header, key string) bool {
	for _, h := range headers {
		if h.GetKey() == key {
			return true
		}
	}
	return false
}

// snapshot builds the HttpSnapshot the demo processor reports to the controller
// for the phase being processed, from the observed headers and body. Body phases
// report at most snapshotBodyLimit bytes; longer bodies are truncated and flagged
// via BodyTruncated.
func (s *demoRequestProcessor) snapshot(rc *extproc.RequestContext, body []byte) *v1.HttpSnapshot {
	snap := &v1.HttpSnapshot{}
	for name, values := range rc.AllHeaders.Headers {
		for _, v := range values {
			snap.Headers = append(snap.Headers, &v1.Header{Key: name, Value: v})
		}
	}
	if rc.Method != "" && !snapshotHasHeader(snap.Headers, ":method") {
		snap.Headers = append(snap.Headers, &v1.Header{Key: ":method", Value: rc.Method})
	}
	if rc.Path != "" && !snapshotHasHeader(snap.Headers, ":path") {
		snap.Headers = append(snap.Headers, &v1.Header{Key: ":path", Value: rc.Path})
	}
	if len(body) > snapshotBodyLimit {
		snap.Body = body[:snapshotBodyLimit]
		snap.BodyTruncated = true
	} else {
		snap.Body = body
	}
	return snap
}

// sendPhaseAndDecide sends a phase update carrying the phase snapshot to the
// controller for the session and waits for the controller's decision.
func (s *demoRequestProcessor) sendPhaseAndDecide(ctx context.Context, phase int32, rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) (*v1.ProcessResponse, error) {
	sessionID := s.sessionIDFor(headers)
	snap := s.snapshot(rc, body)
	return s.sendPhase(ctx, phase, sessionID, snap)
}

func (s *demoRequestProcessor) ProcessRequestHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	dec, err := s.sendPhaseAndDecide(context.Background(), extproc.REQUEST_PHASE_REQUEST_HEADERS, ctx, headers, nil)
	if err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_HEADERS)), slog.String("error", err.Error()))
		return err
	}

	decision := dec.GetDecision()
	if decision != nil && decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := ctx.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := ctx.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := ctx.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return ctx.ContinueRequest()
	}
	if decision != nil && decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return ctx.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ProcessResponseHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	dec, err := s.sendPhaseAndDecide(context.Background(), extproc.REQUEST_PHASE_RESPONSE_HEADERS, ctx, headers, nil)
	if err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_HEADERS)), slog.String("error", err.Error()))
		return err
	}

	decision := dec.GetDecision()
	if decision != nil && decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := ctx.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := ctx.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := ctx.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return ctx.ContinueRequest()
	}
	if decision != nil && decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return ctx.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ProcessResponseBody(ctx *extproc.RequestContext, body []byte) error {
	dec, err := s.sendPhaseAndDecide(context.Background(), extproc.REQUEST_PHASE_RESPONSE_BODY, ctx, ctx.AllHeaders, body)
	if err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_BODY)), slog.String("error", err.Error()))
		return err
	}

	decision := dec.GetDecision()
	if decision != nil && decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := ctx.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := ctx.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := ctx.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return ctx.ContinueRequest()
	}
	if decision != nil && decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return ctx.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ProcessResponseTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	dec, err := s.sendPhaseAndDecide(context.Background(), extproc.REQUEST_PHASE_RESPONSE_TRAILERS, ctx, trailers, nil)
	if err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_TRAILERS)), slog.String("error", err.Error()))
		return err
	}

	decision := dec.GetDecision()
	if decision != nil && decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := ctx.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := ctx.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := ctx.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return ctx.ContinueRequest()
	}
	if decision != nil && decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return ctx.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ProcessRequestBody(ctx *extproc.RequestContext, body []byte) error {
	dec, err := s.sendPhaseAndDecide(context.Background(), extproc.REQUEST_PHASE_REQUEST_BODY, ctx, ctx.AllHeaders, body)
	if err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_BODY)), slog.String("error", err.Error()))
		return err
	}

	decision := dec.GetDecision()
	if decision != nil && decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := ctx.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := ctx.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := ctx.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return ctx.ContinueRequest()
	}
	if decision != nil && decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return ctx.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ProcessRequestTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	dec, err := s.sendPhaseAndDecide(context.Background(), extproc.REQUEST_PHASE_REQUEST_TRAILERS, ctx, trailers, nil)
	if err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_TRAILERS)), slog.String("error", err.Error()))
		return err
	}

	decision := dec.GetDecision()
	if decision != nil && decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := ctx.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := ctx.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := ctx.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return ctx.ContinueRequest()
	}
	if decision != nil && decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return ctx.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ErrorHandler(ctx *extproc.RequestContext, phase int, err error) {
	s.logger.Error("demo processor error",
		slog.String("phase", extproc.RequestPhaseToString(phase)),
		slog.String("error", err.Error()),
	)
}

func (s *demoRequestProcessor) Close(gracePeriodSeconds int32) error {
	return nil
}

func main() {
	sopts := extproc.NewDefaultServerOptions()
	sopts.ExtProcPort = 50051
	sopts.MetricsHTTPPort = 9090
	sopts.TerminationGracePeriodSeconds = 10

	proc := newDemoRequestProcessor()

	extproc.MustServe(sopts, proc, slog.Default())
}

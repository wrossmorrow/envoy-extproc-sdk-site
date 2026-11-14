package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	defaultControllerAddr = "controller:9093"
	sessionIDHeader       = "x-demo-session-id"

	// controllerStreamKey is the RequestContext data key holding the controller
	// bidi stream handle for the current exchange phase.
	controllerStreamKey = "controllerDemoStream"

	// controllerStreamCancelKey is the RequestContext data key holding the cancel
	// function for the controller bidi stream of the current exchange phase.
	controllerStreamCancelKey = "controllerDemoStreamCancel"

	// controllerSessionIDKey is the RequestContext data key holding the controller
	// session id for the current exchange phase.
	controllerSessionIDKey = "controllerDemoSessionID"

	// defaultExchangeTimeout bounds a single controller exchange phase stream.
	defaultExchangeTimeout = 30 * time.Second
)

// demoRequestProcessor is the demo extproc processor. It drives the controller's
// internal DemoProcessorService channel over a bidi gRPC stream, sending phase updates
// and applying the decisions returned by the controller (continue/cancel).
type demoRequestProcessor struct {
	opts   *extproc.ProcessingOptions
	logger *slog.Logger

	// controllerConn is the single controller client connection shared by all
	// exchanges. Stream handles are held per exchange in the request context and
	// retired individually; the connection itself is shared, not per exchange.
	controllerMu   sync.Mutex
	controllerConn *grpc.ClientConn

	// exchangeTimeout bounds a single controller exchange phase stream.
	exchangeTimeout time.Duration

	sessionID string

	controllerAddr string
}

func newDemoRequestProcessor() *demoRequestProcessor {
	addr := os.Getenv("DEMO_CONTROLLER_ADDR")
	if addr == "" {
		addr = defaultControllerAddr
	}
	return &demoRequestProcessor{
		opts:            &extproc.ProcessingOptions{AbortOnProcessorFailure: true},
		logger:          slog.Default(),
		sessionID:       "",
		controllerAddr:  addr,
		exchangeTimeout: defaultExchangeTimeout,
	}
}

func (s *demoRequestProcessor) GetName() string {
	return "demo"
}

func (s *demoRequestProcessor) GetOptions() *extproc.ProcessingOptions {
	return s.opts
}

// controllerClient returns the shared controller client connection, opening it
// lazily on first use.
func (s *demoRequestProcessor) controllerClient() (*grpc.ClientConn, error) {
	s.controllerMu.Lock()
	defer s.controllerMu.Unlock()
	if s.controllerConn != nil {
		return s.controllerConn, nil
	}
	conn, err := grpc.NewClient(s.controllerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.controllerConn = conn
	return conn, nil
}

// controllerStream returns the controller bidi stream for the current exchange
// phase, reading it from the per-request context first and opening a fresh one
// when absent. The handle is stored on the RequestContext (which persists across
// phases of an exchange), scoping it to the exchange rather than the processor
// instance. The underlying connection is the shared controller connection.
func (s *demoRequestProcessor) controllerStream(ctx context.Context, rc *extproc.RequestContext) (grpc.BidiStreamingClient[v1.ProcessRequest, v1.ProcessResponse], error) {
	conn, err := s.controllerClient()
	if err != nil {
		return nil, err
	}
	client := v1.NewDemoProcessorServiceClient(conn)
	// Open the exchange phase stream with a deadline so the stream cannot outlive
	// its exchange budget, and keep the cancel function alongside the handle.
	streamCtx, cancel := context.WithTimeout(ctx, s.exchangeTimeout)
	st, err := client.Process(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	rc.SetValue(controllerStreamKey, st)
	rc.SetValue(controllerStreamCancelKey, cancel)
	return st, nil
}

// retireStream closes and clears the controller stream handle held in the
// request context. It must be called when a phase fails or the phase has ended
// (end-of-stream) after a decision, so a dead stream is never reused by a later
// phase of the exchange and a resolved exchange does not leave the stream open.
// Closing the send direction and cancelling the stream context releases the
// exchange phase stream.
func (s *demoRequestProcessor) retireStream(rc *extproc.RequestContext) {
	if v, err := rc.GetValue(controllerStreamKey); err == nil {
		if st, ok := v.(grpc.BidiStreamingClient[v1.ProcessRequest, v1.ProcessResponse]); ok {
			_ = st.CloseSend()
		}
	}
	if cancel, err := rc.GetValue(controllerStreamCancelKey); err == nil {
		if c, ok := cancel.(context.CancelFunc); ok {
			c()
		}
	}
	rc.SetValue(controllerStreamKey, nil)
	rc.SetValue(controllerStreamCancelKey, nil)
}

// sendPhase sends a phase update carrying the phase snapshot to the controller
// for the session and waits for the controller's decision. Returns the decision
// (continue/cancel).
func (s *demoRequestProcessor) sendPhase(ctx context.Context, phase int32, rc *extproc.RequestContext, sessionID string, snapshot *v1.HttpSnapshot) (*v1.ProcessResponse, error) {
	stream, err := s.controllerStream(ctx, rc)
	if err != nil {
		return nil, err
	}
	if err = stream.Send(&v1.ProcessRequest{SessionId: sessionID, Phase: v1.Phase(phase), Snapshot: snapshot}); err != nil {
		s.retireStream(rc)
		return nil, err
	}
	dec, err := stream.Recv()
	if err != nil {
		s.retireStream(rc)
		return nil, err
	}
	// Phase resolved (decision received); retire the controller stream when the
	// phase signaled end-of-stream, closing the exchange. Otherwise keep the
	// handle in the request context so the next phase of the same exchange
	// reuses it.
	if rc.EndOfStream {
		s.retireStream(rc)
	}
	return dec, nil
}

// sendPhaseAndDecide sends a phase update carrying the phase snapshot to the
// controller for the session and waits for the controller's decision. Returns
// the decision (continue/cancel).
func (s *demoRequestProcessor) sendPhaseAndDecide(ctx context.Context, phase int32, rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) (*v1.ProcessResponse, error) {
	sessionID := s.sessionIDFor(rc, headers)
	snap := s.snapshot(rc, headers, body)
	return s.sendPhase(ctx, phase, rc, sessionID, snap)
}

// sessionIDFor resolves the controller session id for an incoming phase from
// the request context (falling back to the request headers, then the stored
// session id when absent). Resolution is per request context, so concurrent
// exchanges do not clobber each other.
func (s *demoRequestProcessor) sessionIDFor(rc *extproc.RequestContext, headers extproc.AllHeaders) string {
	if id, err := rc.GetValue(controllerSessionIDKey); err == nil {
		if sid, ok := id.(string); ok && sid != "" {
			return sid
		}
	}
	if id, ok := headers.Get(sessionIDHeader); ok && id != "" {
		rc.SetValue(controllerSessionIDKey, id)
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

// snapshotMasked reports whether a snapshot header value should be masked
// (cookie, set-cookie, authorization).
func snapshotMasked(name string) bool {
	switch name {
	case "cookie", "set-cookie", "authorization":
		return true
	}
	return false
}

// snapshot builds the HttpSnapshot the demo processor reports to the controller
// for the phase being processed, from the observed headers and body. Body phases
// report at most snapshotBodyLimit bytes; longer bodies are truncated and flagged
// via BodyTruncated. Response phases surface the response status via the
// ":status" header the data plane sends downstream. Headers are emitted in
// sorted key order for deterministic snapshots.
func (s *demoRequestProcessor) snapshot(rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) *v1.HttpSnapshot {
	snap := &v1.HttpSnapshot{}
	headerList := make([]*v1.Header, 0, len(headers.Headers))
	for name, values := range headers.Headers {
		for _, v := range values {
			if snapshotMasked(name) {
				headerList = append(headerList, &v1.Header{Key: name, Value: fmt.Sprintf("[redacted %d chars]", len(v))})
				continue
			}
			headerList = append(headerList, &v1.Header{Key: name, Value: v})
		}
	}
	sort.SliceStable(headerList, func(i, j int) bool { return headerList[i].GetKey() < headerList[j].GetKey() })
	snap.Headers = headerList
	if status, ok := headers.Get(":status"); ok {
		if code, err := strconv.Atoi(status); err == nil {
			snap.Status = int32(code)
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

// resolvePhase sends a phase update carrying the phase snapshot to the
// controller for the session and applies the controller's decision (continue
// mutations or cancel) to the phase context. It returns any error from sending
// the phase update or applying the decision.
func (s *demoRequestProcessor) resolvePhase(ctx context.Context, phase int32, rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) error {
	dec, err := s.sendPhaseAndDecide(ctx, phase, rc, headers, body)
	if err != nil {
		return err
	}
	decision := dec.GetDecision()
	if decision == nil {
		return nil
	}
	if decision.GetContinue() != nil {
		continueDecision := decision.GetContinue()
		if len(continueDecision.GetRemoveHeaders()) > 0 {
			if err := rc.RemoveHeaders(continueDecision.GetRemoveHeaders()); err != nil {
				return err
			}
		}
		if len(continueDecision.GetBody()) > 0 {
			if err := rc.ReplaceBody(continueDecision.GetBody()); err != nil {
				return err
			}
		}
		for name, value := range continueDecision.GetHeaders() {
			if err := rc.OverwriteHeader(name, extproc.HeaderValue{RawValue: []byte(value)}); err != nil {
				return err
			}
		}
		if err := rc.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
			return err
		}
		return rc.ContinueRequest()
	}
	if decision.GetCancel() != nil {
		cancel := decision.GetCancel()
		return rc.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *demoRequestProcessor) ProcessRequestHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_REQUEST_HEADERS, ctx, headers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_HEADERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *demoRequestProcessor) ProcessResponseHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_RESPONSE_HEADERS, ctx, headers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_HEADERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *demoRequestProcessor) ProcessResponseBody(ctx *extproc.RequestContext, body []byte) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_RESPONSE_BODY, ctx, ctx.AllHeaders, body); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_BODY)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *demoRequestProcessor) ProcessResponseTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_RESPONSE_TRAILERS, ctx, trailers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_TRAILERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *demoRequestProcessor) ProcessRequestBody(ctx *extproc.RequestContext, body []byte) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_REQUEST_BODY, ctx, ctx.AllHeaders, body); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_BODY)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *demoRequestProcessor) ProcessRequestTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_REQUEST_TRAILERS, ctx, trailers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_TRAILERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *demoRequestProcessor) ErrorHandler(ctx *extproc.RequestContext, phase int, err error) {
	if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
		// Envoy tore the downstream down and cancelled the extproc stream; treat
		// it as the cancellation signal for this exchange phase and clean up the
		// controller stream handle held in the request context.
		s.retireStream(ctx)
		return
	}
	s.logger.Error("demo processor error",
		slog.String("phase", extproc.RequestPhaseToString(phase)),
		slog.String("error", err.Error()),
	)
}

func (s *demoRequestProcessor) Close(gracePeriodSeconds int32) error {
	s.controllerMu.Lock()
	defer s.controllerMu.Unlock()
	if s.controllerConn != nil {
		return s.controllerConn.Close()
	}
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

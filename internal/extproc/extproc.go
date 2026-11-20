package extproc

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
	sessionIDHeader     = "x-demo-session-id"
	controllerStreamKey = "controllerDemoStream"

	// controllerStreamCancelKey is the RequestContext data key holding the cancel
	// function for the controller bidi stream of the current exchange phase.
	controllerStreamCancelKey = "controllerDemoStreamCancel"

	// controllerSessionIDKey is the RequestContext data key holding the controller
	// session id for the current exchange phase.
	controllerSessionIDKey = "controllerDemoSessionID"

	controllerAddrEnv          = "CONTROLLER_ADDR"
	defaultControllerAddr      = "controller:9093"
	exchangeTimeoutEnv         = "EXCHANGE_TIMEOUT"
	abortOnProcessorFailureEnv = "ABORT_ON_PROCESSOR_FAILURE"

	// defaultExchangeTimeout bounds a single controller exchange phase stream (the
	// per-exchange budget across the exchange's phases).
	defaultExchangeTimeout = 5 * time.Minute

	// snapshotBodyLimit caps the bodies reported in phase snapshots.
	snapshotBodyLimit = 16 * 1024 // cap on bodies reported in phase snapshots
)

// RequestProcessor drives the controller's internal DemoProcessorService channel
// over a bidi gRPC stream, sending phase updates and applying the decisions
// returned by the controller (continue/cancel).
type RequestProcessor struct {
	opts   *extproc.ProcessingOptions
	logger *slog.Logger

	// controllerConn is the single controller client connection shared by all
	// exchanges. Stream handles are held per exchange in the request context and
	// retired individually; the connection itself is shared, not per exchange.
	controllerMu   sync.Mutex
	controllerConn *grpc.ClientConn

	// exchangeTimeout bounds a single controller exchange phase stream (the
	// per-exchange budget across the exchange's phases).
	exchangeTimeout time.Duration

	controllerAddr string
}

// NewRequestProcessor returns a RequestProcessor wired to the controller gRPC
// address that owns the DemoProcessorService channel. The phase exchange timeout
// and the fail-closed abort flag are read from the environment (EXCHANGE_TIMEOUT
// and ABORT_ON_PROCESSOR_FAILURE respectively), defaulting to 5 minutes and
// true.
func NewRequestProcessor(controllerAddr string) *RequestProcessor {
	return &RequestProcessor{
		opts:            &extproc.ProcessingOptions{AbortOnProcessorFailure: abortOnProcessorFailure()},
		logger:          slog.Default(),
		controllerAddr:  controllerAddr,
		exchangeTimeout: exchangeTimeout(),
	}
}

// NewRequestProcessorFromEnv returns a RequestProcessor wired to the controller
// gRPC address from the CONTROLLER_ADDR environment variable, with the phase
// exchange timeout and fail-closed abort flag read from the environment as well.
func NewRequestProcessorFromEnv() *RequestProcessor {
	return NewRequestProcessor(controllerAddrFromEnv())
}

// controllerAddrFromEnv returns the controller gRPC address from the
// CONTROLLER_ADDR environment variable, defaulting to controller:9093.
func controllerAddrFromEnv() string {
	return envOr(controllerAddrEnv, defaultControllerAddr)
}

// exchangeTimeout returns the phase exchange timeout, in seconds, from the
// EXCHANGE_TIMEOUT environment variable, defaulting to 5 minutes.
func exchangeTimeout() time.Duration {
	seconds, err := strconv.Atoi(envOr(exchangeTimeoutEnv, ""))
	if err != nil {
		return defaultExchangeTimeout
	}
	return time.Duration(seconds) * time.Second
}

// abortOnProcessorFailure returns whether the demo extproc aborts the phase
// stream when the processor fails, from the ABORT_ON_PROCESSOR_FAILURE
// environment variable, defaulting to true.
func abortOnProcessorFailure() bool {
	enabled, err := strconv.ParseBool(envOr(abortOnProcessorFailureEnv, ""))
	if err != nil {
		return true
	}
	return enabled
}

// envOr returns the value of the environment variable named key, or fallback if
// unset or empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// GetName returns the processor name reported to the SDK and Envoy.
func (s *RequestProcessor) GetName() string {
	return "demo"
}

// GetOptions returns the processing options for the processor.
func (s *RequestProcessor) GetOptions() *extproc.ProcessingOptions {
	return s.opts
}

// controllerClient returns the shared controller client connection, opening it
// lazily on first use.
func (s *RequestProcessor) controllerClient() (*grpc.ClientConn, error) {
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
func (s *RequestProcessor) controllerStream(ctx context.Context, rc *extproc.RequestContext) (grpc.BidiStreamingClient[v1.ProcessRequest, v1.ProcessResponse], error) {
	if v, err := rc.GetValue(controllerStreamKey); err == nil {
		if st, ok := v.(grpc.BidiStreamingClient[v1.ProcessRequest, v1.ProcessResponse]); ok {
			return st, nil
		}
	}
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
// request context. It must be called when a phase fails or the phase signaled
// end-of-stream during a response phase after a decision, so a dead stream is
// never reused by a later phase of the exchange and a resolved exchange does not
// leave the stream open.
// Closing the send direction and cancelling the stream context releases the
// exchange phase stream.
func (s *RequestProcessor) retireStream(rc *extproc.RequestContext) {
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
func (s *RequestProcessor) sendPhase(ctx context.Context, phase int32, rc *extproc.RequestContext, sessionID string, snapshot *v1.HttpSnapshot) (*v1.ProcessResponse, error) {
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
	// phase signaled end-of-stream and this was a response phase, closing the
	// exchange. End-of-stream marks the end of a single direction (e.g. request
	// headers for a bodyless GET), not the end of the exchange, so only retire for
	// response phases. Otherwise keep the handle in the request context so the
	// next phase of the same exchange reuses it.
	if rc.EndOfStream && isResponsePhase(phase) {
		s.retireStream(rc)
	}
	return dec, nil
}

// isResponsePhase reports whether the extproc phase is a response phase. Only
// response phases (headers/body/trailers) mark the end of the direction the demo
// retires on, so end-of-stream is only honored for them.
func isResponsePhase(phase int32) bool {
	return phase >= extproc.REQUEST_PHASE_RESPONSE_HEADERS && phase <= extproc.REQUEST_PHASE_RESPONSE_TRAILERS
}

// sendPhaseAndDecide sends a phase update carrying the phase snapshot to the
// controller for the session and waits for the controller's decision. Returns
// the decision (continue/cancel).
func (s *RequestProcessor) sendPhaseAndDecide(ctx context.Context, phase int32, rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) (*v1.ProcessResponse, error) {
	sessionID := s.sessionIDFor(rc, headers)
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing x-demo-session-id header")
	}
	snap := s.snapshot(rc, headers, body)
	return s.sendPhase(ctx, phase, rc, sessionID, snap)
}

// sessionIDFor resolves the controller session id for an incoming phase from
// the request context (falling back to the request headers). Resolution is per
// request context, so concurrent exchanges do not clobber each other.
func (s *RequestProcessor) sessionIDFor(rc *extproc.RequestContext, headers extproc.AllHeaders) string {
	if id, err := rc.GetValue(controllerSessionIDKey); err == nil {
		if sid, ok := id.(string); ok && sid != "" {
			return sid
		}
	}
	if id, ok := headers.Get(sessionIDHeader); ok && id != "" {
		rc.SetValue(controllerSessionIDKey, id)
		return id
	}
	return ""
}

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
func (s *RequestProcessor) snapshot(rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) *v1.HttpSnapshot {
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
func (s *RequestProcessor) resolvePhase(ctx context.Context, phase int32, rc *extproc.RequestContext, headers extproc.AllHeaders, body []byte) error {
	dec, err := s.sendPhaseAndDecide(ctx, phase, rc, headers, body)
	if err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument:
			s.retireStream(rc)
			return rc.CancelRequest(400, extproc.BuildHeaderValuesFromMap(map[string]string{"content-type": "text/plain"}), "missing x-demo-session-id header")
		case codes.NotFound:
			s.retireStream(rc)
			return rc.CancelRequest(404, extproc.BuildHeaderValuesFromMap(map[string]string{"content-type": "text/plain"}), "session not found")
		}
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
		// A cancel ends the exchange, so retire the controller phase stream before
		// surfacing the cancellation to the phase context.
		s.retireStream(rc)
		return rc.CancelRequest(cancel.GetStatus(), extproc.BuildHeaderValuesFromMap(cancel.GetHeaders()), cancel.GetBody())
	}
	return nil
}

func (s *RequestProcessor) ProcessRequestHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_REQUEST_HEADERS, ctx, headers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_HEADERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *RequestProcessor) ProcessResponseHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_RESPONSE_HEADERS, ctx, headers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_HEADERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *RequestProcessor) ProcessResponseBody(ctx *extproc.RequestContext, body []byte) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_RESPONSE_BODY, ctx, ctx.AllHeaders, body); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_BODY)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *RequestProcessor) ProcessResponseTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_RESPONSE_TRAILERS, ctx, trailers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_RESPONSE_TRAILERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *RequestProcessor) ProcessRequestBody(ctx *extproc.RequestContext, body []byte) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_REQUEST_BODY, ctx, ctx.AllHeaders, body); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_BODY)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *RequestProcessor) ProcessRequestTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	if err := s.resolvePhase(context.Background(), extproc.REQUEST_PHASE_REQUEST_TRAILERS, ctx, trailers, nil); err != nil {
		s.logger.Error("demo processor failed to send phase update", slog.String("phase", extproc.RequestPhaseToString(extproc.REQUEST_PHASE_REQUEST_TRAILERS)), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (s *RequestProcessor) ErrorHandler(ctx *extproc.RequestContext, phase int, err error) {
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

// Close releases the shared controller client connection.
func (s *RequestProcessor) Close(gracePeriodSeconds int32) error {
	s.controllerMu.Lock()
	defer s.controllerMu.Unlock()
	if s.controllerConn != nil {
		return s.controllerConn.Close()
	}
	return nil
}

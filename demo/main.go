package main

import (
	"log/slog"

	extproc "github.com/wrossmorrow/envoy-extproc-sdk-go"
)

// demoRequestProcessor is the minimal M1 demo processor. It currently handles
// the request headers phase with a minimal mutation and continues; the rest of
// the phases simply continue. Human-in-the-loop decisions and phase timeouts are
// layered on in later milestones.
type demoRequestProcessor struct {
	opts   *extproc.ProcessingOptions
	logger *slog.Logger
}

func newDemoRequestProcessor() *demoRequestProcessor {
	return &demoRequestProcessor{
		opts:   &extproc.ProcessingOptions{AbortOnProcessorFailure: true},
		logger: slog.Default(),
	}
}

func (s *demoRequestProcessor) GetName() string {
	return "demo"
}

func (s *demoRequestProcessor) GetOptions() *extproc.ProcessingOptions {
	return s.opts
}

func (s *demoRequestProcessor) ProcessRequestHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	// minimal interception: mark the request as processed and continue
	if err := ctx.AddHeader("x-demo-processed", extproc.HeaderValue{RawValue: []byte("true")}); err != nil {
		return err
	}
	return ctx.ContinueRequest()
}

func (s *demoRequestProcessor) ProcessRequestTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	return ctx.ContinueRequest()
}

func (s *demoRequestProcessor) ProcessResponseHeaders(ctx *extproc.RequestContext, headers extproc.AllHeaders) error {
	return ctx.ContinueRequest()
}

func (s *demoRequestProcessor) ProcessResponseTrailers(ctx *extproc.RequestContext, trailers extproc.AllHeaders) error {
	return ctx.ContinueRequest()
}

func (s *demoRequestProcessor) ProcessResponseBody(ctx *extproc.RequestContext, body []byte) error {
	return ctx.ContinueRequest()
}

func (s *demoRequestProcessor) ProcessRequestBody(ctx *extproc.RequestContext, body []byte) error {
	return ctx.ContinueRequest()
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

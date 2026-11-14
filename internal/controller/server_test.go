package controller

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
)

func TestSessionController_CreateSession(t *testing.T) {
	c := NewSessionController()

	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if s.ID == "" {
		t.Fatalf("CreateSession() ID = %q, want non-empty", s.ID)
	}
	if s.Status != SessionStatusNew {
		t.Fatalf("CreateSession() Status = %v, want %v", s.Status, SessionStatusNew)
	}
	if s.Updates == nil {
		t.Fatalf("CreateSession() Updates = nil, want non-nil updates channel")
	}
}

func TestSessionController_CreateSession_GeneratesUUIDv7SessionID(t *testing.T) {
	c := NewSessionController()
	for i := 0; i < 2; i++ {
		s, err := c.CreateSession()
		if err != nil {
			t.Fatalf("CreateSession() error = %v, want nil", err)
		}
		u, err := uuid.Parse(s.ID)
		if err != nil {
			t.Fatalf("CreateSession() ID = %q, want valid UUID: %v", s.ID, err)
		}
		if u.Version() != 7 {
			t.Fatalf("CreateSession() ID = %q, want UUID version 7, got version %d", s.ID, u.Version())
		}
	}
}

func TestSessionController_SubmitDecision_RejectsWhenNotInFlight(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err == nil {
		t.Fatalf("SubmitDecision() error = nil, want %v", errSessionNotInFlight)
	}
}

func TestSessionController_SubmitDecision_RejectsPhaseMismatch(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	s.Status = SessionStatusInFlight
	s.Phase = int32(v1.Phase_PHASE_REQUEST_HEADERS)
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_RESPONSE_BODY), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err == nil {
		t.Fatalf("SubmitDecision() error = nil, want %v", errPhaseMismatch)
	}
}

func TestSessionController_BeginPhase(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	updated, err := c.GetSession(s.ID)
	if err != nil {
		t.Fatalf("GetSession() error = %v, want nil", err)
	}
	if updated.Status != SessionStatusInFlight {
		t.Fatalf("BeginPhase() Status = %v, want %v", updated.Status, SessionStatusInFlight)
	}
	if updated.Phase != int32(v1.Phase_PHASE_REQUEST_HEADERS) {
		t.Fatalf("BeginPhase() Phase = %v, want %v", updated.Phase, int32(v1.Phase_PHASE_REQUEST_HEADERS))
	}
	if updated.Pending == nil {
		t.Fatalf("BeginPhase() Pending = nil, want pending decision")
	}
}

func TestSessionController_BeginPhase_SessionNotFound(t *testing.T) {
	c := NewSessionController()
	if err := c.BeginPhase("missing", int32(v1.Phase_PHASE_REQUEST_HEADERS)); err == nil {
		t.Fatalf("BeginPhase() error = nil, want %v", errSessionNotFound)
	}
}

func TestSessionController_ResolveDecision_AwaitsHumanDecision(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	decisionCh := make(chan *v1.ProcessResponse, 1)
	decisionErrCh := make(chan error, 1)
	go func() {
		decision, decisionErr := c.ResolveDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS))
		decisionCh <- decision
		decisionErrCh <- decisionErr
	}()
	s.Pending.Ch <- &pendingResolution{decision: &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}}
	dec := <-decisionCh
	decisionErr := <-decisionErrCh
	if decisionErr != nil {
		t.Fatalf("ResolveDecision() error = %v, want nil", decisionErr)
	}
	if dec.GetDecision().GetContinue() == nil {
		t.Fatalf("ResolveDecision() decision = %+v, want continue", dec)
	}
}

func TestSessionController_ResolveDecision_ReturnsCancelOnPhaseTimeout(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	oldTimeout := PhaseTimeout
	PhaseTimeout = 50 * time.Millisecond
	defer func() { PhaseTimeout = oldTimeout }()
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	dec, err := c.ResolveDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS))
	if err != nil {
		t.Fatalf("ResolveDecision() error = %v, want nil", err)
	}
	cancelDecision := dec.GetDecision().GetCancel()
	if cancelDecision == nil {
		t.Fatalf("ResolveDecision() decision = %+v, want cancel", dec)
	}
	if cancelDecision.GetStatus() != http.StatusGatewayTimeout {
		t.Fatalf("ResolveDecision() cancel status = %v, want %v", cancelDecision.GetStatus(), http.StatusGatewayTimeout)
	}
}

// TestSessionController_SubmitDecision_RejectsLateDecisionAfterResolution
// asserts that a decision submitted after a phase already resolved (human
// decision) is rejected, so late decisions cannot drive an already-resolved
// phase.
func TestSessionController_SubmitDecision_RejectsLateDecisionAfterResolution(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	if err = c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err != nil {
		t.Fatalf("SubmitDecision() error = %v, want nil", err)
	}
	if err = c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err == nil {
		t.Fatalf("SubmitDecision() error = nil, want %v", errSessionNotInFlight)
	}
}

// TestSessionController_ResolveDecision_TimesOutMarksSessionResolved asserts
// that a phase timeout marks the session resolved, so a late human decision can
// no longer be accepted for the timed-out phase.
func TestSessionController_ResolveDecision_TimesOutMarksSessionResolved(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	oldTimeout := PhaseTimeout
	PhaseTimeout = 50 * time.Millisecond
	defer func() { PhaseTimeout = oldTimeout }()
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	if _, resolveErr := c.ResolveDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); resolveErr != nil {
		t.Fatalf("ResolveDecision() error = %v, want nil", resolveErr)
	}
	if s.Status != SessionStatusResolved {
		t.Fatalf("ResolveDecision() status = %v, want %v", s.Status, SessionStatusResolved)
	}
	if err = c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err == nil {
		t.Fatalf("SubmitDecision() error = nil, want %v", errSessionNotInFlight)
	}
}

func TestSessionController_ResolveDecision_RejectsWhenNotInFlight(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if _, err := c.ResolveDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err == nil {
		t.Fatalf("ResolveDecision() error = nil, want %v", errSessionNotInFlight)
	}
}

func TestDemoControllerHandler_SubmitDecision_MapsContinue(t *testing.T) {
	c := NewSessionController()
	h := &DemoControllerHandler{Controller: c}
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	resp, err := h.SubmitDecision(context.Background(), connect.NewRequest(&v1.SubmitDecisionRequest{
		SessionId: s.ID,
		Phase:     v1.Phase_PHASE_REQUEST_HEADERS,
		Decision:  &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}},
	}))
	if err != nil {
		t.Fatalf("SubmitDecision() error = %v, want nil", err)
	}
	if resp == nil {
		t.Fatalf("SubmitDecision() response = nil, want response")
	}
	if s.Status != SessionStatusResolved {
		t.Fatalf("SubmitDecision() Status = %v, want %v", s.Status, SessionStatusResolved)
	}
	res := <-s.Pending.Ch
	if res.decision == nil || res.decision.GetContinue() == nil {
		t.Fatalf("SubmitDecision() resolved decision = %+v, want continue", res.decision)
	}
}

func TestDemoControllerHandler_SubmitDecision_MapsCancel(t *testing.T) {
	c := NewSessionController()
	h := &DemoControllerHandler{Controller: c}
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	resp, err := h.SubmitDecision(context.Background(), connect.NewRequest(&v1.SubmitDecisionRequest{
		SessionId: s.ID,
		Phase:     v1.Phase_PHASE_REQUEST_HEADERS,
		Decision:  &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: 403, Headers: map[string]string{}}}},
	}))
	if err != nil {
		t.Fatalf("SubmitDecision() error = %v, want nil", err)
	}
	if resp == nil {
		t.Fatalf("SubmitDecision() response = nil, want response")
	}
	if s.Status != SessionStatusResolved {
		t.Fatalf("SubmitDecision() Status = %v, want %v", s.Status, SessionStatusResolved)
	}
	res := <-s.Pending.Ch
	if res.decision == nil || res.decision.GetCancel() == nil {
		t.Fatalf("SubmitDecision() resolved decision = %+v, want cancel", res.decision)
	}
	if res.decision.GetCancel().GetStatus() != 403 {
		t.Fatalf("SubmitDecision() resolved cancel status = %d, want 403", res.decision.GetCancel().GetStatus())
	}
}

func TestSessionController_ResolveDecision_RejectsPhaseMismatch(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err := c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	if _, err := c.ResolveDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_RESPONSE_BODY)); err == nil {
		t.Fatalf("ResolveDecision() error = nil, want %v", errPhaseMismatch)
	}
}

func TestSessionController_SubmitDecision_ResolvesPending(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	s.Status = SessionStatusInFlight
	s.Phase = int32(v1.Phase_PHASE_REQUEST_HEADERS)
	s.Pending = &pendingDecision{Phase: int32(v1.Phase_PHASE_REQUEST_HEADERS), Ch: make(chan *pendingResolution, 1)}
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}); err != nil {
		t.Fatalf("SubmitDecision() error = %v, want nil", err)
	}
	if s.Status != SessionStatusResolved {
		t.Fatalf("SubmitDecision() resolved status = %v, want %v", s.Status, SessionStatusResolved)
	}
	res := <-s.Pending.Ch
	if res.decision == nil || res.decision.GetContinue() == nil {
		t.Fatalf("pending resolution decision = %+v, want continue", res.decision)
	}
}

func TestSessionController_SubmitDecision_SanitizesCancelDecision(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	invalidBody := strings.Repeat("x", int(MaxCancelBodyBytes)+10)
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), &v1.Decision{
		Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: 999, Body: invalidBody}},
	}); err != nil {
		t.Fatalf("SubmitDecision() error = %v, want nil", err)
	}
	res := <-s.Pending.Ch
	cancel := res.decision.GetCancel()
	if cancel == nil {
		t.Fatalf("pending resolution decision = %+v, want cancel", res.decision)
	}
	if cancel.GetStatus() != http.StatusForbidden {
		t.Fatalf("ResolveDecision() cancel status = %d, want %d", cancel.GetStatus(), http.StatusForbidden)
	}
	if got := int64(len(cancel.GetBody())); got > MaxCancelBodyBytes {
		t.Fatalf("ResolveDecision() cancel body length = %d, want <= %d", got, MaxCancelBodyBytes)
	}
}

// demoControllerGaugeValue reads the current value of a named controller
// logical metric from the controller metrics registry.
func demoControllerGaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := demoControllerMetrics.Gather()
	if err != nil {
		t.Fatalf("gather controller metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		metrics := mf.GetMetric()
		if len(metrics) > 0 && metrics[0].GetGauge() != nil {
			return metrics[0].GetGauge().GetValue()
		}
	}
	return 0
}

func TestSessionController_Metrics_SessionsActive(t *testing.T) {
	c := NewSessionController()
	before := demoControllerGaugeValue(t, "demo_controller_sessions_active")
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if got := demoControllerGaugeValue(t, "demo_controller_sessions_active"); got != before+1 {
		t.Fatalf("demo_controller_sessions_active = %v, want %v", got, before+1)
	}
	c.removeSession(s.ID)
	if got := demoControllerGaugeValue(t, "demo_controller_sessions_active"); got != before {
		t.Fatalf("demo_controller_sessions_active = %v, want %v after remove", got, before)
	}
}

func TestSessionController_Metrics_PhasesPending(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	defer c.removeSession(s.ID)

	before := demoControllerGaugeValue(t, "demo_controller_phases_pending")
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	if got := demoControllerGaugeValue(t, "demo_controller_phases_pending"); got != before+1 {
		t.Fatalf("demo_controller_phases_pending = %v, want %v", got, before+1)
	}

	decisionCh := make(chan *v1.ProcessResponse, 1)
	decisionErrCh := make(chan error, 1)
	go func() {
		decision, decisionErr := c.ResolveDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS))
		decisionCh <- decision
		decisionErrCh <- decisionErr
	}()
	s.Pending.Ch <- &pendingResolution{decision: &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}}
	dec := <-decisionCh
	decisionErr := <-decisionErrCh
	if decisionErr != nil {
		t.Fatalf("ResolveDecision() error = %v, want nil", decisionErr)
	}
	if dec.GetDecision().GetContinue() == nil {
		t.Fatalf("ResolveDecision() decision = %+v, want continue", dec)
	}
	if got := demoControllerGaugeValue(t, "demo_controller_phases_pending"); got != before {
		t.Fatalf("demo_controller_phases_pending = %v, want %v after resolve", got, before)
	}
}

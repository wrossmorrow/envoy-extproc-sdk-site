package controller

import (
	"context"
	"net/http"
	"testing"
	"time"

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

func TestSessionController_SubmitDecision_RejectsWhenNotInFlight(t *testing.T) {
	c := NewSessionController()
	s, err := c.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil", err)
	}
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), "continue"); err == nil {
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
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_RESPONSE_BODY), "continue"); err == nil {
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
	s.Pending.Ch <- &pendingResolution{Action: "continue"}
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
	if err = c.BeginPhase(s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS)); err != nil {
		t.Fatalf("BeginPhase() error = %v, want nil", err)
	}
	oldTimeout := PhaseTimeout
	PhaseTimeout = 50 * time.Millisecond
	defer func() { PhaseTimeout = oldTimeout }()
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
	if err := c.SubmitDecision(context.Background(), s.ID, int32(v1.Phase_PHASE_REQUEST_HEADERS), "continue"); err != nil {
		t.Fatalf("SubmitDecision() error = %v, want nil", err)
	}
	if s.Status != SessionStatusResolved {
		t.Fatalf("SubmitDecision() resolved status = %v, want %v", s.Status, SessionStatusResolved)
	}
	res := <-s.Pending.Ch
	if res.Action != "continue" {
		t.Fatalf("pending resolution action = %q, want %q", res.Action, "continue")
	}
}

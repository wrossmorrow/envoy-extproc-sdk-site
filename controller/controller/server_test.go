package controller

import (
	"context"
	"testing"

	v1 "github.com/wrossmorrow/envoy-exproc-sdk-controller/gen/go/proto/demo/controller/v1"
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

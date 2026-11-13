package controller

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
)

// SessionStatus reflects the lifecycle state of a demo session.
type SessionStatus int32

const (
	SessionStatusNew       SessionStatus = 0
	SessionStatusInFlight  SessionStatus = 1
	SessionStatusResolved  SessionStatus = 2
	SessionStatusCompleted SessionStatus = 3
	SessionStatusTimedOut  SessionStatus = 4
)

// Update is a single phase-update event sent to the browser for a session.
type Update = v1.CreateSessionResponse

// Session is a single interactive extproc demo session.
type Session struct {
	ID        string
	Status    SessionStatus
	Phase     int32
	Pending   *pendingDecision
	CreatedAt time.Time
	ExpiresAt time.Time
	Updates   chan *Update
}

type pendingDecision struct {
	Phase int32
	Ch    chan *pendingResolution
}

type pendingResolution struct {
	Action string // "continue" or "cancel"
}

// SessionController owns demo sessions and their pending decisions/updates.
type SessionController struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

var (
	errSessionNotFound    = errors.New("session not found")
	errSessionNotInFlight = errors.New("session not in flight")
	errPhaseMismatch      = errors.New("session phase mismatch")
	errSessionTimedOut    = errors.New("session timed out")
)

func NewSessionController() *SessionController {
	return &SessionController{sessions: map[string]*Session{}}
}

func (c *SessionController) CreateSession() (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &Session{
		ID:        newSessionID(),
		Status:    SessionStatusNew,
		Updates:   make(chan *Update, 16),
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(30 * time.Second),
	}
	c.sessions[s.ID] = s
	return s, nil
}

func (c *SessionController) GetSession(id string) (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return nil, errSessionNotFound
	}
	return s, nil
}

func (c *SessionController) GetUpdates(id string, send func(*v1.CreateSessionResponse) error) error {
	s, err := c.GetSession(id)
	if err != nil {
		return err
	}
	for {
		select {
		case u, ok := <-s.Updates:
			if !ok {
				return nil // stream closed
			}
			if err := send(u); err != nil {
				return err
			}
		case <-time.After(30 * time.Second):
			return errSessionTimedOut
		}
	}
}

func (c *SessionController) SubmitDecision(ctx context.Context, id string, phase int32, action string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return errSessionNotFound
	}
	if s.Status != SessionStatusInFlight {
		return errSessionNotInFlight
	}
	if phase != s.Phase {
		return errPhaseMismatch
	}
	if s.Pending == nil || s.Pending.Ch == nil {
		return errors.New("no pending decision channel")
	}
	res := &pendingResolution{Action: action}
	s.Pending.Ch <- res
	s.Status = SessionStatusResolved
	return nil
}

// PhaseTimeout is how long ResolveDecision waits for a human decision on a
// session phase before failing the phase closed with an immediate cancel.
var PhaseTimeout = 30 * time.Second

// ResolveDecision resolves the decision for a session phase and returns the
// decision to send back to the extproc over the Process bidi stream. It awaits
// the pending human decision (see SubmitDecision) and, on phase timeout, fails
// closed with an immediate cancel decision.
func (c *SessionController) ResolveDecision(ctx context.Context, sessionID string, phase int32) (*v1.ProcessResponse, error) {
	c.mu.Lock()
	s, ok := c.sessions[sessionID]
	c.mu.Unlock()
	if !ok {
		return nil, errSessionNotFound
	}
	if s.Status != SessionStatusInFlight {
		return nil, errSessionNotInFlight
	}
	if phase != s.Phase {
		return nil, errPhaseMismatch
	}
	c.mu.Lock()
	if s.Pending == nil {
		s.Pending = &pendingDecision{Phase: phase}
	}
	if s.Pending.Ch == nil {
		s.Pending.Ch = make(chan *pendingResolution, 1)
	}
	c.mu.Unlock()
	select {
	case res := <-s.Pending.Ch:
		return decisionResponse(s, phase, res.Action), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(PhaseTimeout):
		return decisionResponse(s, phase, "cancel"), nil
	}
}

// SendUpdate sends an update on the session's CreateSession stream to clients
// with open streams for the session.
func (c *SessionController) SendUpdate(id string, update *v1.CreateSessionResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return errSessionNotFound
	}
	select {
	case s.Updates <- update:
		return nil
	default:
		return nil
	}
}

// phaseFromExtproc converts an extproc phase number to the controller Phase
// enum value used for session phases, aligning the extproc phase numbering with
// the controller's Phase enum.
func phaseFromExtproc(phase int32) v1.Phase {
	switch phase {
	case 0:
		return v1.Phase_PHASE_NOT_STARTED
	case 1:
		return v1.Phase_PHASE_REQUEST_HEADERS
	case 2:
		return v1.Phase_PHASE_REQUEST_BODY
	case 3:
		return v1.Phase_PHASE_REQUEST_TRAILERS
	case 4:
		return v1.Phase_PHASE_RESPONSE_HEADERS
	case 5:
		return v1.Phase_PHASE_RESPONSE_BODY
	case 6:
		return v1.Phase_PHASE_RESPONSE_TRAILERS
	}
	return v1.Phase(phase)
}

// BeginPhase transitions a session into in-flight at a phase and opens a
// pending decision channel for the human to resolve.
func (c *SessionController) BeginPhase(id string, phase int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return errSessionNotFound
	}
	s.Status = SessionStatusInFlight
	s.Phase = phase
	if s.Pending == nil {
		s.Pending = &pendingDecision{Phase: phase}
	}
	if s.Pending.Ch == nil {
		s.Pending.Ch = make(chan *pendingResolution, 1)
	}
	return nil
}

// decisionResponse builds the ProcessResponse decision for a session phase
// from a human action ("continue" or "cancel").
func decisionResponse(s *Session, phase int32, action string) *v1.ProcessResponse {
	decision := &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}
	if action == "cancel" {
		decision.Action = &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: http.StatusGatewayTimeout, Headers: map[string]string{}}}
	}
	return &v1.ProcessResponse{SessionId: s.ID, Phase: v1.Phase(phase), Decision: decision}
}

// DemoControllerHandler implements the Connect service.
type DemoControllerHandler struct {
	Controller *SessionController
}

// NewDemoControllerHandler builds an http.Handler from the demo controller service.
func NewDemoControllerHandler(controller *SessionController) (string, http.Handler) {
	return v1connect.NewDemoControllerServiceHandler(&DemoControllerHandler{Controller: controller})
}

// CreateSession creates a new session and opens a stream of updates. The
// first Update carries the session id with phase NOT_STARTED; the browser then
// sends the envoy-bound request carrying that session id and receives phase
// updates as the session resolves.
func (h *DemoControllerHandler) CreateSession(ctx context.Context, req *connect.Request[v1.CreateSessionRequest], stream *connect.ServerStream[v1.CreateSessionResponse]) error {
	s, err := h.Controller.CreateSession()
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.CreateSessionResponse{SessionId: s.ID, Phase: v1.Phase_PHASE_NOT_STARTED}); err != nil {
		return err
	}
	return h.Controller.GetUpdates(s.ID, func(u *v1.CreateSessionResponse) error {
		return stream.Send(u)
	})
}

// decisionAction maps a decision to a human action ("continue" or "cancel").
func decisionAction(decision *v1.Decision) string {
	if decision != nil && decision.GetCancel() != nil {
		return "cancel"
	}
	return "continue"
}

// SubmitDecision submits a human decision for a session at a phase.
func (h *DemoControllerHandler) SubmitDecision(ctx context.Context, req *connect.Request[v1.SubmitDecisionRequest]) (*connect.Response[v1.SubmitDecisionResponse], error) {
	action := decisionAction(req.Msg.GetDecision())
	if err := h.Controller.SubmitDecision(ctx, req.Msg.GetSessionId(), int32(req.Msg.GetPhase()), action); err != nil {
		return nil, err
	}
	return &connect.Response[v1.SubmitDecisionResponse]{}, nil
}

// newSessionID returns a new session id, a UUID version 7 (time-ordered) value.
func newSessionID() string {
	return uuid.Must(uuid.NewV7()).String()
}

package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	connect "connectrpc.com/connect"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1/v1connect"
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
type Update = v1.Update

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
	s, ok := c.sessions[id]
	if !ok {
		return nil, errSessionNotFound
	}
	return s, nil
}

func (c *SessionController) GetUpdates(id string, send func(*v1.Update) error) error {
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
	s, err := c.GetSession(id)
	if err != nil {
		return err
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

// DemoControllerHandler implements the Connect service.
type DemoControllerHandler struct {
	Controller *SessionController
}

// NewDemoControllerHandler builds an http.Handler from the demo controller service.
func NewDemoControllerHandler(controller *SessionController) (string, http.Handler) {
	return v1connect.NewDemoControllerHandler(&DemoControllerHandler{Controller: controller})
}

// CreateSession creates a new session and opens a stream of updates. The
// first Update carries the session id with phase NOT_STARTED; the browser then
// sends the envoy-bound request carrying that session id and receives phase
// updates as the session resolves.
func (h *DemoControllerHandler) CreateSession(ctx context.Context, req *connect.Request[v1.CreateSessionRequest], stream *connect.ServerStream[v1.Update]) error {
	s, err := h.Controller.CreateSession()
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.Update{SessionId: s.ID, Phase: v1.Phase_NOT_STARTED}); err != nil {
		return err
	}
	return h.Controller.GetUpdates(s.ID, func(u *v1.Update) error {
		return stream.Send(u)
	})
}

// UpdateRequest submits a human decision (continue/cancel) for a session at a phase.
func (h *DemoControllerHandler) SubmitDecision(ctx context.Context, req *connect.Request[v1.UpdateRequest]) (*connect.Response[v1.UpdateResponse], error) {
	action := "continue"
	if req.Msg.GetDecision().GetCancel() != nil {
		action = "cancel"
	}
	if err := h.Controller.SubmitDecision(ctx, req.Msg.GetSessionId(), int32(req.Msg.GetPhase()), action); err != nil {
		return nil, err
	}
	return &connect.Response[v1.UpdateResponse]{}, nil
}

// Process carries the extproc<->controller channel as a bidi stream: the extproc
// initiates on request receipt and sends phase updates, the controller sends
// decisions back.
func (h *DemoControllerHandler) Process(ctx context.Context, stream *connect.BidiStream[v1.Update, v1.Update]) error {
	for {
		update, err := stream.Receive()
		if err != io.EOF {
			if err != nil {
				return err
			}
			break
		}
		if err := stream.Send(&v1.Update{SessionId: update.SessionId, Phase: update.Phase}); err != nil {
			return err
		}
	}
	return nil
}

// newSessionID returns a new session id.
func newSessionID() string {
	return "session"
}

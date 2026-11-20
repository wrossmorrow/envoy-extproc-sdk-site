package controller

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	v1connect "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1/controllerv1connect"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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
	ID            string
	Status        SessionStatus
	Phase         int32
	Pending       *pendingDecision
	LastSnapshot  *v1.HttpSnapshot
	CreatedAt     time.Time
	ExpiresAt     time.Time
	PhaseDeadline time.Time
	Updates       chan *Update

	streamCancel context.CancelFunc
	expireTimer  *time.Timer
}

type pendingDecision struct {
	Phase int32
	Ch    chan *pendingResolution
}

type pendingResolution struct {
	decision *v1.Decision
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

// Logical metrics for the demo controller's session and phase handling. The
// controller tracks how many sessions are open, how many phases are awaiting a
// human decision, and how many controller<->extproc process streams are open.
var (
	demoControllerSessionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "demo_controller_sessions_active",
		Help: "Current number of demo controller sessions open.",
	})
	demoControllerPhasesPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "demo_controller_phases_pending",
		Help: "Current number of demo controller phases awaiting a human decision.",
	})
	demoControllerProcessStreamsOpen = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "demo_controller_process_streams_open",
		Help: "Current number of open demo controller process streams.",
	})

	demoControllerMetrics = func() *prometheus.Registry {
		reg := prometheus.NewRegistry()
		reg.MustRegister(demoControllerSessionsActive, demoControllerPhasesPending, demoControllerProcessStreamsOpen)
		return reg
	}()
)

// MetricsRegistry returns the Prometheus registry backing the demo controller's
// logical metrics.
func MetricsRegistry() *prometheus.Registry {
	return demoControllerMetrics
}

func NewSessionController() *SessionController {
	return &SessionController{sessions: map[string]*Session{}}
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

func (c *SessionController) CreateSession() (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	demoControllerSessionsActive.Inc()
	s := &Session{
		ID:            newSessionID(),
		Status:        SessionStatusNew,
		Updates:       make(chan *Update, 16),
		CreatedAt:     time.Now(),
		ExpiresAt:     time.Now().Add(SessionTTL),
		PhaseDeadline: time.Now().Add(PhaseTimeout),
	}
	s.expireTimer = time.AfterFunc(SessionTTL, func() {
		c.removeSession(s.ID)
	})
	c.sessions[s.ID] = s
	return s, nil
}

// GetOrCreateSession returns an existing non-expired session when sessionID is
// non-empty, or creates a fresh session when sessionID is empty. Returns
// errSessionNotFound if sessionID is non-empty and unknown or expired.
func (c *SessionController) GetOrCreateSession(sessionID string) (*Session, bool, error) {
	if sessionID == "" {
		s, err := c.CreateSession()
		return s, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[sessionID]
	if !ok {
		return nil, false, errSessionNotFound
	}
	if time.Now().After(s.ExpiresAt) {
		c.removeSessionLocked(sessionID)
		return nil, false, errSessionNotFound
	}
	return s, true, nil
}

// initialUpdateFor builds the first CreateSessionResponse for a newly opened or
// resumed CreateSession stream, replaying the currently paused phase if the
// session is in flight.
func (c *SessionController) initialUpdateFor(id string) (*v1.CreateSessionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return nil, errSessionNotFound
	}
	if s.Status == SessionStatusInFlight {
		return &v1.CreateSessionResponse{
			SessionId: s.ID,
			Phase:     v1.Phase(s.Phase),
			Snapshot:  s.LastSnapshot,
			Deadline:  timestamppb.New(s.PhaseDeadline),
		}, nil
	}
	return &v1.CreateSessionResponse{
		SessionId: s.ID,
		Phase:     v1.Phase_PHASE_UNSPECIFIED,
		Deadline:  timestamppb.New(s.PhaseDeadline),
	}, nil
}

// GetUpdates streams phase updates for a session to a client with an open
// updates stream. A newer GetUpdates call for the same session supersedes the
// previous stream without deleting the session, so clients can reconnect across
// transport drops. The session is only removed by CancelSession or expiration.
func (c *SessionController) GetUpdates(ctx context.Context, id string, send func(*v1.CreateSessionResponse) error) error {
	s, err := c.GetSession(id)
	if err != nil {
		return err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.mu.Lock()
	if cur, ok := c.sessions[id]; !ok {
		c.mu.Unlock()
		return errSessionNotFound
	} else {
		if cur.streamCancel != nil {
			cur.streamCancel()
		}
		cur.streamCancel = cancel
	}
	c.mu.Unlock()

	expireTimer := time.NewTimer(time.Until(s.ExpiresAt))
	defer expireTimer.Stop()

	for {
		select {
		case u, ok := <-s.Updates:
			if !ok {
				return nil // stream closed
			}
			if err := send(u); err != nil {
				return err
			}
		case <-streamCtx.Done():
			return streamCtx.Err()
		case <-expireTimer.C:
			c.removeSession(id)
			return errSessionTimedOut
		}
	}
}

// CancelSession explicitly terminates an active session, unblocking any
// in-flight phase decision and closing its updates stream.
func (c *SessionController) CancelSession(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.sessions[id]; !ok {
		return errSessionNotFound
	}
	c.removeSessionLocked(id)
	return nil
}

// removeSession drops a session from the controller's session table.
func (c *SessionController) removeSession(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeSessionLocked(id)
}

func (c *SessionController) removeSessionLocked(id string) {
	s, ok := c.sessions[id]
	if !ok {
		return
	}
	delete(c.sessions, id)
	demoControllerSessionsActive.Dec()
	if s.expireTimer != nil {
		s.expireTimer.Stop()
	}
	if s.streamCancel != nil {
		s.streamCancel()
	}
	if s.Status == SessionStatusInFlight && s.Pending != nil && s.Pending.Ch != nil {
		select {
		case s.Pending.Ch <- &pendingResolution{decision: &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: http.StatusServiceUnavailable, Headers: map[string]string{}}}}}:
		default:
		}
		s.Status = SessionStatusResolved
	}
}

func (c *SessionController) SubmitDecision(ctx context.Context, id string, phase int32, decision *v1.Decision) error {
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
	s.Pending.Ch <- &pendingResolution{decision: sanitizeCancelDecision(decision)}
	s.Status = SessionStatusResolved
	return nil
}

// sanitizeCancelDecision validates and bounds a human-submitted cancel
// decision, falling back to a safe status and truncating the body when the
// submitted status is out of range or the body exceeds MaxCancelBodyBytes.
func sanitizeCancelDecision(decision *v1.Decision) *v1.Decision {
	if decision == nil || decision.GetCancel() == nil {
		return decision
	}
	cancel := decision.GetCancel()
	status := cancel.GetStatus()
	if status < http.StatusOK || status > 599 {
		status = http.StatusForbidden
	}
	body := cancel.GetBody()
	if int64(len(body)) > MaxCancelBodyBytes {
		body = body[:MaxCancelBodyBytes]
	}
	return &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: status, Headers: cancel.GetHeaders(), Body: body}}}
}

// PhaseTimeout is how long ResolveDecision waits for a human decision on a
// session phase before failing the phase closed with an immediate cancel.
var PhaseTimeout = 30 * time.Second

// SessionTTL is how long a session stays open for its updates stream before
// GetUpdates gives up on the browser-side stream.
var SessionTTL = 10 * time.Minute

// MaxCancelBodyBytes caps the body length accepted on human-submitted cancel
// decisions through the public SubmitDecision endpoint.
var MaxCancelBodyBytes int64 = 64 * 1024 // 64 KiB

// PhaseDeadline returns the phase decision deadline recorded for the session
// identified by id.
func (c *SessionController) PhaseDeadline(id string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return time.Time{}
	}
	return s.PhaseDeadline
}

// SessionInFlight reports whether the session identified by id currently has
// an exchange in flight (status InFlight).
func (c *SessionController) SessionInFlight(id string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return false, errSessionNotFound
	}
	return s.Status == SessionStatusInFlight, nil
}

// ResolveConflict answers a phase update that arrives for a session already in
// flight with a 409 conflict decision, leaving the in-flight exchange and its
// pending human decision untouched so the session remains usable.
func (c *SessionController) ResolveConflict(sessionID string, phase int32) (*v1.ProcessResponse, error) {
	c.mu.Lock()
	s, ok := c.sessions[sessionID]
	c.mu.Unlock()
	if !ok {
		return nil, errSessionNotFound
	}
	conflict := &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: http.StatusConflict, Headers: map[string]string{}}}}
	return decisionResponse(s, phase, conflict), nil
}

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
		demoControllerPhasesPending.Dec()
		return decisionResponse(s, phase, res.decision), nil
	case <-ctx.Done():
		demoControllerPhasesPending.Dec()
		return nil, ctx.Err()
	case <-time.After(time.Until(s.PhaseDeadline)):
		// Phase timed out: fail closed and mark the session resolved so a late
		// decision can no longer be accepted for the timed-out phase. Clear the
		// pending decision so it cannot be satisfied by a late update.
		c.mu.Lock()
		s.Status = SessionStatusResolved
		s.Pending = nil
		c.mu.Unlock()
		demoControllerPhasesPending.Dec()
		return decisionResponse(s, phase, nil), nil
	}
}

// SendUpdate sends an update on the session's CreateSession stream to clients
// with open streams for the session, and records the snapshot for reconnects.
func (c *SessionController) SendUpdate(id string, update *v1.CreateSessionResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sessions[id]
	if !ok {
		return errSessionNotFound
	}
	if update.GetSnapshot() != nil {
		s.LastSnapshot = update.GetSnapshot()
	}
	select {
	case s.Updates <- update:
		return nil
	default:
		return nil
	}
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
	demoControllerPhasesPending.Inc()
	s.Phase = phase
	s.PhaseDeadline = time.Now().Add(PhaseTimeout)
	if s.Pending == nil {
		s.Pending = &pendingDecision{Phase: phase}
	}
	if s.Pending.Ch == nil {
		s.Pending.Ch = make(chan *pendingResolution, 1)
	}
	return nil
}

// decisionResponse builds the ProcessResponse decision for a session phase
// from the human decision. On phase timeout (decision == nil) it fails closed
// with an immediate cancel decision.
func decisionResponse(s *Session, phase int32, decision *v1.Decision) *v1.ProcessResponse {
	resp := &v1.ProcessResponse{SessionId: s.ID, Phase: v1.Phase(phase)}
	switch {
	case decision != nil && decision.GetContinue() != nil:
		resp.Decision = decision
	case decision != nil && decision.GetCancel() != nil:
		resp.Decision = decision
	default:
		resp.Decision = &v1.Decision{Action: &v1.Decision_Cancel{Cancel: &v1.Cancel{Status: http.StatusGatewayTimeout, Headers: map[string]string{}}}}
	}
	return resp
}

// DemoControllerHandler implements the Connect service.
type DemoControllerHandler struct {
	Controller *SessionController
}

// NewDemoControllerHandler builds an http.Handler from the demo controller service.
func NewDemoControllerHandler(controller *SessionController) (string, http.Handler) {
	return v1connect.NewDemoControllerServiceHandler(&DemoControllerHandler{Controller: controller})
}

// CreateSession creates a new session (or resumes an existing one when
// session_id is set) and opens a stream of updates. The first Update carries the
// session id with phase PHASE_UNSPECIFIED (or the currently paused phase when
// resuming mid-flight); the browser then sends the envoy-bound request carrying
// that session id and receives phase updates as the session resolves.
func (h *DemoControllerHandler) CreateSession(ctx context.Context, req *connect.Request[v1.CreateSessionRequest], stream *connect.ServerStream[v1.CreateSessionResponse]) error {
	s, _, err := h.Controller.GetOrCreateSession(req.Msg.GetSessionId())
	if err != nil {
		if errors.Is(err, errSessionNotFound) {
			return connect.NewError(connect.CodeNotFound, err)
		}
		return err
	}
	initial, err := h.Controller.initialUpdateFor(s.ID)
	if err != nil {
		if errors.Is(err, errSessionNotFound) {
			return connect.NewError(connect.CodeNotFound, err)
		}
		return err
	}
	if err := stream.Send(initial); err != nil {
		return err
	}
	return h.Controller.GetUpdates(ctx, s.ID, func(u *v1.CreateSessionResponse) error {
		return stream.Send(u)
	})
}

// SubmitDecision submits a human decision for a session at a phase.
func (h *DemoControllerHandler) SubmitDecision(ctx context.Context, req *connect.Request[v1.SubmitDecisionRequest]) (*connect.Response[v1.SubmitDecisionResponse], error) {
	if err := h.Controller.SubmitDecision(ctx, req.Msg.GetSessionId(), int32(req.Msg.GetPhase()), req.Msg.GetDecision()); err != nil {
		return nil, err
	}
	return &connect.Response[v1.SubmitDecisionResponse]{}, nil
}

// CancelSession explicitly terminates an active session.
func (h *DemoControllerHandler) CancelSession(ctx context.Context, req *connect.Request[v1.CancelSessionRequest]) (*connect.Response[v1.CancelSessionResponse], error) {
	if err := h.Controller.CancelSession(req.Msg.GetSessionId()); err != nil {
		if errors.Is(err, errSessionNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, err
	}
	return &connect.Response[v1.CancelSessionResponse]{}, nil
}

// newSessionID returns a new session id, a UUID version 7 (time-ordered) value.
func newSessionID() string {
	return uuid.Must(uuid.NewV7()).String()
}

package controller

import (
	"context"
	"errors"
	"io"

	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// DemoControllerGRPCServer implements the DemoControllerService gRPC service. It
// carries the human-facing session flow (CreateSession / SubmitDecision).
type DemoControllerGRPCServer struct {
	Controller *SessionController

	// embed for forward-compatible interface satisfaction
	v1.UnimplementedDemoControllerServiceServer
}

// CreateSession opens a server-streaming RPC that sends session updates (or
// resumes an existing session when session_id is set).
func (s *DemoControllerGRPCServer) CreateSession(req *v1.CreateSessionRequest, stream grpc.ServerStreamingServer[v1.CreateSessionResponse]) error {
	sess, _, err := s.Controller.GetOrCreateSession(req.GetSessionId())
	if err != nil {
		if errors.Is(err, errSessionNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		return err
	}
	initial, err := s.Controller.initialUpdateFor(sess.ID)
	if err != nil {
		if errors.Is(err, errSessionNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		return err
	}
	if err := stream.Send(initial); err != nil {
		return err
	}
	return s.Controller.GetUpdates(stream.Context(), sess.ID, func(u *v1.CreateSessionResponse) error {
		return stream.Send(u)
	})
}

// SubmitDecision receives a human decision for a session phase and resolves
// the session's pending decision.
func (s *DemoControllerGRPCServer) SubmitDecision(ctx context.Context, req *v1.SubmitDecisionRequest) (*v1.SubmitDecisionResponse, error) {
	if err := s.Controller.SubmitDecision(ctx, req.GetSessionId(), int32(req.GetPhase()), req.GetDecision()); err != nil {
		return nil, err
	}
	return &v1.SubmitDecisionResponse{}, nil
}

// CancelSession explicitly terminates an active session.
func (s *DemoControllerGRPCServer) CancelSession(ctx context.Context, req *v1.CancelSessionRequest) (*v1.CancelSessionResponse, error) {
	if err := s.Controller.CancelSession(req.GetSessionId()); err != nil {
		if errors.Is(err, errSessionNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, err
	}
	return &v1.CancelSessionResponse{}, nil
}

// DemoProcessorGRPCServer implements the DemoProcessorService gRPC service. It is
// the internal controller<->extproc channel: the extproc initiates on request
// receipt and sends phase updates, the controller sends decisions back.
type DemoProcessorGRPCServer struct {
	Controller *SessionController

	// embed for forward-compatible interface satisfaction
	v1.UnimplementedDemoProcessorServiceServer
}

// Process carries the extproc<->controller channel as a bidi stream: the extproc
// initiates on request receipt and sends phase updates, the controller sends
// decisions back.
func (s *DemoProcessorGRPCServer) Process(stream grpc.BidiStreamingServer[v1.ProcessRequest, v1.ProcessResponse]) error {
	ctx := stream.Context()
	demoControllerProcessStreamsOpen.Inc()
	defer func() { demoControllerProcessStreamsOpen.Dec() }()
	for {
		update, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		// A phase update means the session is now in flight at that phase; await
		// the human decision (or phase timeout) and reply over the stream.
		phase := v1.Phase(update.Phase)
		inFlight, inErr := s.Controller.SessionInFlight(update.SessionId)
		if inErr == nil && inFlight {
			// The session already has an exchange in flight: reject the phase
			// update with a 409 conflict and resolve the session so the
			// in-flight exchange cannot accept further decisions.
			conflictResp, conflictErr := s.Controller.ResolveConflict(update.SessionId, int32(phase))
			if conflictErr != nil {
				return conflictErr
			}
			if err = stream.Send(conflictResp); err != nil {
				return err
			}
			return nil
		}
		if err = s.Controller.BeginPhase(update.SessionId, int32(phase)); err != nil {
			if errors.Is(err, errSessionNotFound) {
				return status.Error(codes.NotFound, err.Error())
			}
			return err
		}
		if err = s.Controller.SendUpdate(update.SessionId, &v1.CreateSessionResponse{
			SessionId: update.SessionId,
			Phase:     phase,
			Snapshot:  update.Snapshot,
			Deadline:  timestamppb.New(s.Controller.PhaseDeadline(update.SessionId)),
		}); err != nil {
			return err
		}
		decision, err := s.Controller.ResolveDecision(ctx, update.SessionId, int32(phase))
		if err != nil {
			return err
		}
		if err = stream.Send(decision); err != nil {
			return err
		}
	}
	return nil
}

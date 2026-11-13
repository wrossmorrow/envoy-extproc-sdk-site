package controller

import (
	"context"
	"io"

	v1 "github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go/controller/v1"
	grpc "google.golang.org/grpc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// DemoControllerGRPCServer implements the DemoControllerService gRPC service. It
// carries the human-facing session flow (CreateSession / SubmitDecision).
type DemoControllerGRPCServer struct {
	Controller *SessionController

	// embed for forward-compatible interface satisfaction
	v1.UnimplementedDemoControllerServiceServer
}

// CreateSession opens a server-streaming RPC that sends session updates. The
// first CreateSessionResponse carries the session id with phase PHASE_NOT_STARTED
// and the phase decision deadline.
func (s *DemoControllerGRPCServer) CreateSession(req *v1.CreateSessionRequest, stream grpc.ServerStreamingServer[v1.CreateSessionResponse]) error {
	sess, err := s.Controller.CreateSession()
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.CreateSessionResponse{
		SessionId: sess.ID,
		Phase:     v1.Phase_PHASE_NOT_STARTED,
		Deadline:  timestamppb.New(sess.PhaseDeadline),
	}); err != nil {
		return err
	}
	return s.Controller.GetUpdates(sess.ID, func(u *v1.CreateSessionResponse) error {
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
		phase := phaseFromExtproc(int32(update.Phase))
		if err = s.Controller.BeginPhase(update.SessionId, int32(phase)); err != nil {
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

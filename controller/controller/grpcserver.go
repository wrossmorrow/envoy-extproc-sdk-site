package controller

import (
	"context"
	v1 "github.com/wrossmorrow/envoy-exproc-sdk-controller/gen/go/proto/demo/controller/v1"
	grpc "google.golang.org/grpc"
	"io"
)

// DemoControllerGRPCServer implements the DemoControllerService gRPC service. It
// carries the human-facing session flow (CreateSession / SubmitDecision).
type DemoControllerGRPCServer struct {
	Controller *SessionController

	// embed for forward-compatible interface satisfaction
	v1.UnimplementedDemoControllerServiceServer
}

// CreateSession opens a server-streaming RPC that sends session updates. The
// first CreateSessionResponse carries the session id with phase PHASE_NOT_STARTED.
func (s *DemoControllerGRPCServer) CreateSession(req *v1.CreateSessionRequest, stream grpc.ServerStreamingServer[v1.CreateSessionResponse]) error {
	sess, err := s.Controller.CreateSession()
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.CreateSessionResponse{SessionId: sess.ID, Phase: v1.Phase_PHASE_NOT_STARTED}); err != nil {
		return err
	}
	return s.Controller.GetUpdates(sess.ID, func(u *v1.CreateSessionResponse) error {
		return stream.Send(u)
	})
}

// SubmitDecision receives a human decision for a session phase and resolves
// the session's pending decision.
func (s *DemoControllerGRPCServer) SubmitDecision(ctx context.Context, req *v1.SubmitDecisionRequest) (*v1.SubmitDecisionResponse, error) {
	action := "continue"
	if req.GetDecision().GetCancel() != nil {
		action = "cancel"
	}
	if err := s.Controller.SubmitDecision(ctx, req.GetSessionId(), int32(req.GetPhase()), action); err != nil {
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
	for {
		update, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		decision := &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}
		if err := stream.Send(&v1.ProcessResponse{
			SessionId: update.SessionId,
			Phase:     update.Phase,
			Decision:  decision,
		}); err != nil {
			return err
		}
	}
	return nil
}

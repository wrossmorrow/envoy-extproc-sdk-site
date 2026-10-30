package controller

import (
	"context"
	v1 "github.com/wrossmorrow/envoy-extproc-sdk-controller/demo/controller/v1"
	grpc "google.golang.org/grpc"
	"io"
)

// DemoControllerGRPCServer implements the DemoController gRPC service. It
// carries the human-facing session flow (CreateSession / SubmitDecision).
type DemoControllerGRPCServer struct {
	Controller *SessionController

	// embed for forward-compatible interface satisfaction
	v1.UnimplementedDemoControllerServer
}

// CreateSession opens a server-streaming RPC that sends session updates. The
// first Update carries the session id with phase NOT_STARTED.
func (s *DemoControllerGRPCServer) CreateSession(req *v1.CreateSessionRequest, stream grpc.ServerStreamingServer[v1.Update]) error {
	sess, err := s.Controller.CreateSession()
	if err != nil {
		return err
	}
	if err := stream.Send(&v1.Update{SessionId: sess.ID, Phase: v1.Phase_NOT_STARTED}); err != nil {
		return err
	}
	return s.Controller.GetUpdates(sess.ID, func(u *v1.Update) error {
		return stream.Send(u)
	})
}

// SubmitDecision receives a human decision for a session phase and resolves
// the session's pending decision.
func (s *DemoControllerGRPCServer) SubmitDecision(ctx context.Context, req *v1.UpdateRequest) (*v1.UpdateResponse, error) {
	action := "continue"
	if req.GetDecision().GetCancel() != nil {
		action = "cancel"
	}
	if err := s.Controller.SubmitDecision(ctx, req.GetSessionId(), int32(req.GetPhase()), action); err != nil {
		return nil, err
	}
	return &v1.UpdateResponse{}, nil
}

// DemoProcessorGRPCServer implements the DemoProcessor gRPC service. It is
// the internal controller<->extproc channel: the extproc initiates on request
// receipt and sends phase updates, the controller sends decisions back.
type DemoProcessorGRPCServer struct {
	Controller *SessionController

	// embed for forward-compatible interface satisfaction
	v1.UnimplementedDemoProcessorServer
}

// Process carries the extproc<->controller channel as a bidi stream: the extproc
// initiates on request receipt and sends phase updates, the controller sends
// decisions back.
func (s *DemoProcessorGRPCServer) Process(stream grpc.BidiStreamingServer[v1.Update, v1.UpdateRequest]) error {
	for {
		update, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		decision := &v1.Decision{Action: &v1.Decision_Continue{Continue: &v1.Continue{Headers: map[string]string{}}}}
		if err := stream.Send(&v1.UpdateRequest{
			SessionId: update.SessionId,
			Phase:     update.Phase,
			Decision:  decision,
		}); err != nil {
			return err
		}
	}
	return nil
}

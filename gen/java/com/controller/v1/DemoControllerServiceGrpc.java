package com.controller.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * DemoControllerService manages interactive extproc sessions for the demo site.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.68.1)",
    comments = "Source: controller/v1/controller.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class DemoControllerServiceGrpc {

  private DemoControllerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "controller.v1.DemoControllerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.controller.v1.CreateSessionRequest,
      com.controller.v1.CreateSessionResponse> getCreateSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateSession",
      requestType = com.controller.v1.CreateSessionRequest.class,
      responseType = com.controller.v1.CreateSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.controller.v1.CreateSessionRequest,
      com.controller.v1.CreateSessionResponse> getCreateSessionMethod() {
    io.grpc.MethodDescriptor<com.controller.v1.CreateSessionRequest, com.controller.v1.CreateSessionResponse> getCreateSessionMethod;
    if ((getCreateSessionMethod = DemoControllerServiceGrpc.getCreateSessionMethod) == null) {
      synchronized (DemoControllerServiceGrpc.class) {
        if ((getCreateSessionMethod = DemoControllerServiceGrpc.getCreateSessionMethod) == null) {
          DemoControllerServiceGrpc.getCreateSessionMethod = getCreateSessionMethod =
              io.grpc.MethodDescriptor.<com.controller.v1.CreateSessionRequest, com.controller.v1.CreateSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.CreateSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.CreateSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DemoControllerServiceMethodDescriptorSupplier("CreateSession"))
              .build();
        }
      }
    }
    return getCreateSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.controller.v1.SubmitDecisionRequest,
      com.controller.v1.SubmitDecisionResponse> getSubmitDecisionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubmitDecision",
      requestType = com.controller.v1.SubmitDecisionRequest.class,
      responseType = com.controller.v1.SubmitDecisionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.controller.v1.SubmitDecisionRequest,
      com.controller.v1.SubmitDecisionResponse> getSubmitDecisionMethod() {
    io.grpc.MethodDescriptor<com.controller.v1.SubmitDecisionRequest, com.controller.v1.SubmitDecisionResponse> getSubmitDecisionMethod;
    if ((getSubmitDecisionMethod = DemoControllerServiceGrpc.getSubmitDecisionMethod) == null) {
      synchronized (DemoControllerServiceGrpc.class) {
        if ((getSubmitDecisionMethod = DemoControllerServiceGrpc.getSubmitDecisionMethod) == null) {
          DemoControllerServiceGrpc.getSubmitDecisionMethod = getSubmitDecisionMethod =
              io.grpc.MethodDescriptor.<com.controller.v1.SubmitDecisionRequest, com.controller.v1.SubmitDecisionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubmitDecision"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.SubmitDecisionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.SubmitDecisionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DemoControllerServiceMethodDescriptorSupplier("SubmitDecision"))
              .build();
        }
      }
    }
    return getSubmitDecisionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.controller.v1.CancelSessionRequest,
      com.controller.v1.CancelSessionResponse> getCancelSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CancelSession",
      requestType = com.controller.v1.CancelSessionRequest.class,
      responseType = com.controller.v1.CancelSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.controller.v1.CancelSessionRequest,
      com.controller.v1.CancelSessionResponse> getCancelSessionMethod() {
    io.grpc.MethodDescriptor<com.controller.v1.CancelSessionRequest, com.controller.v1.CancelSessionResponse> getCancelSessionMethod;
    if ((getCancelSessionMethod = DemoControllerServiceGrpc.getCancelSessionMethod) == null) {
      synchronized (DemoControllerServiceGrpc.class) {
        if ((getCancelSessionMethod = DemoControllerServiceGrpc.getCancelSessionMethod) == null) {
          DemoControllerServiceGrpc.getCancelSessionMethod = getCancelSessionMethod =
              io.grpc.MethodDescriptor.<com.controller.v1.CancelSessionRequest, com.controller.v1.CancelSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CancelSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.CancelSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.CancelSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DemoControllerServiceMethodDescriptorSupplier("CancelSession"))
              .build();
        }
      }
    }
    return getCancelSessionMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DemoControllerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DemoControllerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DemoControllerServiceStub>() {
        @java.lang.Override
        public DemoControllerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DemoControllerServiceStub(channel, callOptions);
        }
      };
    return DemoControllerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DemoControllerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DemoControllerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DemoControllerServiceBlockingStub>() {
        @java.lang.Override
        public DemoControllerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DemoControllerServiceBlockingStub(channel, callOptions);
        }
      };
    return DemoControllerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DemoControllerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DemoControllerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DemoControllerServiceFutureStub>() {
        @java.lang.Override
        public DemoControllerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DemoControllerServiceFutureStub(channel, callOptions);
        }
      };
    return DemoControllerServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * DemoControllerService manages interactive extproc sessions for the demo site.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * CreateSession creates a new session (or resumes an existing one when
     * session_id is set and still active) and opens a stream of updates. The
     * first Update carries the session id with phase PHASE_UNSPECIFIED (or the
     * currently paused phase/snapshot/deadline when resuming mid-flight).
     * </pre>
     */
    default void createSession(com.controller.v1.CreateSessionRequest request,
        io.grpc.stub.StreamObserver<com.controller.v1.CreateSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateSessionMethod(), responseObserver);
    }

    /**
     * <pre>
     * SubmitDecision submits a human decision (continue/cancel) for a session at a phase.
     * </pre>
     */
    default void submitDecision(com.controller.v1.SubmitDecisionRequest request,
        io.grpc.stub.StreamObserver<com.controller.v1.SubmitDecisionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitDecisionMethod(), responseObserver);
    }

    /**
     * <pre>
     * CancelSession explicitly terminates an active session, failing closed any
     * in-flight phase and releasing session state.
     * </pre>
     */
    default void cancelSession(com.controller.v1.CancelSessionRequest request,
        io.grpc.stub.StreamObserver<com.controller.v1.CancelSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCancelSessionMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DemoControllerService.
   * <pre>
   * DemoControllerService manages interactive extproc sessions for the demo site.
   * </pre>
   */
  public static abstract class DemoControllerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DemoControllerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DemoControllerService.
   * <pre>
   * DemoControllerService manages interactive extproc sessions for the demo site.
   * </pre>
   */
  public static final class DemoControllerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<DemoControllerServiceStub> {
    private DemoControllerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DemoControllerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DemoControllerServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * CreateSession creates a new session (or resumes an existing one when
     * session_id is set and still active) and opens a stream of updates. The
     * first Update carries the session id with phase PHASE_UNSPECIFIED (or the
     * currently paused phase/snapshot/deadline when resuming mid-flight).
     * </pre>
     */
    public void createSession(com.controller.v1.CreateSessionRequest request,
        io.grpc.stub.StreamObserver<com.controller.v1.CreateSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getCreateSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * SubmitDecision submits a human decision (continue/cancel) for a session at a phase.
     * </pre>
     */
    public void submitDecision(com.controller.v1.SubmitDecisionRequest request,
        io.grpc.stub.StreamObserver<com.controller.v1.SubmitDecisionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitDecisionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * CancelSession explicitly terminates an active session, failing closed any
     * in-flight phase and releasing session state.
     * </pre>
     */
    public void cancelSession(com.controller.v1.CancelSessionRequest request,
        io.grpc.stub.StreamObserver<com.controller.v1.CancelSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCancelSessionMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DemoControllerService.
   * <pre>
   * DemoControllerService manages interactive extproc sessions for the demo site.
   * </pre>
   */
  public static final class DemoControllerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DemoControllerServiceBlockingStub> {
    private DemoControllerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DemoControllerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DemoControllerServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * CreateSession creates a new session (or resumes an existing one when
     * session_id is set and still active) and opens a stream of updates. The
     * first Update carries the session id with phase PHASE_UNSPECIFIED (or the
     * currently paused phase/snapshot/deadline when resuming mid-flight).
     * </pre>
     */
    public java.util.Iterator<com.controller.v1.CreateSessionResponse> createSession(
        com.controller.v1.CreateSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getCreateSessionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * SubmitDecision submits a human decision (continue/cancel) for a session at a phase.
     * </pre>
     */
    public com.controller.v1.SubmitDecisionResponse submitDecision(com.controller.v1.SubmitDecisionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitDecisionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * CancelSession explicitly terminates an active session, failing closed any
     * in-flight phase and releasing session state.
     * </pre>
     */
    public com.controller.v1.CancelSessionResponse cancelSession(com.controller.v1.CancelSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCancelSessionMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DemoControllerService.
   * <pre>
   * DemoControllerService manages interactive extproc sessions for the demo site.
   * </pre>
   */
  public static final class DemoControllerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<DemoControllerServiceFutureStub> {
    private DemoControllerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DemoControllerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DemoControllerServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * SubmitDecision submits a human decision (continue/cancel) for a session at a phase.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.controller.v1.SubmitDecisionResponse> submitDecision(
        com.controller.v1.SubmitDecisionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitDecisionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * CancelSession explicitly terminates an active session, failing closed any
     * in-flight phase and releasing session state.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.controller.v1.CancelSessionResponse> cancelSession(
        com.controller.v1.CancelSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCancelSessionMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_SESSION = 0;
  private static final int METHODID_SUBMIT_DECISION = 1;
  private static final int METHODID_CANCEL_SESSION = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_SESSION:
          serviceImpl.createSession((com.controller.v1.CreateSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.controller.v1.CreateSessionResponse>) responseObserver);
          break;
        case METHODID_SUBMIT_DECISION:
          serviceImpl.submitDecision((com.controller.v1.SubmitDecisionRequest) request,
              (io.grpc.stub.StreamObserver<com.controller.v1.SubmitDecisionResponse>) responseObserver);
          break;
        case METHODID_CANCEL_SESSION:
          serviceImpl.cancelSession((com.controller.v1.CancelSessionRequest) request,
              (io.grpc.stub.StreamObserver<com.controller.v1.CancelSessionResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getCreateSessionMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.controller.v1.CreateSessionRequest,
              com.controller.v1.CreateSessionResponse>(
                service, METHODID_CREATE_SESSION)))
        .addMethod(
          getSubmitDecisionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.controller.v1.SubmitDecisionRequest,
              com.controller.v1.SubmitDecisionResponse>(
                service, METHODID_SUBMIT_DECISION)))
        .addMethod(
          getCancelSessionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.controller.v1.CancelSessionRequest,
              com.controller.v1.CancelSessionResponse>(
                service, METHODID_CANCEL_SESSION)))
        .build();
  }

  private static abstract class DemoControllerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DemoControllerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.controller.v1.ControllerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DemoControllerService");
    }
  }

  private static final class DemoControllerServiceFileDescriptorSupplier
      extends DemoControllerServiceBaseDescriptorSupplier {
    DemoControllerServiceFileDescriptorSupplier() {}
  }

  private static final class DemoControllerServiceMethodDescriptorSupplier
      extends DemoControllerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DemoControllerServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (DemoControllerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DemoControllerServiceFileDescriptorSupplier())
              .addMethod(getCreateSessionMethod())
              .addMethod(getSubmitDecisionMethod())
              .addMethod(getCancelSessionMethod())
              .build();
        }
      }
    }
    return result;
  }
}

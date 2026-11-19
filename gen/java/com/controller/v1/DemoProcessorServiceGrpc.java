package com.controller.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * DemoProcessorService is the internal controller&lt;-&gt;extproc channel service. It is
 * not part of the human-facing surface; it exists for the demo's extproc to
 * exchange phase updates and decisions with the controller.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.68.1)",
    comments = "Source: controller/v1/controller.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class DemoProcessorServiceGrpc {

  private DemoProcessorServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "controller.v1.DemoProcessorService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.controller.v1.ProcessRequest,
      com.controller.v1.ProcessResponse> getProcessMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Process",
      requestType = com.controller.v1.ProcessRequest.class,
      responseType = com.controller.v1.ProcessResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<com.controller.v1.ProcessRequest,
      com.controller.v1.ProcessResponse> getProcessMethod() {
    io.grpc.MethodDescriptor<com.controller.v1.ProcessRequest, com.controller.v1.ProcessResponse> getProcessMethod;
    if ((getProcessMethod = DemoProcessorServiceGrpc.getProcessMethod) == null) {
      synchronized (DemoProcessorServiceGrpc.class) {
        if ((getProcessMethod = DemoProcessorServiceGrpc.getProcessMethod) == null) {
          DemoProcessorServiceGrpc.getProcessMethod = getProcessMethod =
              io.grpc.MethodDescriptor.<com.controller.v1.ProcessRequest, com.controller.v1.ProcessResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Process"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.ProcessRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.controller.v1.ProcessResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DemoProcessorServiceMethodDescriptorSupplier("Process"))
              .build();
        }
      }
    }
    return getProcessMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DemoProcessorServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DemoProcessorServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DemoProcessorServiceStub>() {
        @java.lang.Override
        public DemoProcessorServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DemoProcessorServiceStub(channel, callOptions);
        }
      };
    return DemoProcessorServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DemoProcessorServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DemoProcessorServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DemoProcessorServiceBlockingStub>() {
        @java.lang.Override
        public DemoProcessorServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DemoProcessorServiceBlockingStub(channel, callOptions);
        }
      };
    return DemoProcessorServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DemoProcessorServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DemoProcessorServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DemoProcessorServiceFutureStub>() {
        @java.lang.Override
        public DemoProcessorServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DemoProcessorServiceFutureStub(channel, callOptions);
        }
      };
    return DemoProcessorServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * DemoProcessorService is the internal controller&lt;-&gt;extproc channel service. It is
   * not part of the human-facing surface; it exists for the demo's extproc to
   * exchange phase updates and decisions with the controller.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Process carries the extproc&lt;-&gt;controller channel as a bidi stream: the
     * extproc initiates on request receipt and sends phase updates, the controller
     * sends decisions back.
     * </pre>
     */
    default io.grpc.stub.StreamObserver<com.controller.v1.ProcessRequest> process(
        io.grpc.stub.StreamObserver<com.controller.v1.ProcessResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getProcessMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DemoProcessorService.
   * <pre>
   * DemoProcessorService is the internal controller&lt;-&gt;extproc channel service. It is
   * not part of the human-facing surface; it exists for the demo's extproc to
   * exchange phase updates and decisions with the controller.
   * </pre>
   */
  public static abstract class DemoProcessorServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DemoProcessorServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DemoProcessorService.
   * <pre>
   * DemoProcessorService is the internal controller&lt;-&gt;extproc channel service. It is
   * not part of the human-facing surface; it exists for the demo's extproc to
   * exchange phase updates and decisions with the controller.
   * </pre>
   */
  public static final class DemoProcessorServiceStub
      extends io.grpc.stub.AbstractAsyncStub<DemoProcessorServiceStub> {
    private DemoProcessorServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DemoProcessorServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DemoProcessorServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Process carries the extproc&lt;-&gt;controller channel as a bidi stream: the
     * extproc initiates on request receipt and sends phase updates, the controller
     * sends decisions back.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.controller.v1.ProcessRequest> process(
        io.grpc.stub.StreamObserver<com.controller.v1.ProcessResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getProcessMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DemoProcessorService.
   * <pre>
   * DemoProcessorService is the internal controller&lt;-&gt;extproc channel service. It is
   * not part of the human-facing surface; it exists for the demo's extproc to
   * exchange phase updates and decisions with the controller.
   * </pre>
   */
  public static final class DemoProcessorServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DemoProcessorServiceBlockingStub> {
    private DemoProcessorServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DemoProcessorServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DemoProcessorServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DemoProcessorService.
   * <pre>
   * DemoProcessorService is the internal controller&lt;-&gt;extproc channel service. It is
   * not part of the human-facing surface; it exists for the demo's extproc to
   * exchange phase updates and decisions with the controller.
   * </pre>
   */
  public static final class DemoProcessorServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<DemoProcessorServiceFutureStub> {
    private DemoProcessorServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DemoProcessorServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DemoProcessorServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_PROCESS = 0;

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
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PROCESS:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.process(
              (io.grpc.stub.StreamObserver<com.controller.v1.ProcessResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getProcessMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              com.controller.v1.ProcessRequest,
              com.controller.v1.ProcessResponse>(
                service, METHODID_PROCESS)))
        .build();
  }

  private static abstract class DemoProcessorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DemoProcessorServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.controller.v1.ControllerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DemoProcessorService");
    }
  }

  private static final class DemoProcessorServiceFileDescriptorSupplier
      extends DemoProcessorServiceBaseDescriptorSupplier {
    DemoProcessorServiceFileDescriptorSupplier() {}
  }

  private static final class DemoProcessorServiceMethodDescriptorSupplier
      extends DemoProcessorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DemoProcessorServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (DemoProcessorServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DemoProcessorServiceFileDescriptorSupplier())
              .addMethod(getProcessMethod())
              .build();
        }
      }
    }
    return result;
  }
}

package com.github.wrossmorrow.extproc.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.buf.gen.envoy.config.core.v3.HeaderMap;
import build.buf.gen.envoy.config.core.v3.HeaderValue;
import build.buf.gen.envoy.config.core.v3.HeaderValueOption;
import build.buf.gen.envoy.service.ext_proc.v3.CommonResponse.ResponseStatus;
import build.buf.gen.envoy.service.ext_proc.v3.ExternalProcessorGrpc;
import build.buf.gen.envoy.service.ext_proc.v3.HeaderMutation;
import build.buf.gen.envoy.service.ext_proc.v3.HttpHeaders;
import build.buf.gen.envoy.service.ext_proc.v3.ImmediateResponse;
import build.buf.gen.envoy.service.ext_proc.v3.ProcessingRequest;
import build.buf.gen.envoy.service.ext_proc.v3.ProcessingResponse;
import com.controller.v1.Phase;
import com.github.wrossmorrow.extproc.ExternalProcessorServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DemoProcessorExchangeTest {

  private static final AtomicInteger controllerDecisionsReceived = new AtomicInteger();
  private static volatile boolean controllerResponds = true;
  private static volatile com.controller.v1.Decision forcedDecision;
  private static final Queue<ProcessingResponse> exchangeResponse = new ConcurrentLinkedQueue<>();
  private static final AtomicReference<Throwable> exchangeError = new AtomicReference<>();

  private static ManagedChannel channel;
  private static ExternalProcessorGrpc.ExternalProcessorStub extProcStub;
  private static Server controllerServer;
  private static DemoRequestProcessor processor;
  private static int demoPort;

  @BeforeAll
  static void setUp() throws Exception {
    int controllerPort = freePort();
    controllerServer =
        ServerBuilder.forPort(controllerPort)
            .addService(
                new com.controller.v1.DemoProcessorServiceGrpc.DemoProcessorServiceImplBase() {
                  @Override
                  public StreamObserver<com.controller.v1.ProcessRequest> process(
                      StreamObserver<com.controller.v1.ProcessResponse> responseObserver) {
                    return new StreamObserver<com.controller.v1.ProcessRequest>() {
                      @Override
                      public void onNext(com.controller.v1.ProcessRequest request) {
                        controllerDecisionsReceived.incrementAndGet();
                        Phase phase = request.getPhase();
                        if (forcedDecision != null) {
                          com.controller.v1.ProcessResponse forcedResponse =
                              com.controller.v1.ProcessResponse.newBuilder()
                                  .setSessionId(request.getSessionId())
                                  .setPhase(phase)
                                  .setDecision(forcedDecision)
                                  .build();
                          responseObserver.onNext(forcedResponse);
                          responseObserver.onCompleted();
                          return;
                        }
                        if (!controllerResponds) {
                          // never answer; the demo should time out and fail closed
                          return;
                        }
                        boolean finalPhase =
                            phase == Phase.PHASE_RESPONSE_HEADERS
                                || phase == Phase.PHASE_RESPONSE_BODY
                                || phase == Phase.PHASE_RESPONSE_TRAILERS;
                        com.controller.v1.ProcessResponse decision =
                            com.controller.v1.ProcessResponse.newBuilder()
                                .setSessionId(request.getSessionId())
                                .setPhase(phase)
                                .setDecision(
                                    com.controller.v1.Decision.newBuilder()
                                        .setContinue(
                                            com.controller.v1.Continue.newBuilder().build())
                                        .build())
                                .build();
                        responseObserver.onNext(decision);
                        // The demo retires the controller phase stream itself once the exchange is
                        // resolved (e.g. when a response phase signaled end-of-stream).
                      }

                      @Override
                      public void onError(Throwable t) {
                        // nothing to do; the exchange is finalized by the decision above
                      }

                      @Override
                      public void onCompleted() {
                        // nothing to do; the decision above finalizes the exchange
                      }
                    };
                  }
                })
            .build()
            .start();

    processor = new DemoRequestProcessor("127.0.0.1:" + controllerPort);
    processor.exchangeTimeoutSeconds = 3L;
    demoPort = freePort();
    ExternalProcessorServer server = new ExternalProcessorServer().builder(demoPort);
    server.setMetricsPort(freePort());
    server.addRequestProcessor(processor);
    server.start();

    channel = ManagedChannelBuilder.forAddress("127.0.0.1", demoPort).usePlaintext().build();
    extProcStub = ExternalProcessorGrpc.newStub(channel);
  }

  @org.junit.jupiter.api.BeforeEach
  void resetExchangeState() {
    forcedDecision = null;
    controllerResponds = true;
    exchangeError.set(null);
    exchangeResponse.clear();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (controllerServer != null) {
      controllerServer.shutdown();
      controllerServer.awaitTermination(10, TimeUnit.SECONDS);
    }
    if (channel != null) {
      channel.shutdownNow();
    }
  }

  @Test
  void demoContinuesBodylessGetAndMarksProcessed() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-1");
    headers.put(":method", "GET");
    headers.put(":path", "/health");
    ProcessingRequest request =
        envoyRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true);

    StreamObserver<ProcessingResponse> responseObserver =
        new StreamObserver<ProcessingResponse>() {
          @Override
          public void onNext(ProcessingResponse value) {
            exchangeResponse.add(value);
          }

          @Override
          public void onError(Throwable t) {
            exchangeError.set(t);
          }

          @Override
          public void onCompleted() {
            // the exchange is finalized by the decision that was applied
          }
        };
    StreamObserver<ProcessingRequest> sender = extProcStub.process(responseObserver);
    sender.onNext(request);
    ProcessingResponse response = awaitExchangeResponse(10);

    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, response.getRequestHeaders().getResponse().getStatus());
    HeaderMutation mutation = response.getRequestHeaders().getResponse().getHeaderMutation();
    assertTrue(hasProcessedHeader(mutation), "expected the demo to set x-demo-processed");
  }

  @Test
  void demoAppliesCancelImmediately() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-2");
    headers.put(":method", "GET");
    headers.put(":path", "/health");
    ProcessingRequest request =
        envoyRequest(ProcessingRequest.RequestCase.RESPONSE_HEADERS, headers, true);
    forcedDecision =
        com.controller.v1.Decision.newBuilder()
            .setCancel(
                com.controller.v1.Cancel.newBuilder()
                    .setStatus(503)
                    .setBody("upstream unavailable")
                    .build())
            .build();

    StreamObserver<ProcessingResponse> responseObserver =
        new StreamObserver<ProcessingResponse>() {
          @Override
          public void onNext(ProcessingResponse value) {
            exchangeResponse.add(value);
          }

          @Override
          public void onError(Throwable t) {
            exchangeError.set(t);
          }

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ProcessingRequest> sender = extProcStub.process(responseObserver);
    sender.onNext(request);
    ProcessingResponse response = awaitExchangeResponse(10);

    assertEquals(ProcessingResponse.ResponseCase.IMMEDIATE_RESPONSE, response.getResponseCase());
    ImmediateResponse immediate = response.getImmediateResponse();
    assertEquals(503, immediate.getStatus().getCode().getNumber());
  }

  @Test
  void demoFailsClosedOnPhaseTimeout() throws Exception {
    processor.exchangeTimeoutSeconds = 2L;
    controllerResponds = false;
    try {
      Map<String, String> headers = new HashMap<>();
      headers.put("x-demo-session-id", "sess-3");
      headers.put(":method", "GET");
      headers.put(":path", "/health");
      ProcessingRequest request =
          envoyRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true);
      StreamObserver<ProcessingResponse> responseObserver =
          new StreamObserver<ProcessingResponse>() {
            @Override
            public void onNext(ProcessingResponse value) {
              exchangeResponse.add(value);
            }

            @Override
            public void onError(Throwable t) {
              exchangeError.set(t);
            }

            @Override
            public void onCompleted() {}
          };
      StreamObserver<ProcessingRequest> sender = extProcStub.process(responseObserver);
      sender.onNext(request);
      assertThrows(RuntimeException.class, () -> awaitExchangeResponse(10));
    } finally {
      controllerResponds = true;
      forcedDecision = null;
      exchangeError.set(null);
      exchangeResponse.clear();
      processor.exchangeTimeoutSeconds = 3L;
    }
  }

  @Test
  void demoContinuesAcrossPhasesOverSharedControllerStream() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-4");
    headers.put(":method", "GET");
    headers.put(":path", "/health");
    StreamObserver<ProcessingResponse> responseObserver =
        new StreamObserver<ProcessingResponse>() {
          @Override
          public void onNext(ProcessingResponse value) {
            exchangeResponse.add(value);
          }

          @Override
          public void onError(Throwable t) {
            exchangeError.set(t);
          }

          @Override
          public void onCompleted() {}
        };
    StreamObserver<ProcessingRequest> sender = extProcStub.process(responseObserver);
    sender.onNext(envoyRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, false));
    ProcessingResponse first = awaitExchangeResponse(10);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, first.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, first.getRequestHeaders().getResponse().getStatus());

    sender.onNext(envoyRequest(ProcessingRequest.RequestCase.RESPONSE_HEADERS, headers, true));
    ProcessingResponse second = awaitExchangeResponse(10);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_HEADERS, second.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, second.getResponseHeaders().getResponse().getStatus());
  }

  // ------------------------------------------------------------------
  // Test helpers
  // ------------------------------------------------------------------

  private static int freePort() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  private static ProcessingRequest envoyRequest(
      ProcessingRequest.RequestCase phase, Map<String, String> headers, boolean endOfStream) {
    HttpHeaders.Builder builder = HttpHeaders.newBuilder();
    HeaderMap.Builder headerMapBuilder = HeaderMap.newBuilder();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      HeaderValue headerValue =
          HeaderValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build();
      headerMapBuilder.addHeaders(headerValue);
    }
    HeaderMap headerMap = headerMapBuilder.build();
    builder.setHeaders(headerMap);
    builder.setEndOfStream(endOfStream);
    ProcessingRequest.Builder builder2 = ProcessingRequest.newBuilder();
    switch (phase) {
      case REQUEST_HEADERS:
        builder2.setRequestHeaders(builder.build());
        break;
      case RESPONSE_HEADERS:
        builder2.setResponseHeaders(builder.build());
        break;
      default:
        throw new IllegalArgumentException("unsupported phase for envoy test request: " + phase);
    }
    return builder2.build();
  }

  private static ProcessingResponse awaitExchangeResponse(long timeoutSeconds) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    while (exchangeResponse.isEmpty()) {
      if (exchangeError.get() != null) {
        Throwable error = exchangeError.get();
        exchangeError.set(null);
        throw new RuntimeException("demo extproc stream errored", error);
      }
      if (System.nanoTime() > deadline) {
        throw new TimeoutException("timed out waiting for demo extproc response");
      }
      TimeUnit.MILLISECONDS.sleep(5);
    }
    ProcessingResponse response = exchangeResponse.poll();
    exchangeResponse.clear();
    exchangeError.set(null);
    return response;
  }

  private static boolean hasProcessedHeader(HeaderMutation mutation) {
    if (mutation == null || mutation.getSetHeadersList().isEmpty()) {
      return false;
    }
    for (int i = 0; i < mutation.getSetHeadersCount(); i++) {
      HeaderValueOption option = mutation.getSetHeaders(i);
      if ("x-demo-processed".equals(option.getHeader().getKey())) {
        return "true".equals(option.getHeader().getValue());
      }
    }
    return false;
  }
}

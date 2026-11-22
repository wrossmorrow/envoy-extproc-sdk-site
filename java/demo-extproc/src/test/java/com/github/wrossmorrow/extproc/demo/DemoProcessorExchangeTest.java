package com.github.wrossmorrow.extproc.demo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.buf.gen.envoy.config.core.v3.HeaderMap;
import build.buf.gen.envoy.config.core.v3.HeaderValue;
import build.buf.gen.envoy.config.core.v3.HeaderValueOption;
import build.buf.gen.envoy.service.ext_proc.v3.CommonResponse.ResponseStatus;
import build.buf.gen.envoy.service.ext_proc.v3.ExternalProcessorGrpc;
import build.buf.gen.envoy.service.ext_proc.v3.HeaderMutation;
import build.buf.gen.envoy.service.ext_proc.v3.HttpBody;
import build.buf.gen.envoy.service.ext_proc.v3.HttpHeaders;
import build.buf.gen.envoy.service.ext_proc.v3.ImmediateResponse;
import build.buf.gen.envoy.service.ext_proc.v3.ProcessingRequest;
import build.buf.gen.envoy.service.ext_proc.v3.ProcessingResponse;
import com.controller.v1.Cancel;
import com.controller.v1.Continue;
import com.controller.v1.Decision;
import com.controller.v1.DemoProcessorServiceGrpc;
import com.controller.v1.Phase;
import com.controller.v1.ProcessRequest;
import com.controller.v1.ProcessResponse;
import com.github.wrossmorrow.extproc.ExternalProcessorServer;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DemoProcessorExchangeTest {

  private static final AtomicInteger controllerDecisionsReceived = new AtomicInteger();
  private static final AtomicInteger controllerStreamsOpened = new AtomicInteger();
  private static final BlockingQueue<Boolean> controllerStreamCompletions =
      new LinkedBlockingQueue<>();
  private static final BlockingQueue<Throwable> controllerStreamErrors =
      new LinkedBlockingQueue<>();

  private static volatile boolean controllerResponds = true;
  private static volatile Decision forcedDecision;
  private static volatile Function<ProcessRequest, Decision> decisionProvider;

  private static ManagedChannel channel;
  private static ExternalProcessorGrpc.ExternalProcessorStub extProcStub;
  private static Server controllerServer;
  private static ExternalProcessorServer extProcServer;
  private static DemoRequestProcessor processor;
  private static int demoPort;

  @BeforeAll
  static void setUp() throws Exception {
    int controllerPort = freePort();
    controllerServer =
        ServerBuilder.forPort(controllerPort)
            .addService(
                new DemoProcessorServiceGrpc.DemoProcessorServiceImplBase() {
                  @Override
                  public StreamObserver<ProcessRequest> process(
                      StreamObserver<ProcessResponse> responseObserver) {
                    controllerStreamsOpened.incrementAndGet();
                    return new StreamObserver<ProcessRequest>() {
                      @Override
                      public void onNext(ProcessRequest request) {
                        controllerDecisionsReceived.incrementAndGet();
                        if (!controllerResponds) {
                          // Never answer; the demo should time out, abort the controller call, and
                          // fail closed.
                          return;
                        }
                        Phase phase = request.getPhase();
                        Decision decision = null;
                        Function<ProcessRequest, Decision> provider = decisionProvider;
                        if (provider != null) {
                          try {
                            decision = provider.apply(request);
                          } catch (io.grpc.StatusRuntimeException e) {
                            responseObserver.onError(e);
                            return;
                          }
                        } else if (forcedDecision != null) {
                          decision = forcedDecision;
                        } else {
                          decision =
                              Decision.newBuilder()
                                  .setContinue(Continue.newBuilder().build())
                                  .build();
                        }
                        ProcessResponse response =
                            ProcessResponse.newBuilder()
                                .setSessionId(request.getSessionId())
                                .setPhase(phase)
                                .setDecision(decision)
                                .build();
                        responseObserver.onNext(response);
                      }

                      @Override
                      public void onError(Throwable t) {
                        controllerStreamErrors.offer(t);
                      }

                      @Override
                      public void onCompleted() {
                        controllerStreamCompletions.offer(Boolean.TRUE);
                        responseObserver.onCompleted();
                      }
                    };
                  }
                })
            .build()
            .start();

    processor = new DemoRequestProcessor("127.0.0.1:" + controllerPort);
    processor.exchangeTimeoutSeconds = 3L;
    demoPort = freePort();
    extProcServer = new ExternalProcessorServer().builder(demoPort);
    extProcServer.setMetricsPort(freePort());
    extProcServer.addRequestProcessor(processor);
    extProcServer.start();

    channel = ManagedChannelBuilder.forAddress("127.0.0.1", demoPort).usePlaintext().build();
    extProcStub = ExternalProcessorGrpc.newStub(channel);
  }

  @BeforeEach
  void resetExchangeState() {
    forcedDecision = null;
    decisionProvider = null;
    controllerResponds = true;
    controllerStreamsOpened.set(0);
    controllerStreamCompletions.clear();
    controllerStreamErrors.clear();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (extProcServer != null) {
      extProcServer.stop();
    }
    if (controllerServer != null) {
      controllerServer.shutdown();
      controllerServer.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void demoContinuesBodylessGetAcrossAllDirectionsAndRetiresCleanly() throws Exception {
    TestExchangeStream exchange = openExchange();

    // 1. REQUEST_HEADERS with end_of_stream=true (bodyless GET) ends only the request direction.
    Map<String, String> reqHeaders = new HashMap<>();
    reqHeaders.put("x-demo-session-id", "sess-get-1");
    reqHeaders.put(":method", "GET");
    reqHeaders.put(":path", "/status");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, reqHeaders, true));

    ProcessingResponse reqHeadersResp = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, reqHeadersResp.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE, reqHeadersResp.getRequestHeaders().getResponse().getStatus());
    assertTrue(
        hasHeaderValue(
            reqHeadersResp.getRequestHeaders().getResponse().getHeaderMutation(),
            "x-demo-processed",
            "true"));
    assertFalse(
        exchange.isCompleted(),
        "extproc stream must stay open after REQUEST_HEADERS(end_of_stream=true)");
    assertNull(
        controllerStreamCompletions.poll(100, TimeUnit.MILLISECONDS),
        "controller stream must stay open for response phases");

    // 2. RESPONSE_HEADERS with end_of_stream=false (response body follows).
    Map<String, String> respHeaders = new HashMap<>();
    respHeaders.put(":status", "200");
    respHeaders.put("content-type", "application/json");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.RESPONSE_HEADERS, respHeaders, false));

    ProcessingResponse respHeadersResp = exchange.awaitResponse(5);
    assertEquals(
        ProcessingResponse.ResponseCase.RESPONSE_HEADERS, respHeadersResp.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE, respHeadersResp.getResponseHeaders().getResponse().getStatus());
    assertTrue(
        hasHeaderValue(
            respHeadersResp.getResponseHeaders().getResponse().getHeaderMutation(),
            "x-demo-processed",
            "true"));
    assertFalse(
        exchange.isCompleted(),
        "extproc stream must stay open when RESPONSE_HEADERS has end_of_stream=false");

    // 3. RESPONSE_BODY with end_of_stream=true completes the exchange and retires cleanly.
    exchange.send(
        envoyBodyRequest(ProcessingRequest.RequestCase.RESPONSE_BODY, "{\"ok\":true}", true));

    ProcessingResponse respBodyResp = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_BODY, respBodyResp.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, respBodyResp.getResponseBody().getResponse().getStatus());
    assertFalse(
        respBodyResp.getResponseBody().getResponse().hasHeaderMutation(),
        "RESPONSE_BODY must not emit x-demo-processed or other HeaderMutations");

    assertTrue(exchange.awaitCompleted(5), "extproc stream must complete after final response");
    assertEquals(
        Boolean.TRUE,
        controllerStreamCompletions.poll(5, TimeUnit.SECONDS),
        "controller stream must half-close cleanly via onCompleted()");
    assertNull(
        controllerStreamErrors.poll(100, TimeUnit.MILLISECONDS),
        "normal retirement must not race call.cancel() / RST_STREAM CANCEL");
    assertEquals(1, controllerStreamsOpened.get(), "all phases must share one controller stream");
  }

  @Test
  void demoMutatesRequestAndResponseBodiesForPostAndIgnoresPseudoHeaders() throws Exception {
    decisionProvider =
        req -> {
          switch (req.getPhase()) {
            case PHASE_REQUEST_HEADERS:
              return Decision.newBuilder()
                  .setContinue(
                      Continue.newBuilder()
                          .putHeaders(":method", "DELETE")
                          .putHeaders(":path", "/hijack")
                          .putHeaders("x-demo-intercepted", "true")
                          .addRemoveHeaders(":authority")
                          .addRemoveHeaders("x-strip-req")
                          .build())
                  .build();
            case PHASE_REQUEST_BODY:
              return Decision.newBuilder()
                  .setContinue(
                      Continue.newBuilder()
                          .putHeaders("x-demo-intercepted", "true")
                          .setBody(ByteString.copyFromUtf8("{\"qty\":99}"))
                          .build())
                  .build();
            case PHASE_RESPONSE_HEADERS:
              return Decision.newBuilder()
                  .setContinue(
                      Continue.newBuilder()
                          .putHeaders(":status", "201")
                          .putHeaders("x-resp-tag", "mutated")
                          .build())
                  .build();
            case PHASE_RESPONSE_BODY:
              return Decision.newBuilder()
                  .setContinue(
                      Continue.newBuilder()
                          .putHeaders("x-demo-intercepted", "true")
                          .setBody(ByteString.copyFromUtf8("{\"accepted\":true}"))
                          .build())
                  .build();
            default:
              return Decision.newBuilder().setContinue(Continue.newBuilder().build()).build();
          }
        };

    TestExchangeStream exchange = openExchange();

    // 1. REQUEST_HEADERS (end_of_stream=false)
    Map<String, String> reqHeaders = new HashMap<>();
    reqHeaders.put("x-demo-session-id", "sess-post-1");
    reqHeaders.put(":method", "POST");
    reqHeaders.put(":path", "/orders");
    reqHeaders.put(":authority", "demo.local");
    reqHeaders.put("content-type", "application/json");
    reqHeaders.put("x-strip-req", "drop-me");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, reqHeaders, false));

    ProcessingResponse r1 = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, r1.getResponseCase());
    HeaderMutation reqHdrMut = r1.getRequestHeaders().getResponse().getHeaderMutation();
    assertTrue(hasHeaderValue(reqHdrMut, "x-demo-intercepted", "true"));
    assertTrue(hasHeaderValue(reqHdrMut, "x-demo-processed", "true"));
    assertFalse(hasHeaderKey(reqHdrMut, ":method"));
    assertFalse(hasHeaderKey(reqHdrMut, ":path"));
    assertEquals(1, reqHdrMut.getRemoveHeadersCount());
    assertEquals("x-strip-req", reqHdrMut.getRemoveHeaders(0));

    // 2. REQUEST_BODY (end_of_stream=true)
    exchange.send(
        envoyBodyRequest(ProcessingRequest.RequestCase.REQUEST_BODY, "{\"qty\":1}", true));

    ProcessingResponse r2 = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_BODY, r2.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, r2.getRequestBody().getResponse().getStatus());
    assertTrue(
        hasHeaderValue(
            r2.getRequestBody().getResponse().getHeaderMutation(), "content-length", "10"));
    assertArrayEquals(
        "{\"qty\":99}".getBytes(StandardCharsets.UTF_8),
        r2.getRequestBody().getResponse().getBodyMutation().getBody().toByteArray());
    assertFalse(exchange.isCompleted(), "extproc stream must remain open after REQUEST_BODY");

    // 3. RESPONSE_HEADERS (end_of_stream=false)
    Map<String, String> respHeaders = new HashMap<>();
    respHeaders.put(":status", "200");
    respHeaders.put("content-type", "application/json");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.RESPONSE_HEADERS, respHeaders, false));

    ProcessingResponse r3 = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_HEADERS, r3.getResponseCase());
    HeaderMutation respHdrMut = r3.getResponseHeaders().getResponse().getHeaderMutation();
    assertTrue(hasHeaderValue(respHdrMut, "x-resp-tag", "mutated"));
    assertTrue(hasHeaderValue(respHdrMut, "x-demo-processed", "true"));
    assertFalse(hasHeaderKey(respHdrMut, ":status"));
    assertFalse(exchange.isCompleted());

    // 4. RESPONSE_BODY (end_of_stream=true)
    exchange.send(
        envoyBodyRequest(
            ProcessingRequest.RequestCase.RESPONSE_BODY, "{\"accepted\":false}", true));

    ProcessingResponse r4 = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_BODY, r4.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, r4.getResponseBody().getResponse().getStatus());
    assertTrue(
        hasHeaderValue(
            r4.getResponseBody().getResponse().getHeaderMutation(), "content-length", "17"));
    assertArrayEquals(
        "{\"accepted\":true}".getBytes(StandardCharsets.UTF_8),
        r4.getResponseBody().getResponse().getBodyMutation().getBody().toByteArray());

    assertTrue(exchange.awaitCompleted(5));
    assertEquals(Boolean.TRUE, controllerStreamCompletions.poll(5, TimeUnit.SECONDS));
    assertNull(controllerStreamErrors.poll(100, TimeUnit.MILLISECONDS));
    assertEquals(1, controllerStreamsOpened.get());
  }

  @Test
  void demoCompletesAndRetiresOnResponseHeadersContinueAndReplaceBodyOverride() throws Exception {
    decisionProvider =
        req -> {
          if (req.getPhase() == Phase.PHASE_RESPONSE_HEADERS) {
            return Decision.newBuilder()
                .setContinue(
                    Continue.newBuilder()
                        .setBody(ByteString.copyFromUtf8("overridden-on-headers"))
                        .build())
                .build();
          }
          return Decision.newBuilder().setContinue(Continue.newBuilder().build()).build();
        };

    TestExchangeStream exchange = openExchange();

    Map<String, String> reqHeaders = new HashMap<>();
    reqHeaders.put("x-demo-session-id", "sess-replace-resp-hdr");
    reqHeaders.put(":method", "GET");
    reqHeaders.put(":path", "/items");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, reqHeaders, true));
    ProcessingResponse reqResp = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, reqResp.getResponseCase());
    assertFalse(exchange.isCompleted());

    // RESPONSE_HEADERS arrives with end_of_stream=false because upstream had a response body,
    // but CONTINUE_AND_REPLACE replaces the body immediately so Envoy will never send
    // RESPONSE_BODY.
    Map<String, String> respHeaders = new HashMap<>();
    respHeaders.put(":status", "200");
    respHeaders.put("content-length", "100");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.RESPONSE_HEADERS, respHeaders, false));

    ProcessingResponse resp = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_HEADERS, resp.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE_AND_REPLACE, resp.getResponseHeaders().getResponse().getStatus());
    assertArrayEquals(
        "overridden-on-headers".getBytes(StandardCharsets.UTF_8),
        resp.getResponseHeaders().getResponse().getBodyMutation().getBody().toByteArray());
    assertTrue(
        hasHeaderValue(
            resp.getResponseHeaders().getResponse().getHeaderMutation(),
            "content-length",
            String.valueOf("overridden-on-headers".length())));

    assertTrue(
        exchange.awaitCompleted(5),
        "extproc stream must complete immediately after RESPONSE_HEADERS CONTINUE_AND_REPLACE");
    assertEquals(
        Boolean.TRUE,
        controllerStreamCompletions.poll(5, TimeUnit.SECONDS),
        "controller stream must retire cleanly via onCompleted()");
    assertNull(controllerStreamErrors.poll(100, TimeUnit.MILLISECONDS));
  }

  @Test
  void demoContinuesRequestHeadersWithoutBodyAsPlainContinue() throws Exception {
    forcedDecision = Decision.newBuilder().setContinue(Continue.newBuilder().build()).build();

    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-5");
    headers.put(":method", "GET");
    headers.put(":path", "/health");

    TestExchangeStream exchange = openExchange();
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true));
    ProcessingResponse response = exchange.awaitResponse(5);

    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, response.getRequestHeaders().getResponse().getStatus());
    assertFalse(response.getRequestHeaders().getResponse().getBodyMutation().getClearBody());
    assertFalse(response.getRequestHeaders().getResponse().getBodyMutation().hasBody());
    assertEquals(0, response.getRequestHeaders().getResponse().getBodyMutation().getBody().size());
    exchange.completeClient();
    assertTrue(exchange.awaitCompleted(5));
    controllerStreamErrors.poll(1, TimeUnit.SECONDS);
  }

  @Test
  void demoContinuesRequestHeadersWithBodyAsContinueAndReplace() throws Exception {
    forcedDecision =
        Decision.newBuilder()
            .setContinue(
                Continue.newBuilder().setBody(ByteString.copyFromUtf8("rewritten-body")).build())
            .build();

    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-6");
    headers.put(":method", "POST");
    headers.put(":path", "/submit");

    TestExchangeStream exchange = openExchange();
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, false));
    ProcessingResponse response = exchange.awaitResponse(5);

    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE_AND_REPLACE,
        response.getRequestHeaders().getResponse().getStatus());
    assertTrue(response.getRequestHeaders().getResponse().hasBodyMutation());
    assertArrayEquals(
        "rewritten-body".getBytes(StandardCharsets.UTF_8),
        response.getRequestHeaders().getResponse().getBodyMutation().getBody().toByteArray());
    assertTrue(
        hasHeaderValue(
            response.getRequestHeaders().getResponse().getHeaderMutation(),
            "content-length",
            String.valueOf("rewritten-body".length())));
    exchange.completeClient();
    assertTrue(exchange.awaitCompleted(5));
    controllerStreamErrors.poll(1, TimeUnit.SECONDS);
  }

  @Test
  void demoContinuesRequestHeadersWithEmptyBodyAsClearBodyAndZeroContentLength() throws Exception {
    forcedDecision =
        Decision.newBuilder()
            .setContinue(Continue.newBuilder().setBody(ByteString.EMPTY).build())
            .build();

    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-empty-body");
    headers.put(":method", "POST");
    headers.put(":path", "/submit");

    TestExchangeStream exchange = openExchange();
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, false));
    ProcessingResponse response = exchange.awaitResponse(5);

    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE_AND_REPLACE,
        response.getRequestHeaders().getResponse().getStatus());
    assertTrue(response.getRequestHeaders().getResponse().getBodyMutation().getClearBody());
    assertTrue(
        hasHeaderValue(
            response.getRequestHeaders().getResponse().getHeaderMutation(), "content-length", "0"));
    exchange.completeClient();
    assertTrue(exchange.awaitCompleted(5));
    controllerStreamErrors.poll(1, TimeUnit.SECONDS);
  }

  @Test
  void demoAppliesCancelImmediatelyAndRetiresControllerStreamCleanly() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "sess-2");
    headers.put(":method", "GET");
    headers.put(":path", "/health");
    forcedDecision =
        Decision.newBuilder()
            .setCancel(
                Cancel.newBuilder()
                    .setStatus(503)
                    .setBody("upstream unavailable")
                    .putHeaders("x-cancel-reason", "maintenance")
                    .putHeaders(":status", "503")
                    .build())
            .build();

    TestExchangeStream exchange = openExchange();
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true));
    ProcessingResponse response = exchange.awaitResponse(5);

    assertEquals(ProcessingResponse.ResponseCase.IMMEDIATE_RESPONSE, response.getResponseCase());
    ImmediateResponse immediate = response.getImmediateResponse();
    assertEquals(503, immediate.getStatus().getCode().getNumber());
    assertEquals("upstream unavailable", immediate.getBody().toStringUtf8());
    assertTrue(hasHeaderValue(immediate.getHeaders(), "x-cancel-reason", "maintenance"));
    assertFalse(hasHeaderKey(immediate.getHeaders(), ":status"));

    assertTrue(exchange.awaitCompleted(5), "CANCEL decision must complete the extproc stream");
    assertEquals(
        Boolean.TRUE,
        controllerStreamCompletions.poll(5, TimeUnit.SECONDS),
        "CANCEL decision must retire the controller stream cleanly via onCompleted()");
    assertNull(
        controllerStreamErrors.poll(100, TimeUnit.MILLISECONDS),
        "CANCEL retirement must not send RST_STREAM CANCEL to controller");
  }

  @Test
  void demoFailsClosedAndAbortsControllerStreamOnPhaseTimeout() throws Exception {
    processor.exchangeTimeoutSeconds = 1L;
    controllerResponds = false;
    try {
      Map<String, String> headers = new HashMap<>();
      headers.put("x-demo-session-id", "sess-3");
      headers.put(":method", "GET");
      headers.put(":path", "/health");

      TestExchangeStream exchange = openExchange();
      exchange.send(
          envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true));
      assertThrows(RuntimeException.class, () -> exchange.awaitResponse(5));

      Throwable controllerErr = controllerStreamErrors.poll(5, TimeUnit.SECONDS);
      assertNotNull(controllerErr, "phase timeout must abort the in-flight controller stream");
      assertEquals(Status.Code.CANCELLED, Status.fromThrowable(controllerErr).getCode());
    } finally {
      controllerResponds = true;
      processor.exchangeTimeoutSeconds = 3L;
    }
  }

  @Test
  void demoAbortsControllerStreamWhenEnvoyCancelsDownstreamStream() throws Exception {
    controllerResponds = false;
    CountDownLatch controllerReceivedPhase = new CountDownLatch(1);
    int beforeCount = controllerDecisionsReceived.get();
    try (Context.CancellableContext cancellable = Context.current().withCancellation()) {
      TestExchangeStream exchange = cancellable.call(() -> openExchange());
      Map<String, String> headers = new HashMap<>();
      headers.put("x-demo-session-id", "sess-cancel-downstream");
      headers.put(":method", "GET");
      headers.put(":path", "/slow");
      exchange.send(
          envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true));

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (controllerDecisionsReceived.get() <= beforeCount && System.nanoTime() < deadline) {
        TimeUnit.MILLISECONDS.sleep(10);
      }
      if (controllerDecisionsReceived.get() > beforeCount) {
        controllerReceivedPhase.countDown();
      }
      assertTrue(
          controllerReceivedPhase.await(1, TimeUnit.SECONDS),
          "controller must receive phase update before downstream cancellation");

      cancellable.cancel(new RuntimeException("envoy downstream disconnected"));

      Throwable controllerErr = controllerStreamErrors.poll(5, TimeUnit.SECONDS);
      assertNotNull(
          controllerErr,
          "downstream Envoy cancellation must abort the in-flight controller stream");
      assertEquals(Status.Code.CANCELLED, Status.fromThrowable(controllerErr).getCode());
    } finally {
      controllerResponds = true;
    }
  }

  @Test
  void demoReturnsImmediate400WhenSessionHeaderIsMissingWithoutCallingController()
      throws Exception {
    int beforeStreams = controllerStreamsOpened.get();
    Map<String, String> headers = new HashMap<>();
    headers.put(":method", "GET");
    headers.put(":path", "/");

    TestExchangeStream exchange = openExchange();
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true));
    ProcessingResponse response = exchange.awaitResponse(5);

    assertEquals(ProcessingResponse.ResponseCase.IMMEDIATE_RESPONSE, response.getResponseCase());
    assertEquals(400, response.getImmediateResponse().getStatus().getCode().getNumber());
    assertEquals(
        "missing x-demo-session-id header",
        response.getImmediateResponse().getBody().toStringUtf8());
    assertTrue(exchange.awaitCompleted(5));
    assertEquals(
        beforeStreams,
        controllerStreamsOpened.get(),
        "missing session header must not open a controller stream");
  }

  @Test
  void demoReturnsImmediate404WhenControllerReportsSessionNotFound() throws Exception {
    decisionProvider =
        req -> {
          throw Status.NOT_FOUND.withDescription("session not found").asRuntimeException();
        };

    Map<String, String> headers = new HashMap<>();
    headers.put("x-demo-session-id", "nonexistent-session");
    headers.put(":method", "GET");
    headers.put(":path", "/");

    TestExchangeStream exchange = openExchange();
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, headers, true));
    ProcessingResponse response = exchange.awaitResponse(5);

    assertEquals(ProcessingResponse.ResponseCase.IMMEDIATE_RESPONSE, response.getResponseCase());
    assertEquals(404, response.getImmediateResponse().getStatus().getCode().getNumber());
    assertEquals("session not found", response.getImmediateResponse().getBody().toStringUtf8());
    assertTrue(exchange.awaitCompleted(5));
  }

  @Test
  void demoClearsResponseBodyAndSetsZeroContentLength() throws Exception {
    decisionProvider =
        req -> {
          if (req.getPhase() == Phase.PHASE_RESPONSE_BODY) {
            return Decision.newBuilder()
                .setContinue(Continue.newBuilder().setBody(ByteString.EMPTY).build())
                .build();
          }
          return Decision.newBuilder().setContinue(Continue.newBuilder().build()).build();
        };

    TestExchangeStream exchange = openExchange();
    Map<String, String> reqHeaders = new HashMap<>();
    reqHeaders.put("x-demo-session-id", "sess-clear-resp-body");
    reqHeaders.put(":method", "GET");
    reqHeaders.put(":path", "/echo");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.REQUEST_HEADERS, reqHeaders, true));
    assertEquals(
        ProcessingResponse.ResponseCase.REQUEST_HEADERS,
        exchange.awaitResponse(5).getResponseCase());

    Map<String, String> respHeaders = new HashMap<>();
    respHeaders.put(":status", "200");
    respHeaders.put("content-length", "11");
    exchange.send(
        envoyHeadersRequest(ProcessingRequest.RequestCase.RESPONSE_HEADERS, respHeaders, false));
    assertEquals(
        ProcessingResponse.ResponseCase.RESPONSE_HEADERS,
        exchange.awaitResponse(5).getResponseCase());

    exchange.send(
        envoyBodyRequest(ProcessingRequest.RequestCase.RESPONSE_BODY, "hello world", true));
    ProcessingResponse r = exchange.awaitResponse(5);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_BODY, r.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, r.getResponseBody().getResponse().getStatus());
    assertTrue(r.getResponseBody().getResponse().getBodyMutation().getClearBody());
    assertTrue(
        hasHeaderValue(
            r.getResponseBody().getResponse().getHeaderMutation(), "content-length", "0"));
    assertTrue(exchange.awaitCompleted(5));
  }

  // ------------------------------------------------------------------
  // Test helpers
  // ------------------------------------------------------------------

  private static TestExchangeStream openExchange() {
    return new TestExchangeStream(extProcStub);
  }

  private static final class TestExchangeStream {
    private final BlockingQueue<ProcessingResponse> responses = new LinkedBlockingQueue<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final CountDownLatch completed = new CountDownLatch(1);
    private final StreamObserver<ProcessingRequest> sender;

    TestExchangeStream(ExternalProcessorGrpc.ExternalProcessorStub stub) {
      this.sender =
          stub.process(
              new StreamObserver<ProcessingResponse>() {
                @Override
                public void onNext(ProcessingResponse value) {
                  responses.offer(value);
                }

                @Override
                public void onError(Throwable t) {
                  error.set(t);
                  completed.countDown();
                }

                @Override
                public void onCompleted() {
                  completed.countDown();
                }
              });
    }

    void send(ProcessingRequest request) {
      sender.onNext(request);
    }

    void completeClient() {
      sender.onCompleted();
    }

    boolean isCompleted() {
      return completed.getCount() == 0 && error.get() == null;
    }

    boolean awaitCompleted(long timeoutSeconds) throws InterruptedException {
      boolean done = completed.await(timeoutSeconds, TimeUnit.SECONDS);
      if (error.get() != null) {
        throw new RuntimeException("demo extproc stream errored", error.get());
      }
      return done;
    }

    ProcessingResponse awaitResponse(long timeoutSeconds) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
      while (System.nanoTime() < deadline) {
        ProcessingResponse next = responses.poll(25, TimeUnit.MILLISECONDS);
        if (next != null) {
          return next;
        }
        Throwable err = error.get();
        if (err != null) {
          throw new RuntimeException("demo extproc stream errored", err);
        }
      }
      throw new TimeoutException("timed out waiting for demo extproc response");
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static ProcessingRequest envoyHeadersRequest(
      ProcessingRequest.RequestCase phase, Map<String, String> headers, boolean endOfStream) {
    HeaderMap.Builder headerMapBuilder = HeaderMap.newBuilder();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      headerMapBuilder.addHeaders(
          HeaderValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
    }
    HttpHeaders httpHeaders =
        HttpHeaders.newBuilder()
            .setHeaders(headerMapBuilder.build())
            .setEndOfStream(endOfStream)
            .build();
    ProcessingRequest.Builder reqBuilder = ProcessingRequest.newBuilder();
    switch (phase) {
      case REQUEST_HEADERS:
        return reqBuilder.setRequestHeaders(httpHeaders).build();
      case RESPONSE_HEADERS:
        return reqBuilder.setResponseHeaders(httpHeaders).build();
      default:
        throw new IllegalArgumentException("unsupported headers phase: " + phase);
    }
  }

  private static ProcessingRequest envoyBodyRequest(
      ProcessingRequest.RequestCase phase, String body, boolean endOfStream) {
    HttpBody httpBody =
        HttpBody.newBuilder()
            .setBody(ByteString.copyFromUtf8(body))
            .setEndOfStream(endOfStream)
            .build();
    ProcessingRequest.Builder reqBuilder = ProcessingRequest.newBuilder();
    switch (phase) {
      case REQUEST_BODY:
        return reqBuilder.setRequestBody(httpBody).build();
      case RESPONSE_BODY:
        return reqBuilder.setResponseBody(httpBody).build();
      default:
        throw new IllegalArgumentException("unsupported body phase: " + phase);
    }
  }

  private static boolean hasHeaderValue(HeaderMutation mutation, String key, String expectedValue) {
    if (mutation == null) {
      return false;
    }
    for (HeaderValueOption option : mutation.getSetHeadersList()) {
      String actual =
          !option.getHeader().getValue().isEmpty()
              ? option.getHeader().getValue()
              : option.getHeader().getRawValue().toStringUtf8();
      if (key.equals(option.getHeader().getKey()) && expectedValue.equals(actual)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasHeaderKey(HeaderMutation mutation, String key) {
    if (mutation == null) {
      return false;
    }
    for (HeaderValueOption option : mutation.getSetHeadersList()) {
      if (key.equals(option.getHeader().getKey())) {
        return true;
      }
    }
    return false;
  }
}

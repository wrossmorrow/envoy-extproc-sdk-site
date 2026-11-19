package com.github.wrossmorrow.extproc.demo;

import build.buf.gen.envoy.service.ext_proc.v3.ProcessingRequest.RequestCase;
import com.controller.v1.Cancel;
import com.controller.v1.Continue;
import com.controller.v1.Decision;
import com.controller.v1.DemoProcessorServiceGrpc;
import com.controller.v1.Header;
import com.controller.v1.HttpSnapshot;
import com.controller.v1.Phase;
import com.controller.v1.ProcessRequest;
import com.controller.v1.ProcessResponse;
import com.github.wrossmorrow.extproc.AllHeaders;
import com.github.wrossmorrow.extproc.ProcessingOptions;
import com.github.wrossmorrow.extproc.RequestContext;
import com.github.wrossmorrow.extproc.RequestProcessor;
import com.github.wrossmorrow.extproc.RequestProcessorHealthManager;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Interactive demo request processor for the envoy extproc demo. Mirrors the Go and Rust demo
 * processors: it owns the controller<->extproc phase exchange over the {@code
 * DemoProcessorService.Process} bidi channel, sends phase snapshots to the controller, applies the
 * controller's continue/cancel decisions, and fails closed when the processor fails.
 */
public class DemoRequestProcessor implements RequestProcessor {

  /** Header carrying the controller session id on the envoy-bound demo request. */
  public static final String SESSION_ID_HEADER = "x-demo-session-id";

  /** Header marking a request as processed by the demo. */
  public static final String PROCESSED_HEADER = "x-demo-processed";

  /** Cap on the bodies reported in phase snapshots. */
  public static final int SNAPSHOT_BODY_LIMIT = 16 * 1024;

  public static final String CONTROLLER_ADDR_ENV = "CONTROLLER_ADDR";
  public static final String DEFAULT_CONTROLLER_ADDR = "controller:9093";
  public static final String EXCHANGE_TIMEOUT_ENV = "EXCHANGE_TIMEOUT";
  public static final String ABORT_ON_PROCESSOR_FAILURE_ENV = "ABORT_ON_PROCESSOR_FAILURE";

  /** Default phase exchange timeout, in seconds. */
  public static final long DEFAULT_EXCHANGE_TIMEOUT_SECONDS = 300L;

  // RequestContext custom data keys (the context persists across phases of an exchange).
  static final String CONTROLLER_STREAM_KEY = "controllerDemoStream";
  static final String CONTROLLER_RESPONSE_KEY = "controllerDemoResponseObserver";
  static final String CONTROLLER_DECISION_KEY = "controllerDemoDecision";
  static final String CONTROLLER_SESSION_ID_KEY = "controllerDemoSessionID";
  static final String SNAPSHOT_NAMES_KEY = "controllerDemoSnapshotHeaderNames";

  private static final Logger logger = Logger.getLogger(DemoRequestProcessor.class.getName());

  protected final ProcessingOptions options;
  protected volatile ManagedChannel controllerChannel;
  protected volatile String controllerAddr;
  protected volatile long exchangeTimeoutSeconds = DEFAULT_EXCHANGE_TIMEOUT_SECONDS;
  protected volatile boolean abortOnProcessorFailure = true;
  protected volatile RequestProcessorHealthManager health;

  public DemoRequestProcessor() {
    this(controllerAddrFromEnv());
  }

  public DemoRequestProcessor(String controllerAddr) {
    this.controllerAddr = controllerAddr;
    this.options = new ProcessingOptions();
    this.options.abortOnProcessorFailure = abortOnProcessorFailure();
    this.exchangeTimeoutSeconds = exchangeTimeoutSeconds();
  }

  /**
   * Returns the controller gRPC address from the {@code CONTROLLER_ADDR} environment variable,
   * defaulting to {@value #DEFAULT_CONTROLLER_ADDR}.
   */
  public static String controllerAddrFromEnv() {
    return envOr(CONTROLLER_ADDR_ENV, DEFAULT_CONTROLLER_ADDR);
  }

  /**
   * Returns the phase exchange timeout in seconds from the {@code EXCHANGE_TIMEOUT} environment
   * variable, defaulting to {@value #DEFAULT_EXCHANGE_TIMEOUT_SECONDS}.
   */
  public static long exchangeTimeoutSecondsFromEnv() {
    return parseTimeoutSeconds(
        envOr(EXCHANGE_TIMEOUT_ENV, Long.toString(DEFAULT_EXCHANGE_TIMEOUT_SECONDS)));
  }

  /**
   * Returns whether the demo aborts the phase stream when the processor fails, from the {@code
   * ABORT_ON_PROCESSOR_FAILURE} environment variable, defaulting to true.
   */
  public static boolean abortOnProcessorFailureFromEnv() {
    return parseBoolean(envOr(ABORT_ON_PROCESSOR_FAILURE_ENV, "true"));
  }

  protected static String envOr(String key, String fallback) {
    String value = System.getenv(key);
    if (value == null || value.isEmpty()) {
      return fallback;
    }
    return value;
  }

  protected static long parseTimeoutSeconds(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return DEFAULT_EXCHANGE_TIMEOUT_SECONDS;
    }
  }

  protected static boolean parseBoolean(String value) {
    if (value == null || value.isEmpty()) {
      return true;
    }
    try {
      return Boolean.parseBoolean(value);
    } catch (NumberFormatException e) {
      return true;
    }
  }

  // ------------------------------------------------------------------
  // Env-driven configuration (overridable for tests)
  // ------------------------------------------------------------------

  protected String controllerAddr() {
    return controllerAddrFromEnv();
  }

  protected long exchangeTimeoutSeconds() {
    return exchangeTimeoutSecondsFromEnv();
  }

  protected boolean abortOnProcessorFailure() {
    return abortOnProcessorFailureFromEnv();
  }

  // ------------------------------------------------------------------
  // RequestProcessor interface
  // ------------------------------------------------------------------

  @Override
  public String getName() {
    return "demo";
  }

  @Override
  public ProcessingOptions getOptions() {
    return options;
  }

  @Override
  public void setHealthManager(RequestProcessorHealthManager health) {
    this.health = health;
  }

  @Override
  public void shutdown() {
    ManagedChannel channel = controllerChannel;
    if (channel != null) {
      controllerChannel = null;
      channel.shutdownNow();
    }
  }

  @Override
  public void close(int gracePeriodSeconds) {
    shutdown();
  }

  @Override
  public void processRequestHeaders(RequestContext ctx, Map<String, String> headers) {
    resolvePhase(ctx, RequestCase.REQUEST_HEADERS, headers, null);
  }

  @Override
  public void processRequestBody(RequestContext ctx, String body) {
    resolvePhase(ctx, RequestCase.REQUEST_BODY, null, body);
  }

  @Override
  public void processRequestTrailers(RequestContext ctx, Map<String, String> trailers) {
    resolvePhase(ctx, RequestCase.REQUEST_TRAILERS, trailers, null);
  }

  @Override
  public void processResponseHeaders(RequestContext ctx, Map<String, String> headers) {
    resolvePhase(ctx, RequestCase.RESPONSE_HEADERS, headers, null);
  }

  @Override
  public void processResponseBody(RequestContext ctx, String body) {
    resolvePhase(ctx, RequestCase.RESPONSE_BODY, null, body);
  }

  @Override
  public void processResponseTrailers(RequestContext ctx, Map<String, String> trailers) {
    resolvePhase(ctx, RequestCase.RESPONSE_TRAILERS, trailers, null);
  }

  @Override
  public void errorHandler(RequestContext ctx, RequestCase phase, Throwable err) {
    if (isStreamCancelled(err)) {
      // Envoy tore the downstream down and cancelled the extproc stream; treat it as the
      // cancellation signal for this exchange phase and clean up the controller stream handle
      // held in the request context.
      retireStream(ctx);
      return;
    }
    logger.severe(
        "Demo processor error: phase="
            + phaseName(phase)
            + " error="
            + (err == null ? "null" : err.toString()));
  }

  // ------------------------------------------------------------------
  // Session id
  // ------------------------------------------------------------------

  protected String sessionIdFor(RequestContext ctx, Map<String, String> headers) {
    String sessionID = (String) ctx.getValue(CONTROLLER_SESSION_ID_KEY, String.class);
    if (sessionID != null && !sessionID.isEmpty()) {
      return sessionID;
    }
    if (headers != null) {
      String id = headers.get(SESSION_ID_HEADER);
      if (id != null && !id.isEmpty()) {
        ctx.setValue(CONTROLLER_SESSION_ID_KEY, id);
        return id;
      }
    }
    return "";
  }

  // ------------------------------------------------------------------
  // Phase exchange
  // ------------------------------------------------------------------

  protected void resolvePhase(
      RequestContext ctx, RequestCase phase, Map<String, String> headers, String body) {
    String sessionID = sessionIdFor(ctx, headers);
    if (headers != null) {
      List<String> names = new ArrayList<>();
      for (String key : headers.keySet()) {
        names.add(key);
      }
      names.sort(String::compareTo);
      ctx.setValue(SNAPSHOT_NAMES_KEY, names);
    }
    HttpSnapshot snapshot = buildSnapshot(ctx, phase, headers, body);
    ProcessResponse decision = sendPhaseAndDecide(ctx, phase, sessionID, snapshot);
    boolean endOfStream = ctx.streamComplete() != null && ctx.streamComplete();
    boolean responsePhase = isResponsePhase(phase);
    Decision action = decision != null ? decision.getDecision() : null;
    if (action != null && action.getActionCase() == Decision.ActionCase.CANCEL) {
      // A cancel ends the exchange, so retire the controller phase stream before surfacing the
      // cancellation to the phase context.
      retireStream(ctx);
      Cancel cancelDecision = action.getCancel();
      ctx.cancelRequest(
          cancelDecision.getStatus(), cancelDecision.getHeaders(), cancelDecision.getBody());
      return;
    }
    if (action != null && action.getActionCase() == Decision.ActionCase.CONTINUE) {
      applyDecision(ctx, action.getContinue());
    }
    if (responsePhase && endOfStream) {
      // The phase signaled end-of-stream during a response phase, which marks the end of the
      // direction the demo retires on. End-of-stream marks the end of a single direction (e.g. the
      // request headers for a bodyless GET), not the end of the exchange, so retire only for
      // response phases; otherwise keep the handle in the request context for the next phase.
      retireStream(ctx);
      ctx.setProcessingComplete();
    } else if (!responsePhase && endOfStream) {
      // A request phase that signaled end-of-stream ends the direction as well (e.g. a bodyless
      // GET reporting the request headers phase as its final phase).
      ctx.setProcessingComplete();
    }
  }

  protected ProcessResponse sendPhaseAndDecide(
      RequestContext ctx, RequestCase phase, String sessionID, HttpSnapshot snapshot) {
    CompletableFuture<ProcessResponse> decisionFuture =
        (CompletableFuture<ProcessResponse>) ctx.getValue(CONTROLLER_DECISION_KEY);
    final CompletableFuture<ProcessResponse> decision =
        decisionFuture == null ? new CompletableFuture<>() : decisionFuture;
    if (decisionFuture == null) {
      ctx.setValue(CONTROLLER_DECISION_KEY, decision);
    }
    StreamObserver<ProcessResponse> responseObserver =
        (StreamObserver<ProcessResponse>) ctx.getValue(CONTROLLER_RESPONSE_KEY);
    if (responseObserver == null) {
      responseObserver =
          new StreamObserver<ProcessResponse>() {
            @Override
            public void onNext(ProcessResponse value) {
              decision.complete(value);
            }

            @Override
            public void onError(Throwable t) {
              decision.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
              decision.completeExceptionally(
                  new IllegalStateException("controller closed phase stream without a decision"));
            }
          };
      ctx.setValue(CONTROLLER_RESPONSE_KEY, responseObserver);
    }
    StreamObserver<ProcessRequest> sender = controllerStream(ctx, responseObserver);
    sender.onNext(
        ProcessRequest.newBuilder()
            .setSessionId(sessionID)
            .setPhase(phaseOf(phase))
            .setSnapshot(snapshot)
            .build());
    try {
      return decision.get(exchangeTimeoutSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("timed out waiting for controller decision", e);
    } catch (TimeoutException e) {
      throw new RuntimeException("timed out waiting for controller decision", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException("controller phase exchange failed", cause);
    }
  }

  protected StreamObserver<ProcessRequest> controllerStream(
      RequestContext ctx, StreamObserver<ProcessResponse> responseObserver) {
    StreamObserver<ProcessRequest> existing =
        (StreamObserver<ProcessRequest>) ctx.getValue(CONTROLLER_STREAM_KEY);
    if (existing != null) {
      // The controller phase stream for the exchange is still open; phases after the first reuse
      // it and wait on the same response observer for the next decision.
      ctx.setValue(CONTROLLER_RESPONSE_KEY, responseObserver);
      return existing;
    }
    MethodDescriptor<ProcessRequest, ProcessResponse> method =
        DemoProcessorServiceGrpc.getProcessMethod();
    ManagedChannel channel = controllerChannel();
    ClientCall<ProcessRequest, ProcessResponse> call = channel.newCall(method, CallOptions.DEFAULT);
    StreamObserver<ProcessRequest> sender =
        ClientCalls.asyncBidiStreamingCall(call, responseObserver);
    ctx.setValue(CONTROLLER_STREAM_KEY, sender);
    ctx.setValue(CONTROLLER_RESPONSE_KEY, responseObserver);
    return sender;
  }

  protected void applyDecision(RequestContext ctx, Continue continueDecision) {
    if (continueDecision.getRemoveHeadersList().size() > 0) {
      ctx.removeHeaders(continueDecision.getRemoveHeadersList());
    }
    if (continueDecision.hasBody()) {
      ctx.replaceBody(continueDecision.getBody().toByteArray());
    }
    Map<String, String> continueHeaders = continueDecision.getHeaders();
    for (Map.Entry<String, String> entry : continueHeaders.entrySet()) {
      ctx.overwriteHeader(entry.getKey(), entry.getValue());
    }
    ctx.addHeader(PROCESSED_HEADER, "true");
  }

  protected void retireStream(RequestContext ctx) {
    StreamObserver<ProcessRequest> sender =
        (StreamObserver<ProcessRequest>) ctx.getValue(CONTROLLER_STREAM_KEY);
    if (sender != null) {
      try {
        sender.onCompleted();
      } catch (Exception e) {
        // best effort retire of the controller phase stream
      }
    }
    ctx.setValue(CONTROLLER_STREAM_KEY, null);
  }

  // ------------------------------------------------------------------
  // Snapshot / phases
  // ------------------------------------------------------------------

  protected HttpSnapshot buildSnapshot(
      RequestContext ctx, RequestCase phase, Map<String, String> headers, String body) {
    HttpSnapshot.Builder builder = HttpSnapshot.newBuilder();
    List<Map.Entry<String, String>> entries = snapshotEntries(ctx, phase, headers);
    for (Map.Entry<String, String> entry : entries) {
      builder.addHeaders(
          Header.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
    }
    byte[] bodyBytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
    if (bodyBytes.length > SNAPSHOT_BODY_LIMIT) {
      builder.setBody(ByteString.copyFrom(bodyBytes, 0, SNAPSHOT_BODY_LIMIT));
      builder.setBodyTruncated(true);
    } else {
      builder.setBody(ByteString.copyFrom(bodyBytes));
    }
    builder.setStatus(ctx.getStatus());
    if (ctx.getMethod() != null && !ctx.getMethod().isEmpty() && !hasEntry(entries, ":method")) {
      builder.addHeaders(Header.newBuilder().setKey(":method").setValue(ctx.getMethod()).build());
    }
    if (ctx.getPath() != null && !ctx.getPath().isEmpty() && !hasEntry(entries, ":path")) {
      builder.addHeaders(Header.newBuilder().setKey(":path").setValue(ctx.getPath()).build());
    }
    return builder.build();
  }

  protected List<Map.Entry<String, String>> snapshotEntries(
      RequestContext ctx, RequestCase phase, Map<String, String> headers) {
    List<Map.Entry<String, String>> entries = new ArrayList<>();
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        entries.add(Map.entry(entry.getKey(), masked(entry.getKey(), entry.getValue())));
      }
      entries.sort(Comparator.comparing(Map.Entry::getKey));
      return entries;
    }
    // Body phases carry no header map; derive the names from the phase that preceded them.
    List<String> names = (List<String>) ctx.getValue(SNAPSHOT_NAMES_KEY);
    Map<String, String> derived = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    if (names != null) {
      AllHeaders all =
          isResponsePhase(phase) ? ctx.getAllResponseHeaders() : ctx.getAllRequestHeaders();
      for (String name : names) {
        List<String> values = all.values(name);
        if (!values.isEmpty()) {
          derived.put(name, masked(name, values.get(0)));
        }
      }
    }
    entries.addAll(derived.entrySet());
    entries.sort(Comparator.comparing(Map.Entry::getKey));
    return entries;
  }

  protected String masked(String key, String value) {
    if (key == null) {
      return value;
    }
    String lower = key.toLowerCase(java.util.Locale.ROOT);
    if (lower.equals("cookie") || lower.equals("set-cookie") || lower.equals("authorization")) {
      return "[redacted " + (value == null ? 0 : value.length()) + " chars]";
    }
    return value;
  }

  protected boolean hasEntry(List<Map.Entry<String, String>> entries, String key) {
    for (Map.Entry<String, String> entry : entries) {
      if (entry.getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  protected boolean isResponsePhase(RequestCase phase) {
    switch (phase) {
      case RESPONSE_HEADERS:
      case RESPONSE_BODY:
      case RESPONSE_TRAILERS:
        return true;
      default:
        return false;
    }
  }

  protected boolean isStreamCancelled(Throwable err) {
    if (err == null) {
      return false;
    }
    if (err instanceof io.grpc.StatusRuntimeException) {
      return ((io.grpc.StatusRuntimeException) err).getStatus().getCode()
          == Status.CANCELLED.getCode();
    }
    return err instanceof java.util.concurrent.CancellationException;
  }

  protected String phaseName(RequestCase phase) {
    switch (phase) {
      case REQUEST_HEADERS:
        return "REQUEST_HEADERS";
      case REQUEST_BODY:
        return "REQUEST_BODY";
      case REQUEST_TRAILERS:
        return "REQUEST_TRAILERS";
      case RESPONSE_HEADERS:
        return "RESPONSE_HEADERS";
      case RESPONSE_BODY:
        return "RESPONSE_BODY";
      case RESPONSE_TRAILERS:
        return "RESPONSE_TRAILERS";
      default:
        return "UNDETERMINED";
    }
  }

  protected Phase phaseOf(RequestCase phase) {
    switch (phase) {
      case REQUEST_HEADERS:
        return Phase.PHASE_REQUEST_HEADERS;
      case REQUEST_BODY:
        return Phase.PHASE_REQUEST_BODY;
      case REQUEST_TRAILERS:
        return Phase.PHASE_REQUEST_TRAILERS;
      case RESPONSE_HEADERS:
        return Phase.PHASE_RESPONSE_HEADERS;
      case RESPONSE_BODY:
        return Phase.PHASE_RESPONSE_BODY;
      case RESPONSE_TRAILERS:
        return Phase.PHASE_RESPONSE_TRAILERS;
      default:
        return Phase.PHASE_UNSPECIFIED;
    }
  }

  // ------------------------------------------------------------------
  // Controller channel
  // ------------------------------------------------------------------

  protected ManagedChannel controllerChannel() {
    ManagedChannel existing = controllerChannel;
    if (existing != null) {
      return existing;
    }
    synchronized (this) {
      ManagedChannel existingLocked = controllerChannel;
      if (existingLocked == null) {
        ManagedChannel channel =
            ManagedChannelBuilder.forTarget(controllerAddr).usePlaintext().build();
        controllerChannel = channel;
        return channel;
      }
      return existingLocked;
    }
  }
}

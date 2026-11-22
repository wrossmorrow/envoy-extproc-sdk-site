package com.github.wrossmorrow.extproc.demo;

import build.buf.gen.envoy.service.ext_proc.v3.CommonResponse.ResponseStatus;
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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
  static final String CONTROLLER_DECISIONS_KEY = "controllerDemoDecisions";
  static final String CONTROLLER_DECISION_RECEIVED_KEY = "controllerDemoDecisionReceived";
  static final String CONTROLLER_SESSION_ID_KEY = "controllerDemoSessionID";
  static final String SNAPSHOT_NAMES_KEY = "controllerDemoSnapshotHeaderNames";
  static final String CONTROLLER_CALL_KEY = "controllerDemoCall";

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
      // cancellation signal for this exchange phase and abort the controller stream handle held in
      // the request context.
      abortStream(ctx, "downstream extproc stream cancelled", err);
      return;
    }
    abortStream(ctx, "demo processor error", err);
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
    if (sessionID.isEmpty()) {
      retireStream(ctx);
      ctx.cancelRequest(
          400, Map.of("content-type", "text/plain"), "missing x-demo-session-id header");
      return;
    }
    if (headers != null) {
      List<String> names = new ArrayList<>();
      for (String key : headers.keySet()) {
        names.add(key);
      }
      names.sort(String::compareTo);
      ctx.setValue(SNAPSHOT_NAMES_KEY, names);
    }
    HttpSnapshot snapshot = buildSnapshot(ctx, phase, headers, body);
    ProcessResponse decision;
    try {
      decision = sendPhaseAndDecide(ctx, phase, sessionID, snapshot);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        retireStream(ctx);
        ctx.cancelRequest(404, Map.of("content-type", "text/plain"), "session not found");
        return;
      }
      throw e;
    }
    boolean endOfStream = ctx.streamComplete() != null && ctx.streamComplete();
    boolean responsePhase = isResponsePhase(phase);
    Decision action = decision != null ? decision.getDecision() : null;
    if (action != null) {
      logPhaseDecisionReceived(phase, action);
    }
    if (action != null && action.getActionCase() == Decision.ActionCase.CANCEL) {
      // A cancel ends the exchange, so retire the controller phase stream before surfacing the
      // cancellation to the phase context.
      retireStream(ctx);
      Cancel cancelDecision = action.getCancel();
      ctx.cancelRequest(
          cancelDecision.getStatus(),
          filterMutableHeaders(cancelDecision.getHeaders()),
          cancelDecision.getBody());
      return;
    }
    boolean replacedResponseOnHeaders = false;
    if (action != null && action.getActionCase() == Decision.ActionCase.CONTINUE) {
      Continue continueDecision = action.getContinue();
      applyDecision(ctx, phase, continueDecision);
      logPhaseDecisionSent(phase, continueDecision);
      replacedResponseOnHeaders =
          phase == RequestCase.RESPONSE_HEADERS && continueDecision.hasBody();
    }
    if (responsePhase && (endOfStream || replacedResponseOnHeaders)) {
      // Finalize and retire the controller stream when a response phase ends the exchange: either
      // Envoy signaled end-of-stream on the response direction, or RESPONSE_HEADERS returned
      // CONTINUE_AND_REPLACE with a replacement body (in which case Envoy replaces the response
      // body immediately and never sends RESPONSE_BODY even though end_of_stream was false).
      retireStream(ctx);
      ctx.setProcessingComplete();
    }
    // NOTE: end-of-stream during a request phase (e.g. the final REQUEST_BODY of a POST, or
    // REQUEST_HEADERS of a bodyless GET) ends that direction only; the exchange continues with the
    // response phases, so we must not finalize here.
  }

  protected ProcessResponse sendPhaseAndDecide(
      RequestContext ctx, RequestCase phase, String sessionID, HttpSnapshot snapshot) {
    LinkedBlockingQueue<Object> decisions =
        (LinkedBlockingQueue<Object>) ctx.getValue(CONTROLLER_DECISIONS_KEY);
    final LinkedBlockingQueue<Object> phaseDecisions =
        decisions != null ? decisions : new LinkedBlockingQueue<>();
    if (decisions == null) {
      ctx.setValue(CONTROLLER_DECISIONS_KEY, phaseDecisions);
    }
    Object decisionReceivedValue = ctx.getValue(CONTROLLER_DECISION_RECEIVED_KEY);
    final java.util.concurrent.atomic.AtomicBoolean decisionReceived =
        decisionReceivedValue instanceof java.util.concurrent.atomic.AtomicBoolean
            ? (java.util.concurrent.atomic.AtomicBoolean) decisionReceivedValue
            : new java.util.concurrent.atomic.AtomicBoolean();
    ctx.setValue(CONTROLLER_DECISION_RECEIVED_KEY, decisionReceived);
    StreamObserver<ProcessResponse> responseObserver =
        (StreamObserver<ProcessResponse>) ctx.getValue(CONTROLLER_RESPONSE_KEY);
    if (responseObserver == null) {
      responseObserver =
          new StreamObserver<ProcessResponse>() {
            @Override
            public void onNext(ProcessResponse value) {
              phaseDecisions.offer(value);
              decisionReceived.set(true);
            }

            @Override
            public void onError(Throwable t) {
              phaseDecisions.offer(t);
              decisionReceived.set(true);
            }

            @Override
            public void onCompleted() {
              // The controller may close the phase stream right after sending a decision (e.g. a
              // final phase); only report a missing decision when nothing was sent for the phase.
              if (!decisionReceived.get()) {
                phaseDecisions.offer(
                    new IllegalStateException("controller closed phase stream without a decision"));
              }
            }
          };
      ctx.setValue(CONTROLLER_RESPONSE_KEY, responseObserver);
    }
    // Phases are resolved one at a time over this stream, so clear any decision received for a
    // previous phase before asking for the next one.
    decisionReceived.set(false);
    StreamObserver<ProcessRequest> sender = controllerStream(ctx, responseObserver);
    try {
      sender.onNext(
          ProcessRequest.newBuilder()
              .setSessionId(sessionID)
              .setPhase(phaseOf(phase))
              .setSnapshot(snapshot)
              .build());
    } catch (RuntimeException e) {
      abortStream(ctx, "failed to send phase update to controller", e);
      throw e;
    }
    try {
      Object next = phaseDecisions.poll(exchangeTimeoutSeconds, TimeUnit.SECONDS);
      if (next == null) {
        RuntimeException timeout =
            new RuntimeException("timed out waiting for controller decision");
        abortStream(ctx, "timed out waiting for controller decision", timeout);
        throw timeout;
      }
      if (next instanceof RuntimeException e) {
        clearControllerStreamState(ctx);
        throw e;
      }
      if (next instanceof Throwable t) {
        clearControllerStreamState(ctx);
        throw new RuntimeException("controller phase exchange failed", t);
      }
      return (ProcessResponse) next;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      abortStream(ctx, "interrupted waiting for controller decision", e);
      throw new RuntimeException("timed out waiting for controller decision", e);
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
    CallOptions callOptions =
        CallOptions.DEFAULT.withDeadlineAfter(exchangeTimeoutSeconds, TimeUnit.SECONDS);
    ClientCall<ProcessRequest, ProcessResponse> call = channel.newCall(method, callOptions);
    StreamObserver<ProcessRequest> sender =
        ClientCalls.asyncBidiStreamingCall(call, responseObserver);
    ctx.setValue(CONTROLLER_STREAM_KEY, sender);
    ctx.setValue(CONTROLLER_RESPONSE_KEY, responseObserver);
    ctx.setValue(CONTROLLER_CALL_KEY, call);
    return sender;
  }

  protected void applyDecision(RequestContext ctx, RequestCase phase, Continue continueDecision) {
    // Header mutations apply to headers and trailers phases only; body phases must not carry
    // HeaderMutations (for instance, the web UI keeps sticky set-headers across phases).
    // Disallowed HTTP/2 ':pseudo-headers' (such as :method, :path, :status, :authority echoed from
    // snapshots) are filtered out because Envoy rejects them under default HeaderMutationRules.
    if (isHeadersPhase(phase) || isTrailersPhase(phase)) {
      List<String> removable = filterMutableHeaderNames(continueDecision.getRemoveHeadersList());
      if (!removable.isEmpty()) {
        ctx.removeHeaders(removable);
      }
      Map<String, String> continueHeaders = filterMutableHeaders(continueDecision.getHeaders());
      for (Map.Entry<String, String> entry : continueHeaders.entrySet()) {
        ctx.overwriteHeader(entry.getKey(), entry.getValue());
      }
      if (isHeadersPhase(phase)) {
        ctx.addHeader(PROCESSED_HEADER, "true");
      }
    }
    if (continueDecision.hasBody()) {
      byte[] bodyBytes = continueDecision.getBody().toByteArray();
      if (isHeadersPhase(phase)) {
        // In envoy-extproc-sdk-java 0.3.1-alpha2+, replaceBody(byte[]) delegates 0-length arrays
        // to clearBody() and updates content-length when RequestContext is in a headers phase.
        // Envoy requires CONTINUE_AND_REPLACE to apply body mutations from a headers phase.
        ctx.replaceBody(bodyBytes);
        ctx.continueAndReplace();
      } else if (isBodyPhase(phase)) {
        // Envoy runs in BUFFERED mode, so body-phase mutations ride on a plain CONTINUE without
        // user header mutations, while still updating content-length (via replaceBody / clearBody)
        // so Envoy does not fail closed on a body/content-length mismatch.
        ctx.replaceBody(bodyBytes);
      }
      // Trailers phases ignore stray body mutations.
    }
  }

  protected boolean isMutableHeaderName(String name) {
    if (name == null) {
      return false;
    }
    String trimmed = name.trim();
    return !trimmed.isEmpty() && !trimmed.startsWith(":");
  }

  protected List<String> filterMutableHeaderNames(List<String> names) {
    List<String> filtered = new ArrayList<>();
    if (names == null) {
      return filtered;
    }
    for (String name : names) {
      if (isMutableHeaderName(name)) {
        filtered.add(name.trim());
      }
    }
    return filtered;
  }

  protected Map<String, String> filterMutableHeaders(Map<String, String> headers) {
    Map<String, String> filtered = new LinkedHashMap<>();
    if (headers == null) {
      return filtered;
    }
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (isMutableHeaderName(entry.getKey()) && entry.getValue() != null) {
        filtered.put(entry.getKey().trim(), entry.getValue());
      }
    }
    return filtered;
  }

  /**
   * Logs a phase decision received from the controller for the phase being resolved, including the
   * body mutation and header changes the decision carries.
   */
  protected void logPhaseDecisionReceived(RequestCase phase, Decision action) {
    String phaseName = phaseName(phase);
    switch (action.getActionCase()) {
      case CANCEL:
        Cancel cancel = action.getCancel();
        logger.info(
            "Demo processor phase decision received: phase="
                + phaseName
                + " action=CANCEL status="
                + cancel.getStatus()
                + " body="
                + cancel.getBody()
                + " headers="
                + cancel.getHeaders());
        break;
      case CONTINUE:
        Continue continueDecision = action.getContinue();
        logger.info(
            "Demo processor phase decision received: phase="
                + phaseName
                + " action=CONTINUE removeHeaders="
                + continueDecision.getRemoveHeadersList()
                + " bodyBytes="
                + (continueDecision.hasBody() ? continueDecision.getBody().size() : 0)
                + " headers="
                + continueDecision.getHeaders());
        break;
      default:
        logger.info(
            "Demo processor phase decision received: phase="
                + phaseName
                + " action="
                + action.getActionCase());
    }
  }

  /**
   * Logs the phase decision sent to envoy for the phase being resolved, derived from the
   * controller's continue decision (which drives the envoy phase response mutation).
   */
  protected void logPhaseDecisionSent(RequestCase phase, Continue continueDecision) {
    String phaseName = phaseName(phase);
    ResponseStatus envoyStatus =
        continueDecision.hasBody() && isHeadersPhase(phase)
            ? ResponseStatus.CONTINUE_AND_REPLACE
            : ResponseStatus.CONTINUE;
    logger.info(
        "Demo processor phase decision sent to envoy: phase="
            + phaseName
            + " action=CONTINUE status="
            + envoyStatus
            + " hasBody="
            + continueDecision.hasBody()
            + " bodyBytes="
            + (continueDecision.hasBody() ? continueDecision.getBody().size() : 0));
  }

  protected void retireStream(RequestContext ctx) {
    StreamObserver<ProcessRequest> sender =
        (StreamObserver<ProcessRequest>) ctx.getValue(CONTROLLER_STREAM_KEY);
    clearControllerStreamState(ctx);
    if (sender != null) {
      try {
        sender.onCompleted();
      } catch (Exception e) {
        // best effort half-close of the controller phase stream
      }
    }
  }

  protected void abortStream(RequestContext ctx, String reason, Throwable cause) {
    ClientCall<ProcessRequest, ProcessResponse> call =
        (ClientCall<ProcessRequest, ProcessResponse>) ctx.getValue(CONTROLLER_CALL_KEY);
    clearControllerStreamState(ctx);
    if (call != null) {
      try {
        call.cancel(reason, cause);
      } catch (Exception e) {
        // best effort cancel of the controller phase call
      }
    }
  }

  private void clearControllerStreamState(RequestContext ctx) {
    ctx.setValue(CONTROLLER_STREAM_KEY, null);
    ctx.setValue(CONTROLLER_RESPONSE_KEY, null);
    ctx.setValue(CONTROLLER_DECISIONS_KEY, null);
    ctx.setValue(CONTROLLER_DECISION_RECEIVED_KEY, null);
    ctx.setValue(CONTROLLER_CALL_KEY, null);
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

  protected boolean isHeadersPhase(RequestCase phase) {
    if (phase == null) {
      return false;
    }
    switch (phase) {
      case REQUEST_HEADERS:
      case RESPONSE_HEADERS:
        return true;
      default:
        return false;
    }
  }

  protected boolean isBodyPhase(RequestCase phase) {
    if (phase == null) {
      return false;
    }
    switch (phase) {
      case REQUEST_BODY:
      case RESPONSE_BODY:
        return true;
      default:
        return false;
    }
  }

  protected boolean isTrailersPhase(RequestCase phase) {
    if (phase == null) {
      return false;
    }
    switch (phase) {
      case REQUEST_TRAILERS:
      case RESPONSE_TRAILERS:
        return true;
      default:
        return false;
    }
  }

  protected boolean isResponsePhase(RequestCase phase) {
    if (phase == null) {
      return false;
    }
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

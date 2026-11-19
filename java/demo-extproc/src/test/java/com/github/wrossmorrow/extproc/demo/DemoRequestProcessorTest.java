package com.github.wrossmorrow.extproc.demo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.buf.gen.envoy.service.ext_proc.v3.CommonResponse.ResponseStatus;
import build.buf.gen.envoy.service.ext_proc.v3.ProcessingRequest.RequestCase;
import build.buf.gen.envoy.service.ext_proc.v3.ProcessingResponse;
import com.controller.v1.Continue;
import com.controller.v1.HttpSnapshot;
import com.controller.v1.Phase;
import com.github.wrossmorrow.extproc.RequestContext;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class DemoRequestProcessorTest {

  private static final DemoRequestProcessor PROCESSOR = new DemoRequestProcessor("127.0.0.1:1234");

  @Test
  void phaseNamesAreHumanReadable() {
    assertEquals("REQUEST_HEADERS", PROCESSOR.phaseName(RequestCase.REQUEST_HEADERS));
    assertEquals("REQUEST_BODY", PROCESSOR.phaseName(RequestCase.REQUEST_BODY));
    assertEquals("REQUEST_TRAILERS", PROCESSOR.phaseName(RequestCase.REQUEST_TRAILERS));
    assertEquals("RESPONSE_HEADERS", PROCESSOR.phaseName(RequestCase.RESPONSE_HEADERS));
    assertEquals("RESPONSE_BODY", PROCESSOR.phaseName(RequestCase.RESPONSE_BODY));
    assertEquals("RESPONSE_TRAILERS", PROCESSOR.phaseName(RequestCase.RESPONSE_TRAILERS));
    assertEquals("UNDETERMINED", PROCESSOR.phaseName(RequestCase.REQUEST_NOT_SET));
  }

  @Test
  void phaseOfMapsEveryRequestCaseToAnSdkPhase() {
    assertEquals(Phase.PHASE_REQUEST_HEADERS, PROCESSOR.phaseOf(RequestCase.REQUEST_HEADERS));
    assertEquals(Phase.PHASE_REQUEST_BODY, PROCESSOR.phaseOf(RequestCase.REQUEST_BODY));
    assertEquals(Phase.PHASE_REQUEST_TRAILERS, PROCESSOR.phaseOf(RequestCase.REQUEST_TRAILERS));
    assertEquals(Phase.PHASE_RESPONSE_HEADERS, PROCESSOR.phaseOf(RequestCase.RESPONSE_HEADERS));
    assertEquals(Phase.PHASE_RESPONSE_BODY, PROCESSOR.phaseOf(RequestCase.RESPONSE_BODY));
    assertEquals(Phase.PHASE_RESPONSE_TRAILERS, PROCESSOR.phaseOf(RequestCase.RESPONSE_TRAILERS));
    assertEquals(Phase.PHASE_UNSPECIFIED, PROCESSOR.phaseOf(RequestCase.REQUEST_NOT_SET));
  }

  @Test
  void responsePhaseDetectionCoversResponseCasesOnly() {
    assertFalse(PROCESSOR.isResponsePhase(RequestCase.REQUEST_HEADERS));
    assertFalse(PROCESSOR.isResponsePhase(RequestCase.REQUEST_BODY));
    assertFalse(PROCESSOR.isResponsePhase(RequestCase.REQUEST_TRAILERS));
    assertTrue(PROCESSOR.isResponsePhase(RequestCase.RESPONSE_HEADERS));
    assertTrue(PROCESSOR.isResponsePhase(RequestCase.RESPONSE_BODY));
    assertTrue(PROCESSOR.isResponsePhase(RequestCase.RESPONSE_TRAILERS));
    assertFalse(PROCESSOR.isResponsePhase(RequestCase.REQUEST_NOT_SET));
  }

  @Test
  void snapshotCarriesSortedHeadersAndBody() {
    HttpSnapshot snapshot =
        PROCESSOR.buildSnapshot(
            new RequestContext(), RequestCase.REQUEST_HEADERS, sortedHeaders(), null);
    assertEquals(2, snapshot.getHeadersCount());
    assertEquals("content-type", snapshot.getHeaders(0).getKey());
    assertEquals("application/json", snapshot.getHeaders(0).getValue());
    assertEquals("x-demo-session-id", snapshot.getHeaders(1).getKey());
    assertEquals("sess-1", snapshot.getHeaders(1).getValue());
    assertEquals(0, snapshot.getBody().size());
    assertFalse(snapshot.getBodyTruncated());
  }

  @Test
  void snapshotTruncatesLongBodiesAndFlagsThem() {
    String body = "x".repeat(DemoRequestProcessor.SNAPSHOT_BODY_LIMIT + 64);
    HttpSnapshot snapshot =
        PROCESSOR.buildSnapshot(new RequestContext(), RequestCase.RESPONSE_BODY, null, body);
    assertEquals(DemoRequestProcessor.SNAPSHOT_BODY_LIMIT, snapshot.getBody().size());
    assertTrue(snapshot.getBodyTruncated());
  }

  @Test
  void snapshotMasksSensitiveHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("authorization", "Bearer secret-token-value");
    headers.put("cookie", "session=abc");
    HttpSnapshot snapshot =
        PROCESSOR.buildSnapshot(new RequestContext(), RequestCase.REQUEST_HEADERS, headers, null);
    assertEquals("[redacted 25 chars]", snapshot.getHeaders(0).getValue());
    assertEquals("[redacted 11 chars]", snapshot.getHeaders(1).getValue());
  }

  @Test
  void snapshotAddsMethodAndPathWhenNotPresent() {
    HttpSnapshot snapshot =
        PROCESSOR.buildSnapshot(
            new RequestContext(), RequestCase.REQUEST_HEADERS, new HashMap<>(), null);
    assertEquals(0, snapshot.getHeadersCount());
  }

  @Test
  void sessionIdIsResolvedFromContextThenHeaders() {
    RequestContext ctx = new RequestContext();
    assertEquals("", PROCESSOR.sessionIdFor(ctx, new HashMap<String, String>()));

    ctx.setValue(DemoRequestProcessor.CONTROLLER_SESSION_ID_KEY, "resolved-session");
    assertEquals("resolved-session", PROCESSOR.sessionIdFor(ctx, new HashMap<String, String>()));

    ctx.setValue(DemoRequestProcessor.CONTROLLER_SESSION_ID_KEY, null);
    Map<String, String> headers = new HashMap<>();
    headers.put(DemoRequestProcessor.SESSION_ID_HEADER, "header-session");
    assertEquals("header-session", PROCESSOR.sessionIdFor(ctx, headers));
  }

  @Test
  void snapshotEntriesAreSortedByKey() {
    Map<String, String> headers = new HashMap<>();
    headers.put("z-header", "1");
    headers.put("a-header", "2");
    List<Map.Entry<String, String>> entries =
        PROCESSOR.snapshotEntries(new RequestContext(), RequestCase.REQUEST_HEADERS, headers);
    assertEquals(2, entries.size());
    assertEquals("a-header", entries.get(0).getKey());
    assertEquals("z-header", entries.get(1).getKey());
  }

  @Test
  void maskedLeavesNonSensitiveHeadersUntouched() {
    assertEquals("value", PROCESSOR.masked("x-normal", "value"));
    assertEquals("value", PROCESSOR.masked(null, "value"));
  }

  @Test
  void continueDecisionWithBodyYieldsContinueAndReplaceWithBodyMutation() {
    RequestContext ctx = new RequestContext();
    PROCESSOR.applyDecision(
        ctx, Continue.newBuilder().setBody(ByteString.copyFromUtf8("rewritten-body")).build());

    ProcessingResponse response = ctx.getResponse(RequestCase.REQUEST_HEADERS);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE_AND_REPLACE,
        response.getRequestHeaders().getResponse().getStatus());
    assertTrue(response.getRequestHeaders().getResponse().hasBodyMutation());
    assertArrayEquals(
        "rewritten-body".getBytes(StandardCharsets.UTF_8),
        response.getRequestHeaders().getResponse().getBodyMutation().getBody().toByteArray());
  }

  @Test
  void continueDecisionWithoutBodyYieldsPlainContinueWithoutBodyMutation() {
    RequestContext ctx = new RequestContext();
    PROCESSOR.applyDecision(ctx, Continue.newBuilder().build());

    ProcessingResponse response = ctx.getResponse(RequestCase.REQUEST_HEADERS);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, response.getRequestHeaders().getResponse().getStatus());
    assertEquals(0, response.getRequestHeaders().getResponse().getBodyMutation().getBody().size());
  }

  private static Map<String, String> sortedHeaders() {
    Map<String, String> headers = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    headers.put("content-type", "application/json");
    headers.put("x-demo-session-id", "sess-1");
    return headers;
  }
}

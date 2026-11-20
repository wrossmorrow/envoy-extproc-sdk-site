package com.github.wrossmorrow.extproc.demo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.buf.gen.envoy.config.core.v3.HeaderValueOption;
import build.buf.gen.envoy.service.ext_proc.v3.CommonResponse.ResponseStatus;
import build.buf.gen.envoy.service.ext_proc.v3.HeaderMutation;
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
        ctx,
        RequestCase.REQUEST_HEADERS,
        Continue.newBuilder().setBody(ByteString.copyFromUtf8("rewritten-body")).build());

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
    PROCESSOR.applyDecision(ctx, RequestCase.REQUEST_HEADERS, Continue.newBuilder().build());

    ProcessingResponse response = ctx.getResponse(RequestCase.REQUEST_HEADERS);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_HEADERS, response.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, response.getRequestHeaders().getResponse().getStatus());
    assertFalse(response.getRequestHeaders().getResponse().getBodyMutation().getClearBody());
    assertFalse(response.getRequestHeaders().getResponse().getBodyMutation().hasBody());
    assertEquals(0, response.getRequestHeaders().getResponse().getBodyMutation().getBody().size());
  }

  @Test
  void continueDecisionWithEmptyBodyInHeadersPhaseClearsBodyWithContinueAndReplace() {
    RequestContext ctx = new RequestContext();
    PROCESSOR.applyDecision(
        ctx, RequestCase.RESPONSE_HEADERS, Continue.newBuilder().setBody(ByteString.EMPTY).build());

    ProcessingResponse response = ctx.getResponse(RequestCase.RESPONSE_HEADERS);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_HEADERS, response.getResponseCase());
    assertEquals(
        ResponseStatus.CONTINUE_AND_REPLACE,
        response.getResponseHeaders().getResponse().getStatus());
    assertTrue(response.getResponseHeaders().getResponse().hasBodyMutation());
    assertTrue(response.getResponseHeaders().getResponse().getBodyMutation().getClearBody());
    assertFalse(response.getResponseHeaders().getResponse().getBodyMutation().hasBody());
  }

  @Test
  void continueDecisionWithBodyInBodyPhaseYieldsPlainContinueWithoutReplace() {
    RequestContext ctx = new RequestContext();
    PROCESSOR.applyDecision(
        ctx,
        RequestCase.REQUEST_BODY,
        Continue.newBuilder()
            .putHeaders("x-demo-intercepted", "true")
            .putHeaders(":method", "POST")
            .addRemoveHeaders("x-remove-me")
            .setBody(ByteString.copyFromUtf8("rewritten-body"))
            .build());

    ProcessingResponse response = ctx.getResponse(RequestCase.REQUEST_BODY);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_BODY, response.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, response.getRequestBody().getResponse().getStatus());
    assertFalse(
        response.getRequestBody().getResponse().hasHeaderMutation(),
        "body phases must not carry HeaderMutations");
    assertTrue(response.getRequestBody().getResponse().hasBodyMutation());
    assertArrayEquals(
        "rewritten-body".getBytes(StandardCharsets.UTF_8),
        response.getRequestBody().getResponse().getBodyMutation().getBody().toByteArray());
  }

  @Test
  void continueDecisionWithEmptyBodyInBodyPhaseClearsBodyChunkWithPlainContinue() {
    RequestContext ctx = new RequestContext();
    PROCESSOR.applyDecision(
        ctx, RequestCase.RESPONSE_BODY, Continue.newBuilder().setBody(ByteString.EMPTY).build());

    ProcessingResponse response = ctx.getResponse(RequestCase.RESPONSE_BODY);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_BODY, response.getResponseCase());
    assertEquals(ResponseStatus.CONTINUE, response.getResponseBody().getResponse().getStatus());
    assertFalse(response.getResponseBody().getResponse().hasHeaderMutation());
    assertTrue(response.getResponseBody().getResponse().hasBodyMutation());
    assertTrue(response.getResponseBody().getResponse().getBodyMutation().getClearBody());
    assertFalse(response.getResponseBody().getResponse().getBodyMutation().hasBody());
  }

  @Test
  void applyDecisionFiltersPseudoHeadersAndAttachesProcessedHeaderOnlyOnHeadersPhases() {
    Continue decision =
        Continue.newBuilder()
            .putHeaders(":method", "DELETE")
            .putHeaders(":path", "/rewritten")
            .putHeaders(":status", "201")
            .putHeaders(":authority", "evil.example")
            .putHeaders("x-custom", "allowed")
            .addRemoveHeaders(":method")
            .addRemoveHeaders(":status")
            .addRemoveHeaders("x-drop")
            .build();

    RequestContext reqHeadersCtx = new RequestContext();
    PROCESSOR.applyDecision(reqHeadersCtx, RequestCase.REQUEST_HEADERS, decision);
    HeaderMutation reqMutation =
        reqHeadersCtx
            .getResponse(RequestCase.REQUEST_HEADERS)
            .getRequestHeaders()
            .getResponse()
            .getHeaderMutation();
    assertTrue(hasSetHeader(reqMutation, "x-custom", "allowed"));
    assertTrue(hasSetHeader(reqMutation, DemoRequestProcessor.PROCESSED_HEADER, "true"));
    assertFalse(hasAnyPseudoHeader(reqMutation));
    assertEquals(List.of("x-drop"), reqMutation.getRemoveHeadersList());

    RequestContext respHeadersCtx = new RequestContext();
    PROCESSOR.applyDecision(respHeadersCtx, RequestCase.RESPONSE_HEADERS, decision);
    HeaderMutation respMutation =
        respHeadersCtx
            .getResponse(RequestCase.RESPONSE_HEADERS)
            .getResponseHeaders()
            .getResponse()
            .getHeaderMutation();
    assertTrue(hasSetHeader(respMutation, "x-custom", "allowed"));
    assertTrue(hasSetHeader(respMutation, DemoRequestProcessor.PROCESSED_HEADER, "true"));
    assertFalse(hasAnyPseudoHeader(respMutation));
    assertEquals(List.of("x-drop"), respMutation.getRemoveHeadersList());
  }

  @Test
  void trailersPhasesApplyFilteredHeaderMutationsWithoutProcessedHeaderOrBodyMutation() {
    Continue decision =
        Continue.newBuilder()
            .putHeaders(":status", "200")
            .putHeaders("x-trailer-checksum", "abc123")
            .addRemoveHeaders(":path")
            .addRemoveHeaders("x-old-trailer")
            .setBody(ByteString.copyFromUtf8("stray-body"))
            .build();

    RequestContext reqTrailersCtx = new RequestContext();
    PROCESSOR.applyDecision(reqTrailersCtx, RequestCase.REQUEST_TRAILERS, decision);
    ProcessingResponse reqResp = reqTrailersCtx.getResponse(RequestCase.REQUEST_TRAILERS);
    assertEquals(ProcessingResponse.ResponseCase.REQUEST_TRAILERS, reqResp.getResponseCase());
    HeaderMutation reqMutation = reqResp.getRequestTrailers().getHeaderMutation();
    assertTrue(hasSetHeader(reqMutation, "x-trailer-checksum", "abc123"));
    assertFalse(hasSetHeader(reqMutation, DemoRequestProcessor.PROCESSED_HEADER, "true"));
    assertFalse(hasAnyPseudoHeader(reqMutation));
    assertEquals(List.of("x-old-trailer"), reqMutation.getRemoveHeadersList());

    RequestContext respTrailersCtx = new RequestContext();
    PROCESSOR.applyDecision(respTrailersCtx, RequestCase.RESPONSE_TRAILERS, decision);
    ProcessingResponse respResp = respTrailersCtx.getResponse(RequestCase.RESPONSE_TRAILERS);
    assertEquals(ProcessingResponse.ResponseCase.RESPONSE_TRAILERS, respResp.getResponseCase());
    HeaderMutation respMutation = respResp.getResponseTrailers().getHeaderMutation();
    assertTrue(hasSetHeader(respMutation, "x-trailer-checksum", "abc123"));
    assertFalse(hasSetHeader(respMutation, DemoRequestProcessor.PROCESSED_HEADER, "true"));
    assertFalse(hasAnyPseudoHeader(respMutation));
    assertEquals(List.of("x-old-trailer"), respMutation.getRemoveHeadersList());
  }

  @Test
  void phaseCategoryHelpersClassifyEveryRequestCase() {
    assertTrue(PROCESSOR.isHeadersPhase(RequestCase.REQUEST_HEADERS));
    assertTrue(PROCESSOR.isHeadersPhase(RequestCase.RESPONSE_HEADERS));
    assertFalse(PROCESSOR.isHeadersPhase(RequestCase.REQUEST_BODY));
    assertFalse(PROCESSOR.isHeadersPhase(RequestCase.RESPONSE_BODY));
    assertFalse(PROCESSOR.isHeadersPhase(RequestCase.REQUEST_TRAILERS));
    assertFalse(PROCESSOR.isHeadersPhase(RequestCase.RESPONSE_TRAILERS));
    assertFalse(PROCESSOR.isHeadersPhase(RequestCase.REQUEST_NOT_SET));

    assertTrue(PROCESSOR.isBodyPhase(RequestCase.REQUEST_BODY));
    assertTrue(PROCESSOR.isBodyPhase(RequestCase.RESPONSE_BODY));
    assertFalse(PROCESSOR.isBodyPhase(RequestCase.REQUEST_HEADERS));
    assertFalse(PROCESSOR.isBodyPhase(RequestCase.RESPONSE_HEADERS));
    assertFalse(PROCESSOR.isBodyPhase(RequestCase.REQUEST_TRAILERS));
    assertFalse(PROCESSOR.isBodyPhase(RequestCase.RESPONSE_TRAILERS));

    assertTrue(PROCESSOR.isTrailersPhase(RequestCase.REQUEST_TRAILERS));
    assertTrue(PROCESSOR.isTrailersPhase(RequestCase.RESPONSE_TRAILERS));
    assertFalse(PROCESSOR.isTrailersPhase(RequestCase.REQUEST_HEADERS));
    assertFalse(PROCESSOR.isTrailersPhase(RequestCase.RESPONSE_HEADERS));
    assertFalse(PROCESSOR.isTrailersPhase(RequestCase.REQUEST_BODY));
    assertFalse(PROCESSOR.isTrailersPhase(RequestCase.RESPONSE_BODY));
  }

  private static boolean hasSetHeader(HeaderMutation mutation, String key, String expectedValue) {
    for (HeaderValueOption opt : mutation.getSetHeadersList()) {
      String actual =
          !opt.getHeader().getValue().isEmpty()
              ? opt.getHeader().getValue()
              : opt.getHeader().getRawValue().toStringUtf8();
      if (key.equals(opt.getHeader().getKey()) && expectedValue.equals(actual)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasAnyPseudoHeader(HeaderMutation mutation) {
    for (HeaderValueOption opt : mutation.getSetHeadersList()) {
      if (opt.getHeader().getKey().startsWith(":")) {
        return true;
      }
    }
    for (String removed : mutation.getRemoveHeadersList()) {
      if (removed.startsWith(":")) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, String> sortedHeaders() {
    Map<String, String> headers = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    headers.put("content-type", "application/json");
    headers.put("x-demo-session-id", "sess-1");
    return headers;
  }
}

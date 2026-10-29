# Envoy ExtProc Interactive Demo — Plan

> **Status: draft.** Living document. Will change. Not yet implemented.

## Goal

A demonstration website where a human, through the browser, drives an Envoy
`ext_proc` processor phase by phase, mutating the request as it goes, and
choosing between `continue` (forward the mutated request downstream) and
`cancel` (short-circuit with an immediate response).

## Decisions made so far

1. **Failure mode: fail-closed.** If the demo processor fails during a phase,
   the request should fail rather than continue. Continuing past failure would
   make for a confusing demo. (Corresponds to the SDK's
   `ProcessingOptions.AbortOnProcessorFailure` and an Envoy
   `failure_mode_allow: false`.)

2. **Timeouts: configurable, higher.** The Envoy ext_proc example config ships
   with a very short `message_timeout`; interactive human-in-the-loop processing
   needs a much larger, configurable value (their own test setup raised it to 60s
   for delayed phases). The phase processors should also know the timeout and be
   able to issue an immediate response rather than waiting for the transport
   timeout to fire.

3. **Phase scope: request phases first.** Start with the request phase set
   (`REQUEST_HEADERS`, `REQUEST_BODY`, …), possibly starting with just request
   header interception to keep scope controlled; body-phase mutation is a
   natural follow-on. Response phases may come later if the demo proves
   compelling.

4. **Session model: controller-owned.** The website asks a controller service
   for a session id first; the browser's Envoy-bound HTTP requests then carry
   that session id (via a header the extproc reads). The extproc is
   reader-only; the controller owns session state. The controller may fail a
   request (e.g. same-IP collision) when a session is already in flight, and the
   extproc can respond `409` for requests whose session id is in flight.

5. **Mutations / responses:** add / overwrite / remove headers; `continue` with
   mutations, or `cancel` with a status code, headers, and body.

6. **Frontend / deployment topology:** static assets served via CloudFront;
   APIs via same-site CloudFront behaviors or CORS in Envoy. Envoy assumes HTTP
   internally and never supports public HTTP; TLS terminates at the load
   balancer. Docker compose on EC2.

## Proposed architecture

```
browser (static site, CloudFront) ──TLS──▶ ALB/CloudFront ──▶ Envoy (HTTP internal) ──▶ extproc (Go, SDK)
                                                                                       │
                                                                                       ▼
                                              controller (Go, Connect RPC) ◀── phase updates / decisions
```

- **Echo backend**: `simple-echo-server`, unmodified. Envoy upstream. Provides
  the JSON echo, `X-Echo-*` delay/status knobs, CORS-friendly responses.
- **Envoy**: ext_proc filter with
  `processing_mode: {request_header_mode: SEND, request_body_mode: BUFFERED,
  ...}`, raised `message_timeout`, `failure_mode_allow: false`,
  `mutation_rules: {allow_all_routing: true}` (if `:authority` rewrites are used).
  `/no-extproc` bypass route. Admin/stats ports. Envoy assumes HTTP internally.
- **Demo extproc**: Go, built on `envoy-extproc-sdk-go`. Phase handlers block
  waiting on the human decision; then `ContinueRequest()` / `CancelRequest(...)`.
  Fail-closed. On phase timeout, issue immediate response (proposal: status `504`;
  open).
- **Controller**: owns sessions. RPCs under discussion: `CreateSession`,
  `GetSession`, and a decision-submission mechanism (possibly aligned with
  `CreateSession`, e.g. a streaming RPC). Phase feedback flows processor →
  controller → browser. Decisions flow browser → controller → processor. Open
  question on the exact RPC shape.

## Open questions

- **Controller RPC shape**: `CreateSession`, `GetSession`, and … `StreamRequest`
  (or aligned with `CreateSession`) streaming for phase updates and accepting
  mutations? I suspect a simpler v1 surface is Create/Get + explicit decision
  submission, since phases are human-paced and processor-blocked. Need a
  decision here.
- **Timeout semantics**: processor-side enforcement (phase handler waits on
  decision channel with a deadline, then sends immediate response) vs controller
  TTL. Suitable timeout status code (proposal: `504`) still open.
- **Scope**: v1 = request headers interception only, or headers + body phases?
  (Body mutation ops were accepted in principle; scope control suggests starting
  minimal.)
- **In-flight session policy**: controller may 409 same-IP concurrent sessions;
  exact policy open.
- **Response phases**: deferred for now.

## Milestones

- **M0**: `PLAN.md` (this document).
- **M1**: Demo processor implementing request-headers-phase interception
  (minimal), `Continue`/`Cancel`, fail-closed, timeout → immediate response. Wire
  Envoy + echo; verify end-to-end with `curl`.
- **M2**: Body-phase handling and header/body mutations, cancel-with-body.
- **M3**: Controller + Connect service + website wiring (phase feedback, mutation
  editing, continue/cancel UI).
- **M4**: Static site polish, deploy via docker compose to EC2, CloudFront/ALB
  topology.

## Decisions log

(to be appended as the plan solidifies)

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
   for delayed phases). Refined: the phase timeout is configurable (proposal:
   default 30s), applied per-phase; the processor arms a phase deadline itself
   when entering a phase and, on expiry, issues an immediate response rather than
   waiting for the transport timeout to fire. Envoy `message_timeout` should be
   set to accommodate the phase timeout.

3. **Phase scope: request headers first (v1).** Start with request-header
   interception only. Body-phase handling and mutation are follow-ons. Response
   phases may come later if the demo proves compelling.

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

7. **Controller design (refined):** browser-facing uses server-streaming
   `GetUpdates` + unified `UpdateRequest` decision RPC (action oneof), no browser
   bidi; controller↔extproc uses plain gRPC bidi internally. See
   [Controller design (refined)](#controller-design-refined).

8. **Privacy: session binding by hashed source IP.** Tie a session id to a source
   IP via hash (e.g. SHA-256 of the IP string) so no PII is stored. Requires an
   LB that sets `X-Forwarded-For` (e.g. ALB) since envoy sits behind the LB. The
   binding lets the controller validate that updates correspond to the session's
   bound source IP and reject mismatches. (The exact rejection semantics for
   "cross origin"/mismatched updates are still to be nailed down.)

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
- **Controller**: owns sessions. RPCs: `CreateSession`, `GetSession`,
  `GetUpdates` (server-streaming), and unified decision RPC `UpdateRequest`
  (action: `CONTINUE` or `CANCEL` via oneof; cancel carries status/headers/body).
  Phase feedback flows processor → controller → browser. Decisions flow browser →
  controller → processor. (Unified decision shape now settled; full RPC surface
  still open.)

## Controller design (refined)

- **Browser ↔ controller:** no bidi. Browser-facing channel uses server-streaming
  (`GetUpdates`) plus a single decision RPC, `UpdateRequest`, carrying an action
  (`CONTINUE` / `CANCEL`) via a oneof (cancel carries status/headers/body).
  Connect supports bidi, but the protocol requires HTTP/2 for bidi while its
  browser-friendly core is HTTP/1.1; server-streaming/client-streaming/unary all
  work over HTTP/1.1. So: Connect server-streaming for phase updates, decisions
  submitted as the unified `UpdateRequest`. (SSE is the fallback transport for
  server→client streaming; Connect server-streaming is the modern equivalent and
  fits the HTTP/1.1 story.)
- **Controller ↔ extproc:** plain gRPC bidi (Go server ↔ Go client, h2c on the
  compose network) is acceptable and desirable. Phase reports and decisions ride
  the bidi stream; the extproc blocks on the stream while holding the Envoy
  phase; the controller streams phase updates to the browser via `GetUpdates` and
  delivers decisions back through the stream. Extproc closes the stream on
  completion; controller marks the session complete and ends the `GetUpdates`
  stream. Timeout: processor-side deadline on the wait → immediate response
  (proposal: status `504`).
- **Create/GetUpdates flow:** `CreateSession` returns a session id; the browser
  then opens `GetUpdates(session_id)` (server-streaming) and holds it while the
  envoy-bound request (carrying the session id) is in flight. Feasible and
  coherent. Keeping the two-RPC shape (Create → id, then GetUpdates) is the
  preferred design; folding the stream into `CreateSession` would mean client
  streaming and is not preferred.

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

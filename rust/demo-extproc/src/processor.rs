//! Interactive demo extproc processor.

use std::time::Duration;

use async_trait::async_trait;
use envoy_extproc_sdk::context::{
    build_header_values_from_map, request_phase_to_string, HeaderValue, RequestContext,
    RequestPhase,
};
use envoy_extproc_sdk::headers::AllHeaders;
use envoy_extproc_sdk::{is_stream_cancelled, Error, ProcessingOptions, RequestProcessor};

use crate::controller::v1::decision::Action;
use crate::controller::v1::demo_processor_service_client::DemoProcessorServiceClient;
use crate::controller::v1::{Decision, Header, HttpSnapshot, ProcessRequest, ProcessResponse};

const SESSION_ID_HEADER: &str = "x-demo-session-id";
const PROCESSED_HEADER: &str = "x-demo-processed";
const SNAPSHOT_BODY_LIMIT: usize = 16 * 1024;
const DEFAULT_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(300);

/// Controller phase-stream handle for the current exchange, held in the
/// request context so later phases of the same exchange reuse it.
#[derive(Clone, Debug)]
pub struct ControllerPhaseStream {
    pub tx: tokio::sync::mpsc::Sender<ProcessRequest>,
    pub response: std::sync::Arc<tokio::sync::Mutex<tonic::codec::Streaming<ProcessResponse>>>,
}

#[derive(Clone, Debug)]
pub struct ControllerExchange {
    pub stream: Option<ControllerPhaseStream>,
}

pub struct DemoRequestProcessor {
    controller_addr: String,
    options: ProcessingOptions,
    exchange_timeout: Duration,
    controller:
        std::sync::Arc<tokio::sync::Mutex<Option<tonic::client::Grpc<tonic::transport::Channel>>>>,
}

impl DemoRequestProcessor {
    pub fn new(controller_addr: &str) -> Self {
        Self {
            controller_addr: Self::controller_endpoint(controller_addr),
            options: ProcessingOptions {
                abort_on_processor_failure: true,
                ..Default::default()
            },
            exchange_timeout: DEFAULT_EXCHANGE_TIMEOUT,
            controller: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Normalize a controller gRPC endpoint for the tonic client. tonic
    /// endpoint URLs require a scheme (e.g. `http://controller:9093`); a bare
    /// host such as `controller:9093` is rejected with `invalid URL, scheme is
    /// missing`, so the controller phase stream can never be opened and the
    /// controller is never contacted. Prefix the default scheme when the
    /// address does not carry one.
    fn controller_endpoint(addr: &str) -> String {
        if addr.contains("://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        }
    }

    /// Return the controller phase stream for the current exchange, opening a
    /// fresh one when the request context has none. The handle is stored in the
    /// request context (which persists across phases of an exchange), scoping it
    /// to the exchange rather than the processor instance. The underlying
    /// controller connection is shared, not per exchange.
    async fn controller_stream(
        &self,
        ctx: &mut RequestContext,
    ) -> Result<Option<ControllerPhaseStream>, Error> {
        if let Ok(Some(handle)) = ctx.get_value::<Option<ControllerPhaseStream>>() {
            return Ok(Some(handle.clone()));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        eprintln!(
            "[demo-extproc] opening controller phase stream at {} for session exchange",
            self.controller_addr
        );
        let mut client = DemoProcessorServiceClient::connect(self.controller_addr.clone())
            .await
            .map_err(|e| {
                eprintln!(
                    "[demo-extproc] failed to connect to controller at {}: {}",
                    self.controller_addr, e
                );
                format!("failed to connect to controller: {}", e)
            })?;
        let response = client
            .process(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .map_err(|e| {
                eprintln!(
                    "[demo-extproc] controller phase stream to {} failed: {}",
                    self.controller_addr, e
                );
                Error::from(e)
            })?
            .into_inner();
        let handle = ControllerPhaseStream {
            tx,
            response: std::sync::Arc::new(tokio::sync::Mutex::new(response)),
        };
        ctx.set_value::<Option<ControllerPhaseStream>>(Some(handle.clone()))?;
        Ok(Some(handle))
    }

    /// Send a phase update carrying the phase snapshot to the controller for
    /// the session and wait for the controller's decision. Returns the decision
    /// (continue/cancel), if any.
    async fn send_phase_and_decide(
        &self,
        ctx: &mut RequestContext,
        phase: RequestPhase,
        session_id: String,
        snapshot: HttpSnapshot,
    ) -> Result<Option<Decision>, Error> {
        let handle = self.controller_stream(ctx).await?;
        let handle = match handle {
            Some(h) => h,
            None => return Ok(None),
        };
        let outcome = tokio::time::timeout(self.exchange_timeout, async {
            eprintln!(
                "[demo-extproc] phase={} session_id={} sending phase update to controller",
                request_phase_to_string(phase),
                session_id
            );
            handle
                .tx
                .send(ProcessRequest {
                    session_id: session_id.clone(),
                    phase: controller_phase(phase),
                    snapshot: Some(snapshot),
                })
                .await
                .map_err(|e| {
                    eprintln!(
                        "[demo-extproc] phase={} session_id={} failed to send phase update: {}",
                        request_phase_to_string(phase),
                        session_id,
                        e
                    );
                    Error::from(e)
                })?;
            let mut response = handle.response.lock().await;
            let maybe = response.message().await.map_err(|e| {
                eprintln!(
                    "[demo-extproc] phase={} session_id={} failed to receive controller decision: {}",
                    request_phase_to_string(phase),
                    session_id,
                    e
                );
                Error::from(e.clone())
            });
            let decision = match maybe {
                Ok(Some(resp)) => resp.decision,
                _ => None,
            };
            eprintln!(
                "[demo-extproc] phase={} session_id={} decision={}",
                request_phase_to_string(phase),
                session_id,
                decision.is_some()
            );
            Ok::<Option<Decision>, Error>(decision)
        })
        .await;
        let decision: Option<Decision> = match outcome {
            Ok(result) => result?,
            Err(_elapsed) => {
                eprintln!(
                    "[demo-extproc] phase={} session_id={} timed out waiting for controller decision",
                    request_phase_to_string(phase),
                    session_id
                );
                return Err("timed out waiting for controller decision"
                    .to_string()
                    .into());
            }
        };
        if ctx.end_of_stream && is_response_phase(phase) {
            // The phase signaled end-of-stream during a response phase, which
            // marks the end of the direction the demo retires on. End-of-stream
            // marks the end of a single direction (e.g. request headers for a
            // bodyless GET), not the end of the exchange, so retire only for
            // response phases; otherwise keep the handle in the request context
            // so the next phase of the same exchange reuses it.
            self.retire_stream(ctx);
        }
        Ok(decision)
    }

    /// Clean up the controller phase-stream handle held in the request context.
    /// Closing the send direction and dropping the response stream releases the
    /// exchange phase stream.
    fn retire_stream(&self, ctx: &mut RequestContext) {
        ctx.set_value::<Option<ControllerPhaseStream>>(None)
            .expect("failed to retire controller stream");
    }

    /// Resolve the session id for an incoming phase from the request context
    /// (falling back to the request headers). Resolution is per request context,
    /// so concurrent exchanges do not clobber each other.
    fn session_id_for(&self, ctx: &mut RequestContext, headers: &AllHeaders) -> String {
        if let Ok(session_id) = ctx.get_value::<String>() {
            if !session_id.is_empty() {
                return session_id.clone();
            }
        }
        if let Some(id) = headers.get(SESSION_ID_HEADER) {
            if !id.is_empty() {
                ctx.set_value::<String>(id.clone())
                    .expect("failed to store controller session id");
                return id.clone();
            }
        }
        String::new()
    }

    /// Build the HttpSnapshot the demo processor reports to the controller for
    /// the phase being processed, from the observed headers and body. Body
    /// phases report at most SNAPSHOT_BODY_LIMIT bytes; longer bodies are
    /// truncated and flagged via body_truncated. Response phases surface the
    /// response status via the ":status" header the data plane sends
    /// downstream. Headers are emitted in sorted key order for deterministic
    /// snapshots.
    fn snapshot(
        &self,
        ctx: &RequestContext,
        headers: &AllHeaders,
        body: Option<&[u8]>,
    ) -> HttpSnapshot {
        let mut snapshot = HttpSnapshot {
            headers: Vec::new(),
            body: Vec::new(),
            body_truncated: false,
            status: 0,
        };
        let mut names: Vec<&String> = headers.headers.keys().collect();
        names.sort();
        for name in names {
            let Some(values) = headers.headers.get(name) else {
                continue;
            };
            for value in values {
                let value = if is_masked(name) {
                    format!("[redacted {} chars]", value.len())
                } else {
                    value.clone()
                };
                snapshot.headers.push(Header {
                    key: name.clone(),
                    value,
                });
            }
        }
        if let Some(status) = headers.get(":status") {
            if let Ok(code) = status.parse::<i32>() {
                snapshot.status = code;
            }
        }
        if !ctx.method.is_empty() && !snapshot.headers.iter().any(|h| h.key == ":method") {
            snapshot.headers.push(Header {
                key: ":method".to_string(),
                value: ctx.method.clone(),
            });
        }
        if !ctx.path.is_empty() && !snapshot.headers.iter().any(|h| h.key == ":path") {
            snapshot.headers.push(Header {
                key: ":path".to_string(),
                value: ctx.path.clone(),
            });
        }
        if let Some(body) = body {
            if body.len() > SNAPSHOT_BODY_LIMIT {
                snapshot.body = body[..SNAPSHOT_BODY_LIMIT].to_vec();
                snapshot.body_truncated = true;
            } else {
                snapshot.body = body.to_vec();
            }
        }
        snapshot
    }

    /// Send a phase update carrying the phase snapshot to the controller for
    /// the session and apply the controller's decision (continue mutations or
    /// cancel) to the phase context. Returns any error from sending the phase
    /// update or applying the decision.
    async fn resolve_phase(
        &self,
        ctx: &mut RequestContext,
        phase: RequestPhase,
        headers: &AllHeaders,
        body: Option<&[u8]>,
    ) -> Result<(), Error> {
        let session_id = self.session_id_for(ctx, headers);
        let snapshot = self.snapshot(ctx, headers, body);
        log_phase_sent(phase, &session_id, &snapshot);
        let decision = self
            .send_phase_and_decide(ctx, phase, session_id, snapshot)
            .await?;
        let Some(decision) = decision else {
            // The controller closed the phase stream without a decision; the
            // phase completes without one.
            return Ok(());
        };
        if let Some(action) = decision.action {
            match action {
                Action::Continue(continue_decision) => {
                    if !continue_decision.remove_headers.is_empty() {
                        ctx.remove_headers(&continue_decision.remove_headers)?;
                    }
                    if let Some(body) = &continue_decision.body {
                        ctx.replace_body(body)?;
                    }
                    let header_values = build_header_values_from_map(&continue_decision.headers);
                    for (name, value) in header_values {
                        ctx.overwrite_header(&name, &value)?;
                    }
                    ctx.add_header(
                        PROCESSED_HEADER,
                        &HeaderValue {
                            value: "true".to_string(),
                            raw_value: Some(b"true".to_vec()),
                        },
                    )?;
                    ctx.continue_request()?;
                }
                Action::Cancel(cancel) => {
                    // A cancel ends the exchange, so retire the controller phase
                    // stream before surfacing the cancellation to the phase context.
                    self.retire_stream(ctx);
                    ctx.cancel_request(
                        cancel.status,
                        &build_header_values_from_map(&cancel.headers),
                        cancel.body.as_bytes(),
                    )?;
                }
            }
        }
        Ok(())
    }
}

/// Log the phase update sent to the controller for the current exchange phase,
/// including the snapshot reported for the phase, so the phase flow observed by
/// the demo extproc is visible in the extproc logs.
fn log_phase_sent(phase: RequestPhase, session_id: &str, snapshot: &HttpSnapshot) {
    eprintln!(
        "[demo-extproc] phase={} controller_phase={} session_id={} headers={} body_bytes={} status={} body_truncated={}",
        request_phase_to_string(phase),
        controller_phase(phase),
        session_id,
        snapshot.headers.len(),
        snapshot.body.len(),
        snapshot.status,
        snapshot.body_truncated,
    );
}

/// Whether the given phase is a response phase. Only response phases (headers,
/// body, trailers) mark the end of the direction the demo retires on, so
/// end-of-stream is only honored for them.
fn is_response_phase(phase: RequestPhase) -> bool {
    matches!(
        phase,
        RequestPhase::ResponseHeaders | RequestPhase::ResponseBody | RequestPhase::ResponseTrailers
    )
}

/// Whether a snapshot header value should be masked (cookie, set-cookie,
/// authorization).
fn is_masked(name: &str) -> bool {
    matches!(name, "cookie" | "set-cookie" | "authorization")
}

/// Translate the Rust SDK phase to the phase number the controller expects in
/// the `ProcessRequest.phase` field. The controller's Phase enum is aligned
/// with the envoy/ext_proc phase numbering, and the Rust SDK numbers phases
/// the same way (Undetermined = 0 for the pre-phase, then the request/response
/// phases), so the SDK discriminant is the phase number the controller expects.
fn controller_phase(phase: RequestPhase) -> i32 {
    match phase {
        RequestPhase::Undetermined => 0,
        RequestPhase::RequestHeaders => 1,
        RequestPhase::RequestBody => 2,
        RequestPhase::RequestTrailers => 3,
        RequestPhase::ResponseHeaders => 4,
        RequestPhase::ResponseBody => 5,
        RequestPhase::ResponseTrailers => 6,
    }
}

#[async_trait]
impl RequestProcessor for DemoRequestProcessor {
    fn name(&self) -> String {
        "demo".to_string()
    }

    fn options(&self) -> &ProcessingOptions {
        &self.options
    }

    async fn process_request_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error> {
        self.resolve_phase(ctx, RequestPhase::RequestHeaders, headers, None)
            .await
    }

    async fn process_request_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        self.resolve_phase(
            ctx,
            RequestPhase::RequestBody,
            &ctx.all_headers.clone(),
            Some(body),
        )
        .await
    }

    async fn process_request_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        self.resolve_phase(ctx, RequestPhase::RequestTrailers, trailers, None)
            .await
    }

    async fn process_response_headers(
        &self,
        ctx: &mut RequestContext,
        headers: &AllHeaders,
    ) -> Result<(), Error> {
        self.resolve_phase(ctx, RequestPhase::ResponseHeaders, headers, None)
            .await
    }

    async fn process_response_body(
        &self,
        ctx: &mut RequestContext,
        body: &[u8],
    ) -> Result<(), Error> {
        self.resolve_phase(
            ctx,
            RequestPhase::ResponseBody,
            &ctx.all_headers.clone(),
            Some(body),
        )
        .await
    }

    async fn process_response_trailers(
        &self,
        ctx: &mut RequestContext,
        trailers: &AllHeaders,
    ) -> Result<(), Error> {
        self.resolve_phase(ctx, RequestPhase::ResponseTrailers, trailers, None)
            .await
    }

    async fn error_handler(&self, ctx: &mut RequestContext, phase: RequestPhase, err: Error) {
        if is_stream_cancelled(Some(&err)) {
            // Envoy tore the downstream down and cancelled the extproc stream;
            // treat it as the cancellation signal for this exchange phase and
            // clean up the controller stream handle held in the request context.
            self.retire_stream(ctx);
            return;
        }
        // Surface the processing failure. The SDK aborts the stream when
        // abort_on_processor_failure is set (the demo's default), so a failed
        // phase fails the exchange closed rather than continuing. Phase
        // failures returned from the phase handlers are logged there and flow
        // through the phase error path; this handler reports stream-level
        // failures the SDK itself detects.
        eprintln!(
            "[demo-extproc] processing error: phase={} error={}",
            request_phase_to_string(phase),
            err
        );
    }

    fn close(&self, _grace_period_seconds: i32) -> Result<(), Error> {
        // `close` is a synchronous trait method, so it cannot block the runtime
        // thread; take the controller connection if it is free, else leave it.
        if let Ok(mut guard) = self.controller.try_lock() {
            *guard = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_response_phase() {
        assert!(is_response_phase(RequestPhase::ResponseHeaders));
        assert!(is_response_phase(RequestPhase::ResponseBody));
        assert!(is_response_phase(RequestPhase::ResponseTrailers));
        assert!(!is_response_phase(RequestPhase::RequestHeaders));
        assert!(!is_response_phase(RequestPhase::RequestBody));
        assert!(!is_response_phase(RequestPhase::RequestTrailers));
        assert!(!is_response_phase(RequestPhase::Undetermined));
    }

    #[test]
    fn test_is_masked() {
        assert!(is_masked("cookie"));
        assert!(is_masked("set-cookie"));
        assert!(is_masked("authorization"));
        assert!(!is_masked("x-demo-session-id"));
        assert!(!is_masked("content-type"));
    }

    fn all_headers(entries: &[(&str, &str)]) -> AllHeaders {
        let mut headers = AllHeaders::default();
        for (key, value) in entries {
            headers
                .headers
                .insert(key.to_lowercase(), vec![value.to_string()]);
            headers
                .raw_headers
                .insert(key.to_lowercase(), vec![value.as_bytes().to_vec()]);
        }
        headers
    }

    #[test]
    fn test_snapshot_captures_status_and_body() {
        let processor = DemoRequestProcessor::new("controller:9093");
        let ctx = RequestContext::new();
        let headers = all_headers(&[(":method", "GET"), (":path", "/foo")]);
        let snapshot = processor.snapshot(&ctx, &headers, Some(b"hello"));
        assert_eq!(snapshot.status, 0);
        assert_eq!(snapshot.body.as_slice(), b"hello".as_slice());
        assert!(!snapshot.body_truncated);
        assert_eq!(snapshot.headers.len(), 2);
    }

    #[test]
    fn test_snapshot_masks_and_sorts_headers() {
        let processor = DemoRequestProcessor::new("controller:9093");
        let ctx = RequestContext::new();
        let headers = all_headers(&[
            (":method", "POST"),
            (":path", "/foo"),
            ("authorization", "Bearer secret"),
        ]);
        let snapshot = processor.snapshot(&ctx, &headers, None);
        assert_eq!(snapshot.headers.len(), 3);
        // Sorted by key, and masked values are redacted rather than stored raw.
        assert_eq!(snapshot.headers[0].key, ":method");
        assert_eq!(snapshot.headers[1].key, ":path");
        assert_eq!(snapshot.headers[2].key, "authorization");
        assert_eq!(snapshot.headers[2].value, "[redacted 13 chars]");
        assert_eq!(snapshot.body, Vec::<u8>::new());
    }

    #[test]
    fn test_snapshot_truncates_oversized_bodies() {
        let processor = DemoRequestProcessor::new("controller:9093");
        let ctx = RequestContext::new();
        let headers = AllHeaders::default();
        let body = vec![0u8; SNAPSHOT_BODY_LIMIT + 1];
        let snapshot = processor.snapshot(&ctx, &headers, Some(&body));
        assert_eq!(snapshot.body.len(), SNAPSHOT_BODY_LIMIT);
        assert!(snapshot.body_truncated);
    }

    #[test]
    fn test_session_id_from_context_value() {
        let processor = DemoRequestProcessor::new("controller:9093");
        let mut ctx = RequestContext::new();
        assert_eq!(
            processor.session_id_for(&mut ctx, &AllHeaders::default()),
            ""
        );
        ctx.set_value::<String>("sess-1".to_string())
            .expect("failed to store controller session id");
        assert_eq!(
            processor.session_id_for(&mut ctx, &AllHeaders::default()),
            "sess-1"
        );
    }

    #[test]
    fn test_session_id_from_headers() {
        let processor = DemoRequestProcessor::new("controller:9093");
        let mut ctx = RequestContext::new();
        let headers = all_headers(&[("x-demo-session-id", "sess-2")]);
        assert_eq!(processor.session_id_for(&mut ctx, &headers), "sess-2");
    }
}

#[test]
fn test_controller_phase_maps_sdk_phases() {
    assert_eq!(controller_phase(RequestPhase::Undetermined), 0);
    assert_eq!(controller_phase(RequestPhase::RequestHeaders), 1);
    assert_eq!(controller_phase(RequestPhase::RequestBody), 2);
    assert_eq!(controller_phase(RequestPhase::RequestTrailers), 3);
    assert_eq!(controller_phase(RequestPhase::ResponseHeaders), 4);
    assert_eq!(controller_phase(RequestPhase::ResponseBody), 5);
    assert_eq!(controller_phase(RequestPhase::ResponseTrailers), 6);
}

"use strict";

// Interactive envoy extproc demo frontend.
//
// Talks to the human-facing controller Connect API (CreateSession stream of
// phase updates, unary SubmitDecision) and sends demo requests through the
// same-origin /envoy/ proxy, which relays them to the Envoy instance fronting
// the demo extproc.
//
// Forward compatible with richer phase updates: if an update carries a
// `snapshot` ({headers: [{key, value}], body (base64), status}) or a
// `deadline` (RFC 3339), the intercept panel shows the message contents and a
// server-accurate countdown. Without them it falls back to phase-only display
// and a client-side countdown of FALLBACK_PHASE_TIMEOUT_MS.

const CONNECT_STREAM_CONTENT_TYPE = "application/connect+json";
const CONNECT_UNARY_CONTENT_TYPE = "application/json";
const CONNECT_PROTOCOL_VERSION = "1";
const CONTROLLER_BASE = "/controller.v1.DemoControllerService";
// Base URL for demo requests to Envoy. Same-origin "/envoy" by default (path
// routed to Envoy by the LB/CDN); override with <meta name="envoy-base"
// content="https://envoy.example.com"> to call Envoy cross-origin (needs CORS).
const ENVOY_BASE = (document.querySelector('meta[name="envoy-base"]') || {}).content || "/envoy";
const SESSION_HEADER = "x-demo-session-id";

// Mirrors controller.PhaseTimeout; only used when updates carry no deadline.
const FALLBACK_PHASE_TIMEOUT_MS = 30000;

const AVAILABLE_LANGS = { go: true, rust: false, java: false, python: false };
const LANG_NAMES = { go: "Go", rust: "Rust", java: "Java", python: "Python" };

const PHASE_NOT_STARTED = "PHASE_NOT_STARTED";
const PHASE_LABELS = {
  PHASE_REQUEST_HEADERS: "request headers",
  PHASE_REQUEST_BODY: "request body",
  PHASE_REQUEST_TRAILERS: "request trailers",
  PHASE_RESPONSE_HEADERS: "response headers",
  PHASE_RESPONSE_BODY: "response body",
  PHASE_RESPONSE_TRAILERS: "response trailers",
};
const REQUEST_PHASES = new Set(["PHASE_REQUEST_HEADERS", "PHASE_REQUEST_BODY", "PHASE_REQUEST_TRAILERS"]);
const RESPONSE_PHASES = new Set(["PHASE_RESPONSE_HEADERS", "PHASE_RESPONSE_BODY", "PHASE_RESPONSE_TRAILERS"]);

const PRESETS = [
  {
    name: "JSON order",
    method: "POST",
    path: "/orders",
    headers: "content-type: application/json\nx-user: visitor",
    body: JSON.stringify({ item: "poster", qty: 1 }, null, 2),
  },
  { name: "GET", method: "GET", path: "/status", headers: "accept: application/json", body: "" },
  {
    name: "Form",
    method: "POST",
    path: "/login",
    headers: "content-type: application/x-www-form-urlencoded",
    body: "user=visitor&remember=true",
  },
];

const state = {
  lang: "go",
  sessionId: null,
  sessionState: "connecting", // connecting | live | ended | error
  sessionAbort: null,
  requestInFlight: false,
  phase: null, // phase currently paused, or null
  phaseStartedAt: 0,
  deadline: 0,
  snapshot: null,
  deciding: false,
  steps: {}, // pipeline step -> state
};

let countdownTimer = null;

function $(id) {
  return document.getElementById(id);
}

function el(tag, attrs, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else node.setAttribute(k, v);
  }
  for (const c of children) node.append(c);
  return node;
}

function phaseLabel(phase) {
  return PHASE_LABELS[phase] || phase;
}

// ---------------------------------------------------------------------------
// Connect transport

// frameConnectMessage frames a Connect JSON message as a length-prefixed
// envelope: 1 flag byte (0 for data) + 4-byte big-endian length + UTF-8 JSON.
function frameConnectMessage(body) {
  const bytes = new TextEncoder().encode(body);
  const frame = new Uint8Array(5 + bytes.length);
  new DataView(frame.buffer).setUint32(1, bytes.length, false);
  frame.set(bytes, 5);
  return frame;
}

async function connectError(res) {
  let message = "HTTP " + res.status;
  try {
    const body = await res.json();
    if (body && body.message) message = body.message;
    else if (body && body.code) message = body.code;
  } catch (err) {
    // non-JSON error body
  }
  return new Error(message);
}

async function* readConnectStream(res) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = new Uint8Array(0);
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      const merged = new Uint8Array(buffer.length + value.length);
      merged.set(buffer, 0);
      merged.set(value, buffer.length);
      buffer = merged;
      while (buffer.length >= 5) {
        const length = new DataView(buffer.buffer, buffer.byteOffset).getUint32(1, false);
        if (buffer.length < 5 + length) break;
        const flags = buffer[0];
        const message = JSON.parse(decoder.decode(buffer.subarray(5, 5 + length)));
        buffer = buffer.slice(5 + length);
        if ((flags & 0x2) !== 0) {
          // End-of-stream frame; carries an error if the stream failed.
          if (message && message.error) {
            throw new Error(message.error.message || message.error.code || "stream error");
          }
          return;
        }
        yield message;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

async function unary(method, payload) {
  const res = await fetch(CONTROLLER_BASE + "/" + method, {
    method: "POST",
    headers: {
      "Content-Type": CONNECT_UNARY_CONTENT_TYPE,
      "Connect-Protocol-Version": CONNECT_PROTOCOL_VERSION,
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw await connectError(res);
  return res.json();
}

// ---------------------------------------------------------------------------
// Session

async function startSession() {
  if (state.sessionAbort) state.sessionAbort.abort();
  const abort = new AbortController();
  state.sessionAbort = abort;
  state.sessionId = null;
  state.requestInFlight = false;
  clearPause();
  setSession("connecting");

  try {
    const res = await fetch(CONTROLLER_BASE + "/CreateSession", {
      method: "POST",
      headers: {
        "Content-Type": CONNECT_STREAM_CONTENT_TYPE,
        "Connect-Protocol-Version": CONNECT_PROTOCOL_VERSION,
      },
      body: frameConnectMessage("{}"),
      signal: abort.signal,
    });
    if (!res.ok) throw await connectError(res);
    for await (const update of readConnectStream(res)) {
      if (abort.signal.aborted) return;
      handleUpdate(update);
    }
    if (!abort.signal.aborted) {
      setSession("ended");
      log("Session stream closed by the server.", "warn");
    }
  } catch (err) {
    if (abort.signal.aborted) return;
    setSession("error");
    log("Session failed: " + err.message, "bad");
  }
}

function setSession(sessionState) {
  state.sessionState = sessionState;
  renderSession();
  renderComposer();
}

function handleUpdate(update) {
  if (!state.sessionId) {
    state.sessionId = update.sessionId;
    setSession("live");
    log("Session started.");
    return;
  }
  const phase = update.phase;
  if (!phase || phase === PHASE_NOT_STARTED) return;

  state.phase = phase;
  state.phaseStartedAt = Date.now();
  state.deadline = update.deadline ? Date.parse(update.deadline) : Date.now() + FALLBACK_PHASE_TIMEOUT_MS;
  state.snapshot = update.snapshot || null;
  state.deciding = false;
  setStep(phase, "active");
  if (RESPONSE_PHASES.has(phase)) setStep("upstream", "done");
  log("Paused at " + phaseLabel(phase) + describeSnapshot(state.snapshot) + ".", "warn");
  renderIntercept(true);
  startCountdown();
}

function describeSnapshot(snapshot) {
  if (!snapshot) return "";
  const parts = [];
  if (snapshot.headers) parts.push(snapshot.headers.length + " headers");
  if (snapshot.body) parts.push(base64Length(snapshot.body) + " bytes");
  return parts.length ? " · " + parts.join(", ") : "";
}

// ---------------------------------------------------------------------------
// Decisions

function collectSetHeaders() {
  const headers = {};
  for (const row of $("set-headers").querySelectorAll(".header-row")) {
    const [k, v] = row.querySelectorAll("input");
    const key = k.value.trim().toLowerCase();
    if (key) headers[key] = v.value;
  }
  return headers;
}

async function decide(action) {
  if (!state.phase || state.deciding) return;
  state.deciding = true;
  renderIntercept(true);
  const phase = state.phase;

  let decision;
  let summary;
  if (action === "continue") {
    const headers = collectSetHeaders();
    decision = { continue: { headers } };
    const names = Object.keys(headers);
    summary = "Continued " + phaseLabel(phase) + (names.length ? " · set " + names.join(", ") : "");
  } else {
    const status = parseInt($("cancel-status").value, 10);
    decision = { cancel: { status, body: $("cancel-body").value, headers: {} } };
    summary = "Rejected at " + phaseLabel(phase) + " with " + status;
  }

  try {
    await unary("SubmitDecision", { sessionId: state.sessionId, phase, decision });
  } catch (err) {
    state.deciding = false;
    showDecisionError("That decision didn't go through: " + err.message + ".");
    return;
  }
  if (state.phase !== phase) return; // a newer phase already arrived
  setStep(phase, action === "continue" ? "passed" : "rejected");
  log(summary + ".", action === "continue" ? "ok" : "bad");
  clearPause();
}

function clearPause() {
  state.phase = null;
  state.snapshot = null;
  state.deciding = false;
  stopCountdown();
  renderIntercept(false);
}

function startCountdown() {
  stopCountdown();
  const tick = () => {
    const remaining = Math.max(0, state.deadline - Date.now());
    const secs = Math.ceil(remaining / 1000);
    const node = $("countdown");
    node.hidden = false;
    node.textContent = Math.floor(secs / 60) + ":" + String(secs % 60).padStart(2, "0");
    node.dataset.urgent = String(secs <= 5);
    if (remaining === 0 && state.phase) {
      setStep(state.phase, "rejected");
      log("Timed out at " + phaseLabel(state.phase) + "; the processor fails closed.", "bad");
      clearPause();
    }
  };
  tick();
  countdownTimer = setInterval(tick, 250);
}

function stopCountdown() {
  if (countdownTimer) clearInterval(countdownTimer);
  countdownTimer = null;
  $("countdown").hidden = true;
}

// ---------------------------------------------------------------------------
// Sending the demo request

function parseHeaderLines(text) {
  const headers = new Headers();
  for (const line of text.split("\n")) {
    const idx = line.indexOf(":");
    if (idx <= 0) continue;
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim();
    try {
      headers.append(key, value);
    } catch (err) {
      // skip names the browser won't send
    }
  }
  return headers;
}

async function sendRequest() {
  if (!state.sessionId || state.requestInFlight) return;
  const method = $("req-method").value;
  const path = "/" + $("req-path").value.trim().replace(/^\/+/, "");
  const headers = parseHeaderLines($("req-headers").value);
  headers.set(SESSION_HEADER, state.sessionId);
  const body = method === "GET" ? undefined : $("req-body").value;

  state.requestInFlight = true;
  resetSteps();
  setStep("client", "done");
  $("response").hidden = true;
  renderComposer();
  log("Sent " + method + " " + path + " via the " + LANG_NAMES[state.lang] + " processor.");

  const started = performance.now();
  let result;
  try {
    const res = await fetch(ENVOY_BASE.replace(/\/+$/, "") + "/" + state.lang + path, { method, headers, body });
    result = { status: res.status, headers: res.headers, body: await res.text() };
  } catch (err) {
    result = { status: 0, headers: new Headers(), body: String(err) };
  }
  const elapsed = Math.round(performance.now() - started);

  state.requestInFlight = false;
  clearPause();
  const rejectedEarly = Object.entries(state.steps).some(([k, v]) => REQUEST_PHASES.has(k) && v === "rejected");
  if (!rejectedEarly && result.status > 0) setStep("upstream", "done");
  setStep("done", result.status >= 200 && result.status < 400 ? "done" : "rejected");
  log("Response " + (result.status || "failed") + " after " + formatMs(elapsed) + ".", result.status >= 200 && result.status < 400 ? "ok" : "bad");
  renderResponse(result, elapsed);
  renderComposer();
}

function formatMs(ms) {
  return ms < 1000 ? ms + " ms" : (ms / 1000).toFixed(1) + " s";
}

// ---------------------------------------------------------------------------
// Rendering

function renderSession() {
  const dot = $("session-dot");
  const label = $("session-label");
  dot.dataset.state = state.sessionState;
  if (state.sessionState === "live") label.textContent = "session " + shortId(state.sessionId);
  else if (state.sessionState === "connecting") label.textContent = "Connecting…";
  else if (state.sessionState === "ended") label.textContent = "Session ended";
  else label.textContent = "Couldn't connect";
}

function shortId(id) {
  return id && id.length > 10 ? id.slice(0, 8) + "…" : id || "";
}

function renderComposer() {
  const available = AVAILABLE_LANGS[state.lang];
  const ready = available && state.sessionState === "live" && !state.requestInFlight;
  $("send-request").disabled = !ready;
  let status = "";
  if (!available) status = "";
  else if (state.requestInFlight) status = "In flight. Decide each phase on the right.";
  else if (state.sessionState === "connecting") status = "Waiting for a session…";
  else if (state.sessionState !== "live") status = "Start a new session to send requests.";
  $("send-status").textContent = status;
}

function renderIntercept(paused) {
  const card = $("intercept");
  card.dataset.paused = String(paused);
  $("intercept-empty").hidden = paused;
  $("intercept-active").hidden = !paused;
  $("decision-error").hidden = true;
  $("btn-continue").disabled = state.deciding;
  $("btn-cancel").disabled = state.deciding;
  $("intercept-title").textContent = paused ? "Paused: " + phaseLabel(state.phase) : "Intercept";
  if (!paused) {
    const waiting = state.requestInFlight;
    $("intercept-empty-title").textContent = waiting ? "Waiting for the next phase…" : "Nothing paused yet";
    $("intercept-empty-body").textContent = waiting
      ? "Envoy is moving your request along. It'll stop here again at the next phase."
      : "Send a request and it'll stop here at each phase, waiting on you.";
    return;
  }
  if (!state.deciding) renderSnapshot();
}

function renderSnapshot() {
  const root = $("snapshot");
  root.replaceChildren();
  const snap = state.snapshot;
  if (!snap) {
    root.append(
      el("p", {
        class: "snapshot-note",
        text: "This processor doesn't share message contents yet, so you can set headers and decide, but not inspect.",
      }),
    );
    return;
  }
  if (snap.status) root.append(el("p", { class: "snapshot-note", text: "Upstream status " + snap.status }));
  if (snap.headers && snap.headers.length) {
    const table = el("table", { class: "kv" });
    for (const h of snap.headers) {
      table.append(el("tr", {}, el("td", { text: h.key }), el("td", { text: h.value })));
    }
    root.append(table);
  }
  if (snap.body) root.append(el("pre", { text: prettyBody(decodeBase64(snap.body)) }));
}

function renderResponse(result, elapsed) {
  $("response").hidden = false;
  const badge = $("response-status");
  badge.textContent = result.status ? String(result.status) : "error";
  badge.dataset.kind = result.status >= 200 && result.status < 400 ? "ok" : "bad";
  $("response-meta").textContent = formatMs(elapsed);
  const lines = [];
  result.headers.forEach((v, k) => lines.push(k + ": " + v));
  $("response-headers").textContent = lines.join("\n") || "(none visible to the page)";
  $("response-body").textContent = prettyBody(result.body) || "(empty body)";
}

function prettyBody(text) {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch (err) {
    return text;
  }
}

function decodeBase64(b64) {
  try {
    const bin = atob(b64);
    return new TextDecoder().decode(Uint8Array.from(bin, (c) => c.charCodeAt(0)));
  } catch (err) {
    return b64;
  }
}

function base64Length(b64) {
  try {
    return atob(b64).length;
  } catch (err) {
    return 0;
  }
}

function showDecisionError(message) {
  const node = $("decision-error");
  node.textContent = message;
  node.hidden = false;
  $("btn-continue").disabled = false;
  $("btn-cancel").disabled = false;
}

function resetSteps() {
  state.steps = {};
  for (const li of $("pipeline").children) delete li.dataset.state;
}

function setStep(step, stepState) {
  state.steps[step] = stepState;
  const li = $("pipeline").querySelector('[data-step="' + step + '"]');
  if (li) li.dataset.state = stepState;
}

function log(text, kind) {
  const now = new Date();
  const time = now.toTimeString().slice(0, 8);
  const item = el("li", {}, el("time", { datetime: now.toISOString(), text: time }), el("span", { text: text }));
  if (kind) item.dataset.kind = kind;
  $("log").prepend(item);
}

function addHeaderRow(key, value) {
  const row = el("div", { class: "header-row" });
  const k = el("input", { type: "text", placeholder: "x-demo-tag", spellcheck: "false", "aria-label": "Header name" });
  const v = el("input", { type: "text", placeholder: "value", spellcheck: "false", "aria-label": "Header value" });
  k.value = key || "";
  v.value = value || "";
  const remove = el("button", { type: "button", class: "ghost small", "aria-label": "Remove header", text: "×" });
  remove.addEventListener("click", () => row.remove());
  row.append(k, v, remove);
  $("set-headers").append(row);
}

function applyPreset(preset) {
  $("req-method").value = preset.method;
  $("req-path").value = preset.path;
  $("req-headers").value = preset.headers;
  $("req-body").value = preset.body;
}

function selectLang(lang) {
  state.lang = lang;
  for (const tab of document.querySelectorAll(".tabs [role=tab]")) {
    tab.setAttribute("aria-selected", String(tab.dataset.lang === lang));
  }
  const notice = $("lang-notice");
  if (AVAILABLE_LANGS[lang]) {
    notice.hidden = true;
  } else {
    notice.hidden = false;
    notice.textContent =
      "The " + LANG_NAMES[lang] + " processor isn't deployed yet. Same filter, same controller, different SDK; it'll show up here.";
  }
  renderComposer();
}

// ---------------------------------------------------------------------------
// Wiring

function bind() {
  $("new-session").addEventListener("click", startSession);
  $("send-request").addEventListener("click", sendRequest);
  $("btn-continue").addEventListener("click", () => decide("continue"));
  $("btn-cancel").addEventListener("click", () => decide("cancel"));
  $("add-header").addEventListener("click", () => addHeaderRow());
  for (const tab of document.querySelectorAll(".tabs [role=tab]")) {
    tab.addEventListener("click", () => selectLang(tab.dataset.lang));
  }
  for (const preset of PRESETS) {
    const btn = el("button", { type: "button", class: "preset", text: preset.name });
    btn.addEventListener("click", () => applyPreset(preset));
    $("presets").append(btn);
  }
}

function init() {
  bind();
  applyPreset(PRESETS[0]);
  addHeaderRow("x-demo-intercepted", "true");
  selectLang("go");
  renderIntercept(false);
  startSession();
}

init();

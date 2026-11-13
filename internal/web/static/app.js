"use strict";

// Interactive envoy extproc demo frontend.
//
// Talks to the human-facing controller Connect API (create session, follow phase
// updates, submit decisions) and sends envoy-bound demo requests through the
// same-origin envoy proxy, which relays them to the Envoy instance fronting the
// demo extproc.

const CONNECT_CONTENT_TYPE = "application/connect+json"; // server-streaming
const CONNECT_UNARY_CONTENT_TYPE = "application/json"; // unary
const CONNECT_PROTOCOL_VERSION = "1";
const ENVOY_PROXY = "/envoy/";
const SESSION_HEADER = "x-demo-session-id";
const CONTROLLER_BASE = "/controller.v1.DemoControllerService";

const PHASE_NOT_STARTED = "PHASE_NOT_STARTED";
const PHASE_REQUEST_HEADERS = "PHASE_REQUEST_HEADERS";
const PHASE_REQUEST_BODY = "PHASE_REQUEST_BODY";
const PHASE_REQUEST_TRAILERS = "PHASE_REQUEST_TRAILERS";
const PHASE_RESPONSE_HEADERS = "PHASE_RESPONSE_HEADERS";
const PHASE_RESPONSE_BODY = "PHASE_RESPONSE_BODY";
const PHASE_RESPONSE_TRAILERS = "PHASE_RESPONSE_TRAILERS";

const IN_FLIGHT_PHASES = new Set([
  PHASE_REQUEST_HEADERS,
  PHASE_REQUEST_BODY,
  PHASE_REQUEST_TRAILERS,
  PHASE_RESPONSE_HEADERS,
  PHASE_RESPONSE_BODY,
  PHASE_RESPONSE_TRAILERS,
]);

const PHASE_LABELS = {
  [PHASE_NOT_STARTED]: "not started",
  [PHASE_REQUEST_HEADERS]: "request headers",
  [PHASE_REQUEST_BODY]: "request body",
  [PHASE_REQUEST_TRAILERS]: "request trailers",
  [PHASE_RESPONSE_HEADERS]: "response headers",
  [PHASE_RESPONSE_BODY]: "response body",
  [PHASE_RESPONSE_TRAILERS]: "response trailers",
};

const state = {
  sessionId: null,
  phase: PHASE_NOT_STARTED,
  status: "idle",
  envoyResponse: null,
  decisionInFlight: false,
  stream: null,
};

function $(id) {
  return document.getElementById(id);
}

function setStatus(status) {
  state.status = status;
  renderStatus();
}

function renderStatus() {
  const sessionStatus = $("session-status");
  const phaseStatus = $("phase-status");
  const envoyStatus = $("envoy-status");
  if (!sessionStatus || !phaseStatus || !envoyStatus) {
    return;
  }
  if (state.sessionId) {
    sessionStatus.textContent = "Session: " + state.sessionId;
  }
  phaseStatus.textContent = phaseText();
  envoyStatus.textContent = envoyStatusText();
}

function phaseText() {
  if (!state.sessionId) {
    return "Create a session to begin.";
  }
  if (state.status === "complete") {
    return "Phase flow complete. See the envoy response below.";
  }
  if (state.status === "envoy_in_flight") {
    return "Sending envoy-bound request… awaiting phase decisions.";
  }
  if (state.phase === PHASE_NOT_STARTED) {
    return "Session created. Send an envoy-bound request to start the phase flow.";
  }
  if (IN_FLIGHT_PHASES.has(state.phase)) {
    return "Phase in flight: " + (PHASE_LABELS[state.phase] || state.phase) + ". Awaiting your decision.";
  }
  return "Phase complete: " + (PHASE_LABELS[state.phase] || state.phase);
}

function envoyStatusText() {
  if (!state.sessionId) {
    return "No envoy-bound request sent yet.";
  }
  if (state.status === "envoy_in_flight") {
    return "Sending envoy-bound request… awaiting phase decisions.";
  }
  if (state.status === "complete") {
    return "Envoy-bound request finished. See the response below.";
  }
  return "No envoy-bound request sent yet.";
}

function renderSession() {
  const sessionIdEl = $("session-id");
  if (sessionIdEl) {
    sessionIdEl.textContent = state.sessionId || "—";
  }
}

function renderPhase() {
  const phaseStatus = $("phase-status");
  if (!phaseStatus) {
    return;
  }
  phaseStatus.textContent = phaseText();
  const canDecide =
    state.sessionId !== null &&
    state.phase !== PHASE_NOT_STARTED &&
    IN_FLIGHT_PHASES.has(state.phase) &&
    (state.status === "awaiting_decision" || state.status === "envoy_in_flight");
  $("btn-continue").disabled = !canDecide;
  $("btn-cancel").disabled = !canDecide;
}

function renderEnvoyResponse() {
  const envoyStatus = $("envoy-status");
  const envoyResponse = $("envoy-response");
  if (!envoyStatus || !envoyResponse) {
    return;
  }
  if (!state.sessionId) {
    envoyStatus.textContent = "No envoy-bound request sent yet.";
    envoyResponse.textContent = "—";
    return;
  }
  if (state.status === "envoy_in_flight") {
    envoyStatus.textContent = "Sending envoy-bound request… awaiting phase decisions.";
    envoyResponse.textContent = "";
    return;
  }
  envoyStatus.textContent = state.envoyResponse
    ? "Envoy-bound request finished with status " + state.envoyResponse.status + "."
    : "No envoy-bound request sent yet.";
  envoyResponse.textContent = state.envoyResponse
    ? JSON.stringify(state.envoyResponse.body)
    : "—";
}

// frameConnectMessage frames a Connect JSON message as a length-prefixed
// Connect envelope: 1 flag byte (0 for data) + 4-byte big-endian byte length
// + UTF-8 JSON message bytes.
function frameConnectMessage(body) {
  const bytes = new TextEncoder().encode(body);
  const frame = new Uint8Array(5 + bytes.length);
  frame[0] = 0;
  frame[1] = (bytes.length >>> 24) & 0xff;
  frame[2] = (bytes.length >>> 16) & 0xff;
  frame[3] = (bytes.length >>> 8) & 0xff;
  frame[4] = bytes.length & 0xff;
  frame.set(bytes, 5);
  return frame;
}

async function fetchConnect(url, body, contentType, frame) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": contentType,
      "Connect-Protocol-Version": CONNECT_PROTOCOL_VERSION,
    },
    body: frame ? frameConnectMessage(body) : body,
  });
  if (!res.ok) {
    let detail = "";
    try {
      detail = (await res.text()) || "";
    } catch (err) {
      // ignore body read errors
    }
    throw new Error("controller Connect request failed: HTTP " + res.status + ": " + detail);
  }
  return res;
}

async function* readFrames(reader) {
  const decoder = new TextDecoder();
  let buffer = new Uint8Array(0);
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      return null;
    }
    const nextBuffer = new Uint8Array(buffer.length + value.length);
    nextBuffer.set(buffer, 0);
    nextBuffer.set(value, buffer.length);
    buffer = nextBuffer;
    for (;;) {
      if (buffer.length < 5) {
        break;
      }
      const flags = buffer[0];
      const length =
        buffer[1] * 0x1000000 + buffer[2] * 0x10000 + buffer[3] * 0x100 + buffer[4];
      if (buffer.length < 5 + length) {
        break;
      }
      const bytes = buffer.slice(5, 5 + length);
      buffer = buffer.slice(5 + length);
      const frame = JSON.parse(decoder.decode(bytes));
      if ((flags & 0x2) !== 0) {
        // Connect end-of-stream frame; carries the optional error.
        return frame;
      }
      yield frame;
    }
  }
  return null;
}

class ConnectStream {
  constructor(res) {
    this.reader = res.body.getReader();
    this.frames = readFrames(this.reader);
  }
  async next() {
    const frame = await this.frames.next();
    if (frame.done) {
      return { done: true, message: frame.value || null };
    }
    return { done: false, message: frame.value };
  }
  close() {
    try {
      this.reader.releaseLock();
    } catch (err) {
      // ignore release errors
    }
  }
}

async function createSession() {
  const res = await fetchConnect(
    location.origin + CONTROLLER_BASE + "/CreateSession",
    JSON.stringify({}),
    CONNECT_CONTENT_TYPE,
    true,
  );
  const stream = new ConnectStream(res);
  state.stream = stream;
  let next = stream.next();
  for (;;) {
    const { done, message } = await next;
    if (done) {
      return;
    }
    next = stream.next();
    if (!state.sessionId) {
      // First update: session created with phase PHASE_NOT_STARTED.
      state.sessionId = message.sessionId;
      state.phase = message.phase;
      renderSession();
      setSendRequestEnabled(true);
      renderPhase();
      setStatus("awaiting_request");
    }
    handleUpdate(message);
  }
}

function handleUpdate(message) {
  state.phase = message.phase;
  // A new phase update means a new phase is awaiting its own decision.
  state.decisionInFlight = false;
  if (message.phase === PHASE_NOT_STARTED) {
    setStatus("awaiting_request");
  } else if (IN_FLIGHT_PHASES.has(message.phase)) {
    setStatus("awaiting_decision");
  }
  renderPhase();
}

async function submitDecision(action) {
  if (!state.sessionId || state.decisionInFlight) {
    return;
  }
  state.decisionInFlight = true;
  setDecisionsEnabled(false);
  const decision =
    action === "cancel"
      ? { action: { cancel: { status: 503, body: "", headers: {} } } }
      : { action: { continue: { headers: {} } } };
  try {
    const res = await fetchConnect(
      location.origin + CONTROLLER_BASE + "/SubmitDecision",
      JSON.stringify({
        sessionId: state.sessionId,
        phase: state.phase,
        decision: decision,
      }),
      CONNECT_UNARY_CONTENT_TYPE,
      false,
    );
    if (!res.ok) {
      throw new Error("controller Connect request failed: HTTP " + res.status);
    }
  } catch (err) {
    state.decisionInFlight = false;
    setDecisionsEnabled(true);
    setStatus("error");
  }
}

async function sendEnvoyRequest() {
  if (!state.sessionId) {
    return;
  }
  setStatus("envoy_in_flight");
  setSendRequestEnabled(false);
  renderPhase();

  let response = { status: 0, body: "" };
  try {
    const res = await fetch(location.origin + ENVOY_PROXY, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        [SESSION_HEADER]: state.sessionId,
      },
      body: JSON.stringify({ hello: "world" }),
    });
    response = { status: res.status, body: await res.text() };
  } catch (err) {
    response = { status: 0, body: String(err) };
  }
  state.envoyResponse = response;
  setStatus("complete");
  setSendRequestEnabled(true);
  renderEnvoyResponse();
}

function setDecisionsEnabled(enabled) {
  const continueBtn = $("btn-continue");
  const cancelBtn = $("btn-cancel");
  if (continueBtn) {
    continueBtn.disabled = !enabled;
  }
  if (cancelBtn) {
    cancelBtn.disabled = !enabled;
  }
}

function setSendRequestEnabled(enabled) {
  const sendBtn = $("send-request");
  if (sendBtn) {
    sendBtn.disabled = !enabled;
  }
}

function bind() {
  $("new-session").addEventListener("click", async function () {
    state.sessionId = null;
    state.phase = PHASE_NOT_STARTED;
    state.status = "idle";
    state.envoyResponse = null;
    state.decisionInFlight = false;
    renderSession();
    setSendRequestEnabled(false);
    renderPhase();
    setStatus("idle");
    await createSession();
  });
  $("send-request").addEventListener("click", sendEnvoyRequest);
  $("btn-continue").addEventListener("click", function () {
    submitDecision("continue");
  });
  $("btn-cancel").addEventListener("click", function () {
    submitDecision("cancel");
  });
}

function init() {
  bind();
  renderStatus();
}

init();

#!/usr/bin/env bash
# End-to-end smoke test for the envoy extproc demo compose stack.
#
# Prerequisites: docker compose up --build (or: just up)
# Usage:
#   ./scripts/e2e.sh
#   PHASE=PHASE_REQUEST_BODY DECISION='{"session_id":"...","phase":"PHASE_REQUEST_BODY","decision":{"cancel":{"status":503}}}' ./scripts/e2e.sh
set -euo pipefail

CONTROLLER="${CONTROLLER:-http://localhost:9092}"
ENVOY="${ENVOY:-http://localhost:8080}"
SESSION_HEADER="${SESSION_HEADER:-x-demo-session-id}"
PHASE="${PHASE:-PHASE_REQUEST_HEADERS}"
DECISION="${DECISION:-}"

create_session() {
  local resp session_id
  for _i in $(seq 1 30); do
    resp="$(curl -s -X POST "$CONTROLLER/controller.v1.DemoControllerService/CreateSession" \
      -H 'content-type: application/connect+json' -d '{}' || true)"
    session_id="$(printf '%s' "$resp" | grep -o '"session_id":"[^"]*"' | head -n1 | sed 's/.*"\([^"]*\)"$/\1/')" || true
    if [[ -n "$session_id" ]]; then
      SESSION_ID="$session_id"
      return 0
    fi
    echo "failed to create session (attempt ${_i})" >&2
    sleep 1
  done
  echo "failed to create session" >&2
  return 1
}

SESSION_ID=""
create_session

echo "session id: $SESSION_ID"

RESP="$(curl -s -X POST "$ENVOY/" \
  -H 'content-type: application/json' \
  -H "$SESSION_HEADER: $SESSION_ID" \
  -d '{"hello":"world"}')" || true
echo "envoy-bound response: $RESP"

if [[ -z "$DECISION" ]]; then
  DECISION="{\"session_id\":\"$SESSION_ID\",\"phase\":\"$PHASE\",\"decision\":{\"continue\":{}}}"
fi
curl -s -X POST "$CONTROLLER/controller.v1.DemoControllerService/SubmitDecision" \
  -H 'content-type: application/connect+json' \
  -d "$DECISION"
echo

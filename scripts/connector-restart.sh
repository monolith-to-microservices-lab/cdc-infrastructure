#!/usr/bin/env bash
# Restart the Debezium connector + its task WITHOUT touching any volume.
# Used to prove the connector resumes from its stored offset (no re-snapshot).
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

PY="$(command -v python || command -v python3 || true)"

echo ">> restart-connector.sh: ${CONNECTOR_NAME}"
curl -sf -X POST "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/restart?includeTasks=true&onlyFailed=false" \
  && echo "   POST /restart accepted"
sleep 4
curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | { [[ -n "${PY}" ]] && "${PY}" -m json.tool || cat; }

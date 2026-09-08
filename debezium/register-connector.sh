#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Register (or safely update) the Debezium PostgreSQL connector.
#
#   1. wait until Kafka Connect REST is up
#   2. render debezium/register-connector.json with values from .env
#   3. if the connector already exists  -> PUT  /connectors/<name>/config
#      otherwise                        -> POST /connectors
#   4. print connector + tasks status
#
# Using PUT /config makes this idempotent: re-running never creates a
# duplicate and never forces a new snapshot (the stream position lives in the
# _connect_offsets Kafka topic, not in the connector definition).
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
set -a; source "${ROOT_DIR}/.env"; set +a

CONNECT_URL="http://localhost:${CONNECT_HOST_PORT:-8083}"
NAME="${CONNECTOR_NAME:?set CONNECTOR_NAME in .env}"
TEMPLATE="${SCRIPT_DIR}/register-connector.json"

PY="$(command -v python || command -v python3 || true)"
[[ -z "${PY}" ]] && { echo "!! python is required"; exit 1; }

echo ">> waiting for Kafka Connect at ${CONNECT_URL} ..."
for i in $(seq 1 60); do
  if curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then echo "   Connect is up"; break; fi
  [[ $i -eq 60 ]] && { echo "!! Connect did not become ready"; exit 1; }
  sleep 2
done

# --- render template with .env values ----------------------------------
RENDERED="$(mktemp)"; trap 'rm -f "${RENDERED}"' EXIT
export CONNECTOR_NAME LEGACY_PG_HOST LEGACY_PG_PORT LEGACY_PG_DEBEZIUM_USER \
       LEGACY_PG_DEBEZIUM_PASSWORD LEGACY_PG_DBNAME TOPIC_PREFIX \
       REPLICATION_SLOT_NAME PUBLICATION_NAME
envsubst < "${TEMPLATE}" > "${RENDERED}"

# --- create or update via the Connect REST API ------------------------
"${PY}" - "${CONNECT_URL}" "${NAME}" "${RENDERED}" <<'PY'
import json, sys, urllib.request, urllib.error

base, name, path = sys.argv[1], sys.argv[2], sys.argv[3]
doc = json.load(open(path))
config = doc["config"]

def req(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

status, _ = req("GET", f"{base}/connectors/{name}")
if status == 200:
    print(f">> connector '{name}' exists -> PUT /connectors/{name}/config")
    code, out = req("PUT", f"{base}/connectors/{name}/config", config)
else:
    print(f">> connector '{name}' not found -> POST /connectors")
    code, out = req("POST", f"{base}/connectors", {"name": name, "config": config})

if code >= 300:
    print(f"!! Connect returned HTTP {code}:\n{out}")
    sys.exit(1)
print(f"   OK (HTTP {code})")
PY

echo ">> waiting for the connector to settle ..."
sleep 6

echo
echo "=== GET /connectors ==="
curl -sf "${CONNECT_URL}/connectors"; echo
echo
echo "=== GET /connectors/${NAME}/status ==="
curl -sf "${CONNECT_URL}/connectors/${NAME}/status" | "${PY}" -m json.tool
echo

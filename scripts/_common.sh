#!/usr/bin/env bash
# Shared helpers for the inspection scripts. Sourced, not executed.
set -euo pipefail

# Git Bash / MSYS on Windows rewrites "/opt/kafka/..." into a Windows path
# before it reaches the container. Disable that for these scripts.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${_COMMON_DIR}/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  # shellcheck disable=SC1091
  set -a; source "${ROOT_DIR}/.env"; set +a
fi

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cdc-kafka}"
CONNECT_URL="http://localhost:${CONNECT_HOST_PORT:-8083}"
LEGACY_PG_CONTAINER="${LEGACY_PG_CONTAINER:-monolito-microservice-postgres-1}"
LEGACY_PG_ADMIN_USER="${LEGACY_PG_ADMIN_USER:-postgres}"
LEGACY_PG_ADMIN_PASSWORD="${LEGACY_PG_ADMIN_PASSWORD:-postgres}"
LEGACY_PG_DBNAME="${LEGACY_PG_DBNAME:-monolith}"
TOPIC_PREFIX="${TOPIC_PREFIX:-legacy}"
CONNECTOR_NAME="${CONNECTOR_NAME:-legacy-cdc-connector}"

kafka() { docker exec -i "${KAFKA_CONTAINER}" "$@"; }

legacy_psql() {
  docker exec -i -e PGPASSWORD="${LEGACY_PG_ADMIN_PASSWORD}" "${LEGACY_PG_CONTAINER}" \
    psql -v ON_ERROR_STOP=1 -U "${LEGACY_PG_ADMIN_USER}" -d "${LEGACY_PG_DBNAME}" "$@"
}

# Pretty-print each line of stdin as JSON when possible, otherwise pass through.
pretty_json_lines() {
  local py; py="$(command -v python || command -v python3 || true)"
  if [[ -z "${py}" ]]; then cat; return; fi
  "${py}" -c '
import sys, json
for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    try:
        print(json.dumps(json.loads(line), indent=2, ensure_ascii=False))
    except Exception:
        print(line)
    print("-" * 72)
'
}

consume_topic() {
  local topic="$1" timeout_ms="${2:-10000}"
  echo ">> consuming '${topic}' from beginning (timeout ${timeout_ms} ms)"
  echo ">> key = Debezium message key (row primary key)"
  echo
  kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${topic}" \
    --from-beginning \
    --timeout-ms "${timeout_ms}" \
    --property print.key=true \
    --property key.separator=$'\n' 2>/dev/null | pretty_json_lines
}

#!/usr/bin/env bash
# One-shot health overview of the whole CDC pipeline.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

PY="$(command -v python || command -v python3 || true)"
line() { printf '%s\n' "----------------------------------------------------------------------"; }

line; echo "1) CONTAINERS"; line
docker ps --filter "name=cdc-" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker ps --filter "name=${LEGACY_PG_CONTAINER}" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

line; echo "2) KAFKA BROKER"; line
if kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
  echo "kafka: UP"
  kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status 2>/dev/null || true
else
  echo "kafka: DOWN"
fi

line; echo "3) KAFKA CONNECT"; line
if curl -sf "${CONNECT_URL}/" >/dev/null 2>&1; then
  echo "connect: UP  ($(curl -sf "${CONNECT_URL}/" ))"
  echo "connectors: $(curl -sf "${CONNECT_URL}/connectors")"
else
  echo "connect: DOWN"
fi

line; echo "4) CONNECTOR STATUS"; line
if [[ -n "${PY}" ]]; then
  curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | "${PY}" -m json.tool 2>/dev/null \
    || echo "connector '${CONNECTOR_NAME}' not registered"
else
  curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || echo "not registered"
fi

line; echo "5) TOPICS"; line
kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null

line; echo "6) LEGACY POSTGRES - wal_level"; line
legacy_psql -c "SELECT name, setting FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots');"

line; echo "7) LEGACY POSTGRES - replication slot"; line
legacy_psql -x -c "SELECT slot_name, plugin, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;"

line; echo "8) LEGACY POSTGRES - publication"; line
legacy_psql -c "SELECT pubname, pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication;"
legacy_psql -c "SELECT * FROM pg_publication_tables;"

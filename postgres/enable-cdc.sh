#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Enable logical CDC on the LEGACY monolith PostgreSQL.
#
# What it does (all idempotent, all NON-destructive - no DROP TABLE, no
# TRUNCATE, no volume changes):
#   1. creates a least-privilege `debezium` replication role
#   2. sets REPLICA IDENTITY FULL on users + sales (for UPDATE/DELETE
#      before-images)
#   3. creates the explicit publication  legacy_cdc_publication (users, sales)
#   4. creates the stable logical replication slot  legacy_cdc_slot (pgoutput)
#
# Pre-requisite: the monolith Postgres must already run with
# wal_level=logical (see postgres/README.md for the one-line compose patch).
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
set -a; source "${ROOT_DIR}/.env"; set +a

PGC="${LEGACY_PG_CONTAINER:?set LEGACY_PG_CONTAINER in .env}"
ADMIN_USER="${LEGACY_PG_ADMIN_USER:-postgres}"
DB="${LEGACY_PG_DBNAME:-monolith}"
DBZ_USER="${LEGACY_PG_DEBEZIUM_USER:-debezium}"
DBZ_PASS="${LEGACY_PG_DEBEZIUM_PASSWORD:?set LEGACY_PG_DEBEZIUM_PASSWORD in .env}"
PUB="${PUBLICATION_NAME:-legacy_cdc_publication}"
SLOT="${REPLICATION_SLOT_NAME:-legacy_cdc_slot}"

echo ">> target container: ${PGC}   db: ${DB}"

psql() { docker exec -i -e PGPASSWORD="${LEGACY_PG_ADMIN_PASSWORD:-postgres}" "${PGC}" \
          psql -v ON_ERROR_STOP=1 -U "${ADMIN_USER}" -d "${DB}" "$@"; }

echo ">> checking wal_level..."
WAL=$(psql -tAc "SHOW wal_level;")
if [[ "${WAL}" != "logical" ]]; then
  echo "!! wal_level is '${WAL}', expected 'logical'."
  echo "!! Apply the compose patch in postgres/README.md and recreate the"
  echo "!! monolith postgres container (docker compose up -d, NOT down -v)."
  exit 1
fi
echo "   wal_level = logical  OK"

echo ">> applying CDC objects..."
psql <<SQL
-- 1. least-privilege replication role -----------------------------------
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DBZ_USER}') THEN
    CREATE ROLE ${DBZ_USER} WITH LOGIN REPLICATION PASSWORD '${DBZ_PASS}';
  END IF;
END\$\$;
ALTER ROLE ${DBZ_USER} WITH LOGIN REPLICATION PASSWORD '${DBZ_PASS}';
GRANT CONNECT ON DATABASE ${DB} TO ${DBZ_USER};
GRANT USAGE ON SCHEMA public TO ${DBZ_USER};
GRANT SELECT ON TABLE public.users, public.sales TO ${DBZ_USER};

-- 2. before-images for UPDATE / DELETE ---------------------------------
ALTER TABLE public.users REPLICA IDENTITY FULL;
ALTER TABLE public.sales REPLICA IDENTITY FULL;

-- 3. explicit publication (ONLY users + sales) -------------------------
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '${PUB}') THEN
    CREATE PUBLICATION ${PUB} FOR TABLE public.users, public.sales;
  END IF;
END\$\$;

-- 4. stable pgoutput replication slot ---------------------------------
--    pins WAL start position to NOW, before Debezium is registered.
SELECT pg_create_logical_replication_slot('${SLOT}', 'pgoutput')
WHERE NOT EXISTS (
  SELECT 1 FROM pg_replication_slots WHERE slot_name = '${SLOT}'
);
SQL

echo
echo ">> result:"
psql -x -c "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '${SLOT}';"
psql -c "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication WHERE pubname = '${PUB}';"
psql -c "SELECT p.pubname, n.nspname, c.relname FROM pg_publication_tables pt JOIN pg_publication p ON p.pubname=pt.pubname JOIN pg_class c ON c.relname=pt.tablename JOIN pg_namespace n ON n.nspname=pt.schemaname WHERE p.pubname='${PUB}';" 2>/dev/null || \
  psql -c "SELECT * FROM pg_publication_tables WHERE pubname = '${PUB}';"
echo
echo ">> done. Now register the connector:  ./debezium/register-connector.sh"

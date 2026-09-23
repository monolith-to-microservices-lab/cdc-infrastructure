#!/usr/bin/env bash
# Quick look at the replication slot + how much WAL it is forcing Postgres to keep.
# A slot that is inactive for a long time keeps restart_lsn frozen and makes
# retained_wal grow without bound -> disk pressure. See README section 10.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

legacy_psql -x -c "
SELECT slot_name,
       plugin,
       active,
       active_pid,
       restart_lsn,
       confirmed_flush_lsn,
       pg_current_wal_lsn() AS current_wal_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
       wal_status
FROM pg_replication_slots;"

# Legacy PostgreSQL — enabling CDC

This folder does **not** run a database. The database is the monolith's
PostgreSQL 16, owned by the `monolito-microservice` repository. Here we only
document and script the **minimal, non-destructive** changes needed to let
Debezium read its Write-Ahead Log.

Nothing here runs `DROP`, `TRUNCATE`, or `docker compose down -v`. The existing
~100 users / ~1001 sales are preserved.

---

## 1. Compose patch (already applied)

`monolito-microservice/docker-compose.yml`, `postgres` service — three settings
and one extra network. This is the whole diff:

```yaml
  postgres:
    image: postgres:16
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"        # <- the one that matters for CDC
      - "-c"
      - "max_wal_senders=4"        # lab sizing (default is 10)
      - "-c"
      - "max_replication_slots=4"  # lab sizing (default is 10)
    networks:
      default: {}                  # keep backend <-> postgres private link
      migration-network:           # join the shared cross-repo network
        aliases:
          - legacy-postgres        # DNS name Debezium uses
    # ... environment / ports / volumes / healthcheck unchanged ...

networks:
  migration-network:
    external: true
```

`wal_level` only takes effect after a **container recreate**:

```bash
docker network create migration-network      # once, if it does not exist
cd monolito-microservice
docker compose up -d                          # recreates postgres, keeps the volume
```

Verify:

```bash
docker exec -e PGPASSWORD=postgres monolito-microservice-postgres-1 \
  psql -U postgres -d monolith -c "SHOW wal_level;"
# wal_level = logical
```

### Why these values

| setting | value | why |
|---|---|---|
| `wal_level` | `logical` | `replica` (default) only records enough to rebuild a physical replica. `logical` adds the row-level information that logical decoding / `pgoutput` needs. |
| `max_wal_senders` | `4` | each streaming replication connection uses one WAL sender. We need exactly 1 (Debezium). 4 leaves headroom for an occasional manual `pg_basebackup` / debugging. |
| `max_replication_slots` | `4` | one slot per consumer that needs a durable position. We use 1 (`legacy_cdc_slot`). |

---

## 2. `enable-cdc.sh` (already run)

`./postgres/enable-cdc.sh` is idempotent and does four things:

### a. Least-privilege replication role

```sql
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD '<from .env>';
GRANT CONNECT ON DATABASE monolith TO debezium;
GRANT USAGE  ON SCHEMA public      TO debezium;
GRANT SELECT ON TABLE public.users, public.sales TO debezium;
```

| privilege | needed for |
|---|---|
| `LOGIN` | open a normal connection |
| `REPLICATION` | open the logical replication stream / read the slot |
| `CONNECT` on `monolith` | reach the database |
| `USAGE` on `public` | see objects in the schema |
| `SELECT` on `users`, `sales` | Debezium reads column metadata and the initial schema; also required if a snapshot is ever run |

Not a superuser. It **cannot** create publications, drop tables, or read any
other table. The publication and the slot are created once by the admin
(`postgres`) in this script.

### b. `REPLICA IDENTITY FULL` on `users` and `sales`

```sql
ALTER TABLE public.users REPLICA IDENTITY FULL;
ALTER TABLE public.sales REPLICA IDENTITY FULL;
```

Default (`DEFAULT`) only puts the **primary key** into the WAL for `UPDATE` /
`DELETE`. With `FULL`, the WAL carries the **complete previous row**, so the
Debezium event's `before` block is fully populated (see Test 2 / Test 3 in the
main README). Cost: slightly larger WAL records. Fine for a lab; for a big,
write-heavy table you would weigh this.

### c. Explicit publication — only `users` + `sales`

```sql
CREATE PUBLICATION legacy_cdc_publication FOR TABLE public.users, public.sales;
```

A publication is the server-side allow-list of tables (and operations) that
`pgoutput` will emit. We name the two tables explicitly instead of
`FOR ALL TABLES` so that:

* a future `alembic` table in the monolith is **not** silently streamed,
* the blast radius of CDC is auditable in one `SELECT * FROM pg_publication_tables;`.

### d. Stable logical replication slot

```sql
SELECT pg_create_logical_replication_slot('legacy_cdc_slot', 'pgoutput');
```

Creating the slot **now**, before the connector is registered, pins the WAL
start position to "this moment". Everything already in the tables is invisible
to the slot; only later changes are decoded. The slot name is fixed in `.env`
(`REPLICATION_SLOT_NAME`) and reused on every connector restart — Debezium
never creates a random one.

---

## 3. Rollback (if you ever need to undo this)

Non-destructive, does not touch business data:

```sql
SELECT pg_drop_replication_slot('legacy_cdc_slot');   -- FIRST: frees retained WAL
DROP PUBLICATION legacy_cdc_publication;
ALTER TABLE public.users REPLICA IDENTITY DEFAULT;
ALTER TABLE public.sales REPLICA IDENTITY DEFAULT;
DROP ROLE debezium;
```

Then remove the `command:` / `networks:` block from the monolith compose and
`docker compose up -d` again. **Always drop the slot before stopping Debezium
for good** — an abandoned slot makes PostgreSQL keep WAL forever (see the main
README, section 10).

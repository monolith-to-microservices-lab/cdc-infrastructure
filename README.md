# cdc-infrastructure

Change Data Capture plane for the monolith → microservices migration.

```
        MONOLITH  (source of truth, unchanged business logic)
            |
            v
     Legacy PostgreSQL 16      monolito-microservice repo
            |
            |  WAL  (Write-Ahead Log)
            v
        DEBEZIUM  (PostgreSQL connector, running inside Kafka Connect)
            |
            v
         KAFKA  (single broker, KRaft — no ZooKeeper)
        /            \
       v              v
 legacy.public.users   legacy.public.sales
       |              |
       v              v
 (User Service)   (Sales Service)   <-- NOT built in this phase
```

This repository owns **only** the pipe in the middle: Kafka, Kafka Connect and
the Debezium connector configuration, plus the scripts to operate and inspect
them. It contains no application code. The monolith, `user-service`,
`sales-service` and `migration-tool` all live in their own repositories and are
**not** modified here (the one exception: a 3-line, non-destructive patch to the
monolith's Postgres — see [`postgres/README.md`](postgres/README.md)).

> **Scope of this phase:** prove `Monolith → Legacy PG → WAL → Debezium → Kafka`.
> No Kafka consumers are added to the services yet. Stop when events are visible
> in Kafka.

---

## Table of contents

1. [The problem](#1-the-problem)
2. [CDC](#2-cdc)
3. [WAL](#3-wal-write-ahead-log)
4. [Logical decoding](#4-logical-decoding)
5. [Replication slot](#5-replication-slot)
6. [Publication](#6-publication)
7. [Kafka Connect](#7-kafka-connect)
8. [Topics](#8-topics)
9. [Event format](#9-event-format-real-captures)
10. [Failure behavior](#10-failure-behavior)
11. [Ordering & delivery guarantees](#11-ordering--delivery-guarantees)
12. [How to run it](#12-how-to-run-it-from-zero)
13. [Networking: localhost vs container DNS](#13-networking-localhost-vs-container-dns)
14. [Test results (real output)](#14-test-results-real-output)
15. [What is intentionally NOT here](#15-what-is-intentionally-not-here)

---

## 1. The problem

`migration-tool` already ran the **initial snapshot**: it read every legacy row
and re-created it through the services' HTTP APIs. At that instant (T0) the three
databases agreed:

```
T0:   legacy.users == user_db.users        legacy.sales == sales_db.sales
```

But the monolith is still live and still the **only writer**. Every `POST /users`
or `POST /sales` after T0 lands **only** in the legacy database. Without
continuous sync:

```
T0 + 1 day:   legacy.users = 140 rows     user_db.users = 100 rows   (diverged)
```

The snapshot is a photograph; we now need a **video**. That continuous stream of
post-T0 changes is what this repository delivers.

---

## 2. CDC

**Change Data Capture** = turn every row-level change in a database into an
event stream, without the application publishing anything.

```
   INSERT / UPDATE / DELETE
            |
            v
     database transaction log   (already written for crash recovery)
            |
            v
      CDC connector reads the log
            |
            v
        change event  ->  Kafka topic
```

The application does not know CDC exists. No dual-write, no outbox table, no
trigger. The monolith keeps doing plain SQL; Debezium tails the log behind it.

---

## 3. WAL (Write-Ahead Log)

PostgreSQL never modifies a data page on disk before first appending a record
describing that change to the **WAL**. It exists for durability and crash
recovery: after a crash, PostgreSQL replays the WAL to reach a consistent state.

Because the WAL already contains *every* change in commit order, it is also the
perfect source for CDC — the connector is just another reader of a log that
Postgres writes anyway.

`wal_level` controls how much detail goes into each record:

| level | contents | CDC? |
|---|---|---|
| `minimal` | just enough to recover after a crash | no |
| `replica` (default) | + enough to feed a physical standby | no |
| `logical` | + row identities needed to reconstruct logical changes | **yes** |

We set `wal_level=logical`. See [`postgres/README.md`](postgres/README.md).

---

## 4. Logical decoding

Raw WAL is a physical format (block numbers, byte offsets). **Logical decoding**
is the PostgreSQL feature that runs the WAL through an *output plugin* which
turns those physical records back into logical statements: "row `{id:101}`
inserted into `public.users` with these column values".

* The output plugin we use is **`pgoutput`** — it is built into PostgreSQL 10+,
  so nothing extra is installed in the database container. (The historical
  alternative, `wal2json`, would require a plugin install.)
* Debezium connects as a **logical replication client**, asks Postgres to stream
  decoded changes for the tables in our publication, and converts each one into
  a Debezium change event.

```
WAL bytes ── pgoutput ──► logical change ── Debezium ──► JSON event ──► Kafka
```

---

## 5. Replication slot

A **replication slot** is a named, server-side bookmark. It records the oldest
WAL position (`restart_lsn`) that its consumer still needs, and the position the
consumer has confirmed as safely processed (`confirmed_flush_lsn`).

What it buys us:

* **Durable position.** If Debezium restarts, it resumes exactly where it left
  off — Postgres still has that WAL because the slot pinned it.
* **No gaps.** Postgres will not recycle/delete WAL a slot still needs.

The trade-off is [WAL retention](#10-failure-behavior): a slot whose consumer is
gone forces Postgres to keep WAL forever.

Our slot:

| property | value |
|---|---|
| name | `legacy_cdc_slot` (fixed in `.env`, never randomized) |
| plugin | `pgoutput` |
| created by | `postgres/enable-cdc.sh`, **before** the connector — this is what pins the start position to "now" and skips the historical data |

```sql
SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
```

---

## 6. Publication

With `pgoutput`, the set of tables (and operations) that get decoded is defined
**in the database** by a `PUBLICATION` — a server-side allow-list.

```sql
CREATE PUBLICATION legacy_cdc_publication FOR TABLE public.users, public.sales;
```

We list the two tables explicitly (not `FOR ALL TABLES`) so future monolith
tables are not streamed by accident. The Debezium connector is told
`publication.name=legacy_cdc_publication` and
`publication.autocreate.mode=disabled`, so it uses this exact object and never
tries to create or widen one itself (its low-privilege role could not anyway).

```sql
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;
```

`table.include.list` in the connector is a **second**, client-side filter. Both
point at `public.users` + `public.sales`.

---

## 7. Kafka Connect

**Kafka Connect** is a runtime for source/sink connectors. **Debezium ships as a
set of Connect source connectors** — you do not run a "Debezium server", you run
Kafka Connect with the Debezium PostgreSQL connector plugin on its classpath
(the `quay.io/debezium/connect` image already has it).

* REST API on `:8083` — you register/inspect connectors over HTTP.
* Connect stores the connector definition, its **stream offset**, and task
  status in three internal **Kafka topics** (`_connect_configs`,
  `_connect_offsets`, `_connect_status`) — *not* on a local disk. That is why a
  `docker restart` of the Connect container loses nothing and does **not**
  trigger a new snapshot ([Test 6](#14-test-results-real-output)).

```
GET  /connectors
GET  /connectors/legacy-cdc-connector/status
GET  /connectors/legacy-cdc-connector/config
GET  /connectors/legacy-cdc-connector/offsets
```

Register it with the idempotent helper:

```bash
./debezium/register-connector.sh
```

It waits for Connect, renders `debezium/register-connector.json` with values from
`.env`, then **POSTs** if the connector is new or **PUTs `/config`** if it
already exists (so re-running never duplicates it and never re-snapshots).

### `snapshot.mode = no_data` — why

Debezium 3.1 PostgreSQL snapshot modes: `initial` (default), `initial_only`,
`always`, `no_data`, `when_needed`, `never`, `configuration_based`, `custom`.

| mode | on first start | fits us? |
|---|---|---|
| `initial` | **SELECT every row**, emit them as `op:"r"` events, then stream | ❌ would republish all ~100 users + ~1001 already-migrated rows |
| `always` | snapshot on *every* start | ❌ worse |
| `never` | never snapshot; stream from the slot position; build table schema lazily from the stream | ✅ works, but stricter — no schema read up front |
| **`no_data`** | read table **structure** only (no rows, no `op:"r"` events), then stream from the slot position | ✅ **chosen** |

`no_data` (older name: `schema_only`) is the documented option for exactly our
situation — *"you don't need the topics to contain a consistent snapshot of the
data, only the changes since the connector started."* The historical data is
already in the target databases (via `migration-tool`); Debezium's job starts
now.

Combined with the slot we pre-created in step 5, the start position is pinned
twice over. Evidence it worked: [Test 5](#14-test-results-real-output).

### Envelope kept raw — no SMT

`key/value.converter = JsonConverter` with `schemas.enable=false`, and **no
Single Message Transforms**. The Kafka value is the full Debezium envelope
(`before`, `after`, `source`, `op`, `ts_ms`, `transaction`) so it can be studied
directly. (`ExtractNewRecordState` and friends come in a later phase.)

---

## 8. Topics

Debezium names a data topic `<topic.prefix>.<schema>.<table>`. With
`topic.prefix=legacy`:

| # | topic | contents |
|---|---|---|
| 1 | `legacy.public.users` | one event per `users` row change; **key = `{"id":<pk>}`** |
| 2 | `legacy.public.sales` | one event per `sales` row change; key = `{"id":<pk>}` |
| 3 | `__debezium-heartbeat.legacy` | periodic heartbeat (`heartbeat.interval.ms=10000`); lets the slot's `confirmed_flush_lsn` advance even when `users`/`sales` are idle but other tables are busy |
| — | `_connect_configs`, `_connect_offsets`, `_connect_status` | Kafka Connect internal state |
| — | `__consumer_offsets` | Kafka internal |

Topics are auto-created on first event (single partition, RF 1 — lab sizing).

```bash
./scripts/list-topics.sh
```

---

## 9. Event format (real captures)

`schemas.enable=false`, so a Kafka **value** is exactly the Debezium envelope.
The `op` field:

| `op` | meaning | `before` | `after` |
|---|---|---|---|
| `c` | create (INSERT) | `null` | new row |
| `u` | update (UPDATE) | previous row (full, thanks to `REPLICA IDENTITY FULL`) | new row |
| `d` | delete (DELETE) | last row state | `null` |
| `r` | read (snapshot row) | `null` | row | ← **we never produce these** (`snapshot.mode=no_data`) |

Plus, after every `d`, a **tombstone**: same key, `value = null` (see
[Test 3](#14-test-results-real-output)).

### CREATE — `legacy.public.users`, key `{"id":101}`

```json
{
  "before": null,
  "after": { "id": 101, "name": "Thiago", "created_at": "2026-09-08T20:00:23.535985Z" },
  "source": {
    "version": "3.1.3.Final", "connector": "postgresql", "name": "legacy",
    "ts_ms": 1788897623553, "snapshot": "false", "db": "monolith",
    "schema": "public", "table": "users", "txId": 793, "lsn": 27550768
  },
  "transaction": null,
  "op": "c",
  "ts_ms": 1788897624026
}
```

### UPDATE — same key `{"id":101}`

```json
{
  "before": { "id": 101, "name": "Thiago",       "created_at": "2026-09-08T20:00:23.535985Z" },
  "after":  { "id": 101, "name": "Thiago Silva", "created_at": "2026-09-08T20:00:23.535985Z" },
  "source": { "...": "...", "table": "users", "txId": 794, "lsn": 27558376 },
  "op": "u",
  "ts_ms": 1788897654431
}
```

### DELETE — key `{"id":102}` — two messages

```json
{
  "before": { "id": 102, "name": "Temp Delete Me", "created_at": "2026-09-08T20:01:18.002374Z" },
  "after": null,
  "source": { "...": "...", "table": "users", "txId": 796, "lsn": 27558864 },
  "op": "d",
  "ts_ms": 1788897680813
}
```

```
key = {"id":102}   value = null      <-- tombstone
```

**Why a tombstone:** it is the log-compaction delete marker. If
`legacy.public.users` is ever configured as a compacted topic, the tombstone
tells Kafka it may erase all earlier messages for key `102`. Downstream
consumers read `value == null` as "this key is gone". Controlled by
`tombstones.on.delete` (default `true`, kept).

### `source` block — what each field tells you

| field | use |
|---|---|
| `ts_ms` (inside `source`) | when the change was **committed in Postgres** |
| `ts_ms` (top level) | when **Debezium processed** it — the gap is the pipeline lag (huge in [Test 7](#14-test-results-real-output) because Kafka was down) |
| `lsn` | WAL position — the absolute ordering key |
| `txId` | Postgres transaction id — changes from the same tx share it |
| `snapshot` | `"false"` for every event we produce (`"true"`/`"last"` only for `op:"r"`) |
| `table` / `schema` / `db` | routing / provenance |

### SALE — `legacy.public.sales`, key `{"id":1002}`

```json
{
  "before": null,
  "after": { "id": 1002, "user_id": 101, "item_name": "CDC Handbook", "quantity": 3,
             "created_at": "2026-09-08T20:01:42.174776Z" },
  "source": { "...": "...", "table": "sales", "txId": 797, "lsn": 27559168 },
  "op": "c",
  "ts_ms": 1788897702638
}
```

Note: no `user_name` — the monolith's API JOINs `users` for its HTTP response,
but the `sales` **table** has only `user_id`. CDC reflects the table, not the API.

---

## 10. Failure behavior

### Debezium / Kafka Connect goes down

* The slot's `restart_lsn` stays frozen at the last confirmed position.
* Postgres **retains all WAL** from that point on.
* On restart, Connect reads its offset from `_connect_offsets` and resumes
  streaming from that LSN. Snapshot is **SKIPPED** ([Test 6](#14-test-results-real-output)):

  ```
  Snapshot ended with SnapshotResult [status=SKIPPED ...]
  Retrieved latest position from stored offset 'LSN{0/1A4A2A0}'
  ```

### Kafka broker goes down (connector stays up)

* The connector task stays `RUNNING` but cannot flush records or offsets.
* Changes made in the legacy DB during the outage stay in the WAL, held by the
  slot.
* When Kafka returns, the buffered events are delivered — late but complete and
  in order ([Test 7](#14-test-results-real-output)). The top-level `ts_ms` jumps
  ~50 s ahead of `source.ts_ms` for those events: that is the visible lag.

### PostgreSQL goes down

* Debezium loses its replication connection and retries with backoff.
* WAL and slot are on Postgres's own durable storage; nothing is lost.
* When Postgres is back, the connector reconnects to the **same named slot** and
  continues.

### Connector restart via REST

`./scripts/connector-restart.sh` → `POST /connectors/<name>/restart?includeTasks=true`.
Same as above: resumes from stored offset, no snapshot.

### ⚠️ WAL retention — the real operational risk

A replication slot **guarantees** WAL availability for its consumer. The flip
side: **if the consumer never comes back, Postgres never frees that WAL**, and
`pg_wal` grows until the disk fills and the database stops accepting writes.

```sql
-- how much WAL is each slot forcing Postgres to keep?
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
       wal_status            -- 'reserved' | 'extended' | 'unreserved' | 'lost'
FROM pg_replication_slots;
```

`./scripts/slot-status.sh` prints exactly this.

For this **lab** no automated monitoring is set up — but the rule is:

> **If you are decommissioning Debezium for good, drop the slot:**
> `SELECT pg_drop_replication_slot('legacy_cdc_slot');`
> An abandoned slot is a slow disk-fill outage waiting to happen.

(`max_slot_wal_keep_size` can cap it in production — at the cost of the slot
going `lost` and the connector needing a re-snapshot. Not set here.)

---

## 11. Ordering & delivery guarantees

### Ordering — per key, within a partition

Kafka guarantees order **only within a single partition**. Debezium sets the
message **key to the row primary key** (`{"id":101}`), and Kafka's default
partitioner hashes the key → **all events for one row land in the same
partition** → they are consumed in the order Postgres committed them.

```
key {"id":101}:   c  →  u (Thiago→Thiago Silva)  →  u (→Thiago Silva Jr)  →  d
                  └──────────── consumed in exactly this order ───────────┘
```

(The first three are the real Tests 1–2 + Test 7 events for id 101; the trailing
`d` is shown to complete the lifecycle — the point is that whatever the sequence,
a consumer of that key sees it in commit order.)

Across *different* keys there is no global order (and you don't need one — user
101 and user 102 are independent). Do **not** repartition these topics or
override the key without a reason; that is what preserves per-row order.

### Delivery — at-least-once, NOT exactly-once

Debezium + Kafka Connect is **at-least-once**. A crash between "record written to
Kafka" and "offset flushed to `_connect_offsets`" means those records are
**re-sent** after restart. [Test 7](#14-test-results-real-output) is the mild
version of this.

> **Consequence for the next phase:** the future User Service / Sales Service
> Kafka consumers **must be idempotent** — applying the same `{op, id, lsn}`
> twice must equal applying it once (e.g. upsert by PK, ignore an event whose
> `source.lsn` ≤ the last one already applied for that row). This is called out
> now so it is designed in, not bolted on.

---

## 12. How to run it from zero

Prerequisites: Docker; the `monolito-microservice` stack running.

```bash
# 0. shared network (once)
docker network create migration-network

# 1. monolith Postgres: wal_level=logical + join the network
#    (patch already in monolito-microservice/docker-compose.yml)
cd ../monolito-microservice && docker compose up -d && cd -

# 2. this repo
cp .env.example .env            # set LEGACY_PG_DEBEZIUM_PASSWORD
docker compose up -d            # kafka + connect

# 3. database side: role, REPLICA IDENTITY, publication, slot
./postgres/enable-cdc.sh

# 4. register the Debezium connector
./debezium/register-connector.sh

# 5. verify
./scripts/health-check.sh
./scripts/list-topics.sh
```

Teardown (keeps all data and the stream position):

```bash
docker compose down            # NEVER -v : the kafka_data volume holds Connect offsets
```

`docker compose down -v` here would drop `_connect_offsets` → next start
re-runs the `no_data` snapshot (harmless, no rows) but loses the LSN position.
The legacy database is in another repo and is never touched by this one.

---

## 13. Networking: localhost vs container DNS

Three compose projects, three private networks, plus one **shared external**
network `migration-network`.

```
                      migration-network  (external, shared)
   ┌───────────────────────┬───────────────────────┐
   │                       │                       │
monolito-microservice-   cdc-kafka             cdc-connect
  postgres-1                                       │
  alias: legacy-postgres                           │
   │                                               │
   └────────── Debezium connects here ─────────────┘
              database.hostname = legacy-postgres
              database.port     = 5432   (container port, NOT 5432 host)
```

| from | to reach the legacy DB use | to reach Kafka use |
|---|---|---|
| **your host shell** (psql, kafka CLI on your laptop) | `localhost:5432` | `localhost:9092` |
| **a container on `migration-network`** (Debezium) | `legacy-postgres:5432` | `kafka:9092` |

`localhost` **inside a container = that container itself.** If Debezium's config
said `database.hostname=localhost` it would try to connect to the Connect
container, not Postgres. Containers must use the other container's **service
name / network alias** as a DNS hostname. That is why:

* `.env` → `LEGACY_PG_HOST=legacy-postgres`, `LEGACY_PG_PORT=5432`
* the Kafka container advertises **two** listeners: `kafka:9092` for containers,
  `localhost:9092` for host tools (`KAFKA_HOST_PORT`, published as `9092:29092`).

Port map recap (no clash with 8000/8001/8080/5432/5433/5434):

| service | host port | container port | purpose |
|---|---|---|---|
| kafka | `9092` | `29092` | host inspection only |
| kafka (internal) | — | `9092` | container-to-container |
| connect | `8083` | `8083` | REST API |

---

## 14. Test results (real output)

All changes below were made **through the monolith** (its `POST /users`,
`POST /sales`) or, where the monolith exposes no endpoint (it has no
`PUT`/`DELETE` for users), **directly against the legacy database** — which is
what the monolith itself would do and produces an identical CDC event. Nothing
was ever written to the User DB or Sales DB directly.

Baseline before testing: `users = 100 (max id 100)`, `sales = 1001 (max id 1001)`.

### Test 5 first — snapshot was NOT republished

Immediately after `register-connector.sh`, before any test write:

```
$ ./scripts/list-topics.sh
__consumer_offsets
__debezium-heartbeat.legacy
_connect_configs
_connect_offsets
_connect_status
```

→ **no `legacy.public.users` / `legacy.public.sales` topic at all.** With
`snapshot.mode=initial` those two topics would already hold 100 and 1001
`op:"r"` messages. They appear only after the first real change, and only ever
contain test events:

```
$ docker exec cdc-kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic legacy.public.users
legacy.public.users:0:6          # 6 messages, all from the tests below — not 100+

$ docker exec cdc-kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic legacy.public.sales
legacy.public.sales:0:1          # 1 message — not 1001
```

Connect log confirms: `Snapshot ended with SnapshotResult [status=SKIPPED ...]`.

### Test 1 — INSERT user

```
$ curl -s -X POST localhost:8000/users -d '{"name":"Thiago"}'
{"id":101,"name":"Thiago","created_at":"2026-09-08T20:00:23.535985Z"}
```

`legacy.public.users`, key `{"id":101}`:

```json
{ "before": null,
  "after": { "id": 101, "name": "Thiago", "created_at": "2026-09-08T20:00:23.535985Z" },
  "source": { "table": "users", "txId": 793, "lsn": 27550768, "ts_ms": 1788897623553, "snapshot": "false" },
  "op": "c", "ts_ms": 1788897624026 }
```

### Test 2 — UPDATE user

```
$ docker exec monolito-microservice-postgres-1 psql -U postgres -d monolith \
    -c "UPDATE users SET name='Thiago Silva' WHERE id=101;"
UPDATE 1
```

key `{"id":101}`:

```json
{ "before": { "id": 101, "name": "Thiago",       "created_at": "2026-09-08T20:00:23.535985Z" },
  "after":  { "id": 101, "name": "Thiago Silva", "created_at": "2026-09-08T20:00:23.535985Z" },
  "source": { "table": "users", "txId": 794, "lsn": 27558376 },
  "op": "u", "ts_ms": 1788897654431 }
```

`before.name = "Thiago"`, `after.name = "Thiago Silva"`, `op = "u"` ✔

### Test 3 — DELETE user

```
$ curl -s -X POST localhost:8000/users -d '{"name":"Temp Delete Me"}'   # -> id 102
$ docker exec monolito-microservice-postgres-1 psql -U postgres -d monolith \
    -c "DELETE FROM users WHERE id=102;"
DELETE 1
```

Two messages on key `{"id":102}`:

```json
{ "before": { "id": 102, "name": "Temp Delete Me", "created_at": "2026-09-08T20:01:18.002374Z" },
  "after": null,
  "source": { "table": "users", "txId": 796, "lsn": 27558864 },
  "op": "d", "ts_ms": 1788897680813 }
```
```
{"id":102}    null          <-- tombstone
```

→ a **delete event** (`op:"d"`, full `before`, `after:null`) **and** a
**tombstone** (`value:null`), because `tombstones.on.delete=true`. Explained in
[section 9](#9-event-format-real-captures).

### Test 4 — INSERT sale

```
$ curl -s -X POST localhost:8000/sales -d '{"user_id":101,"item_name":"CDC Handbook","quantity":3}'
{"id":1002,"user_id":101,"user_name":"Thiago Silva","item_name":"CDC Handbook","quantity":3, ...}
```

`legacy.public.sales` only (nothing on `legacy.public.users`), key `{"id":1002}`:

```json
{ "before": null,
  "after": { "id": 1002, "user_id": 101, "item_name": "CDC Handbook", "quantity": 3,
             "created_at": "2026-09-08T20:01:42.174776Z" },
  "source": { "table": "sales", "txId": 797, "lsn": 27559168 },
  "op": "c", "ts_ms": 1788897702638 }
```

### Test 6 — restart Debezium, no re-snapshot

```
$ docker restart cdc-connect
$ curl -s localhost:8083/connectors/legacy-cdc-connector/status
{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}
```

Connect log:

```
Snapshot ended with SnapshotResult [status=SKIPPED ...]
Starting streaming
Retrieved latest position from stored offset 'LSN{0/1A4A2A0}'
Obtained valid replication slot ReplicationSlot [active=false, latestFlushedLsn=LSN{0/1A4A2A0} ...]
```

New change after the restart:

```
$ curl -s -X POST localhost:8000/users -d '{"name":"After Hard Restart"}'   # -> id 104
$ docker exec cdc-kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic legacy.public.users
legacy.public.users:0:7      # +1, not +100
```

→ connector resumed from offset, streamed exactly the one new event.

### Test 7 — Kafka temporarily down

```
$ docker stop cdc-kafka
$ curl -s localhost:8083/connectors/legacy-cdc-connector/status
{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}   # still RUNNING

# changes made WHILE Kafka is down:
$ curl -s -X POST localhost:8000/users -d '{"name":"Created While Kafka Down"}'   # id 105
$ docker exec ...-postgres-1 psql ... -c "UPDATE users SET name='Thiago Silva Jr' WHERE id=101;"

# Connect logs: WARN Error connecting to node kafka:9092 (retrying, not FAILED)

$ docker start cdc-kafka
```

After Kafka is back, both buffered changes arrive:

```json
{ "after": { "id": 105, "name": "Created While Kafka Down", ... },
  "source": { "ts_ms": 1788897800417 },      // committed 20:03:20
  "op": "c", "ts_ms": 1788897854723 }        // delivered 20:04:14  -> ~54s pipeline lag
```
```json
{ "before": { "id": 101, "name": "Thiago Silva" },
  "after":  { "id": 101, "name": "Thiago Silva Jr" },
  "op": "u", "ts_ms": 1788897854724 }
```

`pg_replication_slots` peaked at a few hundred bytes of `retained_wal` during the
outage and returned to ~288 bytes after catch-up. **No destructive action, no
data loss, nothing `FAILED`.**

### Data integrity after all tests

```
 users | max_user_id | sales | max_sale_id
-------+-------------+-------+-------------
   104 |         105 |  1002 |        1002
```

100 original users + 101,103,104,105 − 102 (deleted in Test 3) = **104**.
1001 original sales + 1002 = **1002**. Every original row preserved.

### Final connector status

```
name:        legacy-cdc-connector
connector:   RUNNING   (worker 172.22.0.4:8083)
task 0:      RUNNING
slot:        legacy_cdc_slot   active=t   plugin=pgoutput
publication: legacy_cdc_publication  ->  public.users, public.sales
wal_level:   logical
topics:      legacy.public.users, legacy.public.sales, __debezium-heartbeat.legacy
```

---

## 15. What is intentionally NOT here

Not built in this phase (each is a later, deliberate step):

Kafka consumers in `user-service` / `sales-service` · API Gateway · cutover ·
new frontend · dual-write · Saga · Outbox pattern · Kubernetes · Schema Registry ·
Avro / Protobuf · DLQ · retry topics · business consumer groups · SMTs /
`ExtractNewRecordState` · multi-broker Kafka · monitoring/alerting.

The pipeline is proven: **Monolith → Legacy PostgreSQL → WAL → Debezium → Kafka.**
Stop here and study the events before wiring up consumers.

---

## Repository layout

```
cdc-infrastructure/
├── docker-compose.yml            # kafka (KRaft) + kafka-connect/debezium
├── .env.example                  # copy to .env
├── debezium/
│   ├── register-connector.json   # connector config template (${VARS} from .env)
│   └── register-connector.sh     # idempotent create-or-update + status
├── postgres/
│   ├── enable-cdc.sh             # role + REPLICA IDENTITY + publication + slot
│   └── README.md                 # the 3-line monolith compose patch, explained
├── scripts/
│   ├── _common.sh                # shared helpers (sourced)
│   ├── health-check.sh           # containers, kafka, connect, connector, slot, publication
│   ├── list-topics.sh
│   ├── inspect-users.sh          # dump legacy.public.users events (pretty JSON)
│   ├── inspect-sales.sh
│   ├── slot-status.sh            # replication slot + retained WAL
│   └── connector-restart.sh      # REST restart (Test 6)
└── README.md                     # this file
```

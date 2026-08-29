# Architecture

`stored` is three concentric layers, mirroring how `seared` → `zeared` nest.

```text
stored.zenoh  (zeared — a core dependency; Chronicler also re-exported lazily as stored.Chronicler)
   Chronicler: on_message → record      |  on_query → serve history
   session (timestamped) · daemon · CLI
──────────────────────────────────────────────────────────────────
stored  (core: seared + stdlib sqlite3)
   Store · StreamRegistry · schema · row · Writer · query · Reaper · streams
──────────────────────────────────────────────────────────────────
StorageBackend
   SQLiteBackend (default)  ·  DuckDBBackend (stored[duckdb])  ·  [ Postgres — later ]
```

`zeared` is a core dependency (plan 02), but the **core** knows only `seared`
classes, tables, rows, and time and imports no `zeared` *at runtime* — so a
Store-only `import stored` stays transport-free (every `zeared` class *is* a
`seared` class, and the top-level `Chronicler` re-export is lazy). The **zenoh
layer** wires the core's `Writer` to a `zeared` subscriber and the core's query
planner to a `zeared` queryable.

## The gap it fills

`zeared` **retention** is a *last-value cache* — one payload per topic, served
lazily by a queryable. It is not history. `stored` is the durable, time-keyed
layer beneath it: it records every sample and serves ranges back.

## Ingest path

```text
Cls.on_message(record, ...)          [zeared subscriber, 2-arg callback]
   record(msg, meta)                 — meta carries the Zenoh HLC timestamp
   → Store.record → build_row        — meta cols + scalar fields + _payload
   → Writer.enqueue                  — per-table buffer
   → Backend.append_batch            — INSERT … ON CONFLICT DO NOTHING
```

- The **2-arg callback** receives a `ZenohMeta` carrying `issued_at` (parsed UTC
  from the HLC timestamp), the raw `timestamp` (HLC string), `key_expr`,
  `source_info`, and `schema`. Timestamping must be enabled on the session
  (`stored.zenoh` opens it that way by default).
- Writes are **batched** — a statement per message is wasteful (DuckDB is
  columnar and dislikes per-row inserts; SQLite pays per-transaction overhead), so
  the `Writer` flushes on a row-count threshold, on a periodic interval, and on
  close.
- The `(_key_expr, _ts_hlc)` primary key makes redelivery an **idempotent**
  no-op (reconnect replays, at-least-once quirks).

## Query path

```text
Cls.query(id=…, params={from,to,limit})   [zeared getter]
   → stored queryable handler
   → Store.query (flush, then plan+select)
   → ctx.reply(inst) per row                — streamed, properly-typed instances
```

`Store.query` **flushes the writer first**, so reads always see prior writes
(read-your-writes). Consumers use the *unchanged* `zeared` getter — history
comes back as ordinary typed `Cls` instances.

### The RETAINED constraint

`zeared` forbids `on_query` on a `RETAINED` class (retention already owns a
queryable on that topic). So the transparent query path serves **non-RETAINED**
classes — which telemetry streams typically are. RETAINED classes are recorded
but not served transparently (a generic `HistoryQuery` contract for them is on
the roadmap).

## Time

Everything stored and queried is **naive UTC** (`TIMESTAMP` columns).
Canonicalizing to naive UTC at the boundary keeps storage dependency-free across
backends — DuckDB's client would otherwise need `pytz` for `TIMESTAMPTZ`, and the
SQLite backend stores ISO-8601 text whose lexical order is chronological.
`_issued_at` (the mesh delivery/issue time) is the default temporal axis for range
queries and retention; a stream may instead key on its **domain event time** via a
`time_field`, normalized into `_event_at` (see storage-model). `_ts_hlc` is the
dedup/ordering tiebreaker (HLC strings are lexicographically == temporally ordered).
Records with no HLC stamp get a synthesized, unique, sortable `_ts_hlc` and fall
back to receive time (`_ts_source = 'recv'`).

## Concurrency

A single `RLock` per `Store` serializes all backend I/O. The writer's periodic
flush thread, the on-message ingest (Zenoh delivery thread), and on-query serves
(Zenoh query thread) all funnel through it, so the single backend connection is
never touched concurrently. A slow backend applies backpressure to the delivery
thread (block semantics); a backend *error* drops the in-flight batch — the
documented, bounded telemetry-loss envelope.

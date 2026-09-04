# Architecture

`stored` is four concentric layers, mirroring how `seared` → `zeared` nest.

```text
stored.mesh   (the contract-shaped surface; AsyncStore needs no transport)
   AsyncStore: await query/latest/iter · sync register/record
   Binding:    record(store_as/via) · serve_range(REQUEST) · serve_latest(project)
   Replayer:   recorded history → the mesh, on a declared replay scope
──────────────────────────────────────────────────────────────────
stored.zenoh  (zeared — a core dependency; Chronicler also re-exported lazily as stored.Chronicler)
   Chronicler: on_message → record      |  on_query → serve history
   session (timestamped) · daemon · CLI
──────────────────────────────────────────────────────────────────
stored  (core: seared + stdlib sqlite3)
   Store · StreamRegistry · schema · row · Writer · query · Reaper · streams · dialect
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

`stored.mesh` sits above both, holding the shapes a *service* binds rather than the
transport itself. Its first inhabitant, `AsyncStore`, needs no transport at all — it
is asyncio over the core, so `from stored.mesh import AsyncStore` stays
Zenoh-free (a guarded invariant), and the names in that namespace that *do* need
`zeared` resolve lazily.

### What a dialect is for

The planner is engine-neutral — quoted identifiers, `?` parameters, ordinary
`WHERE`/`ORDER BY`/`LIMIT` — except for two fragments that are not portable, which
`stored.dialect` owns rather than leaving inline:

- **Wildcard key matching.** SQLite and DuckDB both have `GLOB`; Postgres (below,
  later) has no such operator, so a dialect returns the whole predicate rather than
  an operator name.
- **Reaching into `_payload`.** SQLite's `json_extract` compares against a bound
  scalar of any type. DuckDB's returns JSON, so a *text* comparison needs
  `json_extract_string` — and getting it wrong there is an error, not a silent
  mismatch (measured; the tests pin it).

The backend supplies its dialect; `Store` hands it to the planner. `plan()` also
takes the **table** to read, defaulting to the stream's history table — the latest
projection carries the same columns and the same sort key, so a current-state read
is the same plan pointed elsewhere.

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

### Streaming reads

`Store.iter` is the same plan walked in pages instead of materialized:

```text
Store.iter(cls, since=…)          — flush once, at the call
   → plan(..., after=anchor)      — keyset predicate on (time, _ts_hlc, _key_expr)
   → select LIMIT <chunk>         — lock held for one page, then released
   → yield rehydrate(cls, row)    — a page in memory, never the window
   → anchor = last row            — resume; a short page ends the walk
```

The anchor is the full sort key, and `(_key_expr, _ts_hlc)` is the primary key, so
the triple is a total order: a walk resumes exactly where it stopped even though
the writer kept appending between pages. That makes the walk **not a snapshot** —
rows recorded mid-walk that sort after the cursor are included — and it skips rows
whose temporal axis is `NULL`, which have no place on the axis being resumed
along.

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

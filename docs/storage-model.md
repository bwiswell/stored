# Storage model

Each registered message class gets one table, `stream_<snakecase>`. A row is
*typed columns for scalar fields + a lossless payload*.

## Columns

Meta columns come first, then one column per **scalar** seared field:

| Column       | Type        | Source                                    |
|--------------|-------------|-------------------------------------------|
| `_key_expr`  | `VARCHAR`   | `meta.key_expr` (concrete topic) — PK     |
| `_ts_hlc`    | `VARCHAR`   | `meta.timestamp` (HLC, sortable) — PK      |
| `_issued_at` | `TIMESTAMP` | `meta.issued_at` (naive UTC)              |
| `_event_at`  | `TIMESTAMP` | the stream's `time_field`, normalized (NULL if none) |
| `_source`    | `VARCHAR`   | `meta.source_info`                        |
| `_schema`    | `VARCHAR`   | `meta.schema` (SCHEMA version tag)        |
| `_recv_at`   | `TIMESTAMP` | local receive time                        |
| `_ts_source` | `VARCHAR`   | `'hlc'` \| `'recv'`                        |
| `_payload`   | `VARCHAR`   | full message as a seared JSON string      |
| *(fields)*   | mapped      | one column per scalar field / topic slot  |

- **Primary key / dedup:** `(_key_expr, _ts_hlc)`.
- **Topic-slot fields are columns too** — a `TOPIC = 'robot/{id}/…'` slot `id`
  becomes a queryable dimension, not just part of the key.
- **`_payload` is the lossless rehydration source.** Rehydration reads it back
  via `Cls.loads(...)`; the scalar columns are projections for filtering. (v1
  keeps the whole message in `_payload`; slimming it to non-column fields is a
  later optimization.)

## Event time (`time_field`)

By default `stored` orders, ranges, and expires on `_issued_at` — the mesh
delivery/issue time. A stream may instead key on its **domain event time** by
naming a payload field at registration:

```python
store.register(Location, retention='7d', time_field='observed_at')
```

The row builder normalizes that field into the `_event_at` column — a numeric field
is read as **unix epoch seconds** (UTC), a `datetime` is canonicalized, a bare `date`
becomes midnight UTC — and the reaper and query planner then key on `_event_at`
instead of `_issued_at`. `time_field` must name an `Int`/`Float`/`DateTime`/`Date`
field (an absolute instant), checked at registration. So a late-delivered but older
observation prunes and sorts by *when it happened*, not when it arrived — what a
historian needs. Streams with no `time_field` are unchanged: `_event_at` is `NULL`
and everything keys on `_issued_at` exactly as before. (A single field, no fallback
chain — deliberate; revisit only if a stream needs one.)

## Type map (scalar fields)

`Int→BIGINT`, `Float→DOUBLE`, `Bool→BOOLEAN`, `Str`/`Path`/`UUID`/`Enum→VARCHAR`,
`Bytes→BLOB`, `DateTime→TIMESTAMP`, `Date→DATE`, `Time→TIME`,
`TimeDelta→INTERVAL`, `Decimal→DECIMAL(38,9)`.

These are the **backend-neutral** (DuckDB-spelled) type names; each backend remaps
them to its own dialect. The **SQLite backend** (the default) maps them to SQLite
declared types (`BIGINT→INTEGER`, `DOUBLE→REAL`, `VARCHAR→TEXT`, …) and stores
temporal values as ISO-8601 **text** — lexicographic order matches chronological
order, so range/retention comparisons stay correct — registering adapters/converters
so they still bind and read back as native `datetime`/`date`/`time`/`Decimal`.

**Complex fields** — nested (`T`), tagged unions (`Union`), collections
(`many` / `keyed`), arrays/frames (`NDArray`, `PandasFrame`, `PolarsFrame`) —
have no column; they round-trip through `_payload` only.

## Schema evolution

`register` runs the backend's additive `ensure_table`: new fields become new
(nullable) columns; removed/retyped fields are left in place (readable) and a
migration is a manual step. Each row keeps its `_schema` tag, so mixed-schema
history is self-describing. `stored migrate` re-runs the reconcile for every
configured stream.

## TTL

`stored` runs an **active** reaper (unlike zeared's lazy last-value expiry):

- per-stream `retention` horizon (`'7d'`, `'48h'`, `'30m'`, …; `None` = keep
  forever), validated at `register`;
- `Reaper.sweep` deletes rows whose temporal axis (`_event_at` for an event-time
  stream, else `_issued_at`) is older than `now − retention`, returning the exact
  count;
- driven by `Store.prune`, the `prune` CLI, and the daemon's periodic reaper
  (`prune_interval`, default 300 s; `0` disables).

## Archival roadmap (not v1)

The DuckDB choice makes cold archival nearly free, and the day-grain
`_issued_at` schema is chosen so it stays additive:

1. **Segment** — `COPY (SELECT … WHERE _issued_at < cutoff) TO
   'archive/<stream>/<date>.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)`, then
   delete those rows from the live DB.
2. **Cold reads** — queries reaching past the warm horizon `UNION` the live
   table with `read_parquet('archive/<stream>/**/*.parquet')`.
3. **Tiers** — hot (live DuckDB) → cold (Parquet segments) → expired. Parquet
   segments are engine-neutral, so the archive outlives the live backend.

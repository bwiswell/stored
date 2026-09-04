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

Range queries meet this axis: `store.query(..., since=…, until=…)` bounds accept not
only ISO / relative (`'-1h'`) strings but also **unix epoch seconds** and `datetime`
objects — so a service maps a request's numeric `from_ts` / `to_ts` straight through,
the same unix-seconds axis as `time_field`. `stored` provides these read primitives
(`query` for ranges, `latest` for last-known); *serving* them over a mesh
request/reply contract is the consumer's job (the pure-core split — `stored` stays
`seared`-only and never learns the wire contracts).

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

## Latest projection (`latest_key`)

A stream is an append log — every sample kept until retention. Some consumers want
the opposite: the **single newest value per logical entity**, however old — a tag's
last-known position, a device's last state. Register a `latest_key` and `stored`
maintains a second table, `latest_<snake>`, alongside the history:

```python
store.register(
    Location, retention='7d', time_field='observed_at',
    latest_key=('source', 'epc'), latest_retention='30d',
)
last = store.latest(Location, source='rtls', epc='E28…')   # -> Location | None
```

- **One row per key.** `latest_<snake>` has the *same columns* as the history table
  (so a latest row rehydrates to the full instance via `_payload`), but its primary
  key is `latest_key` — the logical entity, not `(_key_expr, _ts_hlc)`.
- **Newest-wins, order-independent.** On each flush the same batch that appends to
  history is upserted into the projection, overwriting a key only when the incoming
  row's temporal axis (`time_column`) is **≥** the stored one — so a late or
  redelivered older sample never clobbers a newer last-known.
- **Its own, longer retention.** `latest_retention` expires the projection
  separately from the history `retention` — the last-known **survives** history
  expiry (a 30-day last-position over a 7-day history). `None` keeps it forever.
- **`store.latest(cls, **key)`** flushes pending writes, then returns the decoded
  instance for the full key (or `None`). `latest_key` names must be scalar-column
  fields, checked at registration.

This is `stored`'s answer to a durable "last-known" query without retaining one mesh
key per entity — the historian pattern behind e.g. a location store's
`LastKnownLocation`. Serving that over a mesh contract stays the consumer's job;
`stored` provides the read primitive.

## Schema evolution

`register` runs the backend's additive `ensure_table`: new fields become new
(nullable) columns; removed/retyped fields are left in place (readable) and a
migration is a manual step. Each row keeps its `_schema` tag, so mixed-schema
history is self-describing. `stored migrate` re-runs the reconcile for every
configured stream.

## Reaching into a `dict`

A complex field has no column — it round-trips through `_payload` only — which
normally puts it out of the query planner's reach. Some of them have **open-ended
keys by design** (`Location.zones` is `{layer: zone_id}`, and the layers belong to
the deployment), so promoting them to columns is not an option even in principle.

Declaring a path makes one filterable:

```python
store.register(Location, json_index=('zones.department',), …)
store.query_latest(Location, where={'zones.department': 5})
```

Three things the declaration buys:

- **Validation at registration.** The head must name a `Dict` field of the class and
  the path must reach *inside* it, so a typo is a `RegistrationError` at open time
  rather than a filter that silently matches nothing.
- **Wire-name translation.** `_payload` is written with each field's wire name, so a
  field carrying `data_key='zn'` stores `{"zn": …}` while the caller still writes
  `zones.department`. `stored` rewrites the head; getting this wrong is the silent
  failure the declaration exists to prevent.
- **An allow-list.** `where=` accepts only declared paths, exactly as `**filters`
  accepts only declared dimensions.

A row whose dict lacks the key simply does not match. Equality only, for now.

## Indexes

Beyond the `(_key_expr, _ts_hlc)` primary key, `register` emits two kinds of
secondary index — and deliberately no more:

- **The temporal index**, `(time_column, _ts_hlc, _key_expr)` — the exact sort key
  every planned SELECT uses, so one index serves range queries, `Store.iter`'s
  keyset paging, and the reaper's `DELETE … WHERE time < cutoff` alike.
- **One per declared `index=` dimension**, single-column each rather than one
  composite: filters are independent (a query may name `kind` without `source`,
  which a composite's leading-column rule would not serve).

`_key_expr` equality needs no index of its own — it leads the primary key, whose
unique index already serves it. The same specs are emitted for the **latest
projection**, which `query_latest`/`iter_latest` read, minus any dimension the
entity key already leads (a filter on the first `latest_key` field is served by that
table's own primary-key index). The cost is write amplification on the append
path, which is why the set is small and every extra index is opt-in via `index=`.
Emission is idempotent (`CREATE INDEX IF NOT EXISTS`), so an existing store gains
them on the next `register`/`migrate`.

## TTL

`stored` runs an **active** reaper (unlike zeared's lazy last-value expiry):

- per-stream `retention` horizon (`'7d'`, `'48h'`, `'30m'`, … — or a number of
  seconds / a `datetime.timedelta`, canonicalized to the string form; `None` = keep
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

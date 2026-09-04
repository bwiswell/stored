# stored

`stored` is a small, database-wrapping **persistence layer for `seared`
objects**, with a first-class **Zenoh chronicler** built on
[`zeared`](https://github.com/bwiswell/zeared) (a core dependency). It is the
durable, time-keyed history layer beneath
[`zeared`](https://github.com/bwiswell/zeared)'s last-value **retention** — the
sister that remembers.

## Why `stored`

- **Chronicle a mesh.** Point a `stored` daemon at one or more `zeared` topics;
  it records every sample to a local database, keyed by the Zenoh HLC
  timestamp.
- **Query history transparently.** Mesh consumers use the *existing*
  `Cls.query(params={'from': …, 'to': …})` getter — `stored` answers with
  historical, properly-typed instances over a queryable.
- **Last-known, not just history.** Register a `latest_key` and `stored` keeps a
  newest-per-entity projection alongside the log — a durable "last value however
  old," on its own longer retention. Read one entity with `store.latest(cls, **key)`,
  or the whole population with `store.query_latest(cls, …)` / `iter_latest`.
- **Event-time aware.** Point a stream at a payload timestamp (`time_field`) and
  retention, range queries, and last-known key off **when it happened**, not when
  the mesh delivered it.
- **Stream it, don't buffer it.** `store.iter(cls, …)` walks a window in bounded
  memory — a page at a time, resumable, and without holding the store lock or the
  whole result set.
- **Bounded by design.** Per-stream **TTL** expiry now; compressed **Parquet
  archival** of cold data on the roadmap.
- **Lean, layered deps.** `stored` needs `seared` + `zeared`; persistence rides
  stdlib `sqlite3` (zero extra dependency). The seared-only *core* imports no
  `zeared` at runtime, so a Store-only `import stored` stays transport-free — the
  chronicler (`stored.Chronicler`) is re-exported lazily. DuckDB is the one
  optional backend (`stored[duckdb]`) for analytics.

## Install

```sh
# persist/query seared objects + the Zenoh chronicler (stdlib sqlite3 backend)
uv add git+https://github.com/bwiswell/stored

# with the DuckDB backend (analytics upgrade)
uv add "stored[duckdb] @ git+https://github.com/bwiswell/stored"
```

The Zenoh chronicler ships with the base install (`zeared` is a core dependency);
the retired `stored[zenoh]` extra remains a no-op alias for one release.

Requires Python ≥ 3.14.

## Core: persist & query seared objects

```python
import stored

store = stored.Store('chronicle.db')
store.register(Telemetry, retention='7d', index=('id',), latest_key=('id',))
# retention also takes seconds or a timedelta: 604800, timedelta(days=7)

store.record(Telemetry, Telemetry(id=7, x=1.5))          # buffered write
history = store.query(Telemetry, id=7, since='-1h', limit=5000)   # → list[Telemetry]
newest  = store.latest(Telemetry, id=7)                  # → Telemetry | None, however old

for row in store.iter(Telemetry, since='-30d'):          # streamed, uncapped
    ...                                                  # a page in memory at a time

current = store.query_latest(Telemetry, since='-1h')     # every entity's newest, filtered

# filter on a key *inside* a dict field, declared with json_index=(…)
here = store.query_latest(Location, where={'zones.department': 5})

store.flush(); store.prune(); store.close()
```

`record` buffers through a batched writer; `query` flushes first, so reads
always see prior writes. Reads are **typed on the class you ask for** —
`query(Telemetry)` is a `list[Telemetry]` and `latest(Telemetry, …)` a
`Telemetry | None` to a type checker, with no cast at the call site. Retention
horizons accept the duration grammar (`'7d'`), a number of seconds, or a
`datetime.timedelta`.

Some fields have **open-ended keys by design** — a location's `zones` is
`{layer: zone_id}`, and the layer names belong to the deployment, so they can never
be columns. Declare a path with `json_index=('zones.department',)` and it becomes
filterable through `where=` on every read; `stored` translates the path into the
payload's own spelling, so a field aliased with `data_key=` still matches — and
indexes it on both tables (a real expression index on SQLite; DuckDB says in the log
that it will scan instead).

`query`/`iter` read the **history** table; `query_latest`/`iter_latest` read the
**latest projection** — one row per entity instead of one per observation, for the
"where is everything right now" question. The projection carries the same columns and
sort key, so filters, ordering, paging and indexes work identically on both; on it,
`since`/`until` mean *last seen* in that window.

`query` returns one list (capped, and the right shape for a request/reply). `iter`
**streams** the same window a page at a time, for windows larger than memory: it
flushes once when called, then walks a keyset cursor, releasing the store lock
between pages so recording continues underneath it. Registration emits the
secondary indexes those reads want — one on the temporal axis, one per declared
`index=` dimension.

## Async services: `stored.mesh.AsyncStore`

A service on an event loop must not call a blocking store inline. `AsyncStore`
wraps one so it doesn't have to hand-roll the thread hop:

```python
from stored.mesh import AsyncStore

store = AsyncStore(stored.Store('history.db', flush_rows=5000))
store.register(Location, retention=timedelta(days=3), latest_key=('source', 'epc'))

store.record(Location, location)                  # sync — a buffered enqueue
newest = await store.latest(Location, source='rtls', epc=epc)
async for row in store.iter(Location, since='-7d'):   # one thread hop per page
    ...
```

`register` and `record` stay synchronous on purpose — the first is one-time DDL on
the open path, the second an enqueue cheaper than the hop that would defer it.
Everything reaching the backend is awaited, and `iter` hands control back to the
loop between pages, so a long walk never stalls the service around it.

## Services: declare the bindings, don't write them

`Binding` turns the subscriber and the two queryables every historian ends up
writing into declarations:

```python
from stored.mesh import Binding

binding = Binding(store)
binding.record(Alarm, store_as=Event, via=from_alarm)   # subscribe A → persist B
binding.serve_range(Event, filters=('source', 'kind'), since='from_ts', limit='limit')
binding.serve_latest(LastPosition, of=Position, key=('source', 'epc'),
                     project=to_last_position, missing=no_position)

binding.serve_range(Event, filters=('source',), stream=True)   # reply row by row
```

Recorded history can also go back **onto** the mesh, where ordinary subscribers
see it with no client change at all:

```python
from stored.mesh import Replayer

await Replayer(store).replay(Reading, since='-1d')       # backfill a pipeline stage
handle = Replayer(store).start(Reading, speed=1.0)       # …or re-run it in real time
```

A replay publishes on a scope the class declares (`EXTRA_TOPICS`), which is what
lets a historian keep recording live traffic while ignoring the replay
(`record(..., live_only=True)`).

Handlers are `async def` and await the store off the loop. The binding speaks only
`zeared` vocabulary — a message class, its `REQUEST` payload, a queryable, a
projection — so it serves any `@zeared` contract without knowing a thing about it.
See [docs/mesh.md](docs/mesh.md).

## Mesh: the chronicler

```python
import zeared as z
import stored, stored.zenoh as sz

session = z.peer()                          # timestamped session
store = stored.Store('chronicle.db')
chronicler = sz.Chronicler(store, session)
chronicler.add(Telemetry, retention='7d')   # subscribe + serve history
chronicler.run()
```

Consumers then retrieve history with the ordinary `zeared` getter:

```python
history = Telemetry.query(id=7, params={'from': '-1h', 'limit': '5000'})
```

## Daemon

```sh
stored -c stored.toml validate-config
stored -c stored.toml run                   # systemd Type=notify service
stored -c stored.toml migrate               # create/reconcile tables
stored -c stored.toml prune                 # force a TTL sweep
```

See `systemd/stored.service` for the unit template.

## Documentation

- [docs/architecture.md](docs/architecture.md) — the four layers, ingest/query/
  streaming paths, timestamps, concurrency.
- [docs/storage-model.md](docs/storage-model.md) — row shape, type map, dedup,
  indexes, TTL, archival roadmap.
- [docs/mesh.md](docs/mesh.md) — the `stored.mesh` layer: `AsyncStore` + `Binding`.
- [docs/chronicler.md](docs/chronicler.md) — the `stored.zenoh` layer + daemon.
- [docs/configuration.md](docs/configuration.md) — `StoredConfig` / `StreamSpec`.

## Development

```sh
uv sync --extra duckdb
uv run pytest tests/
```

Tests mirror the source layout. The mesh tests spin up a loopback Zenoh peer
(`zeared` is a core dependency, always present); the DuckDB-backend tests need the
`duckdb` extra (synced above); the SQLite core tests need no transport.

## Status

The v1 core is functional: SQLite-backed persistence (stdlib; DuckDB an optional
backend), a batched writer, event-time keying (`time_field`), latest-per-key
projections (`latest_key` / `store.latest`), secondary indexes, bounded-memory
streaming (`store.iter`), TTL pruning, the service layer (`stored.mesh`:
`AsyncStore`, declarative record / range / last-known bindings with optionally
streamed ranges, and replay-as-publication), the Zenoh chronicler (record + serve history), and a runnable
daemon. Deferred: the Postgres backend, complex-field column promotion, the
cold-archival tiers.

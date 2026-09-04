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
  old," on its own longer retention, read with `store.latest(cls, **key)`.
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

for row in store.query(Telemetry, id=7, since='-1h', limit=5000):
    ...                                                  # one list, capped

for row in store.iter(Telemetry, since='-30d'):          # streamed, unbounded
    ...                                                  # a page in memory at a time

store.flush(); store.prune(); store.close()
```

`record` buffers through a batched writer; `query` flushes first, so reads
always see prior writes. Reads are **typed on the class you ask for** —
`query(Telemetry)` is a `list[Telemetry]` and `latest(Telemetry, …)` a
`Telemetry | None` to a type checker, with no cast at the call site. Retention
horizons accept the duration grammar (`'7d'`), a number of seconds, or a
`datetime.timedelta`.

`query` returns one list (capped, and the right shape for a request/reply). `iter`
**streams** the same window a page at a time, for windows larger than memory: it
flushes once when called, then walks a keyset cursor, releasing the store lock
between pages so recording continues underneath it. Registration emits the
secondary indexes those reads want — one on the temporal axis, one per declared
`index=` dimension.

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

- [docs/architecture.md](docs/architecture.md) — the three layers, ingest/query
  paths, timestamps, concurrency.
- [docs/storage-model.md](docs/storage-model.md) — row shape, type map, dedup,
  TTL, archival roadmap.
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
projections (`latest_key` / `store.latest`), TTL pruning, the Zenoh chronicler
(record + serve history), and a runnable daemon. Deferred: the Postgres backend, a
generic `HistoryQuery` contract, complex-field column promotion, and the
cold-archival tiers.

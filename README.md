# stored

`stored` is a small, database-wrapping **persistence layer for `seared`
objects**, with a first-class (but optional) **Zenoh chronicler** built on
[`zeared`](https://github.com/bwiswell/zeared). It is the durable, time-keyed
history layer beneath [`zeared`](https://github.com/bwiswell/zeared)'s last-value
**retention** — the sister that remembers.

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
- **Bounded by design.** Per-stream **TTL** expiry now; compressed **Parquet
  archival** of cold data on the roadmap.
- **Lean, layered deps.** The core needs only `seared` (persistence rides stdlib
  `sqlite3` — zero extra dependency). DuckDB is an optional backend
  (`stored[duckdb]`) for analytics; the Zenoh integration is an optional extra
  (`stored[zenoh]`) — the core never imports `zeared`.

## Install

```sh
# core only (persist/query seared objects on stdlib sqlite3, no mesh)
uv add git+https://github.com/bwiswell/stored

# with the Zenoh chronicler
uv add "stored[zenoh] @ git+https://github.com/bwiswell/stored"

# with the DuckDB backend (analytics upgrade)
uv add "stored[duckdb] @ git+https://github.com/bwiswell/stored"
```

Requires Python ≥ 3.14.

## Core: persist & query seared objects

```python
import stored

store = stored.Store('chronicle.db')
store.register(Telemetry, retention='7d', index=('id',), latest_key=('id',))

store.record(Telemetry, Telemetry(id=7, x=1.5))          # buffered write
history = store.query(Telemetry, id=7, since='-1h', limit=5000)   # → [Telemetry, …]
newest  = store.latest(Telemetry, id=7)                  # → last value for id 7, however old

store.flush(); store.prune(); store.close()
```

`record` buffers through a batched writer; `query` flushes first, so reads
always see prior writes.

## Mesh: the chronicler (needs `stored[zenoh]`)

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
uv sync --extra zenoh --extra duckdb
uv run pytest tests/
```

Tests mirror the source layout. The mesh tests spin up a loopback Zenoh peer and
the DuckDB-backend tests need their extras (both synced above); the SQLite core
tests need no transport and no extra.

## Status

The v1 core is functional: SQLite-backed persistence (stdlib; DuckDB an optional
backend), a batched writer, event-time keying (`time_field`), latest-per-key
projections (`latest_key` / `store.latest`), TTL pruning, the Zenoh chronicler
(record + serve history), and a runnable daemon. Deferred: the Postgres backend, a
generic `HistoryQuery` contract, complex-field column promotion, and the
cold-archival tiers.

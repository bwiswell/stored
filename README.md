# stored

`stored` is a small, database-wrapping **persistence layer for `seared`
objects**, with a first-class (but optional) **Zenoh chronicler** built on
[`zeared`](https://github.com/bwiswell/zeared). It is the durable, time-keyed
history layer beneath `zeared`'s last-value **retention** — the sister that
remembers.

> **Status: pre-alpha (M0 scaffold).** The package imports and the public
> surface is stubbed; storage, ingest, and query are landing milestone by
> milestone (see `project-plans/`).

## Why `stored`

- **Chronicle a mesh.** Point a `stored` daemon at one or more `zeared` topics;
  it records every sample to a local database, keyed by the Zenoh HLC
  timestamp.
- **Query history transparently.** Mesh consumers use the *existing*
  `Cls.query(params={'from': …, 'to': …})` getter — `stored` answers with
  historical, properly-typed instances over a queryable.
- **Bounded by design.** Per-stream **TTL** expiry now; compressed **Parquet
  archival** of cold data on the roadmap.
- **Lean, layered deps.** The core needs only `seared` + `duckdb`. The Zenoh
  integration is an optional extra (`stored[zenoh]`) — the core never imports
  `zeared`.

## Install

```sh
# core only (persist/query seared objects, no mesh)
uv add git+https://github.com/bwiswell/stored

# with the Zenoh chronicler
uv add "stored[zenoh] @ git+https://github.com/bwiswell/stored"
```

Requires Python ≥ 3.14.

## Quick start (target API)

```python
import stored

store = stored.Store(path='chronicle.duckdb')
store.register(Telemetry, retention='7d', index=('id',))

# core persistence
store.record(Telemetry, msg, meta=meta)
rows = store.query(Telemetry, id='42', since='-1h', limit=5000)

# mesh chronicler (needs stored[zenoh])
import stored.zenoh as sz
chronicler = sz.Chronicler(store, session=sess)
chronicler.add(Telemetry)
chronicler.run()
```

## Architecture

Three concentric layers — `seared`-only core, optional `zeared` chronicler,
pluggable storage backend (DuckDB first). See
[`project-plans/01-architecture-and-scaffold.md`](project-plans/01-architecture-and-scaffold.md)
for the full design.

## Development

```sh
uv sync
uv run pytest tests/
```

Tests mirror the source layout, one `test_*.py` per source area.

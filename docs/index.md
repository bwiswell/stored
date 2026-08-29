# stored — docs

`stored` is a database-wrapping **persistence layer for `seared` objects**, with
a first-class **Zenoh chronicler** built on `zeared` (a core dependency). It is
the durable, time-keyed history layer beneath `zeared`'s last-value retention.

- **[architecture.md](architecture.md)** — the three layers, the ingest and
  query paths, timestamps, and concurrency.
- **[storage-model.md](storage-model.md)** — how a message becomes a row: the
  type map, meta columns, the payload, dedup, TTL, and the archival roadmap.
- **[chronicler.md](chronicler.md)** — the `stored.zenoh` layer: recording a
  mesh, serving history transparently, the daemon, CLI, and systemd unit.
- **[configuration.md](configuration.md)** — `StoredConfig` / `StreamSpec`, from
  the environment or TOML.

The authoritative design rationale lives in the workspace plan doc,
`~/stored/project-plans/01-architecture-and-scaffold.md`.

## Status

The v1 core is functional: persist and query `seared` objects (SQLite backend by
default; DuckDB optional),
a batched writer, TTL pruning, the Zenoh chronicler (record + serve history),
and a runnable daemon. Deferred: the Postgres backend, a generic `HistoryQuery`
contract, complex-field column promotion, and the cold-archival tiers (see the
plan doc).

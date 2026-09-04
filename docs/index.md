# stored — docs

`stored` is a database-wrapping **persistence layer for `seared` objects**, with
a first-class **Zenoh chronicler** built on `zeared` (a core dependency). It is
the durable, time-keyed history layer beneath `zeared`'s last-value retention.

- **[architecture.md](architecture.md)** — the four layers, the ingest, query and
  streaming read paths, timestamps, and concurrency.
- **[storage-model.md](storage-model.md)** — how a message becomes a row: the
  type map, meta columns, the payload, dedup, indexes, TTL, and the archival
  roadmap.
- **[mesh.md](mesh.md)** — the `stored.mesh` layer: `AsyncStore` for services on
  an event loop, and `Binding` for declaring subscribers and typed queryables.
- **[chronicler.md](chronicler.md)** — the `stored.zenoh` layer: recording a
  mesh, serving history transparently, the daemon, CLI, and systemd unit.
- **[configuration.md](configuration.md)** — `StoredConfig` / `StreamSpec`, from
  the environment or TOML.

## Status

The core is functional: persist and query `seared` objects (SQLite by default; DuckDB
optional), a batched writer, event-time keying, latest-per-key projections and
population reads, secondary and JSON-path indexes, bounded-memory streaming, TTL
pruning, the service layer (`stored.mesh`), the Zenoh chronicler, and a runnable daemon.
Deferred: the Postgres backend, complex-field column promotion, and the cold-archival
tiers.

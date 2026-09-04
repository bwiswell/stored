# stored — docs

`stored` is a database-wrapping **persistence layer for `seared` objects**, with
a first-class **Zenoh chronicler** built on `zeared` (a core dependency). It is
the durable, time-keyed history layer beneath `zeared`'s last-value retention.

- **[architecture.md](architecture.md)** — the four layers, the ingest, query and
  streaming read paths, timestamps, and concurrency.
- **[storage-model.md](storage-model.md)** — how a message becomes a row: the
  type map, meta columns, the payload, dedup, indexes, TTL, and the archival
  roadmap.
- **[chronicler.md](chronicler.md)** — the `stored.zenoh` layer: recording a
  mesh, serving history transparently, the daemon, CLI, and systemd unit.
- **[configuration.md](configuration.md)** — `StoredConfig` / `StreamSpec`, from
  the environment or TOML.

## Status

The v1 core is functional: persist and query `seared` objects (SQLite backend by
default; DuckDB optional), a batched writer, event-time keying, latest-per-key
projections, secondary indexes, bounded-memory streaming (`Store.iter`), TTL
pruning, an async facade for services (`stored.mesh.AsyncStore`), the Zenoh
chronicler (record + serve history), and a runnable daemon. Deferred: the
Postgres backend, complex-field column promotion, and the cold-archival tiers.

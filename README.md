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

Each release is tagged `v<version>` on GitHub (the first is `v0.3.5`; the latest is
`v0.4.3`). Install one by its tag:

```sh
# persist/query seared objects + the Zenoh chronicler (stdlib sqlite3 backend)
uv add git+https://github.com/bwiswell/stored --tag v0.4.3

# with the DuckDB backend (analytics upgrade)
uv add "stored[duckdb] @ git+https://github.com/bwiswell/stored@v0.4.3"
```

`uv add` records the tag as the dependency's source:

```toml
[tool.uv.sources]
stored = { git = "https://github.com/bwiswell/stored", tag = "v0.4.3" }
```

**The tag is the pin.** uv doesn't check a version specifier such as
`stored>=0.4,<0.5` against a git source: it takes whatever the source points at,
and a source without a tag points at the tip of `master`. Moving to a new release
means moving the tag.

The Zenoh chronicler ships with the base install (`zeared` is a core dependency).
The `stored[zenoh]` extra was retired in 0.3.0 — it had been an empty alias since the
chronicler moved into the base install.

Requires Python ≥ 3.14. What changed in each release, and what may break on the way,
is in [`CHANGELOG.md`](CHANGELOG.md).

## Upgrading to 0.4.0

0.4.0 moves `stored` to seared 0.4.0 and zeared 0.4.1, both breaking releases, and
fixes how rows with no event time are kept. What changes for code and deployments
written against 0.3.x, and what to do:

**1. Pins.** Move the three together: `stored` to a 0.4 tag (`v0.4.3` is the
latest), and `seared` and `zeared` to the tags it pins, `v0.4.3` each. (stored
0.4.0 and 0.4.1 pinned seared `v0.4.0` and zeared `v0.4.1`, and 0.4.2 pinned both
at `v0.4.2`.) `stored` pins its siblings by tag, and uv refuses two different URLs
for one package, so a project that names either needs the same tag.

**2. Decoding is seared 0.4.0's.** A sample seared now refuses — a `null` in a
required field, a fractional number into an `Int`, a `2` into a `Bool` — goes to the
stream's `on_error` rather than being recorded. A `null` on a field with a default
records the default, so declare a `time_field` that may be `null` as `X | None` with
`default=None` (or `nullable=True`): with `Float(default=0.0)` it reads as 1970 and
expires at the next sweep. A binding's requests decode the same way; see
*The sentinel policy* in [docs/mesh.md](docs/mesh.md).

**3. History reads are strict.** A row the stream's current class can't hold exactly,
written before a lossy retype (`3.7` under an `Int`), raises `seared.ValidationError`
from a read, and a history query answers with an error reply. Retype only to a type
that holds every stored value.

**4. Liveliness departures aren't recorded.** zeared 0.4.0 delivers them to
`on_offline`, as an identity with no message, and the chronicler subscribes
`on_message` only.

**5. Slots zeared refuses.** A class with a `Path` slot, or a `Bytes` slot in its
default base64 form, in a single key segment raises `zeared.TopicError` when the
chronicler or a binding first subscribes it, so a daemon configured with one fails at
start. Use `Bytes(encoding='hex')` or a `Str` slot, or a trailing `{name**}` slot for a
path. A stored row whose slot value holds a `/` is skipped when history is served or
replayed.

**6. The daemon's identity and presence.** `stored run` holds a liveliness token at
`stored/presence/{identity}`, and refuses an `identity` that isn't one key segment
(empty, or holding `/`, `*`, `$`, `?` or `#`) before it opens anything. With access
rules, grant it `liveliness_token` on `stored/presence/**`.

**7. Rows with no event time.** On a stream with a `time_field`, a row whose field
was unset now expires on `_issued_at`, and stays out of the latest projection, so an
entity with only such rows has no `latest()`. The first sweep after upgrading removes
the ones already past retention.

**8. Smaller breaks.** `replay()` raises `ConfigError` for a `topic=` the class can't
publish on, before reading anything. `stored.WriterError` is gone: nothing raised it.
The systemd unit template no longer sets `STORED_LOG_LEVEL`, which `-c` never read;
set `log_level` in the TOML.

**9. New, not breaking.** The `[zenoh]` table takes zeared 0.4's `auto_reconnect`,
`probe_interval`, `resync_jitter` and `resync_concurrency`
([docs/configuration.md](docs/configuration.md)). A retained value replayed to a
chronicler as it starts is recorded once when its publisher runs zeared 0.4.0 or
later.

## Core: persist & query seared objects

```python
import stored

store = stored.Store('chronicle.db')
store.register(Telemetry, retention='7d', index=('id',), latest_key=('id',),
               latest_run_gap='7d')  # optional: keep when each id's current run began
# retention also takes seconds or a timedelta: 604800, timedelta(days=7)

store.record(Telemetry, Telemetry(id=7, x=1.5))          # buffered write
history = store.query(Telemetry, id=7, since='-1h', limit=5000)   # → list[Telemetry]
newest  = store.latest(Telemetry, id=7)                  # → Telemetry | None, however old
either  = store.latest(Telemetry, id=(7, 8))             # the newer of two keys for one thing
row, meta = store.latest_with_meta(Telemetry, id=7)      # + meta.run_start, with latest_run_gap=…

for row in store.iter(Telemetry, since='-30d'):          # streamed, uncapped
    ...                                                  # a page in memory at a time

current = store.query_latest(Telemetry, since='-1h')     # every entity's newest, filtered
page, anchor = store.query_page(Telemetry, since='-1d', limit=500)          # one page…
more, anchor = store.query_page(Telemetry, since='-1d', limit=500, after=anchor)  # …the next
history_rows, latest_rows = store.counts(Telemetry)      # row counts, for status

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
between pages so recording continues underneath it. `query_page` /
`query_latest_page` cut the same walk into request-sized pages for a caller on the far
side of a query: each returns its rows and an anchor to pass back as `after=` (`None`
once a short page ends the walk), which `stored.query.encode_anchor` /
`decode_anchor` turn into an opaque cursor string. Registration emits the secondary
indexes those reads want — one on the temporal axis, one per declared `index=`
dimension.

## Async services: `stored.mesh.AsyncStore`

A service on an event loop must not call a blocking store inline. `AsyncStore`
wraps one so it doesn't have to hand-roll the thread hop:

```python
from stored.mesh import AsyncStore

store = AsyncStore(stored.Store('history.db', flush_rows=5000))
store.register(Location, retention=timedelta(days=3), latest_key=('source', 'item'))

store.record(Location, location)                  # sync — a buffered enqueue
newest = await store.latest(Location, source='site-a', item=item)
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
binding.serve_latest(LastPosition, of=Position, key=('source', 'item'),
                     project=to_last_position, missing=no_position)

binding.serve_range(Event, filters=('source',), stream=True)   # reply row by row
binding.serve_range(Event, filters=('source',), limit='limit', cursor='cursor')  # paged: the last reply of a full page carries the next cursor

binding.serve_snapshot(Placed, filters={'zone': 'zones.department'})  # current state, by zone
binding.serve_latest(LastPosition, of=Position, key=('source', 'item'),  # one thing under several keys:
                     project=to_last_position, expand={'item': spellings})  # the newest of them
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

The session's connection is the config's `[zenoh]` table, or, when it has none,
zeared's `ZEARED_SESSION_*` variables, so the daemon can run from the environment
alone (see `docs/configuration.md`).

See `systemd/stored.service` for the unit template. Health is systemd supervision
plus a Zenoh liveliness token the daemon holds at `stored/presence/{identity}`
(`stored.zenoh.ChroniclerPresence`), so a watcher sees it come and go; with access
rules, grant it `liveliness_token` there. See `docs/chronicler.md`.

## Documentation

- [docs/architecture.md](docs/architecture.md) — the four layers, ingest/query/
  streaming paths, timestamps, concurrency.
- [docs/storage-model.md](docs/storage-model.md) — row shape, type map, dedup,
  indexes, TTL, archival roadmap.
- [docs/mesh.md](docs/mesh.md) — the `stored.mesh` layer: `AsyncStore`, `Binding`
  and `Replayer`.
- [docs/chronicler.md](docs/chronicler.md) — the `stored.zenoh` layer, the daemon and
  its presence.
- [docs/configuration.md](docs/configuration.md) — `StoredConfig` / `StreamSpec`.

## Development

```sh
uv sync --extra duckdb
uv run prek install        # once per clone: the commit hooks
uv run pytest tests/
```

The hooks are the house set (`.pre-commit-config.yaml`): `ruff check`, `ruff format`,
`deptry`, `ty check` — the same gate `seared` and `zeared` run, so a commit that passes
here passes there. A fifth refuses any commit on `master`.

**Branches and releases.**
- **`master` holds released commits only,** and moves only by merge.
- **A release is a `version/x.y.z` branch** off `master`.
- **Work on it happens in `feat/…` and `fix/…` branches,** each a pull request
  squash-merged into the version branch. Each adds its own lines to the `unreleased`
  entry at the top of `CHANGELOG.md`.
- **A minor release then takes a `chore/code-hygiene-and-docs` branch:** docs
  checked against the code, pins and deferred lint swept.
- **Once those are all in, one `chore: version bump` commit** on the version branch
  bumps the version in `pyproject.toml` and `stored/__init__.py` together (a test
  checks that they agree), refreshes the lockfile, and dates the changelog entry.
- **The version branch then merges into `master`.** There, the `Release Tag` workflow
  (`.github/workflows/release-tag.yml`) tags it `v<version>` and publishes a GitHub
  release.

Tests mirror the source layout. The mesh tests spin up a loopback Zenoh peer
(`zeared` is a core dependency, always present); the DuckDB-backend tests need the
`duckdb` extra (synced above); the SQLite core tests need no transport.

## Status

The core is functional: SQLite-backed persistence (stdlib; DuckDB an optional backend),
a batched writer, event-time keying (`time_field`), latest-per-key projections
(`latest_key` / `store.latest`), population reads (`query_latest` / `iter_latest`),
bounded-memory streaming (`store.iter`), path filters into `dict` fields
(`json_index` / `where=`) with secondary and expression indexes, TTL pruning, the
service layer (`stored.mesh`: an async facade, declarative record / range / last-known /
snapshot bindings with optionally streamed replies, and replay-as-publication), the
Zenoh chronicler, and a runnable daemon.

Deferred: the Postgres backend, complex-field column promotion, and the cold-archival
tiers.

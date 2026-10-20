# Changelog

Each version of stored, newest first.

A **Breaking** line means code written for the version before may fail, or behave differently. A **Storage change**
line means an existing database's tables, columns or indexes change, or need a migration or a rebuild.

The top entry, while it reads *unreleased*, collects what has merged for the next version; each change adds its own
lines as it lands, and the entry is dated when that version is released.

## 0.4.4 — unreleased

**On seared and zeared 0.4.4.**

### Changed

- **Dependencies:** seared `>=0.4.4,<0.5` and zeared `>=0.4.4,<0.5`, pinned to `tag = "v0.4.4"` each. seared 0.4.4
  adds field constraints (bounds on `Int` and `Float`, lengths and a portable pattern on `Str`), which a recorded
  class's fields may carry: a sample that fails one fails to decode, so it reaches `on_error` and isn't recorded, as
  no undecodable sample is. A `Union` tag that isn't a string now fails to decode, fallback or not. Unconstrained
  `Int`, `Float` and `Str` decode faster on the pure-Python path. zeared 0.4.4's `fetch_retained` changes don't
  reach stored, which doesn't call it. The pins change nothing stored writes. See their changelogs.

## 0.4.3 — 2026-10-17

**On seared and zeared 0.4.3, where type hints resolve again.**

### Changed

- **Dependencies:** seared `>=0.4.3,<0.5` and zeared `>=0.4.3,<0.5`, pinned to `tag = "v0.4.3"` each. seared 0.4.3
  imports at runtime the types its base's and its fields' annotations name, so `typing.get_type_hints()` works on
  seared classes again, stored's config classes included; zeared 0.4.3 takes it. Nothing that loads, dumps or
  records changes. See their changelogs.

### Fixed

- **`typing.get_type_hints()` works on `LatestMeta`.** Its `run_start` annotation names the `datetime` module,
  imported only for type checking, so `get_type_hints` raised `NameError` on it. Every class stored exports is now
  checked. Since 0.3.3.

## 0.4.2 — 2026-10-17

**Config that refuses its typos, and streams checked before `run`.** On seared and zeared 0.4.2, a key the config
doesn't declare is refused anywhere in the file, `validate-config` registers every stream as `run` would, and a
subclass of a field is stored as its base is.

### Added

- **`stored.streams.resolve_message_class(spec)`**, the chronicler's resolver: `resolve_stream_class`, then the
  check that the class is a zeared Message, with zeared imported only when it runs. `run` and `validate-config`
  share it, so they refuse a stream in the same words; the daemon's own copy is gone.

### Changed

- **Breaking:** a key the config doesn't declare is refused, anywhere in the file. `StoredConfig`, `StreamSpec` and
  `LatestSpec` are declared `unknown='raise'`, so `load`, `loads` and the `from_*` readers raise one
  `ValidationError` naming every unknown key, `[[streams]]` and their `latest` tables included, where a misspelt key
  was dropped and its setting kept its default. `validate-config` and `run` both refuse such a file. A `[zenoh]`
  typo is refused by zeared 0.4.2's `SessionConfig`, where the table is loaded. `from_env` reads only the variables
  it knows.
- **Breaking:** `validate-config` registers every stream as `run` would, into an in-memory store of the configured
  backend. A stream class that won't import or isn't a zeared Message, a duration that won't parse, a `time_field`,
  `latest` key or index naming no field, a duplicate stream, or an unknown backend now fails it, where each passed
  and then failed `run`. Nothing is written to disk. A config of plain seared classes, which `migrate` and `prune`
  still accept, fails it too.
- **Breaking:** `schema.SCALAR_TYPES`, `COMPLEX_FIELDS` and `TIME_FIELD_KINDS` are keyed on `seared.FieldKind`
  (`FieldKind.INT`, whose value is `'int'`), where they were keyed on field class names (`'Int'`).
- **Dependencies:** seared `>=0.4.2,<0.5` and zeared `>=0.4.2,<0.5`, pinned to `tag = "v0.4.2"` each, and the
  lockfile moves from seared 0.4.0 and zeared 0.4.1. seared brings `unknown=` and `field_kind`; zeared a
  `SessionConfig` that refuses unknown keys, `zeared.keyexpr`, and retention that answers `$*` queries. On these
  releases `typing.get_type_hints()` fails on any seared class (a seared issue); stored never calls it. See their
  changelogs.

### Fixed

- **A subclass of a field is stored as its base is.** Columns, the time axis and JSON indexes go by
  `seared.field_kind` rather than the field class's name: a subclass of `Str` gets its `VARCHAR` column, where it
  lived only in `_payload` and couldn't be indexed or queried on; a subclass of `Int`, `Float`, `DateTime` or `Date`
  can be a `time_field`; and a subclass of `Dict` takes a `json_index`. Since 0.1.0.

## 0.4.1 — 2026-10-17

**The daemon's health, as systemd sees it, and config it can check.** The daemon pings a systemd watchdog while its
store can still do its work, and its status shows whether its mesh link is up. A config with no `[zenoh]` table
takes its session from zeared's environment variables, so it can `run` from the environment alone, and
`validate-config` refuses what `run` would refuse at start. The pins are unchanged: seared 0.4.0 and zeared 0.4.1.

### Added

- **A systemd watchdog.** The unit template sets `WatchdogSec=30`, and the daemon pings `WATCHDOG=1` every half
  period while the store can still do its work: its lock comes free within half a tick and its writer is flushing. A
  wedged backend or a stopped writer stops the pings, so systemd restarts the daemon, and each missed ping logs why.
  The daemon pings only when systemd sets `WATCHDOG_USEC` for it.
- **`Store.health(timeout)`**, that check on its own: `None` when the store's lock comes free within `timeout`
  seconds and its writer is flushing, else a short reason. `Writer.alive` says whether the periodic flush is
  running.
- **The mesh link's state in `systemctl status`.** Once ready, the daemon's status reads
  `recording N stream(s) as stored/presence/{identity}`. On a `ManagedSession` (`auto_reconnect` in `[zenoh]`) a
  lost link turns it to `mesh link lost; reconnecting`, with a warning in the log, until the link returns. The
  watchdog keeps pinging meanwhile: the session recovers by itself, and a restart would undo that.
- **The session from the environment.** With no `[zenoh]` table, the daemon's session comes from zeared's
  `ZEARED_SESSION_*` variables (`SessionConfig.from_env`), so an environment-only config can `run`, where it had no
  `mode`. A table, when there is one, is still used whole. With neither, `run` raises a `ConfigError` saying where
  to set a `mode`, as it does for a table without one. `stored.zenoh.session.session_config` makes the choice.
- **`stored.config.check_identity(identity)`**, the daemon's identity rule as a function of the string alone: a
  `ConfigError` for an identity that can't be one key segment.

### Changed

- **Breaking:** `validate-config` refuses what `run` would refuse at start, so a config that passed it may not. An
  `identity` that can't be one key segment fails it, and so does a `[zenoh]` table with no `mode` or a wrong value
  (`mode = "bogus"`), loaded with zeared as `run` loads it. With no table, zeared isn't imported, and the
  `ZEARED_SESSION_*` variables `run` would read go unchecked.
- **`validate-config`'s summary ends `zenoh=table` or `zenoh=env`,** saying where `run` takes its session from.

## 0.4.0 — 2026-10-17

**On seared 0.4.0 and zeared 0.4.1, with a presence token for the daemon.** stored adopts both siblings' breaking
releases, which change what it records and serves in the ways marked below. A chronicler records each retained value
once, history replies and replays skip a row with no key, and a row with no event time expires and stays out of the
projection. The README's *Upgrading to 0.4.0* lists every break with what to do.

### Added

- **`ReplayHandle.skipped`,** the rows a replay skipped because they had no key to publish on, beside `sent`. A
  `progress` dict passed to `replay()` gains a `'skipped'` count too.
- **The daemon holds a presence token.** `stored run` declares a Zenoh liveliness token at
  `stored/presence/{identity}` once every stream is subscribed, just before `READY=1`, and withdraws it first as it
  shuts down; a crash withdraws it too. So health is systemd supervision plus Zenoh liveliness, as the docs and the
  systemd unit say again; 0.3.5 had taken the claim out, since no token was declared then. A watcher subscribes
  `stored.zenoh.ChroniclerPresence` with `on_online` / `on_offline`. With access rules, grant the daemon
  `liveliness_token` on `stored/presence/**`.

### Changed

- **Breaking:** a liveliness class's departures are no longer recorded. zeared 0.4.0 delivers them to `on_offline`,
  as a key's identity with no message, where it delivered them to `on_message` as a typed will, and the chronicler
  subscribes `on_message` only. stored records the messages published on the mesh, and nothing else.
- **Breaking:** samples are decoded by seared 0.4.0's stricter rules before stored sees them, so one that 0.3.x
  recorded can go to the stream's `on_error` instead: a `null` in a required field, a fractional number into an
  `Int`, a `2` into a `Bool`. A `null` on a field with a default records the default, where it recorded `None`, so a
  `time_field` declared `Float(default=0.0)` reads 1970 rather than `NULL`.
- **Breaking:** a binding's request is decoded the same way. A `null` on a request field with a default reads as
  that default, so a sentinel policy that lists `None` counts the field as provided; a `null` in a required request
  field leaves the request undecoded, and the binding logs that the query carried no payload and replies with
  nothing.
- **Breaking:** history is read back strictly. A row the stream's current class can't hold exactly, written before a
  field was retyped (`3.7` under an `Int`, `2` under a `Bool`), raises `seared.ValidationError` from a query, where
  0.3.x coerced it, and a history queryable replies with that error in seared 0.4.0's `Cls.path: reason` form.
- **Breaking:** a message class with a `Path` slot, or a `Bytes` slot in its default base64 form, in a single key
  segment raises zeared's `TopicError` when the chronicler or a binding first subscribes it, so a daemon configured
  with one fails at start. Either can render a `/` and split the key. Use `Bytes(encoding='hex')` or a `Str` slot,
  or a trailing `{name**}` slot for a path.
- **Breaking:** `stored run` refuses an `identity` that can't be one key segment (empty, or holding `/`, `*`, `$`,
  `?` or `#`) with a `ConfigError`, before it opens anything. It names the presence key, where it had been unused; a
  `*` would have declared a token on every identity at once.
- **Breaking:** `replay()` checks its destination before reading anything, and a `topic=` the class can't publish on
  (undeclared, subscribe-only, or with a slot the class doesn't declare) raises `ConfigError`, as its docstring
  said. It raised zeared's `TopicError` at the first row, or returned 0 for an empty window.
- **The README gains *Upgrading to 0.4.0*,** which lists each break with what to do.
- **The docs say what the code does.** Among the corrections:
  - the `[zenoh]` table's keys are listed, zeared 0.4's four included, with what the docs didn't say: an unknown key
    is dropped without a word, `validate-config` doesn't check the table, and neither it nor `streams` is read from
    the environment, so `run` needs the TOML;
  - a stored row stops loading after a lossy retype or a new required field, where the docs called retyped fields
    readable;
  - a `time_field` that may arrive as `null` should be `X | None` with `default=None`, since a real default records
    1970;
  - a `RETAINED` class's history is served through a `Binding` with a reply class of its own, where the docs had a
    contract for it on the roadmap, and serving the read primitives over the mesh is the `Binding`'s job, not the
    consumer's;
  - SQLite is the default backend in the docstrings and the archival plan, which had called DuckDB the default;
  - the request sentinel policy under seared's null rule, and the APIs the docs left out: `cursor=`, `max_limit=`,
    `query_page`, `counts` and the `ReplayHandle`.
- **The systemd unit template no longer sets `STORED_LOG_LEVEL`,** which `stored -c` never read: under `-c` the
  config comes from the TOML alone, so the level is its `log_level`.
- **The examples and docstrings use no particular fleet's vocabulary,** and point at nothing outside the repository.
- **The tests mirror every module,** and the ones meant for both engines run on DuckDB too, where they had run
  SQLite twice.
- **seared and zeared are pinned to release tags,** seared `v0.4.0` and zeared `v0.4.1`, and the declared ranges
  rise to `seared>=0.4.0,<0.5` and `zeared>=0.4.1,<0.5`. zeared 0.4.1 is the floor because a query whose reply fails
  ends at once there; on 0.4.0 it stayed open until the serving process next collected garbage.
- **Dependencies:** the lockfile resolves seared 0.4.0 (was 0.3.4) and zeared 0.4.1 (was 0.3.13). The dev group's
  `pytest` range lifts to `>=8.0,<10.0`, and the refreshed lockfile resolves pytest 9.1.1 and ruff 0.17.1, whose
  import convention the code now follows (`import datetime as dt`). Nothing else moves.

### Fixed

- **A restarted chronicler records each retained value once.** A chronicler fetches the retained values already on
  the mesh when it starts, and a managed session fetches them again when its link comes back. The replies carried no
  timestamp, so each was given a fresh `_ts_hlc`, the primary key never matched, and every start or reconnect added
  a duplicate row. zeared 0.4.0 replies with the value's original timestamp, so a replay is the row already stored
  and its insert does nothing. The publisher's zeared decides it: replies from a session still on 0.3.x carry no
  timestamp, and their duplicates continue until it upgrades.
- **A row with no key no longer ends a history reply or a replay.** A row whose single-segment slot holds a `/`,
  written through `Store.record` or a `via=` mapper, has no key to go out on: zeared 0.4.0 refuses it, where 0.3.x
  published it on a key nothing matched. The chronicler's history queryable stopped at such a row with an error
  reply, and a replay ended there, a paged replay losing the rest of the page too. Each now skips the row and
  carries on: the history queryable and a binding's queryables pass it to their `on_error`, or log it, and a replay
  logs the first, counts them all on `skipped`, and logs the total. A row missing a slot value is skipped the same
  way.
- **A row with no event time expires.** On a stream with a `time_field`, retention compares each row's event time
  with the cutoff, and a row whose optional `time_field` was unset has none. `NULL` is older than no cutoff, so the
  row was never pruned, from the history or the projection. It now expires on `_issued_at`, as on a stream without a
  `time_field`; so do the rows written before a stream took its `time_field`, which 0.2.0 noted would never expire.
  Since 0.2.0.
- **The latest projection keeps out rows with no event time.** Such a row went in, and since `NULL` compares as
  neither older nor newer, no later record replaced it: `latest()` went on answering with it, and it never expired.
  It now stays out, as `iter` and paging already skip it, so an entity with only such rows has no `latest()`. One a
  projection already holds yields to the entity's first timed record, which also starts its run. Since 0.2.0.
- **`archive` reaches the registry.** `run`, `migrate` and `prune` registered a stream without the `archive` horizon
  its config gave, though the docs said they register every field. Archival is still to come, so only the registry
  records it.

### Removed

- **Breaking:** `stored.WriterError`. Nothing raised it: a batch the writer fails to flush is logged and dropped, so
  the writer thread outlives it.

## 0.3.5 — 2026-10-16

**Tagged releases, an in-repo changelog, sibling tag pins, and a daemon that starts and honours its config.**

### Added

- **`CHANGELOG.md`**, this file: every release from 0.1.0 on, newest first. It is kept in the repository and grows
  with each change as it merges. The entries before 0.3.5 were written afterwards, from each release's changes.
- **Releases are tagged.** A `Release Tag` workflow (`.github/workflows/release-tag.yml`) runs when a version merges
  to `master`. It checks that `pyproject.toml` and `stored/__init__.py` stamp the same version, then tags the merge
  `v<version>` and publishes a GitHub release; a version already released is skipped. 0.3.5 is the first tagged
  release, and earlier ones stay untagged.
- **A test that the two version stamps agree,** read from `pyproject.toml` rather than spelled out, so a mismatched
  bump fails before it reaches `master`. It replaces a test that held the version as a literal.
- **A `no-commit-to-branch` hook** refuses a commit on `master`, which now moves only by merge. It is prek's
  built-in copy, so it fetches nothing.

### Changed

- **The README installs a release by its tag,** `uv add git+https://github.com/bwiswell/stored --tag v0.3.5`, and
  shows the `[tool.uv.sources]` line that records. It says why: uv doesn't check a version specifier against a git
  source, so a source without a tag follows the tip of `master`.
- **README Development describes the release process:** `feat/` and `fix/` branches merge into a `version/` branch,
  a `chore: version bump` commit closes it, and the version branch merges into `master`, where it is tagged.
- **seared and zeared are pinned to release tags,** `v0.3.4` and `v0.3.13`, and the declared ranges rise to the
  versions tested against: `seared>=0.3.4,<0.4` and `zeared>=0.3.13,<0.4`. uv checks no range against a git source,
  so the tags are what pin them. Relocking works again: stored's seared source carried no tag, zeared's pins seared
  by one, and uv refuses two different URLs for one package.
- **Dependencies:** the lockfile resolves seared 0.3.4 (was 0.3.2) and zeared 0.3.13 (was 0.3.7). It is the suite's
  first run against zeared 0.3.8 to 0.3.13, and nothing stored relies on changed. The dev tools refresh too (`ty`
  0.0.89, `prek` 0.5.5); `ruff` stays at 0.16.10, since 0.17 adds an import convention not yet adopted here.

### Fixed

- **`stored run` applies a stream's whole spec.** The daemon registered each configured stream with its retention
  and indexes only, so a `time_field` and a `[streams.latest]` projection did nothing under `run`: the reaper
  pruned on delivery time, and no projection was kept. `migrate` and `prune` applied them, so the three disagreed.
  All three now register through one function, `stored.streams.register_stream`. Since 0.2.0.
- `docs/configuration.md`'s stream table gains the `latest` row it had left out.
- **The systemd unit starts the daemon.** Its `ExecStart` passed `--config` after `run`, but `-c` / `--config` is
  the CLI's top-level option, so the parser rejected it and the unit couldn't start. It now runs
  `stored -c /etc/stored/stored.toml run`, and a test parses the unit's command with the CLI's own parser, so the
  two can't drift apart again. Since 0.1.0.
- **The `stored` command exists.** The README and docs ran `stored -c … <verb>`, but no console script was declared,
  so only `python -m stored` worked. `pyproject.toml` now declares `stored = "stored.cli:main"`.
- **The unit's comments and `docs/chronicler.md` describe the daemon as it is:** it sends `READY=1` and `STOPPING=1`
  already, and declares no Zenoh liveliness token, so health is systemd supervision alone.
- **A stream that keeps no runs reports no run start.** Re-registering a stream without `latest_run_gap` stopped the
  upsert updating `_run_start` but left the column, and the `*_with_meta` reads went on returning its old value,
  where `LatestMeta` promised `None`. They now read the column only when the stream keeps runs, on SQLite and DuckDB
  alike. `stored.latest.meta_of` takes `keeps_runs=`, which defaults to the old behaviour. Since 0.3.3.

## 0.3.4 — 2026-10-03

**Per-field sentinel policies for bindings.** A request whose fields spell "not provided" in different ways can say
so field by field.

### Added

- **`unset=` takes a mapping.** `serve_range`, `serve_snapshot` and `serve_latest` accept `{field: sentinels}` as
  well as one tuple for every field. The mapping names only the fields that differ, and every other field keeps
  `UNSET_FALSY`. So an optional id where `0` is a real value (`{'floor_id': (None,)}`) can sit beside a time bound
  where `0` still means open, and `(None, [])` lets an empty list mean "any", where under `UNSET_FALSY` it is
  present, as a set that matches nothing. The policy applies wherever a binding reads a request field: filters,
  latest keys, time bounds, `limit` and the page cursor. Its type is `stored.mesh.binding.Unset`.
- **A per-field policy that names a field the request doesn't declare is a `ConfigError`** when the binding is
  declared, since a misspelt name would otherwise leave the real field on the default without a word.
- `docs/mesh.md` shows a request with mixed conventions.

### Changed

- **Dependencies:** the lockfile resolves seared 0.3.2 (was 0.3.1) and zeared 0.3.7 (was 0.3.6). The declared
  ranges, `seared>=0.2.1` and `zeared>=0.3.5`, are unchanged.

## 0.3.3 — 2026-10-03

**Run starts in the latest projection, and a `*_with_meta` twin for every latest read.**

### Added

- **`latest_run_gap=` on `Store.register`** (a duration: `'7d'`, seconds or a `timedelta`) makes the latest
  projection keep each entity's **run start**: when its current run of records began, where a silence longer than
  the gap starts a new run. It is one more column in the same upsert, so it costs no extra write.
  - A new entity's run starts at its first record.
  - A newer record keeps the run start, or resets it to its own time when it arrives more than the gap after the
    newest stored record.
  - An older record (redelivered, or from a late batch) can only extend the run backwards, when it lands within the
    gap of the run start. It never resets a run, and newest-wins still holds for the row itself.
  - The gap needs a `latest_key` and must be positive; either mistake is a `RegistrationError`.
- **`stored.LatestMeta`**, a frozen dataclass of what the projection knows about an entity beyond its newest
  record. Its one field, `run_start`, is a naive UTC `datetime`, or `None` when the stream keeps no run or the run
  began before runs were kept.
- **A `*_with_meta` twin for every latest read:** `Store.latest_with_meta`, `query_latest_with_meta`,
  `query_latest_page_with_meta` and `iter_latest_with_meta`, on `AsyncStore` too. Each returns the same rows as
  its twin, every one paired with its `LatestMeta`; `latest_with_meta` returns `(row, meta)` or `None`.
- **`with_meta=True` on `serve_latest` and `serve_snapshot`** calls `project(row, request, meta)`, so a reply can
  carry a fact that is in no recorded message, such as when an entity's current run began. It works for collected,
  paged and streamed replies. The meta comes from the same `SELECT` as the row, so it costs nothing, and a
  two-argument hook never sees it. `with_meta=True` without a `project` is a `ConfigError`: the identity projection
  has nowhere to put it.
- `docs/storage-model.md` describes runs, and `docs/mesh.md` describes `with_meta`.

### Changed

- **Storage change:** a stream registered with `latest_run_gap` keeps a nullable `_run_start` timestamp column in
  its latest table. Registering adds it to an existing table in place, on SQLite and DuckDB alike, and nothing
  needs migrating. Rows already there read `run_start=None` (the run began before anyone was counting) until a
  silence longer than the gap starts a run that is known. A stream registered without a gap is untouched.
- A registration duration that doesn't parse raises a `ConfigError` reading `invalid duration …`, where it read
  `invalid retention …`, since the run gap shares the check.

## 0.3.2 — 2026-10-02

**Set-valued filters and keys, so one thing recorded under several keys answers as one.**

### Added

- **A filter value can be a set.** Any column or path filter on the history and latest reads (`query`, `iter`,
  `query_latest` and the rest) takes a list, tuple, set or frozenset and matches any member, as `IN (…)`. An empty
  collection matches nothing; it is never read as no filter. A string is always one value.
  - A path filter's set takes its comparison from its first member, so it shouldn't mix strings and numbers.
- **`Store.latest` takes a set for a key field**, and then answers the newest row across every entity the key
  matches, for one thing recorded under more than one key (an identifier rewritten partway through its life, say).
  Member order isn't precedence, a member with nothing stored is no obstacle, and an empty set answers `None`.
- **`expand=` on `serve_range`, `serve_snapshot` and `serve_latest`:** `{field: fn(request)}` computes what a
  filter (or key) field matches, per request, instead of reading the field as sent. The function returns one value,
  or a collection to match any of, so `serve_latest` answers the newest across every key it names.
  - An expander runs only when its field is present in the request; an absent field stays unfiltered.
  - An `expand` entry for a field that isn't among the declaration's filters (or key fields) is a `ConfigError` at
    bind time, since it would never run.
  - The page cursor walks the sort order, not the filter, so an expanded range pages like any other.
- The README, `docs/storage-model.md` and `docs/mesh.md` describe both.

### Changed

- **Dependencies:** the lockfile is refreshed: `eclipse-zenoh` 1.10.1 and `msgpack` 1.2.3 (both through zeared),
  `duckdb` 1.5.6 (the `duckdb` extra), and the dev tools. No ranges changed, and seared and zeared stay at 0.3.1 and
  0.3.6.

## 0.3.1 — 2026-09-06

**Resumable pages, for a caller on the far side of a query.**

### Added

- **`Store.query_page` and `Store.query_latest_page`** cut `iter`'s keyset walk into request-sized steps. Each
  takes `query`'s arguments (or `query_latest`'s) plus `after=`, and returns `(rows, anchor)`: the page, and the
  anchor to pass back as `after` for the next one, or `None` when the walk is over.
  - A full page always returns an anchor, so a walk that ends on a full page finishes with one more, empty page. A
    short page ends the walk.
  - Resume with the same `order`. Rows with no event time are left out, as `iter` leaves them out: they have no
    place on the axis the walk resumes along.
  - `query_latest_page` walks current state, one row per entity, by last-seen.
  - Both are on `AsyncStore` too.
- **`stored.query.encode_anchor` and `decode_anchor`** turn an anchor into an opaque, URL-safe cursor string and
  back. A string they didn't produce raises `QueryError`.
- **`cursor=` on `serve_range` and `serve_snapshot`** pages a binding's replies. It names a field that both the
  request and the reply declare. An empty request cursor asks for the first page, and the binding puts the next
  page's cursor on the last reply of a full page, and on no other, since a multi-reply queryable has no envelope
  to carry it.
  - A cursor the store didn't produce makes the query an error, never a silent restart from the first page.
  - It is checked at bind time: a request or reply without the field, or `cursor=` with `stream=True` (a streamed
    reply has no last row to carry it), is a `ConfigError`.
- The README shows a paged `serve_range`.

### Changed

- **Dependencies:** the lockfile resolves seared 0.3.1 (was 0.2.1) and zeared 0.3.6 (was 0.3.5). The declared
  ranges, `seared>=0.2.1` and `zeared>=0.3.5`, are unchanged.

## 0.3.0 — 2026-09-04

**A ceiling on how many rows a binding answers, and the `stored[zenoh]` alias removed.**

### Added

- **`max_limit=` on `serve_range` and `serve_snapshot`:** the service's ceiling on rows per query, applied whatever
  the request asks. `default_limit` only fills in an omitted limit; `max_limit` clamps an explicit one, and a
  `default_limit` above it too. A request that names no limit gets `default_limit` if one is set, and otherwise the
  ceiling itself rather than the planner's default of 1000, which could exceed a lower ceiling. A streamed reply
  with no limit falls back to `max_limit`, or `MAX_LIMIT` (100,000) as before.
  - It is worth setting wherever replies are collected into one frame downstream, so that a request for a hundred
    thousand rows doesn't get them.
  - `serve_latest` takes none: it answers one entity, so there is no row count to inflate.
  - A binding declared without `max_limit` behaves as before.

### Changed

- The README and `docs/index.md` status sections are rewritten for 0.3, and the docs and docstrings no longer
  refer to design documents outside the repository.

### Removed

- **Breaking:** the `stored[zenoh]` extra is gone. It had been an empty alias, kept for one release, since the Zenoh
  chronicler moved into the base install with zeared as a core dependency. A dependency that still names
  `stored[zenoh]` should drop the extra; plain `stored` installs everything it provided.

## 0.2.17 — 2026-09-04

**The house lint and hook set.** Tooling and formatting only: no change to the public API, to behaviour, or to the
storage model.

### Added

- **Commit hooks, run by `prek`** (`.pre-commit-config.yaml`): `ruff check`, `ruff format`, `deptry` and
  `ty check`, the same gate seared and zeared run. They are installed once per clone with `uv run prek install`, and
  the README's *Development* section says so.

### Changed

- **`ruff` checks every rule** (`select = ["ALL"]`), and `pyproject.toml` lists each exception with its reason.
  Among them: naive `datetime` construction (`DTZ001`), since every stored time is naive UTC by design;
  parent-relative imports between the subpackages (`TID252`); `Any` at the backend and transport boundaries
  (`ANN401`); and long keyword-only signatures on the read and register surfaces (`PLR0913`, `PLR0917`), which
  replace the per-function `noqa`s those carried. The `stored.zenoh` modules keep `zeared` as a runtime import
  (`TC002`), because importing that layer is how a consumer opts into the transport.
- **The formatter is on:** single quotes, LF line endings, target `py314`, and Markdown left alone, since the docs
  align field declarations into columns and some `python` blocks show JSON. The codebase is reformatted to it,
  except the column-aligned field blocks of `StoredConfig`, `StreamSpec` and `LatestSpec`, kept behind
  `# fmt: off` because they read as a table of the config contract.
- **Imports used only in annotations** move behind `TYPE_CHECKING`, and each exception builds its message before
  raising. Every error message reads as it did.
- **The sdist leaves out `bench/`.**
- **Dependencies:** `prek` joins the dev dependency group, locked at 0.5.2. Runtime dependencies, and the seared and
  zeared versions the lockfile resolves, are unchanged.

## 0.2.16 — 2026-09-04

**Filter targets the request chooses, and `serve_range` replies in a shape of its own.**

### Added

- **A filter target can be a function of the request.** In `Binding.serve_range` and `Binding.serve_snapshot`,
  `filters={'zone': lambda request: f'zones.{request.layer}'}` picks the column or `json_index` path per query, for
  a dimension the caller names, such as which layer of an open-ended dict to look in. The request field's value is
  the filter value; a field holding an `unset` value skips the filter, and the function isn't called.
  - The resolved target must still be an indexed dimension or a declared `json_index` path, but the check moves to
    query time: an unknown target raises `QueryError` for that request, so the queryable never answers as though
    the filter had been applied.
  - zeared drops error replies from `aquery`'s result, so a getter that passes no `on_error` sees an empty list and
    can't tell "nothing matched" from "not a declared dimension". `docs/mesh.md` says so, and prefers a fixed
    target wherever the dimension is known when the binding is declared, since that fails at startup.
- **`Binding.serve_range` takes `of=` and `project=`**, as `serve_snapshot` and `serve_latest` do. `of` names the
  stored class to read when the reply contract differs from it, and `project(row, request)` builds each reply,
  collected or streamed. An `of` naming another class without a `project` raises `ConfigError` when the binding is
  declared.

## 0.2.15 — 2026-09-04

**Current state over the mesh, and binding filters that reach into a `dict`.**

### Added

- **`Binding.serve_snapshot(cls, …)`** serves current state: one reply per matching entity, read from the latest
  projection with `query_latest`, or with `iter_latest` under `stream=True`. It takes `serve_range`'s request-field
  keywords (`filters`, `since`, `until`, `limit`, `default_limit`, `stream`, `chunk`, `unset`, `on_error`), with
  `since` and `until` narrowing on when an entity was last seen. `of=` and `project=` shape a reply that differs
  from the stored row, as in `serve_latest`. Declaring one raises `ConfigError` if the stored class isn't
  registered, keeps no latest projection or needs a `project` it wasn't given, or if `cls` declares no `REQUEST`.
- **Binding filters resolve against the stream's declaration.** A target of `serve_range` or `serve_snapshot` that
  names a declared `json_index` path filters inside the `Dict` field, through `where=`; one that names an indexed
  dimension filters its column, as before. No new keyword is needed: `filters={'zone': 'zones.department'}`.
- `docs/mesh.md` documents both, and the README shows `serve_snapshot`.

### Changed

- **Breaking:** `serve_range` checks its filter targets when the binding is declared, and a target that is neither
  an indexed dimension nor a declared `json_index` path raises `ConfigError` there. Such a binding used to be
  accepted, and each request that set the field failed with a `QueryError`.

## 0.2.14 — 2026-09-04

**Declared paths are indexed.**

### Added

- **`StorageBackend.ensure_json_index(name, table, path, sort_columns=())`**, the backend method that indexes a
  path inside `_payload`, and `schema.json_index_specs`, which names those indexes.

### Changed

- **Storage change:** on SQLite, `register` creates an expression index for each declared `json_index` path, on the
  history table and on the latest projection if there is one. Each is named `idx_<table>_json_<path>`, with any
  character of the declared path that isn't alphanumeric turned into `_`, and covers
  `json_extract("_payload", '$.<wire path>')` followed by the stream's sort key, so one index serves both the
  equality search and the `ORDER BY`. The expression is rendered by the same dialect call the planner uses, because
  SQLite matches an expression index by its text. Emission is idempotent, so an existing store gains the indexes on
  its next `register`.
- **DuckDB doesn't index declared paths:** its binder refuses `CREATE INDEX` over an expression, so the backend
  logs a warning at registration and the path stays queryable by scanning.
- The README and `docs/storage-model.md` describe both.

## 0.2.13 — 2026-09-04

**Filtering on a key inside a `dict` field.**

### Added

- **`json_index=` on `Store.register`** (and `StreamRegistry.add`) declares dotted paths into `Dict` fields, for
  fields whose keys are open-ended by design and so can never be columns: `json_index=('zones.department',)`. Each
  path is checked at registration: its head must name a `Dict` field of the class, and it must reach inside it
  rather than name the whole dict, or `register` raises `RegistrationError`. The registered `Stream` keeps them in
  `json_paths`, mapping each declared path to its spelling in `_payload`.
- **`where=` on every multi-row read:** `Store.query`, `iter`, `query_latest` and `iter_latest`, and the same four
  on `AsyncStore`. `store.query_latest(Cls, where={'zones.department': 5})` matches by equality inside `_payload`,
  and composes with `**filters`, `key` and the time window. Only declared paths are accepted, as `**filters`
  accepts only indexed dimensions; any other raises `QueryError`. A row whose dict lacks the key doesn't match.
  Path filters are not indexed in this release, so they scan.
- **Paths follow `data_key`.** `_payload` is written with wire names, so a field declared with `data_key='zn'` is
  stored as `{"zn": …}`; registration rewrites the path's head to its wire name, so `zones.department` still
  matches. `schema.wire_path` does the translation.
- **A string compares as a string on DuckDB:** the planner extracts a string value with `json_extract_string`,
  since DuckDB's `json_extract` returns JSON, which refuses to compare against a bound string.
- `plan()` takes the same `where=`, and `docs/storage-model.md` gains a *Reaching into a `dict`* section.

## 0.2.12 — 2026-09-04

**Current state for a whole population.**

### Added

- **`Store.query_latest(cls, …)`** reads the latest projection rather than the history table: the newest row of
  every matching entity, one per entity rather than one per observation. It takes `query`'s keywords (`key`,
  `since`, `until`, `limit`, `order`, `**filters`) and filters, orders and caps the same way, since the projection
  carries the same columns and sort key. There, `since` and `until` mean last seen in that window; leaving them out
  gives a plain snapshot.
- **`Store.iter_latest(cls, …)`**, its streaming sibling, walks the projection a page at a time with `iter`'s
  keyset cursor, `chunk` and uncapped default `limit`, for a population too large to return as one list.
- **`AsyncStore.query_latest` and `AsyncStore.iter_latest`** run them off the event loop, as `query` and `iter` do.
- All four raise `ConfigError` for a class registered without `latest_key`.

### Changed

- **Storage change:** `register` indexes the latest projection as it indexes the history table: a temporal index
  over the stream's sort key (`idx_<table>_time`) and one per declared dimension (`idx_<table>_<dimension>`), named
  after the projection's table. A dimension that leads the projection's key gets none, since the primary key
  already serves it. Emission is idempotent, so an existing store gains them on its next `register` or
  `stored migrate`.
- `schema.index_specs` takes `served_by_pk=`, the columns a table's primary key already serves.
- **`Store.latest` on a class with no projection** raises the same `ConfigError`, now worded
  `latest(Cls) needs a latest projection (register with latest_key=…)`.

## 0.2.11 — 2026-09-04

**A dialect seam for the SQL that isn't portable.** Every query returns what it did before.

### Added

- **`stored.dialect`**, where each backend spells the two parts of a query that differ between engines:
  topic-key matching (`GLOB` for a wildcard, `=` otherwise) and extracting a value from the `_payload` JSON. A
  dialect renders fragments, never whole statements.
  - `Dialect` is the SQLite baseline, and `DEFAULT_DIALECT` an instance of it.
  - `DuckDBDialect` differs only in extracting text with `json_extract_string`. DuckDB's `json_extract` returns
    JSON, and comparing that against a bound string is an error there rather than a quiet mismatch.
  - `quote_path` renders a dotted path as a `'$.…'` literal, embedded rather than bound so that the same
    expression can serve an index's DDL, where a parameter isn't allowed.
- **`StorageBackend.dialect`:** each backend names its dialect, and `Store` hands it to the query planner.
- **`plan()` takes `table=` and `dialect=`.** `table` points the same plan at another table with the history
  table's columns and sort key, such as the latest projection, and defaults to the stream's history table;
  `dialect` defaults to the SQLite baseline.
- `docs/architecture.md` explains what a dialect is for.

## 0.2.10 — 2026-09-04

**Replay: recorded history published back onto the mesh, and recorders that can leave it out.**

### Added

- **`stored.mesh.Replayer`** publishes recorded history back onto the mesh as ordinary traffic, so every subscriber
  of the class sees it through its usual `on_message` path, with no client change: a pipeline stage re-run over
  yesterday, a cache warmed after a restart. `Replayer(store, session=None)` takes a `Store` or an `AsyncStore`.
  `await replayer.replay(cls, ...)` publishes a window and returns the number of rows sent. It takes `since`,
  `until`, `limit` and indexed-field filters, as `Store.iter` does, and:
  - publishes on the class's sole `EXTRA_TOPICS` template by default, its replay scope. `topic=` picks one of
    several, and `topic=cls.TOPIC` republishes onto the live topic. A class with no `EXTRA_TOPICS`, or with several,
    raises `ConfigError` unless `topic=` is given;
  - at `speed=0`, the default, publishes as fast as the mesh accepts, a page per thread hop. Above zero it paces to
    the wall clock off the stream's event time (`1.0` is real time, `10.0` ten times faster), and raises
    `ConfigError` for a stream registered without a `time_field`;
  - holds to `max_rate` rows per second, whatever `speed` says;
  - always publishes with `retain=False`, so replaying a `RETAINED` class can't replace the live retained value with
    a historical one;
  - stops early when its `stop=` event is set, waking from a paced sleep at once, and counts the rows it sends into
    an optional `progress=` dict.
- **`Replayer.start(cls, ...)`** runs a replay as a background task and returns a **`ReplayHandle`**: `sent` and
  `done` report on it, `stop()` ends it early, and `await handle.wait()` returns the number of rows sent.
- **`Binding.record(..., live_only=True)`** records only samples arriving on the class's own `TOPIC`, ignoring its
  other declared templates, so a historian can go on recording live traffic without recording a replay as new
  traffic. The topic is the only way to tell the two apart: `ZenohMeta.origin` comes from the local delivery path
  and never crosses the wire. It compares a sample's key with `TOPIC`'s literal prefix, the text before its first
  `{slot}`, so a replay scope must differ before that point; a `TOPIC` that starts with a slot raises `ConfigError`.
- `docs/mesh.md` covers replay: where it lands, who sees it, pacing and bounds. The README gains a short example.

### Changed

- **`Binding.record` warns at bind time** when the class declares `EXTRA_TOPICS` and `live_only` isn't set. A
  subscription covers every template a class declares, so such a recorder records a replay as new traffic. It still
  records from all of them, as before.
- `Replayer` and `ReplayHandle` resolve lazily from `stored.mesh`, as `Binding` does, so importing `AsyncStore` from
  it still pulls in neither zeared nor Zenoh.

## 0.2.9 — 2026-09-04

**Streamed range replies.** A `serve_range` queryable can reply row by row instead of collecting one list.

### Added

- **`Binding.serve_range(..., stream=True)`** replies with each row as it is read, a page at a time, from an
  `async def` generator handler over `AsyncStore.iter`, so neither the historian nor the caller holds the whole
  result. A getter using zeared's `aquery_iter` sees each reply as it lands; one using `aquery` still collects a
  list. `chunk` sets the rows per page (default 1,000) and is ignored without `stream`. Streaming is opt-in, since
  collecting suits the small bounded reads most queryables serve, and switching is a one-argument change.
  - **A streamed range is capped by default.** Zenoh cancels a query only on the caller's side, so an abandoned
    query leaves its queryable producing rows nobody reads. When neither the request nor `default_limit` sets a
    cap, a streamed range stops at `MAX_LIMIT` (100,000 rows); pass `default_limit` to raise or lower it.
  - **Where the two shapes differ:** with no cap given, a collected range stops at the query default of 1,000 rows
    and a streamed one at 100,000; a request's own `limit` above `MAX_LIMIT` is clamped when collected and applied
    as given when streamed; and a streamed range skips rows with no event time, as `Store.iter` does.
- `docs/mesh.md` gains *Streaming a range*: the cap, and that a getter's `timeout` covers the whole query from the
  moment it is called, so a window measured in days belongs on a replay rather than a query.

### Changed

- **Dependencies:** zeared's floor rises from `>=0.2.4` to `>=0.3.5`, and the lockfile moves from zeared 0.2.7 to
  0.3.5. A streamed range needs zeared's `async def` generator handlers (0.3.2), and the getter it pairs with is
  `aquery_iter` (0.3.4), typed in 0.3.5. seared is unchanged, at `>=0.2.1`.

## 0.2.8 — 2026-09-04

**Declarative bindings: a recording subscriber, and typed range and last-known queryables.**

### Added

- **`stored.mesh.Binding`** declares the subscriber and the two queryables a historian otherwise writes by hand. It
  speaks only zeared's vocabulary (a message class, its `REQUEST` payload, a queryable, a projection), so it serves
  any `@zeared` class. `Binding(store, session=None)` takes an `AsyncStore`, or a `Store`, which it wraps in one.
  Its handlers are `async def` and await the store on a worker thread; declare a binding from inside a running event
  loop. `close()` closes every subscriber and queryable it declared, and is safe to call twice.
- **`Binding.record(cls)`** subscribes `cls` and records every message. With `store_as=` and `via=`, it maps each
  message to a row of another registered class, so several contracts can share one history. A mapped row keeps the
  source message's key expression and HLC timestamp, so a redelivered source message still dedups.
- **`Binding.serve_range(cls, ...)`** serves `cls`'s history as a queryable driven by `cls.REQUEST`. `filters` names
  the request fields that become equality filters on indexed dimensions, or maps them to columns
  (`{field: column}`); `since`, `until` and `limit` name the fields that set the window and the cap, and
  `default_limit` caps a request that sets none. `cls` is both the stored class and the reply.
- **`Binding.serve_latest(cls, ...)`** answers "newest for this entity" from a latest-per-key projection. `cls` is
  the reply contract, `of=` the stored class (`cls` by default), and `key` names the request fields that form the
  entity key. When the two classes differ, `project(row, request)` builds the reply. `missing(request)` answers when
  nothing is stored for the key, or the request leaves part of it unset; without it, nothing is replied.
- **The sentinel policy:** a request field holding one of the `unset` values counts as not provided, so its filter,
  bound or key part is left out, and one request type serves "everything", "one tag" and "one tag in an hour". The
  default, `UNSET_FALSY`, is `('', 0, 0.0, None)`, for wire formats that say "absent" with an empty string or a
  zero; a value equal to one of these counts too, so `False` is unset as well. Pass `unset=(None,)` where `''` and
  `0` are real values.
- **Mistakes fail at bind time** with `ConfigError`, not on the first message: an unregistered class, `store_as`
  without `via`, a class with no `REQUEST`, `serve_latest` on a class registered without `latest_key`, or an `of`
  that differs from `cls` without `project`. A query whose payload isn't the class's `REQUEST` logs a warning and
  gets no reply.
- **`docs/mesh.md`**, on the `stored.mesh` layer: `AsyncStore` and `Binding`, the sentinel policy, and what a
  binding leaves to the service (presence, liveliness, mappers and projections). The README gains a short example.

### Changed

- `Binding` and `UNSET_FALSY` resolve lazily from `stored.mesh`: importing the namespace still pulls in no zeared,
  and accessing `Binding` does.

## 0.2.7 — 2026-09-04

**An async facade for services on an event loop.**

### Added

- **`stored.mesh.AsyncStore`** wraps a constructed `Store`, so a service on an event loop no longer writes its own
  thread hop around each read. `query`, `latest`, `counts`, `prune`, `flush` and `close` are awaited, each on a
  worker thread. `register` and `record` stay synchronous: the first is one-time DDL on the open path, the second a
  buffered enqueue cheaper than the hop would be. The wrapped store is at `.store`, for anything not mirrored.
- **`async for row in AsyncStore.iter(...)`** streams as `Store.iter` does, one thread hop per page of `chunk` rows,
  handing control back to the loop between pages. It flushes when iteration starts rather than when it is called,
  since an async generator can't act before its first `await`. Closing it early with `aclose()` closes the
  underlying walk.
- **`stored.mesh`**, a namespace for the shapes a service binds over the transport. Importing `AsyncStore` from it
  pulls in neither zeared nor Zenoh, and a test holds it to that. The README and the architecture page describe it.

### Changed

- **`Store.iter` is typed as returning a `Generator`**, not an `Iterator`, so calling `close()` on a walk left
  part-way type-checks. It always was one at runtime.
- The docs index's status lists what the core now does: event-time keys, latest-per-key projections, secondary
  indexes, `Store.iter` and `AsyncStore`.

## 0.2.6 — 2026-09-04

**Streaming reads in bounded memory, and the secondary indexes they need.**

### Added

- **`Store.iter(cls, ...)`** walks the same window as `query`, with the same `key`, bounds, `order` and filters, but
  yields rows a page at a time, so a window larger than memory can be read. `chunk` sets the rows per page (default
  1,000), and `limit` caps the total. `limit` defaults to `None`, the whole window, where `query` returns 1,000 rows
  by default and 100,000 at most. Things to know:
  - it flushes pending writes once, when called, so the walk sees everything recorded before it; it doesn't flush
    again per page;
  - it holds the store lock for one page at a time, so recording carries on underneath. That makes it **not a
    snapshot**: rows recorded mid-walk that sort after its position are yielded. A keyset cursor on the full sort
    key resumes each page exactly, yielding no row twice and skipping none;
  - a relative bound such as `'-1h'` resolves once, at the call, not per page;
  - rows whose temporal axis is `NULL`, from a nullable `time_field` left unset, are skipped: they have no place on
    the axis the walk resumes along. `query` still returns them when the window has no bounds;
  - a `chunk` below 1 or a negative `limit` raises `QueryError` at the call.

  It works the same on the SQLite and DuckDB backends. `stored.query.plan` gains `after=`, the keyset `Anchor` to
  resume after, and `skip_null_time=`, and `stored.query` exports `Anchor` and `DEFAULT_CHUNK`.
- The README and the architecture page describe the streaming read path.

### Changed

- **Storage change:** registering a stream creates secondary indexes on its table:
  - `idx_<table>_time` on `(time column, _ts_hlc, _key_expr)`, the sort key of every planned read, which serves
    range queries, `iter`'s paging and the TTL reaper's deletes alike;
  - `idx_<table>_<field>` for each `index=` dimension, one single-column index apiece, so a filter on any one of
    them is served without the others.

  The DDL is `CREATE INDEX IF NOT EXISTS`, so an existing database gains them the next time its streams are
  registered, as the chronicler does at start and `stored migrate` does on demand; on a table already holding
  history, that first registration builds them. Key equality needs no index of its own, since `_key_expr` leads the
  primary key, and a latest-per-key table gains none. Every index costs writes, which is why each one past the
  temporal index is opt-in through `index=`. `docs/storage-model.md` gains an *Indexes* section.
- **`StorageBackend` gains `ensure_index(name, table, columns)`**, which registration calls for each index; the
  SQLite and DuckDB backends implement it.
- **Planned reads break ties on the key expression:** rows with the same time and HLC timestamp sort by `_key_expr`,
  in the window's direction, so the order `query` returns is total.

## 0.2.5 — 2026-09-03

**Retention horizons in seconds or timedeltas, and reads typed on the class asked for.**

### Added

- **Retention horizons take seconds or a `timedelta`.** `Store.register`'s `retention`, `latest_retention` and
  `archive` accept the duration grammar (`'7d'`), a number of seconds (`3600`, `0.5`), or a `datetime.timedelta`, so
  a horizon computed elsewhere needs no formatting first. A number or a timedelta is canonicalized to seconds in the
  string form (`timedelta(days=7)` becomes `'604800s'`), which is what the registered `Stream` holds; a string is
  validated and kept as given. A negative or non-finite horizon raises `ConfigError`.
- **The duration grammar takes a decimal count**, `'1.5h'` or `'0.5s'`, in TOML as well as in code.

### Changed

- **`Store.query` and `Store.latest` are typed on the class they are given:** to a type checker, `query(Telemetry)`
  is a `list[Telemetry]` and `latest(Telemetry, …)` a `Telemetry | None`, with no cast at the call site. What they
  return at runtime is unchanged.
- **Breaking:** `stored.row.rehydrate` takes the class to rebuild, `rehydrate(cls, columns)`, where it took the
  registered `Stream`.
- **Breaking:** `archive` is validated at registration like the other horizons, so a value outside the grammar
  raises `ConfigError` where it was accepted and ignored. Archival itself is still to come.
- `StreamRegistry.add` canonicalizes horizons itself and raises `ValueError` on a bad one; it had accepted
  anything, leaving validation to `Store.register`.
- The README and the storage-model and configuration docs describe the new horizon forms.
- **Dependencies:** the lockfile resolves zeared 0.2.7, from 0.2.4. seared stays at 0.2.1.

## 0.2.4 — 2026-08-28

**A typed shape for the mesh metadata a record carries.**

### Added

- **`stored.row.Meta`**, a `Protocol` naming the five attributes `Store.record` reads from `meta`: `key_expr`,
  `timestamp`, `issued_at`, `source_info` and `schema`. It mirrors zeared's `ZenohMeta` without importing it, so the
  core still imports no zeared at runtime, and a type checker sees a real shape where it saw `Any`. zeared's
  `ZenohMeta`, or any object with those attributes, satisfies it.

### Changed

- **Breaking:** a `meta` object must carry all five attributes. They are read directly, where a missing one used to
  read as `None`, so a partial stand-in now raises `AttributeError`. zeared's `ZenohMeta` is unaffected, and so is
  `meta=None`.

## 0.2.3 — 2026-08-28

**zeared as a core dependency, and the chronicler as a top-level export.**

### Added

- **`stored.Chronicler`**, the chronicler at the top level, the same class as `stored.zenoh.Chronicler`. It resolves
  lazily, on first access, so an `import stored` that only uses `Store` still imports neither zeared nor Zenoh.

### Changed

- **zeared is a core dependency.** The chronicler ships with the base install, with no extra to enable, and
  `eclipse-zenoh` and `msgpack` arrive with it. The core still imports no zeared at runtime.
- `stored.zenoh` always imports; the `ImportError` it raised without the extra is gone.
- The package description reads "A zeared-native persistence layer and Zenoh chronicler", and the README and docs
  drop the extra from the install steps.
- **Dependencies:** `zeared>=0.2.4` moves from the `zenoh` extra into `dependencies`. The lockfile still resolves
  seared 0.2.1 and zeared 0.2.4.

### Deprecated

- **The `stored[zenoh]` extra** is an empty alias, kept for one release so that install strings naming it still
  resolve. It is due to be removed in 0.3.0.

## 0.2.2 — 2026-08-27

**A version-stamp release.** Nothing in the library changes.

### Fixed

- **The version stamps agree.** 0.2.1 bumped `pyproject.toml` and `stored.__version__` but left the lockfile's own
  entry, and the test of the version, at 0.1.0; all of them now read 0.2.2.

## 0.2.1 — 2026-08-27

**Row counts per stream, and a `prune` that sees buffered writes.**

### Added

- **`Store.counts(cls)`** returns `(history, latest)`: the row counts of a stream's history table and of its latest
  projection, for status and observability. It flushes pending writes first, and `latest` is `0` for a stream
  without a projection.

### Fixed

- **`Store.prune` flushes pending writes before it sweeps**, as `query` and `latest` already did. It swept only
  rows already written, so a buffered row already past its horizon outlived the sweep.

## 0.2.0 — 2026-08-27

**SQLite by default, event-time streams, and a latest-per-key projection.**

### Added

- **A SQLite backend** on the standard library's `sqlite3`, now the default (`backend='sqlite'`).
  - It runs in WAL mode, with one connection shared by the writer, delivery and query threads under the store's
    lock.
  - The core's column types map to SQLite declared types (`BIGINT` to `INTEGER`, `DOUBLE` to `REAL`, `VARCHAR` to
    `TEXT`, and so on).
  - Temporal values are stored as ISO-8601 text, whose order is chronological, so range queries and retention
    compare correctly. `datetime`, `date`, `time`, `Decimal` and `timedelta` (as seconds) bind and read back as
    native values, through adapters and converters registered with `sqlite3`. The registration is process-wide, so
    it applies to any other `sqlite3` use in the same process.
- **Event time.** `register(..., time_field='observed_at')` names a payload field holding when the event happened,
  and the stream's range queries, ordering and retention key on it instead of the mesh's `_issued_at`.
  - The value lands in a new `_event_at` column: an `Int` or `Float` is read as unix epoch seconds (UTC), a
    `DateTime` is canonicalized to naive UTC, and a `Date` becomes midnight UTC.
  - The field must be an `Int`, `Float`, `DateTime` or `Date`; any other kind, or a name the class lacks, raises
    `RegistrationError`.
  - A row whose field is `None` is kept, but never matches a time bound and never expires.
  - A stream without a `time_field` behaves as before.
- **A latest-per-key projection.** `register(..., latest_key=('site', 'device'), latest_retention='30d')` keeps a
  second table, `latest_<snake_case>`, with one row per key: the newest value however old, such as a device's last
  state.
  - Every flush upserts the batch it appends to history. A row replaces the stored one only when its time
    (`_event_at` or `_issued_at`, whichever the stream keys on) is at least as new, so a late or redelivered sample
    never overwrites a newer one.
  - `latest_retention` expires the projection on its own horizon, usually longer than the history's; `None` keeps
    it forever.
  - `Store.latest(cls, **key)` flushes, then returns the instance for the full key, or `None`. It raises
    `ConfigError` for a stream without a projection, and `QueryError` unless the keywords name exactly the key's
    fields.
  - Each `latest_key` name must be a scalar field, one with its own column; any other raises `RegistrationError`.
- **Config for both:** `StreamSpec` gains `time_field` and `latest`, a `LatestSpec` with `key` and `retention`
  (`[streams.latest]` in TOML). `migrate` and `prune` apply them. The daemon's `run` does not yet: it registers
  each stream with its `retention` and `index` only.
- **Time bounds as numbers or datetimes.** `Store.query`'s `since` and `until` take unix epoch seconds or a
  `datetime` as well as ISO-8601 and relative (`'-1h'`) strings, so a service can pass a request's timestamps
  straight through.
- **`stored.streams`**, with `resolve_stream_class(spec)` and `register_streams(store, config)`: a configured
  stream resolved to its class and registered, with no mesh involved.
- **`StorageBackend.upsert_latest(table, rows, key_columns, compare_column)`**, the newest-wins upsert behind the
  projection, implemented by both backends. A backend written against 0.1.0 needs it only to serve a projection.

### Changed

- **Breaking:** the default backend is SQLite and the default path `chronicle.db`, for `Store` and `StoredConfig`
  alike; they were DuckDB and `chronicle.duckdb`. Nothing converts an existing DuckDB file: to keep one, set
  `backend='duckdb'` and its path.
- **Breaking:** DuckDB is an optional extra, `stored[duckdb]`, no longer a core dependency, and is imported only
  when `backend='duckdb'` is chosen. Without the extra, that choice fails with `ModuleNotFoundError`.
- **Breaking:** `register_streams` moved from `stored.zenoh.daemon` to `stored.streams`.
- **Storage change:** every stream table gains an `_event_at` `TIMESTAMP` column. A new table has it after
  `_issued_at`; an existing one has it appended the next time its stream is registered, or on `migrate`. Rows
  written before hold `NULL`, so if the stream later takes a `time_field` they match no time bound and never expire.
- `migrate` and `prune` accept any seared class as a stream, where they required a zeared `Message`; `run` still
  does.
- The README and docs describe both backends, event time and the projection.
- **Dependencies:** `duckdb>=1.1` moves from `dependencies` to the new `duckdb` extra, leaving seared the only core
  dependency. The lockfile still resolves seared 0.2.1, zeared 0.2.4 and DuckDB 1.5.5.

### Fixed

- **`migrate` and `prune` run without the `zenoh` extra.** They resolved streams through the daemon module, which
  imports zeared, so on a core-only install both failed with an `ImportError`.

## 0.1.0 — 2026-08-27

**A store for seared objects on DuckDB, and a Zenoh chronicler and daemon built on it.**

### Added

- **`stored.Store`**, persistence for `@s.seared` classes with no mesh involved.
  - `register(cls, retention=…, archive=…, index=…)` creates the class's table, `record(cls, msg, meta=…, key=…)`
    buffers an instance, and `query(cls, key=…, since=…, until=…, limit=…, order=…, **filters)` returns typed
    instances in time order. `flush`, `prune` and `close` complete it.
  - `query` takes ISO-8601 or relative (`'-1h'`) bounds, a topic key that matches as a glob when it holds a `*`,
    and equality filters on the fields named in `index`; a filter on any other field raises `QueryError`. Its limit
    defaults to 1,000 rows and is capped at 100,000.
  - `query` flushes pending writes first, so a read sees every earlier `record`.
- **The storage model:** a table per class, `stream_<snake_case>`, holding meta columns (`_key_expr`, `_ts_hlc`,
  `_issued_at`, `_source`, `_schema`, `_recv_at`, `_ts_source`), a typed column per scalar field, and the whole
  message as seared JSON in `_payload`, which instances are rebuilt from.
  - Scalar kinds map to `BIGINT`, `DOUBLE`, `BOOLEAN`, `VARCHAR`, `BLOB`, `TIMESTAMP`, `DATE`, `TIME`, `INTERVAL`
    and `DECIMAL(38, 9)`. Nested, union, collection, array and frame fields live in `_payload` only.
  - The primary key is `(_key_expr, _ts_hlc)`, so a redelivered sample is a no-op.
  - Every time is stored as naive UTC. A record without an HLC timestamp gets a unique, sortable synthetic
    `_ts_hlc` and its receive time, marked `_ts_source = 'recv'`.
  - Registering a class adds columns for fields it has gained; removing or retyping a field is a manual migration.
- **A batched writer**, flushing per table on a row count (`flush_rows`, 1,000), on an interval (`flush_secs`, 1 s;
  `0` disables the thread) and on close. A batch the backend rejects is logged and dropped.
- **Retention:** a per-stream horizon (`'30m'`, `'48h'`, `'7d'`, `'2w'`), validated at registration, and an active
  reaper that deletes older rows on `Store.prune`.
- **A DuckDB backend**, the only one, behind a small `StorageBackend` protocol (`ensure_table`, `append_batch`,
  `select`, `delete_before`, `close`). Batches go in as chunked multi-row inserts.
- **The Zenoh chronicler, `stored.zenoh.Chronicler`**, in the `zenoh` extra.
  `add(cls, retention=…, serve=…, index=…, on_error=…)` records every sample of a zeared class, with its HLC
  metadata, and serves the history back on a queryable.
  - Consumers read it with zeared's own getter, such as `Cls.query(params={'from': '-1h', 'limit': '5000'})`, and
    get typed instances back. The handler reads `from` and `to` (ISO-8601 or relative), `limit` and `order` from
    the selector, and answers a bad one with an error reply.
  - A `RETAINED` class is recorded but not served, since zeared allows no `on_query` on one, and a warning says so.
  - The core never imports zeared, so persistence works without the extra; importing `stored.zenoh` without it
    raises an `ImportError` naming the extra.
- **A daemon and CLI**, `python -m stored [-c config.toml] <command>`:
  - `run` opens the store and a timestamped session, records and serves every configured stream, prunes every
    `prune_interval` seconds (300 by default), signals systemd readiness, and shuts down in order on SIGTERM or
    SIGINT;
  - `validate-config` loads and summarizes the configuration;
  - `migrate` creates or reconciles every stream's table;
  - `prune` forces a retention sweep.

  `run`, `migrate` and `prune` need the `zenoh` extra.
- **Configuration**, `StoredConfig` and `StreamSpec`, as seared classes: from a TOML file (`-c`), or from `STORED_*`
  environment variables for the scalar fields (`StoredConfig.from_env`). `identity` is required. A stream names its
  class as `'module:ClassName'`, and streams are not read from the environment.
- **A systemd unit template**, `systemd/stored.service` (`Type=notify`): the daemon is supervised by systemd,
  with no HTTP health port, and logs to stdout.
- **An error hierarchy** under `StoredError`: `ConfigError`, `RegistrationError`, `SchemaError`, `BackendError`,
  `WriterError` and `QueryError`.
- **Docs** on the architecture, the storage model, the chronicler and configuration, and a rough ingest and query
  benchmark, `bench/ingest.py`.
- **Dependencies:** seared (`>=0.2.1`) and DuckDB (`>=1.1`), with zeared (`>=0.2.4`) in the `zenoh` extra. seared
  and zeared come from their GitHub repositories; the lockfile resolves seared 0.2.1, zeared 0.2.4 and DuckDB
  1.5.5.

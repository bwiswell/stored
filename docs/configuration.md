# Configuration

Configuration is a `@s.seared` dataclass (`StoredConfig`), populated from the
environment (12-factor) or a TOML file. No `pydantic` — the serializer is the
config layer.

## `StoredConfig`

| Field            | Type              | Default             | Notes                                  |
|------------------|-------------------|---------------------|----------------------------------------|
| `identity`       | `str` (required)  | —                   | This instance's name on the mesh: the daemon's presence key is `stored/presence/{identity}`, so one key segment (no `/`, `*`, `$`, `?` or `#`). |
| `db_path`        | `str`             | `chronicle.db`      | Backing database file.                 |
| `backend`        | `str`             | `sqlite`            | `duckdb` (via `stored[duckdb]`); `postgres` later. |
| `streams`        | `list[StreamSpec]`| `[]`                | What to record and serve.              |
| `zenoh`          | `dict`            | `{}`                | Mapped onto `zeared.SessionConfig`; see [the `[zenoh]` table](#the-zenoh-table). |
| `flush_rows`     | `int`             | `1000`              | Writer flush threshold (rows).         |
| `flush_secs`     | `float`           | `1.0`               | Writer flush interval (`0` disables).  |
| `prune_interval` | `float`           | `300.0`             | TTL sweep interval (`0` disables).     |
| `log_level`      | `str`             | `INFO`              | Root logging level. Under `-c` it comes from the TOML, like everything else. |

## `StreamSpec`

| Field       | Type          | Default | Notes                                    |
|-------------|---------------|---------|------------------------------------------|
| `cls`        | `str` (req.)  | —       | Import path, `'module:ClassName'`.       |
| `retention`  | `str \| None` | `None`  | `'7d'`, `'48h'`, … (`None` = forever). TOML horizons are strings; `Store.register` also takes seconds / a `timedelta`. |
| `archive`    | `str \| None` | `None`  | Cold-archival horizon (roadmap).         |
| `index`      | `list[str]`   | `[]`    | Extra field dimensions to filter on.     |
| `time_field` | `str \| None` | `None`  | Payload field to key retention/queries on (domain event time); `None` uses mesh `_issued_at`. See storage-model. |
| `latest`     | `LatestSpec \| None` | `None` | A newest-per-key projection: `key` (field names) and `retention` (`None` = forever). `[streams.latest]` in TOML. |

`run`, `migrate` and `prune` all register a stream with every field above.

## From the environment

Scalar fields are read as `STORED_<FIELD>` (e.g. `STORED_IDENTITY`,
`STORED_DB_PATH`, `STORED_PRUNE_INTERVAL`). The `streams` list is **not**
env-mapped — supply it via TOML (nested-list env nesting is out of scope) — and
nor is the `zenoh` table. zeared's own variables stand in for it: with no
`[zenoh]` table, the session comes from `ZEARED_SESSION_MODE`,
`ZEARED_SESSION_ROUTER` and the rest (zeared's `SessionConfig.from_env`; 0.4.1),
so an environment-only config can `run`. With neither, `run` stops with a
`ConfigError` saying where to set a `mode`. `validate-config`, `migrate` and
`prune` open no session and run without one.

```sh
export STORED_IDENTITY=chronicler-1
export STORED_DB_PATH=/var/lib/stored/chronicle.db
export ZEARED_SESSION_MODE=client
export ZEARED_SESSION_ROUTER=tcp/router:7447
```

## From TOML

```toml
identity = "chronicler-1"
db_path = "/var/lib/stored/chronicle.db"
prune_interval = 300

[zenoh]
mode = "client"
router = "tcp/router:7447"

[[streams]]
cls = "myapp.messages:Reading"
retention = "7d"
index = ["sensor_id"]

[[streams]]
cls = "myapp.messages:Position"
retention = "48h"
time_field = "observed_at"   # prune/range by domain event time, not mesh delivery time

  [streams.latest]           # keep a newest-per-key projection (durable last-known)
  key = ["source", "item"]   # the logical entity key
  retention = "30d"          # outlives the 48h history
```

Load precedence: `stored -c <file>` reads the TOML; otherwise config comes from
the environment. The two don't layer: with `-c`, no `STORED_*` variable is read.
The session's connection follows the same rule on its own: a `[zenoh]` table, when
there is one, is used whole, and `ZEARED_SESSION_*` is read only when there isn't,
with `-c` or without.

## The `[zenoh]` table

The table is loaded as a `zeared.SessionConfig` (`SessionConfig.load`), and the
session opened with `zeared.open`, timestamping on. With no table, each key but
`zenoh_overrides` is read from `ZEARED_SESSION_<KEY>` instead (`connect` and
`listen` as comma-separated lists). Its keys:

| Key                  | Default    | Notes |
|----------------------|------------|-------|
| `mode`               | (required) | `peer` or `client`. |
| `router`             | —          | Client shortcut: the sole `connect` endpoint when `connect` is empty. |
| `connect`, `listen`  | `[]`       | Endpoint lists, e.g. `["tcp/router:7447"]`. |
| `retry`              | `false`    | Retry the first open with exponential backoff, from `initial_backoff` (`0.1` s) doubling to `max_backoff` (`30.0` s), giving up after `max_attempts` if set. |
| `auto_reconnect`     | `false`    | Open a `zeared.ManagedSession`: it reconnects a dead session, watches its router link, re-fetches retained values when the link comes back, and keeps the daemon's presence token across a reconnect. |
| `probe_interval`     | `10.0`     | Seconds between the managed session's health and link checks. |
| `resync_jitter`      | `2.0`      | After the link comes back, wait up to this many seconds before re-fetching retained values, so a fleet doesn't fetch at once. |
| `resync_concurrency` | `4`        | At most this many re-fetches at a time. |
| `zenoh_config_file`  | —          | A JSON5 Zenoh config, for what `SessionConfig` doesn't model: TLS, access control, scouting. |
| `zenoh_overrides`    | `{}`       | JSON5 fragments keyed by config path, applied last, e.g. `"scouting/multicast/enabled" = false`. Don't turn timestamping off here: the chronicler dedups and orders on it. |

`auto_reconnect`, `probe_interval`, `resync_jitter` and `resync_concurrency` are new
in zeared 0.4. A key the table doesn't know is dropped without a word, and `validate-config` doesn't load the table, so
check a key against this list (or zeared's `SessionConfig`) when it seems to do
nothing.

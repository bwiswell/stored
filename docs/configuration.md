# Configuration

Configuration is a `@s.seared` dataclass (`StoredConfig`), populated from the
environment (12-factor) or a TOML file. No `pydantic` — the serializer is the
config layer.

## `StoredConfig`

| Field            | Type              | Default             | Notes                                  |
|------------------|-------------------|---------------------|----------------------------------------|
| `identity`       | `str` (required)  | —                   | This instance's name on the mesh.      |
| `db_path`        | `str`             | `chronicle.duckdb`  | Backing database file.                 |
| `backend`        | `str`             | `duckdb`            | `postgres` is a later backend.         |
| `streams`        | `list[StreamSpec]`| `[]`                | What to record and serve.              |
| `zenoh`          | `dict`            | `{}`                | Mapped onto `zeared.SessionConfig`.    |
| `flush_rows`     | `int`             | `1000`              | Writer flush threshold (rows).         |
| `flush_secs`     | `float`           | `1.0`               | Writer flush interval (`0` disables).  |
| `prune_interval` | `float`           | `300.0`             | TTL sweep interval (`0` disables).     |
| `log_level`      | `str`             | `INFO`              | Root logging level.                    |

## `StreamSpec`

| Field       | Type          | Default | Notes                                    |
|-------------|---------------|---------|------------------------------------------|
| `cls`       | `str` (req.)  | —       | Import path, `'module:ClassName'`.       |
| `retention` | `str \| None` | `None`  | `'7d'`, `'48h'`, … (`None` = forever).   |
| `archive`   | `str \| None` | `None`  | Cold-archival horizon (roadmap).         |
| `index`     | `list[str]`   | `[]`    | Extra field dimensions to filter on.     |

## From the environment

Scalar fields are read as `STORED_<FIELD>` (e.g. `STORED_IDENTITY`,
`STORED_DB_PATH`, `STORED_PRUNE_INTERVAL`). The `streams` list is **not**
env-mapped — supply it via TOML (nested-list env nesting is out of scope).

```sh
export STORED_IDENTITY=chronicler-1
export STORED_DB_PATH=/var/lib/stored/chronicle.duckdb
```

## From TOML

```toml
identity = "chronicler-1"
db_path = "/var/lib/stored/chronicle.duckdb"
prune_interval = 300

[zenoh]
mode = "client"
router = "tcp/router:7447"

[[streams]]
cls = "rio_protocol.messages:RawRead"
retention = "7d"
index = ["reader_id"]

[[streams]]
cls = "rio_protocol.messages:Location"
retention = "48h"
```

Load precedence: `stored -c <file>` reads the TOML; otherwise config comes from
the environment. The `zenoh` table must include `mode` (`peer` or `client`).

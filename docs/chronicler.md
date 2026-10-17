# The chronicler (`stored.zenoh`)

The Zenoh layer. `zeared` is a core dependency, so it ships with the base install
(by release tag; see the README's *Install*) — no extra to enable. The
seared-only core still imports no `zeared` at runtime, so a Store-only
`import stored` stays transport-free; `stored.Chronicler` (re-exported lazily)
and `stored.zenoh.Chronicler` both name the class below.

## Chronicler

```python
import zeared as z
import stored
import stored.zenoh as sz

session = z.peer()                       # timestamped session
store = stored.Store('chronicle.db')
chronicler = sz.Chronicler(store, session)

chronicler.add(Telemetry, retention='7d', index=('id',))
chronicler.run()                         # blocks; stop() to return
```

`add(cls, *, retention=None, serve=True, index=(), on_error=None)`:

- registers the stream (if new) and declares a **2-arg** `on_message`
  subscriber → `store.record` (so the HLC `meta` arrives);
- for **non-RETAINED** classes, declares an `on_query` queryable serving history
  (RETAINED classes are recorded but skip the queryable — `on_query` is
  forbidden there; a warning is logged).

The chronicler owns only the handles it declares; the caller owns the session
and the store. `close()` is idempotent.

**What it records** is what is published on the mesh, as seared decodes it:

- **A sample seared refuses** goes to `on_error` (or a log line) and isn't
  recorded. Since seared 0.4.0 that includes a `null` in a required field, a
  fractional number into an `Int` and a `2` into a `Bool`, which 0.3.x coerced or
  let through. A `null` on a field with a default records the default, so a
  `time_field` that may arrive as `null` should be `X | None` with
  `default=None` (or `nullable=True`): with `Float(default=0.0)` it reads as 1970.
- **A liveliness class's departures** arrive at `on_offline`, as an identity with
  no message. The chronicler subscribes `on_message` only, so they record nothing.
- **A `RETAINED` class's values** already on the mesh are fetched as the
  subscriber starts, and again after a `ManagedSession`'s link comes back. Each is
  the row already stored when its publisher runs zeared 0.4.0 or later, whose
  retained replies carry the value's original timestamp; a reply from an older
  publisher carries none and is recorded as a new row.
- **A class zeared can't key** — a `Path` slot, or a `Bytes` slot in its default
  base64 form, in a single key segment, either of which can render a `/` — raises
  `zeared.TopicError` when `add` subscribes it.

## Serving history

Consumers use the ordinary `zeared` getter — no new contract:

```python
history = Telemetry.query(id=7, params={'from': '-1h', 'to': '-5m', 'limit': '5000'})
```

The handler reads `from` / `to` (ISO-8601 or relative like `-1h`), `limit`, and
`order` (`asc`/`desc`) from the query's selector params, matches stored
`_key_expr` against the queried key expression, and streams each historical row
back as a properly-typed `Telemetry` instance. Bad input → an error reply. A
query with no `limit` gets at most 1,000 rows, and no `limit` takes more than
100,000.

Each row is its own reply. A row with no key to reply on, because a single-segment
slot holds a `/` (which zeared refuses, since it would split the key), is skipped:
zeared logs it, or passes it to the chronicler's `on_error`, and the rest of the
answer still goes out.

## The daemon

`python -m stored run` loads config, opens a store + timestamped session,
resolves every configured stream, registers it with its whole `StreamSpec`
(retention, archive, indexes, `time_field` and `[streams.latest]`, exactly as
`migrate` and `prune` register it) and subscribes it, starts the periodic reaper,
declares its presence, signals `sd_notify(READY=1)`, and runs until SIGTERM/SIGINT
— then tears down in order (presence → reaper → chronicler → `z.release(session)`
→ store).

With `auto_reconnect = true` in `[zenoh]`, the session is a `zeared.ManagedSession`
(see [configuration.md](configuration.md)): it reconnects when its link to the
router is lost, re-fetches retained values when the link comes back, and its
presence token comes back with it.

CLI:

```sh
stored -c stored.toml validate-config   # load + summarize config
stored -c stored.toml run               # the chronicler daemon
stored -c stored.toml migrate           # create/reconcile stream tables
stored -c stored.toml prune             # force a TTL sweep
```

## systemd

Production is a systemd unit (`systemd/stored.service`, `Type=notify`), which
runs `stored -c /etc/stored/stored.toml run`. Logs go to stdout → journald.
Containers are dev/CI only.

**Health is systemd supervision plus Zenoh liveliness — no HTTP health port.** The
daemon signals `sd_notify(READY=1)` once every stream is subscribed, and holds a
liveliness token at `stored/presence/{identity}` from just before then until it
shuts down. Watch it with `stored.zenoh.ChroniclerPresence`:

```python
from stored.zenoh import ChroniclerPresence

ChroniclerPresence.on_message(
    lambda _msg: None,  # a token carries no message, so this never runs
    on_online=lambda meta: print('up', meta.captures['identity']),
    on_offline=lambda meta: print('down', meta.captures['identity']),
)
```

A crash reads the same as a clean stop: Zenoh withdraws the token either way. A
deployment with access rules grants the daemon `liveliness_token` on
`stored/presence/**`, and its watchers `declare_liveliness_subscriber` and
`liveliness_query` there.

**The watchdog** (0.4.1). The unit sets `WatchdogSec=30`, and the daemon pings
systemd with `WATCHDOG=1` every half period, but only while the store can still
do its work: its lock, which every backend read and write takes, must come free
within half a tick, and the writer's periodic flush must be running. A backend
wedged mid-statement, or a writer that has stopped, stops the pings, and systemd
restarts the daemon (`Restart=on-failure`). Each missed ping logs why. The pings
run only when systemd sets `WATCHDOG_USEC` for this process, so a daemon outside
such a unit sends none. `Store.health(timeout)` is the same check, for a service
that embeds a store and has a watchdog of its own: `None` when healthy, else a
short reason.

**The mesh link** (0.4.1). Once ready, the daemon's `systemctl status` line reads
`recording N stream(s) as stored/presence/{identity}`. With `auto_reconnect` in
`[zenoh]` the session is a `zeared.ManagedSession`, and when its link to the mesh
goes stale the status becomes `mesh link lost; reconnecting`, with a warning in
the log, and returns once the link is whole again. Nothing else changes: the
session reconnects, re-fetches retained values and keeps the presence token by
itself, so the watchdog keeps pinging, since a restart would undo that work, and a
hub restart would restart every daemon on the mesh at once. A session without
`auto_reconnect` has no such signals.

`identity` must be one key segment (no `/`, `*`, `$`, `?` or `#`), since it is the
key's last segment; `run` refuses any other with a `ConfigError` before it opens
anything.

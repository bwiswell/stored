# The chronicler (`stored.zenoh`)

The Zenoh layer. `zeared` is a core dependency, so it ships with the base install
(`uv add git+https://github.com/bwiswell/stored`) — no extra to enable. The
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

## Serving history

Consumers use the ordinary `zeared` getter — no new contract:

```python
history = Telemetry.query(id=7, params={'from': '-1h', 'to': '-5m', 'limit': '5000'})
```

The handler reads `from` / `to` (ISO-8601 or relative like `-1h`), `limit`, and
`order` (`asc`/`desc`) from the query's selector params, matches stored
`_key_expr` against the queried key expression, and streams each historical row
back as a properly-typed `Telemetry` instance. Bad input → an error reply.

## The daemon

`python -m stored run` loads config, opens a store + timestamped session,
resolves and subscribes every configured stream, starts the periodic reaper,
signals `sd_notify(READY=1)`, and runs until SIGTERM/SIGINT — then tears down in
order (reaper → chronicler → `z.release(session)` → store).

CLI:

```sh
stored -c stored.toml validate-config   # load + summarize config
stored -c stored.toml run               # the chronicler daemon
stored -c stored.toml migrate           # create/reconcile stream tables
stored -c stored.toml prune             # force a TTL sweep
```

## systemd

Production is a systemd unit (`systemd/stored.service`, `Type=notify`); health
is systemd supervision + Zenoh liveliness — **no HTTP health port**. Logs go to
stdout → journald. Containers are dev/CI only.

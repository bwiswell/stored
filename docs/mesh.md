# `stored.mesh` — binding a store to contracts

`stored.zenoh` is named for the *transport*: a session, the `Chronicler`, the
daemon, a selector-param query handler. `stored.mesh` is the layer above it — the
shapes a **service** binds. Two inhabitants today:

- **`AsyncStore`** — an awaitable view of a blocking `Store`, for services on an
  event loop. Needs no transport at all.
- **`Binding`** — subscribers and queryables declared against typed contracts:
  record, time-range history, last-known for one entity, and current state for a
  population.

Everything here speaks *zeared* vocabulary — a message class, its `REQUEST`
payload, a queryable, a projection function — and nothing else. No contract,
topic or category from any particular fleet appears in this layer, which is what
lets one binding serve any `@zeared` class.

## `AsyncStore`

```python
from stored.mesh import AsyncStore

store = AsyncStore(stored.Store('history.db', flush_rows=5000))
store.register(Position, time_field='observed_at', latest_key=('source', 'epc'))

store.record(Position, position)                       # sync — a buffered enqueue
newest = await store.latest(Position, source='rtls', epc=epc)
async for row in store.iter(Position, since='-7d'):    # one thread hop per page
    ...
```

`register` and `record` are synchronous on purpose: the first is one-time DDL on
the open path, the second an enqueue cheaper than the thread hop that would defer
it. Everything reaching the backend is awaited. `iter` flushes when iteration
*starts* (an async generator cannot act before its first `await`) — the sync
`Store.iter` flushes when *called*.

## `Binding`

```python
from stored.mesh import Binding

binding = Binding(store)                    # a bare Store is wrapped for you

binding.record(Position)                    # subscribe → persist
binding.record(Alarm, store_as=Event, via=from_alarm)   # subscribe A → persist B

binding.serve_range(                        # typed time-range history
    Event,
    filters=('source', 'kind'),
    since='from_ts', until='to_ts', limit='limit',
)
binding.serve_latest(                       # last-known for one entity
    LastPosition, of=Position, key=('source', 'epc'),
    project=to_last_position, missing=no_position,
)
...
binding.close()                             # releases every handle
```

Handlers are `async def`, so they await the store on a worker thread and never
block the loop. Declare the binding from inside a running loop — zeared schedules
an async handler on the loop live at declaration time.

### `record(cls, store_as=…, via=…)`

*Subscribe A, persist B.* Several source contracts normalize into one row type,
which is what makes a unified history query across them possible. The mapper stays
the caller's — it is domain logic that happens to run on the record path.

A mapped row keeps the **source** message's key expression and HLC timestamp, so
redelivery of a source message still dedups on the stored primary key.

### `serve_range(cls, …)`

Reads the request's fields by name: `filters` become equality filters on indexed
dimensions, `since`/`until` the window, `limit` the cap. `cls` is both the stored
class and the reply class — a stream is queried by, and answers with, the class it
is stored as.

### Streaming a range

```python
binding.serve_range(Event, filters=('source',), since='from_ts', limit='limit',
                    stream=True, chunk=500)
```

With `stream=True` the handler is a generator: rows are replied **as they are
read**, a page at a time, so neither side holds the result set. A getter using
`z.aquery_iter(...)` sees each reply as it lands; one using `z.aquery(...)` still
collects the list, and gets identical replies. It is not a contract change — the
same rows, produced lazily — so an existing queryable can switch over without its
consumers noticing.

Two properties of Zenoh queries shape the defaults:

- **A caller that abandons a query does not stop the queryable.** Cancellation is
  client-side only, so an uncapped stream would leave the historian producing rows
  nobody reads. Streaming therefore defaults `default_limit` to the query planner's
  `MAX_LIMIT` rather than to "unbounded"; pass an explicit value to raise or lower
  the bound.
- **`timeout` covers the whole query**, and starts when the getter is *called*. A
  streamed range is right for windows that are large but bounded; a window measured
  in days belongs on a replay, not a query.

`stream` is opt-in. Collecting is the better default for the small bounded reads
most queryables serve, and switching an existing binding is a one-word change.

### `serve_snapshot(cls, of=…, filters=…, project=…)`

*Current state*, multi-reply: the newest row of every matching entity, which is what
an operator console asks for.

```python
binding.serve_snapshot(Placed, filters={'zone': 'zones.department', 'source': 'source'},
                       limit='limit')
```

`serve_range` answers "what happened"; this answers "what is the case". Both take
`of=`/`project=` when the reply contract differs from the stored row. Same
declaration, pointed at the latest projection — so `since`/`until` narrow on **last
seen**, not on when a row was recorded, and `stream=True` works the same way (a floor
with 100k tags is exactly when you want it). `of=` + `project=` shape a reply that
differs from the stored row, as in `serve_latest`; a row type that doubles as its own
reply needs neither.

### Filtering on a path inside a `dict`

A filter target is resolved against **what the stream declared** — a name in
`index=` filters a column, a name in `json_index=` filters inside a `Dict` — so a
path needs no new binding API at all:

```python
store.register(Placed, index=('source',), json_index=('zones.department',), …)

binding.serve_range(Placed,    filters={'zone': 'zones.department'})   # what happened there
binding.serve_snapshot(Placed, filters={'zone': 'zones.department'})   # who is there now
```

A target that names neither is a `ConfigError` **when the binding is declared**,
rather than a query that quietly matches nothing.

### When the caller names the dimension

Zone layers are open-ended by design, so which path to filter on can belong to the
*request* rather than the declaration. A target may therefore be a function of it:

```python
binding.serve_snapshot(
    TagInZone, of=Location,
    filters={'zone': lambda request: f'zones.{request.layer}'},
)
```

The resolved target still has to be something the stream declared — the allow-list is
unchanged — but the check necessarily moves to **query time**. Two consequences worth
knowing before reaching for this:

- An unrecognized target raises `QueryError` for that request, so the historian never
  answers as though the filter had been applied.
- zeared drops error replies from `aquery`'s result, so a caller passing no
  `on_error` sees an empty list and cannot distinguish "nothing matched" from "that
  dimension is not indexed". Callers that need the difference pass `on_error`.

Prefer a fixed target wherever the dimension is known at declaration time; it fails at
startup instead.

### `serve_latest(cls, of=…, project=…, missing=…)`

`cls` is the **reply** contract; `of` is the class actually stored. When they
differ — a `Position` row answered as a `LastPosition` — `project(row, request)`
shapes the reply, because only the caller knows how a stored row becomes its
contract. When one class does double duty as row *and* reply, omit `of` and
`project`: the projection is the identity. `missing(request)` answers when nothing
is stored for the key; omit it to reply nothing at all.

## `Replayer` — history as ordinary traffic

A queryable answers one asker. A **replay** re-publishes what was recorded, so any
number of ordinary subscribers see it through their ordinary `on_message` path:
re-run yesterday through a pipeline stage, warm a cache after a restart, drive a
lab run from real recorded traffic — with no client code at all.

```python
from stored.mesh import Replayer

replayer = Replayer(store)
await replayer.replay(Reading, since='-1d', speed=0)        # backfill, flat out
handle = replayer.start(Reading, since='-1h', speed=1.0)    # real time, in the background
...
handle.stop();  sent = await handle.wait()
```

### Where a replay lands

zeared publishes on **declared** templates only, so a replayable class declares its
replay scope:

```python
class Reading(zeared.Message):
    TOPIC = 'live/reading/{source}'
    EXTRA_TOPICS = ('replay/reading/{source}',)
```

`replay()` defaults to that sole `EXTRA_TOPICS` entry; pass `topic=` to choose among
several, or `topic=cls.TOPIC` to republish onto the live topic (rarely what you
want — recorders then cannot tell replay from live). A class with no declared scope
and no explicit `topic=` is a `ConfigError` rather than a silent republish.

That one declaration carries the whole isolation story, because provenance has
nowhere else to ride: `ZenohMeta.origin` is derived from the local delivery path and
never crosses the wire, so a replayed sample is otherwise indistinguishable from a
live one.

### Who sees a replay

A subscription covers every template a class declares, so:

- **Consumers that want history get it for free** — no client change, which is the
  point of replaying rather than serving a query.
- **Recorders opt out** with `binding.record(cls, live_only=True)`, which records
  only samples arriving on `TOPIC`'s own literal scope. Without it a historian
  re-records the replay as new traffic; `record()` logs a warning at bind time when
  a class declares `EXTRA_TOPICS` and no choice was made.

### Pacing and bounds

- `speed=0` publishes as fast as the mesh accepts, a page per thread hop — the
  backfill case.
- `speed>0` paces to wall clock off the stream's event time (`1.0` real time, `10.0`
  ten times faster). It needs a `time_field` on the stream; asking for pacing without
  one is a `ConfigError` rather than a silently unpaced replay.
- `max_rate` caps rows per second whatever `speed` says, and `limit` caps the window.
- `stop()` interrupts a paced sleep immediately rather than after the current gap.

A replay always publishes with `retain=False`, so replaying a `RETAINED` class can
never clobber the live retained value with a historical one.

## The sentinel policy

Request payloads commonly encode "not provided" as an empty string or a zero
rather than `None` — a wire format without optionals. `UNSET_FALSY`
(`('', 0, 0.0, None)`) is that convention, and it is the default:

```python
HistoryRequest(source='rtls')        # filters on source, ignores the empty kind
HistoryRequest()                     # filters on nothing: the whole history
```

Pass `unset=(None,)` for a strict convention where `''` is a real value to match,
or any tuple a given fleet treats as absent.

## What stays out

Presence, liveliness, retained status/settings, watchdogs and cold-start config
are the *service's* — a binding declares subscribers and queryables, and owns
nothing else. Domain normalizers (the `via=` mappers) and reply projections
(`project=`) are the caller's for the same reason: they encode a fleet's meaning,
not a storage concern.

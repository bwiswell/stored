# `stored.mesh` — binding a store to contracts

`stored.zenoh` is named for the *transport*: a session, the `Chronicler`, the
daemon, a selector-param query handler. `stored.mesh` is the layer above it — the
shapes a **service** binds. Two inhabitants today:

- **`AsyncStore`** — an awaitable view of a blocking `Store`, for services on an
  event loop. Needs no transport at all.
- **`Binding`** — subscribers and queryables declared against typed contracts.

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

### `serve_latest(cls, of=…, project=…, missing=…)`

`cls` is the **reply** contract; `of` is the class actually stored. When they
differ — a `Position` row answered as a `LastPosition` — `project(row, request)`
shapes the reply, because only the caller knows how a stored row becomes its
contract. When one class does double duty as row *and* reply, omit `of` and
`project`: the projection is the identity. `missing(request)` answers when nothing
is stored for the key; omit it to reply nothing at all.

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

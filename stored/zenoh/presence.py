"""The daemon's presence on the mesh: a Zenoh liveliness token at one key.

A stored daemon's health is systemd supervision plus Zenoh liveliness, with no
HTTP health port. While it runs, the daemon holds a token at
``stored/presence/{identity}``. A watcher hears it come online, and go offline
when the daemon stops or dies (a crash reads the same as a clean stop):

```python
from stored.zenoh import ChroniclerPresence

ChroniclerPresence.on_message(
    lambda _msg: None,  # a token carries no message, so this never runs
    on_online=lambda meta: print('up', meta.captures['identity']),
    on_offline=lambda meta: print('down', meta.captures['identity']),
)
```

Only the daemon declares it. A service that embeds a ``Chronicler`` holds its
own presence, under its own contract.

``zeared`` is imported unaliased, per the convention for message-definition
modules.
"""

from __future__ import annotations

import zeared


@zeared.zeared
class ChroniclerPresence(zeared.Message):
    """A stored daemon on the mesh: a liveliness token at its identity's key, with no payload."""

    TOPIC = 'stored/presence/{identity}'
    LIVELINESS = True

    identity: str = zeared.Str(required=True)


__all__ = ['ChroniclerPresence']

"""Shared loopback-peer fixtures for the transport-facing suites (``zenoh`` + ``mesh``).

Lives at the tests/ root (on ``pythonpath``) so both conftests can pull the same
session + state-reset fixtures instead of keeping two copies in step.
"""
from __future__ import annotations

import pytest
import zeared as z
import zenoh

zenoh.init_log_from_env_or('error')


def peer_session():
    """An isolated loopback peer with timestamping on (the HLC the row builder reads)."""
    config = zenoh.Config()
    config.insert_json5('mode', '"peer"')
    config.insert_json5('scouting/multicast/enabled', 'false')
    config.insert_json5('timestamping/enabled', 'true')
    return zenoh.open(config)


@pytest.fixture
def session():
    sess = peer_session()
    try:
        yield sess
    finally:
        sess.close()


@pytest.fixture(autouse=True)
def _reset_zeared_state():
    def _reset():
        z.session._set_default(None)
        z.debug = False
        z.clear_publisher_cache()
        z.clear_retention_cache()
        z.clear_queryable_cache()
        z.clear_observer()
        z.clear_presence_state()

    _reset()
    yield
    _reset()

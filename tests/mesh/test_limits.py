"""The row cap a binding applies: what the caller asks, what the service allows."""

from stored.mesh.binding import _effective_limit
from stored.query import MAX_LIMIT


def test_a_request_is_honoured_below_the_ceiling():
    assert _effective_limit(50, default=None, maximum=2000, streaming=False) == 50


def test_the_ceiling_overrules_an_explicit_request():
    assert _effective_limit(100_000, default=None, maximum=2000, streaming=False) == 2000


def test_an_omitted_limit_takes_the_default():
    assert _effective_limit(None, default=500, maximum=2000, streaming=False) == 500


def test_the_ceiling_also_clamps_the_default():
    """A default above the ceiling is a misconfiguration, not a licence."""
    assert _effective_limit(None, default=5000, maximum=2000, streaming=False) == 2000


def test_the_ceiling_applies_to_an_omitted_limit_too():
    """Asking for nothing is asking for everything, and the ceiling has to answer it.

    Left to the planner an omitted limit lands on ``DEFAULT_LIMIT``, which exceeds any
    ceiling set below that — silently, and in the one case the parameter exists for.
    """
    assert _effective_limit(None, default=None, maximum=500, streaming=False) == 500


def test_nothing_set_leaves_it_to_the_planner():
    # No ceiling declared, so there is nothing to enforce and the planner's own default
    # governs — unchanged by the clause above.
    assert _effective_limit(None, default=None, maximum=None, streaming=False) is None


def test_streaming_always_lands_on_a_bound():
    """None means *unbounded* on the streaming path, and an abandoned client stops nothing."""
    assert _effective_limit(None, default=None, maximum=None, streaming=True) == MAX_LIMIT
    assert _effective_limit(None, default=None, maximum=2000, streaming=True) == 2000

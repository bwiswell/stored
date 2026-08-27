"""Importable zeared Message classes for daemon stream-resolution tests.

Lives at the tests/ root (on ``pythonpath``) so it resolves as the top-level
module ``_support_messages``. Not collected (no ``test_`` prefix).
"""
import zeared as z


@z.zeared
class Beacon(z.Message):
    TOPIC = 'beacon/{id}'
    id: int = z.Int(required=True)
    v: int = z.Int(default=0)

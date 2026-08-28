"""Map ``seared`` classes to backend table shapes.

``_payload`` holds the full message as a seared JSON string — the lossless
source for rehydration — and scalar seared fields are additionally projected
into typed columns (columnar compression + SQL predicates on those dimensions).
Nested/collection/array fields have no column; they round-trip through
``_payload`` only.

.. note::
   v1 keeps the *whole* message in ``_payload`` for simple, lossless
   rehydration; slimming it to just the non-column fields is a later
   optimization.
"""
from __future__ import annotations

from typing import Any

import seared as s

# Backend-neutral column type names (DuckDB spelling; the backend may remap).
# Scalar seared field class name -> column type.
SCALAR_TYPES: dict[str, str] = {
    'Int': 'BIGINT',
    'Float': 'DOUBLE',
    'Bool': 'BOOLEAN',
    'Str': 'VARCHAR',
    'Path': 'VARCHAR',
    'UUID': 'VARCHAR',
    'Enum': 'VARCHAR',
    'Bytes': 'BLOB',
    'DateTime': 'TIMESTAMP',
    'Date': 'DATE',
    'Time': 'TIME',
    'TimeDelta': 'INTERVAL',
    'Decimal': 'DECIMAL(38, 9)',
}

# Field kinds that never map to a scalar column — serialized into ``_payload``.
COMPLEX_FIELDS: frozenset[str] = frozenset(
    {'T', 'Union', 'Dict', 'Tuple', 'NDArray', 'PandasFrame', 'PolarsFrame'},
)

# Metadata columns prepended to every stream table. Ordered; the first two form
# the primary key / dedup key.
META_COLUMNS: dict[str, str] = {
    '_key_expr': 'VARCHAR',
    '_ts_hlc': 'VARCHAR',
    '_issued_at': 'TIMESTAMP',
    '_event_at': 'TIMESTAMP',
    '_source': 'VARCHAR',
    '_schema': 'VARCHAR',
    '_recv_at': 'TIMESTAMP',
    '_ts_source': 'VARCHAR',
    '_payload': 'VARCHAR',
}

PRIMARY_KEY: tuple[str, str] = ('_key_expr', '_ts_hlc')

#: Temporal-axis column names. ``_issued_at`` is the mesh delivery/issue time (the
#: default axis for retention + range queries); ``_event_at`` is the normalized
#: **domain event time**, populated from a stream's ``time_field`` when set (NULL
#: otherwise). A stream keys retention/queries off whichever its ``time_column`` names.
ISSUED_AT: str = '_issued_at'
EVENT_AT: str = '_event_at'

#: Seared field kinds a ``time_field`` may name — an absolute instant only.
TIME_FIELD_KINDS: frozenset[str] = frozenset({'Int', 'Float', 'DateTime', 'Date'})


def column_type(field: Any) -> str | None:
    """Return the scalar column type for a seared ``field``, or ``None``.

    ``None`` means the field is a collection (``many`` / ``keyed``) or a complex
    kind and must be routed to the ``_payload`` blob instead of its own column.

    Args:
        field: A seared ``Field`` instance from ``cls.__seared_fields__``.

    Returns:
        The backend column type name, or ``None`` for blob-routed fields.
    """
    if getattr(field, 'many', False) or getattr(field, 'keyed', False):
        return None
    kind = type(field).__name__
    if kind in COMPLEX_FIELDS:
        return None
    return SCALAR_TYPES.get(kind)


def derive_columns(cls: type[s.Seared]) -> dict[str, str]:
    """Derive the full ordered column set for ``cls``'s stream table.

    Meta columns first, then one column per scalar seared field (blob-routed
    fields are omitted — they live in ``_payload``).

    Args:
        cls: A ``@s.seared`` / ``@z.zeared`` message class.

    Returns:
        Ordered mapping of column name to backend column type.
    """
    columns: dict[str, str] = dict(META_COLUMNS)
    for attr, _wire, field in cls.__seared_fields__:
        col_type = column_type(field)
        if col_type is not None:
            columns.setdefault(attr, col_type)
    return columns


def table_name(cls: type) -> str:
    """Return the table name for ``cls`` (``'stream_<snake>'``)."""
    name = cls.__name__
    snake = ''.join(f'_{c.lower()}' if c.isupper() else c for c in name).lstrip('_')
    return f'stream_{snake}'


__all__ = [
    'SCALAR_TYPES',
    'COMPLEX_FIELDS',
    'META_COLUMNS',
    'PRIMARY_KEY',
    'ISSUED_AT',
    'EVENT_AT',
    'TIME_FIELD_KINDS',
    'column_type',
    'derive_columns',
    'table_name',
]

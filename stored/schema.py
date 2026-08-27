"""Map ``seared`` classes to backend table shapes.

The row model is *typed columns for scalar fields + a blob for the remainder*.
Scalar seared fields become real columns (columnar compression + SQL
predicates); nested/collection/array fields fall back to a single ``_payload``
blob (full fidelity, no server-side filtering) in v1.

.. note::
   M0 scaffold: the type map and meta columns are final; DDL emission and table
   reconciliation land in M1.
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
    'DateTime': 'TIMESTAMPTZ',
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
    '_issued_at': 'TIMESTAMPTZ',
    '_source': 'VARCHAR',
    '_schema': 'VARCHAR',
    '_recv_at': 'TIMESTAMPTZ',
    '_ts_source': 'VARCHAR',
    '_payload': 'BLOB',
}

PRIMARY_KEY: tuple[str, str] = ('_key_expr', '_ts_hlc')


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
    'column_type',
    'derive_columns',
    'table_name',
]

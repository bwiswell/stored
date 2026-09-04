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

from typing import TYPE_CHECKING, Any

from .errors import SchemaError

if TYPE_CHECKING:
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


def json_index_specs(table: str, paths: dict[str, str]) -> tuple[tuple[str, str], ...]:
    """The expression indexes a stream's declared ``json_index`` paths want.

    Named separately from :func:`index_specs` because the shape differs: a column
    index names columns, this one names an *expression* over ``_payload``. The index
    name is derived from the declared path (dots and anything else unsafe collapsed
    to underscores), so it is stable across restarts and readable in ``.schema``.

    Args:
        table: The table the index belongs to.
        paths: ``{declared path: wire path}`` from ``Stream.json_paths``.

    Returns:
        ``(index_name, wire_path)`` pairs.
    """
    specs = []
    for declared, wire in paths.items():
        slug = ''.join(char if char.isalnum() else '_' for char in declared)
        specs.append((f'idx_{table}_json_{slug}', wire))
    return tuple(specs)


def wire_path(cls: type[s.Seared], path: str) -> str:
    """Translate a declared dotted ``path`` into the one ``_payload`` actually uses.

    A ``seared`` field may carry ``data_key='z'``, in which case the payload holds
    ``{"z": …}`` while the caller quite reasonably writes ``zones.department``. The
    head segment is therefore looked up through ``__seared_fields__`` and rewritten;
    everything after it is dict keys, which no aliasing touches.

    Args:
        cls: The registered message class.
        path: A dotted path whose head names a field of ``cls``.

    Returns:
        The same path with its head in wire spelling.

    Raises:
        SchemaError: If the head does not name a field of ``cls``.
    """
    head, _, tail = path.partition('.')
    for attr, wire, _field in cls.__seared_fields__:
        if attr == head:
            return f'{wire}.{tail}' if tail else wire
    msg = f'{path!r}: {head!r} is not a field of {cls.__name__}'
    raise SchemaError(msg)


def index_specs(
    table: str,
    time_column: str,
    dimensions: tuple[str, ...],
    *,
    served_by_pk: tuple[str, ...] = (),
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """The secondary indexes a stream table wants, as ``(name, columns)`` pairs.

    Two kinds, and deliberately no more:

    - **The temporal index** ``(time_column, _ts_hlc, _key_expr)`` — the exact sort
      key of every planned SELECT, so it serves range queries, ``Store.iter``'s
      keyset paging, and the reaper's ``DELETE … WHERE time < cutoff`` alike.
    - **One per declared dimension** — a single-column index each, rather than one
      composite, because filters are independent (a query may name ``kind`` without
      ``source``, which a composite's leading-column rule would not serve).

    A column that **leads** the table's primary key needs no index of its own — the
    PK's unique index already serves equality on it. That is why the history table
    gets no ``_key_expr`` index, and why the latest projection (keyed by the logical
    entity) gets none for the first field of that key.

    Args:
        table: The stream's table name (indexes are named after it).
        time_column: The stream's temporal axis (``_event_at`` or ``_issued_at``).
        dimensions: The declared queryable dimensions (``Stream.index``).
        served_by_pk: Columns the table's primary key already serves — in practice
            its leading column, since only a leading column is served by a composite.

    Returns:
        ``(index_name, columns)`` pairs, temporal index first.
    """
    specs = [(f'idx_{table}_time', (time_column, '_ts_hlc', '_key_expr'))]
    specs.extend((f'idx_{table}_{dim}', (dim,)) for dim in dimensions if dim not in served_by_pk)
    return tuple(specs)


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


def _snake(name: str) -> str:
    """Snake-case a class name (``LocationStore`` -> ``location_store``)."""
    return ''.join(f'_{c.lower()}' if c.isupper() else c for c in name).lstrip('_')


def table_name(cls: type) -> str:
    """Return the history table name for ``cls`` (``'stream_<snake>'``)."""
    return f'stream_{_snake(cls.__name__)}'


def latest_table_name(cls: type) -> str:
    """Return the latest-projection table name for ``cls`` (``'latest_<snake>'``)."""
    return f'latest_{_snake(cls.__name__)}'


__all__ = [
    'COMPLEX_FIELDS',
    'EVENT_AT',
    'ISSUED_AT',
    'META_COLUMNS',
    'PRIMARY_KEY',
    'SCALAR_TYPES',
    'TIME_FIELD_KINDS',
    'column_type',
    'derive_columns',
    'json_index_specs',
    'latest_table_name',
    'table_name',
    'wire_path',
]

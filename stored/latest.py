"""The latest projection's upsert: newest-wins rows, and optionally each entity's run.

Both backends maintain the projection with one ``INSERT … ON CONFLICT DO UPDATE`` per row,
so the statement is built here once and spelled per dialect, rather than twice by hand.

**A run** is an entity's unbroken sequence of records, where a silence longer than the
stream's ``latest_run_gap`` ends one and the next record starts another. The projection can
keep when the current run began (:data:`~stored.schema.RUN_START`) at no extra write cost: it
is one more column in the same statement, computed against the row it replaces.

- **A newer record** keeps the run start, or resets it to its own time when it arrives more
  than the gap after the stored newest record.
- **An older record** — redelivered, or a late batch — can only *extend* the run backwards:
  it lowers the run start when it lands within the gap of it, and changes nothing otherwise.
  It cannot reset a run, because it says nothing about the silence since.
- **A row recorded before the column existed** reads ``NULL``: the run began before anyone
  was counting. It stays ``NULL`` until a gap is crossed and a run starts that is known.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from . import schema

if TYPE_CHECKING:
    import datetime
    from collections.abc import Sequence

    from .dialect import Dialect


@dataclass(frozen=True, slots=True)
class LatestMeta:
    """What the latest projection knows about an entity beyond its newest record.

    Attributes:
        run_start: When the entity's current run began (naive UTC), or ``None`` when the
            stream keeps no run, or the run began before the projection was counting.
    """

    run_start: datetime.datetime | None = None


def meta_of(columns: dict[str, Any]) -> LatestMeta:
    """The :class:`LatestMeta` carried by one latest-projection row (``SELECT *`` columns)."""
    return LatestMeta(run_start=columns.get(schema.RUN_START))


def upsert_sql(
    table: str,
    cols: Sequence[str],
    key_columns: Sequence[str],
    compare_column: str,
    *,
    dialect: Dialect,
    run_gap_s: float | None = None,
) -> str:
    """The per-row upsert that maintains ``table``, newest-wins on ``compare_column``.

    Without a run, the statement only updates when the incoming record is at least as new
    (a ``WHERE`` on the conflict update). With one, the update must also run for an older
    record — which may extend the run — so each column takes its new value only when the
    record is newer, and the run start is computed beside them.

    Args:
        table: The latest-projection table.
        cols: The row's columns, in parameter order (the history row's columns).
        key_columns: The entity key — the table's primary key.
        compare_column: The temporal column compared for newest-wins.
        dialect: How this engine spells timestamp arithmetic.
        run_gap_s: The run gap in seconds, or ``None`` for no run.

    Returns:
        A parameterized statement; :func:`upsert_params` gives its parameters.
    """
    t = f'"{table}"'
    cmp = f'"{compare_column}"'
    names = [*cols, schema.RUN_START] if run_gap_s is not None else list(cols)
    col_list = ', '.join(f'"{col}"' for col in names)
    placeholders = ', '.join('?' for _ in names)
    conflict = ', '.join(f'"{col}"' for col in key_columns)
    head = f'INSERT INTO {t} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO UPDATE SET '  # noqa: S608 (quoted identifiers)
    updated = [col for col in cols if col not in key_columns]
    if run_gap_s is None:
        assignments = ', '.join(f'"{col}" = excluded."{col}"' for col in updated)
        return f'{head}{assignments} WHERE excluded.{cmp} >= {t}.{cmp}'

    newer = f'excluded.{cmp} >= {t}.{cmp}'
    run = f'"{schema.RUN_START}"'
    gap = float(run_gap_s)
    silence = dialect.seconds_between(f'excluded.{cmp}', f'{t}.{cmp}')
    reach = dialect.seconds_between(f'{t}.{run}', f'excluded.{cmp}')
    run_start = (
        f'{run} = CASE '
        f'WHEN {newer} THEN (CASE WHEN {silence} > {gap!r} THEN excluded.{cmp} ELSE {t}.{run} END) '
        f'WHEN {t}.{run} IS NOT NULL AND excluded.{cmp} < {t}.{run} AND {reach} <= {gap!r} THEN excluded.{cmp} '
        f'ELSE {t}.{run} END'
    )
    assignments = ', '.join(
        [f'"{col}" = CASE WHEN {newer} THEN excluded."{col}" ELSE {t}."{col}" END' for col in updated] + [run_start],
    )
    return f'{head}{assignments}'


def upsert_params(
    row: dict[str, Any],
    cols: Sequence[str],
    compare_column: str,
    *,
    run_gap_s: float | None = None,
) -> list[Any]:
    """One row's parameters for :func:`upsert_sql` — a new entity's run starts at its record."""
    values = [row.get(col) for col in cols]
    if run_gap_s is not None:
        values.append(row.get(compare_column))
    return values


__all__ = ['LatestMeta', 'meta_of', 'upsert_params', 'upsert_sql']

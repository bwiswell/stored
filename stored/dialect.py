"""How each backend spells the parts of a query that are not standard SQL.

The query planner is otherwise engine-neutral: quoted identifiers, ``?`` parameters,
``WHERE``/``ORDER BY``/``LIMIT``. Two things are not portable, and this module is
where they live rather than being spelled inline and rediscovered per backend:

- **Wildcard key matching.** SQLite and DuckDB both have ``GLOB``; Postgres (the
  roadmap backend) has no such operator and would need ``LIKE`` over a translated
  pattern. Returning the whole fragment — not just an operator name — leaves room
  for that.
- **Reaching into ``_payload``.** SQLite's ``json_extract`` compares cleanly against
  a bound value of any scalar type. DuckDB's returns JSON, so a *text* comparison
  needs ``json_extract_string`` while numbers work either way.

A dialect renders **fragments**, never whole statements: the planner still decides
what to filter and in what order.
"""
from __future__ import annotations


def quote_path(path: str) -> str:
    """Render a JSON path as a SQL string literal (``zones.department`` → ``'$.zones.department'``).

    Embedded rather than bound, because the same expression has to serve an index
    DDL, where a parameter is not allowed. Callers pass paths that registration has
    already validated; the quote-doubling here is belt, not the braces.

    Args:
        path: A dotted path relative to the payload root.

    Returns:
        The quoted ``$.`` path literal.
    """
    return "'$." + path.replace("'", "''") + "'"


class Dialect:
    """SQL fragments in the spelling SQLite understands — the baseline.

    Attributes:
        name: The dialect's name, for logs and errors.
    """

    name = 'sqlite'

    def key_match(self, column: str, *, wildcard: bool) -> str:
        """A topic-key predicate with one bound parameter.

        Args:
            column: The quoted-in column name to match on.
            wildcard: Whether the pattern contains ``*`` and should glob.

        Returns:
            A SQL fragment ending in a ``?`` placeholder.
        """
        operator = 'GLOB' if wildcard else '='
        return f'"{column}" {operator} ?'

    def json_value(self, column: str, path: str, *, text: bool) -> str:
        """An expression extracting ``path`` from a JSON ``column``.

        Args:
            column: The column holding the JSON document (``_payload``).
            path: A dotted path relative to the document root.
            text: Whether the value being compared is a string. SQLite does not
                care; DuckDB does.

        Returns:
            A SQL expression comparable against a bound value.
        """
        return f'json_extract("{column}", {quote_path(path)})'


class DuckDBDialect(Dialect):
    """DuckDB's spelling: the same, except that JSON text needs its own extractor."""

    name = 'duckdb'

    def json_value(self, column: str, path: str, *, text: bool) -> str:
        """As :meth:`Dialect.json_value`, but ``json_extract`` returns JSON here.

        A string therefore compares as ``"abc"`` — quotes included — unless it is
        pulled out with ``json_extract_string``. Numbers compare correctly either way.
        """
        function = 'json_extract_string' if text else 'json_extract'
        return f'{function}("{column}", {quote_path(path)})'


#: The baseline dialect, used when no backend supplies one.
DEFAULT_DIALECT = Dialect()

__all__ = ['DEFAULT_DIALECT', 'Dialect', 'DuckDBDialect', 'quote_path']

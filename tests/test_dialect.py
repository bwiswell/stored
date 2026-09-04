"""The dialect seam: the fragments that are not portable between engines."""

import pytest

from stored.dialect import DEFAULT_DIALECT, Dialect, DuckDBDialect, quote_path


def test_quote_path_renders_a_json_path_literal():
    assert quote_path('zones.department') == "'$.zones.department'"
    assert quote_path('zones') == "'$.zones'"


def test_quote_path_doubles_embedded_quotes():
    # The path is embedded, not bound (an index DDL cannot take a parameter), so the
    # escaping has to hold even though registration validates paths.
    assert quote_path("zones.dep't") == "'$.zones.dep''t'"


def test_key_match_globs_only_for_a_wildcard():
    assert DEFAULT_DIALECT.key_match('_key_expr', wildcard=False) == '"_key_expr" = ?'
    assert DEFAULT_DIALECT.key_match('_key_expr', wildcard=True) == '"_key_expr" GLOB ?'


def test_sqlite_extracts_json_the_same_way_whatever_the_type():
    """SQLite's json_extract compares cleanly against any bound scalar."""
    numeric = DEFAULT_DIALECT.json_value('_payload', 'zones.department', text=False)
    textual = DEFAULT_DIALECT.json_value('_payload', 'tag.k', text=True)
    assert numeric == 'json_extract("_payload", \'$.zones.department\')'
    assert textual == 'json_extract("_payload", \'$.tag.k\')'


def test_duckdb_needs_its_own_extractor_for_text():
    """DuckDB's json_extract yields JSON, so 'abc' would compare as \"abc\"."""
    duck = DuckDBDialect()
    assert duck.json_value('_payload', 'zones.department', text=False) == (
        'json_extract("_payload", \'$.zones.department\')'
    )
    assert duck.json_value('_payload', 'tag.k', text=True) == (
        'json_extract_string("_payload", \'$.tag.k\')'
    )


def test_duckdb_dialect_is_the_baseline_elsewhere():
    duck = DuckDBDialect()
    assert duck.key_match('_key_expr', wildcard=True) == Dialect().key_match('_key_expr', wildcard=True)
    assert (duck.name, Dialect().name) == ('duckdb', 'sqlite')


@pytest.mark.parametrize('dialect', [Dialect(), DuckDBDialect()])
def test_every_dialect_answers_the_whole_surface(dialect):
    """A new backend implements these two, or the planner cannot spell its queries."""
    assert dialect.key_match('c', wildcard=False)
    assert dialect.json_value('_payload', 'a.b', text=False)

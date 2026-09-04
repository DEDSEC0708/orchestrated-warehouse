"""The ``:name`` to ``%(name)s`` translator.

This module is small, entirely mechanical, and would be silently wrong in six
different ways with a naive regular expression - which is exactly the profile
of code that deserves a dedicated test file. Each case below corresponds to a
construct that appears in the project's real SQL.
"""

from __future__ import annotations

import pytest

from volthive.db.params import extract_param_names, translate_named_params
from volthive.db.sqlfiles import split_statements

pytestmark = pytest.mark.unit


class TestPlaceholderTranslation:
    def test_simple_placeholder_is_translated(self) -> None:
        assert translate_named_params("SELECT :run_id") == "SELECT %(run_id)s"

    def test_cast_operator_is_not_a_placeholder(self) -> None:
        """``value::TEXT`` is two colons and a type, not a parameter."""
        assert translate_named_params("SELECT x::TEXT") == "SELECT x::TEXT"

    def test_time_literal_is_not_a_placeholder(self) -> None:
        """The case a naive regex always gets wrong: '14:30' is a time."""
        assert translate_named_params("SELECT '14:30'::TIME") == "SELECT '14:30'::TIME"

    def test_colon_inside_a_string_literal_is_left_alone(self) -> None:
        assert translate_named_params("SELECT ':not_a_param', :real") == (
            "SELECT ':not_a_param', %(real)s"
        )

    def test_escaped_quote_inside_a_literal_does_not_end_it(self) -> None:
        assert translate_named_params("SELECT 'it''s :x', :y") == ("SELECT 'it''s :x', %(y)s")

    def test_line_comment_is_left_alone(self) -> None:
        assert translate_named_params("-- :note\nSELECT :a") == "-- :note\nSELECT %(a)s"

    def test_block_comment_is_left_alone(self) -> None:
        assert translate_named_params("/* :note */ SELECT :a") == ("/* :note */ SELECT %(a)s")

    def test_dollar_quoted_body_is_left_alone(self) -> None:
        """A plpgsql body can contain anything, including colons and percents."""
        source = "DO $$ BEGIN RAISE NOTICE ':x'; END $$"
        assert translate_named_params(source) == "DO $$ BEGIN RAISE NOTICE ':x'; END $$"

    def test_double_quoted_identifier_is_left_alone(self) -> None:
        assert translate_named_params('SELECT "col:with:colons", :a') == (
            'SELECT "col:with:colons", %(a)s'
        )


class TestPercentEscaping:
    """psycopg treats ``%`` as an escape whenever parameters are supplied.

    Every one of these would raise an unhelpful error from deep inside the
    driver if the percent were not doubled - and the first two are constructs
    that appear in this project's own SQL.
    """

    def test_modulo_operator_is_escaped(self) -> None:
        assert translate_named_params("SELECT 50 % 3, :n") == "SELECT 50 %% 3, %(n)s"

    def test_percent_inside_a_like_pattern_is_escaped(self) -> None:
        assert translate_named_params("WHERE s LIKE '100%'") == "WHERE s LIKE '100%%'"

    def test_percent_inside_a_comment_is_escaped(self) -> None:
        """Comments reach the driver too, and a stray % in one breaks binding."""
        assert translate_named_params("-- 3% of rows\nSELECT 1") == ("-- 3%% of rows\nSELECT 1")

    def test_format_specifier_inside_a_dollar_block_is_escaped(self) -> None:
        source = "DO $$ BEGIN EXECUTE format('%I', x); END $$"
        assert "%%I" in translate_named_params(source)


class TestParamExtraction:
    def test_names_are_deduplicated(self) -> None:
        assert extract_param_names("SELECT :a, :b, :a") == {"a", "b"}

    def test_names_inside_literals_are_not_extracted(self) -> None:
        assert extract_param_names("SELECT ':a', :b") == {"b"}

    def test_no_placeholders_yields_an_empty_set(self) -> None:
        assert extract_param_names("SELECT 1") == set()


class TestStatementSplitting:
    def test_splits_on_top_level_semicolons(self) -> None:
        assert split_statements("SELECT 1; SELECT 2") == ["SELECT 1", "SELECT 2"]

    def test_semicolon_inside_a_dollar_block_is_not_a_separator(self) -> None:
        """A plpgsql body is full of semicolons and is ONE statement."""
        source = "DO $$ BEGIN a; b; END $$; SELECT 1"
        assert split_statements(source) == ["DO $$ BEGIN a; b; END $$", "SELECT 1"]

    def test_semicolon_inside_a_literal_is_not_a_separator(self) -> None:
        assert split_statements("SELECT ';'") == ["SELECT ';'"]

    def test_trailing_semicolon_does_not_produce_an_empty_statement(self) -> None:
        assert split_statements("SELECT 1;\n\n") == ["SELECT 1"]

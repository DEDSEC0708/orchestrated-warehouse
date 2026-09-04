"""Translate ``:name`` placeholders into psycopg's ``%(name)s`` form.

**Why this module exists at all.** Every set-based statement in this project
lives in a ``.sql`` file rather than in a Python string, so that it can be
linted by sqlfluff, reviewed in a diff, and pasted straight into ``psql`` when
something needs debugging at 2 a.m. That only works if the placeholder syntax
in those files is the readable ``:run_id`` form. psycopg, however, binds
parameters as ``%(run_id)s``.

Rather than give up one of those properties, the translation happens here,
once, in about eighty lines - and it is the kind of code that is *obviously*
correct only until you look closely, which is why it is a separate module with
its own tests rather than a regular expression buried in a helper.

**What makes a naive regex wrong.** ``re.sub(r':(\\w+)', ...)`` corrupts all of
these, every one of which appears in real SQL:

===============================  ==========================================
 ``'14:30'``                      a time literal - not a parameter
 ``value::TEXT``                  a cast - two colons, not a placeholder
 ``-- see :run_id below``         a comment
 ``/* :note */``                  a block comment
 ``$$ ... :x ... $$``             a dollar-quoted plpgsql body
 ``jsonb_col ?: 'key'``           an operator containing a colon
===============================  ==========================================

So this is a small hand-written scanner instead. It walks the statement once,
tracking which lexical context it is in, and only substitutes in ordinary SQL
text.

**Literal percent signs** are doubled at the same time. psycopg's client-side
binding treats ``%`` as an escape, so a ``LIKE '100%'`` in a parameterised
statement raises an unhelpful ``IndexError`` deep inside the driver. Escaping
here means the SQL files stay written the way a database engineer expects.

Parameters are always BOUND, never interpolated. There is no code path in this
project that formats a value into SQL text, which is why SQL injection is not
a risk here even though the statements are assembled from files.
"""

from __future__ import annotations

import re

__all__ = ["extract_param_names", "translate_named_params"]

#: A parameter name: a colon followed by a Python-ish identifier.
_PARAM_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def translate_named_params(sql: str) -> str:
    """Rewrite ``:name`` placeholders as ``%(name)s`` and escape bare ``%``.

    Substitution is skipped inside string literals, dollar-quoted blocks, line
    comments and block comments, and ``::`` casts are left alone.

    Args:
        sql: A SQL statement using ``:name`` placeholders.

    Returns:
        The same statement in psycopg's ``pyformat`` parameter style.
    """
    out: list[str] = []
    i = 0
    n = len(sql)

    def verbatim(text: str) -> str:
        """Copy a region unchanged EXCEPT for doubling any percent sign.

        Placeholder substitution must not happen inside a literal or a comment.
        Percent escaping must happen EVERYWHERE, because psycopg's binder scans
        the whole statement and does not care that a stray ``%`` was inside a
        comment or a ``LIKE '100%'`` pattern. It unescapes ``%%`` back to ``%``
        before sending, so the statement the server sees is unchanged.
        """
        return text.replace("%", "%%")

    while i < n:
        char = sql[i]

        # --- line comment: -- ... end of line ---------------------------
        if char == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end == -1 else end
            out.append(verbatim(sql[i:end]))
            i = end
            continue

        # --- block comment: /* ... */ (Postgres nests these) -------------
        if char == "/" and sql.startswith("/*", i):
            depth = 1
            j = i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth += 1
                    j += 2
                elif sql.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            out.append(verbatim(sql[i:j]))
            i = j
            continue

        # --- single-quoted literal, with '' as the escape ----------------
        if char == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            out.append(verbatim(sql[i:j]))
            i = j
            continue

        # --- double-quoted identifier ------------------------------------
        if char == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            out.append(verbatim(sql[i:j]))
            i = j
            continue

        # --- dollar-quoted block: $$ ... $$ or $tag$ ... $tag$ ------------
        if char == "$":
            tag_match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[i:])
            if tag_match:
                tag = tag_match.group(0)
                end = sql.find(tag, i + len(tag))
                end = n if end == -1 else end + len(tag)
                out.append(verbatim(sql[i:end]))
                i = end
                continue

        # --- cast operator :: --------------------------------------------
        if char == ":" and sql.startswith("::", i):
            out.append("::")
            i += 2
            continue

        # --- the placeholder itself ---------------------------------------
        if char == ":":
            name_match = _PARAM_NAME.match(sql, i + 1)
            if name_match:
                out.append(f"%({name_match.group(0)})s")
                i = name_match.end()
                continue

        # --- a bare percent must be doubled for psycopg's binder ----------
        if char == "%":
            out.append("%%")
            i += 1
            continue

        out.append(char)
        i += 1

    return "".join(out)


def extract_param_names(sql: str) -> set[str]:
    """Return the set of ``:name`` placeholders a statement uses.

    Used by :func:`volthive.db.sqlfiles.run_sql` to fail with a message naming
    the missing parameter, rather than letting psycopg raise a bare
    ``KeyError`` from inside its binder with no indication of which file or
    which placeholder was at fault.
    """
    translated = translate_named_params(sql)
    return set(re.findall(r"%\(([A-Za-z_][A-Za-z0-9_]*)\)s", translated))

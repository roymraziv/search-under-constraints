# bench/db.py
# Database primitives for the benchmark runner.
#
# CONTRACT:
# - Small surface area
# - No knowledge of variants/queries/metrics
# - Safe parameterization (no string interpolation for params)
# - Helpers to execute .sql files and fetch simple scalar values
#
# psycopg3 is assumed (import psycopg).

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, Union

import psycopg


# ----------------------------
# Connection management
# ----------------------------

def connect(database_url: str) -> psycopg.Connection:
    """
    Open a psycopg3 connection.

    Why this wrapper exists:
    - central place to set connection options later if needed
    - keeps imports consistent across modules
    - gives you one point to add tracing/logging

    Note:
    - psycopg connections are context managers:
        with connect(url) as conn:
            ...
      which commits on success and rolls back on exception.
    """
    # autocommit False is default; we keep it that way for transactional safety.
    return psycopg.connect(database_url)


# ----------------------------
# SQL execution helpers
# ----------------------------

def exec_sql(conn: psycopg.Connection, sql: str, params: Optional[Sequence[Any]] = None) -> None:
    """
    Execute a SQL statement (no results returned).

    Rules:
    - Use params for values. Never f-string values into sql.
    - Use this for simple commands like SET/ANALYZE and small DDL statements.

    params:
      - list/tuple of positional parameters that psycopg will bind safely
    """
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)


def exec_file(conn: psycopg.Connection, path: Path, params: Optional[Mapping[str, Any]] = None) -> None:
    """
    Execute a .sql file.

    This is used for:
    - schema reset files: sql/schema/00_schema.sql
    - variant up/down files: sql/variants/**/up.sql, down.sql
    - optionally query files if you want (but measurement likely uses EXPLAIN wrapper)

    params:
    - Optional named parameters for psycopg (e.g., %(name)s style).
    - Keep usage minimal; most of your SQL files should be fixed shape.
    """
    sql = path.read_text(encoding="utf-8")
    sql = _strip_bom(sql)

    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)


# ----------------------------
# Fetch helpers (read operations)
# ----------------------------

def fetch_one(conn: psycopg.Connection, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Tuple[Any, ...]]:
    """
    Execute a query and return a single row (tuple), or None if no row.

    Use when you need multiple columns.
    """
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur.fetchone()


def fetch_value(conn: psycopg.Connection, sql: str, params: Optional[Sequence[Any]] = None) -> Any:
    """
    Execute a query and return a single scalar value.

    Example:
      exists = fetch_value(conn, "SELECT to_regclass('public.products') IS NOT NULL;")
    """
    row = fetch_one(conn, sql, params)
    if row is None:
        raise RuntimeError("Expected a row but query returned none")
    if len(row) != 1:
        raise RuntimeError(f"Expected 1 column but query returned {len(row)} columns")
    return row[0]


def fetch_all(conn: psycopg.Connection, sql: str, params: Optional[Sequence[Any]] = None) -> list[Tuple[Any, ...]]:
    """
    Execute a query and return all rows.
    """
    with conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur.fetchall()


# ----------------------------
# Utility helpers
# ----------------------------

def _strip_bom(s: str) -> str:
    """
    Remove UTF-8 BOM if a SQL file accidentally includes it.

    Why:
    - Some editors can save files with BOM.
    - Postgres may choke on the BOM at the beginning of the SQL.
    """
    # BOM char is '\ufeff'
    if s and s[0] == "\ufeff":
        return s[1:]
    return s

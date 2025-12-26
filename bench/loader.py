# bench/loader.py
# Loads deterministic dataset into Postgres using COPY.
#
# CONTRACT:
# - No schema creation (schema is handled by sql/schema/00_schema.sql)
# - No variant work (variants are handled by sql/variants/*)
# - Uses generator.generate_products(cfg) as the only data source
# - Uses COPY for speed and consistency
# - Must be repeatable and must verify that expected row count was loaded

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import StringIO
import time
from typing import Iterable

import psycopg  # psycopg3

from bench.config import SeedConfig
from bench.generator import ProductRow, generate_products


# ----------------------------
# Types returned to orchestrator
# ----------------------------

@dataclass(frozen=True)
class LoadStats:
    # How many rows we inserted.
    rows: int
    # How long the COPY took (seconds, wall clock).
    seconds: float


# ----------------------------
# Public API
# ----------------------------

def copy_products(conn: psycopg.Connection, cfg: SeedConfig) -> LoadStats:
    """
    Load the canonical products dataset via COPY.

    This function assumes:
      - `products` table already exists
      - table is empty (or has been recreated) before this is called

    Steps:
      1) stream rows from generator
      2) write them in CSV format to COPY STDIN
      3) verify row count matches cfg.rows
      4) return timing stats
    """

    # Build the COPY command with an explicit column list.
    # Explicit columns prevent accidental breakage if table order changes.
    copy_sql = """
        COPY products (id, name, brand, category, description, created_at, updated_at)
        FROM STDIN WITH (FORMAT csv)
    """

    # Start timing as close as possible to the actual COPY operation.
    start = time.perf_counter()

    # psycopg3 COPY API:
    # `conn.cursor().copy(copy_sql)` returns a context manager that accepts `.write(str_or_bytes)`.
    #
    # We will stream row-by-row and write one CSV line per row.
    #
    # NOTE: This is streaming: we do NOT hold all rows in memory.
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            # Iterate deterministic rows in stable order.
            for row in generate_products(cfg):
                # Convert one row to one CSV line and write it to the COPY stream.
                # We use a dedicated CSV encoder to avoid broken quoting/escaping.
                copy.write(_row_to_csv_line(row))

    # Stop timing after COPY context is closed (flushes and finalizes COPY on server).
    seconds = time.perf_counter() - start

    # Safety check: confirm we loaded exactly cfg.rows rows.
    # If this fails, your benchmark results are invalid.
    actual = _count_products(conn)
    expected = cfg.rows
    if actual != expected:
        raise RuntimeError(f"COPY row count mismatch: expected={expected} actual={actual}")

    # Return measured timing stats.
    return LoadStats(rows=actual, seconds=seconds)


# ----------------------------
# Private helpers
# ----------------------------

def _count_products(conn: psycopg.Connection) -> int:
    """
    Count rows in products to validate COPY.

    Keep this in loader.py because it is part of correctness for ingestion.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM products;")
        val = cur.fetchone()
        if val is None:
            raise RuntimeError("COUNT(*) returned no row (unexpected)")
        return int(val[0])


def _row_to_csv_line(row: ProductRow) -> str:
    """
    Convert a ProductRow into a single CSV line for COPY.

    Why we do this manually:
      - COPY expects correctly escaped CSV.
      - String concatenation is error-prone for quotes/commas/newlines.
      - Python's csv module handles escaping correctly.

    Performance note:
      - Creating a csv.writer per row would be slow.
      - Instead we reuse a StringIO and csv.writer via a tiny helper object.
    """
    return _csv_encoder().encode_row(row)


# We implement a tiny CSV encoder that reuses its buffers.
# This avoids allocating a new csv.writer + StringIO per row.
# That matters at 1M rows.

import csv  # standard library (placed here to keep the top imports focused)


class _CsvEncoder:
    """
    Reusable CSV encoder for ProductRow.

    - Uses StringIO as an in-memory buffer
    - Uses csv.writer to escape fields correctly
    - Writes one row and returns the resulting line (including newline)
    """

    __slots__ = ("_buf", "_writer")

    def __init__(self) -> None:
        self._buf = StringIO()
        # csv.writer handles commas, quotes, and embedded newlines safely.
        # lineterminator ensures each row is one line ended with '\n' (COPY likes that).
        self._writer = csv.writer(self._buf, lineterminator="\n")

    def encode_row(self, row: ProductRow) -> str:
        # Clear the buffer from the previous row.
        self._buf.seek(0) # sets the file pointer to the beginning of the buffer
        self._buf.truncate(0) # Truncates the buffer to 0 bytes i.e. clears it

        # Ensure datetimes are serialized consistently.
        # We rely on Python's ISO format with timezone included (created_at is tz-aware).
        created = _dt_to_text(row.created_at)
        updated = _dt_to_text(row.updated_at)

        # Write fields in EXACT COPY column order.
        self._writer.writerow(
            [
                row.id,
                row.name,
                row.brand,
                row.category,
                row.description,
                created,
                updated,
            ]
        )

        # Return the buffer content (one CSV line).
        return self._buf.getvalue()


# We keep a single encoder instance per process to reduce overhead.
# This is safe because our generator is single-threaded (per your constraints).
_ENCODER: _CsvEncoder | None = None


def _csv_encoder() -> _CsvEncoder:
    global _ENCODER
    if _ENCODER is None:
        _ENCODER = _CsvEncoder()
    return _ENCODER


def _dt_to_text(dt: datetime) -> str:
    """
    Convert datetime to a stable text format for COPY.

    Important:
      - dt should be tz-aware UTC (enforced by config parsing + generator)
      - isoformat() yields e.g. '2025-01-01T00:00:00+00:00'
      - Postgres parses this reliably as timestamptz
    """
    if dt.tzinfo is None:
        # This should never happen if generator/config are correct.
        raise ValueError("datetime must be timezone-aware (UTC)")
    return dt.isoformat()

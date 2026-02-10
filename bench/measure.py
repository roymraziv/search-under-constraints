# bench/measure.py
# Query execution with timing and EXPLAIN capture.
#
# CONTRACT:
# - Execute queries with parameter binding
# - Capture execution timing
# - Capture EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) plans
# - Return structured Measurement objects

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import psycopg

from bench.db import _strip_bom
from bench.queries import QueryScenario


# ----------------------------
# Data structures
# ----------------------------

@dataclass(frozen=True)
class QueryResult:
    """Result of executing a query (without EXPLAIN)."""
    execution_time_seconds: float
    rows_returned: int


@dataclass(frozen=True)
class Measurement:
    """Complete measurement including timing, plan, and buffer stats."""
    execution_time_ms: float
    planning_time_ms: float
    plan: dict
    rows_returned: int
    buffers: dict


# ----------------------------
# Query execution with timing
# ----------------------------

def execute_query_with_timing(
    conn: psycopg.Connection,
    sql_path: Path,
    params: Mapping[str, Any],
) -> QueryResult:
    """
    Execute a query and measure execution time.

    This is a simple timing wrapper that doesn't capture EXPLAIN.
    Use measure_query() for full measurement including plans.
    """
    sql = sql_path.read_text(encoding="utf-8")
    sql = _strip_bom(sql)

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    execution_time = time.perf_counter() - start

    return QueryResult(
        execution_time_seconds=execution_time,
        rows_returned=len(rows),
    )


# ----------------------------
# EXPLAIN capture
# ----------------------------

def capture_explain(
    conn: psycopg.Connection,
    sql_path: Path,
    params: Mapping[str, Any],
) -> dict:
    """
    Execute EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and return the plan.

    Returns the full JSON plan structure from PostgreSQL.
    """
    sql = sql_path.read_text(encoding="utf-8")
    sql = _strip_bom(sql)

    # Wrap the query with EXPLAIN
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"

    with conn.cursor() as cur:
        cur.execute(explain_sql, params)
        result = cur.fetchone()

    if result is None:
        raise RuntimeError("EXPLAIN returned no result")

    # TODO: Verify return type is consistent and remove extraneous logic
    
    # psycopg3 may return JSON as:
    # - A string (needs parsing)
    # - A dict (already parsed)
    # - A list (already parsed, PostgreSQL wraps plan in a list)
    raw_result = result[0]
    
    # Handle different return types from psycopg3
    if isinstance(raw_result, str):
        # JSON string, need to parse
        plan_json = json.loads(raw_result)
    elif isinstance(raw_result, list):
        # Already a list (psycopg3 parsed it)
        plan_json = raw_result
    elif isinstance(raw_result, dict):
        # Already a dict (psycopg3 parsed it)
        plan_json = raw_result
    else:
        raise RuntimeError(f"Unexpected EXPLAIN result type: {type(raw_result)}")

    # PostgreSQL EXPLAIN (FORMAT JSON) returns a list with one element (the plan)
    # Extract the plan dict from the list
    if isinstance(plan_json, list) and len(plan_json) > 0:
        return plan_json[0]
    elif isinstance(plan_json, dict):
        # Already the plan dict
        return plan_json
    else:
        raise RuntimeError(f"Unexpected EXPLAIN JSON structure: {type(plan_json)}")


# ----------------------------
# Combined measurement
# ----------------------------

def measure_query(
    conn: psycopg.Connection,
    scenario: QueryScenario,
    params: Mapping[str, Any],
) -> Measurement:
    """
    Execute a query with full measurement: timing + EXPLAIN plan.

    This is the main function to use for benchmarking.
    It captures both execution timing and the full EXPLAIN plan.
    """
    # Capture EXPLAIN plan (includes ANALYZE timing)
    plan = capture_explain(conn, scenario.sql_path, params)

    # Extract timing from plan
    # PostgreSQL EXPLAIN JSON structure:
    # {
    #   "Plan": { ... },
    #   "Planning Time": <ms>,
    #   "Execution Time": <ms>
    # }
    planning_time_ms = plan.get("Planning Time", 0.0)
    execution_time_ms = plan.get("Execution Time", 0.0)

    # Extract buffer stats from plan tree
    buffers = _extract_buffer_stats(plan)

    # Extract rows returned from plan
    rows_returned = _extract_rows_returned(plan)

    return Measurement(
        execution_time_ms=execution_time_ms,
        planning_time_ms=planning_time_ms,
        plan=plan,
        rows_returned=rows_returned,
        buffers=buffers,
    )


# ----------------------------
# Plan parsing helpers
# ----------------------------

def _extract_buffer_stats(plan: dict) -> dict:
    """
    Extract buffer statistics from the plan tree.

    Aggregates buffer stats across all plan nodes.
    """
    stats = {
        "shared_hit": 0,
        "shared_read": 0,
        "shared_dirtied": 0,
        "shared_written": 0,
        "temp_read": 0,
        "temp_written": 0,
        "local_hit": 0,
        "local_read": 0,
        "local_written": 0,
    }

    def _walk_plan(node: dict) -> None:
        """Recursively walk plan tree and aggregate buffer stats."""
        # Buffer stats are stored directly on the node with capitalized keys
        stats["shared_hit"] += node.get("Shared Hit Blocks", 0)
        stats["shared_read"] += node.get("Shared Read Blocks", 0)
        stats["shared_dirtied"] += node.get("Shared Dirtied Blocks", 0)
        stats["shared_written"] += node.get("Shared Written Blocks", 0)
        stats["temp_read"] += node.get("Temp Read Blocks", 0)
        stats["temp_written"] += node.get("Temp Written Blocks", 0)
        stats["local_hit"] += node.get("Local Hit Blocks", 0)
        stats["local_read"] += node.get("Local Read Blocks", 0)
        stats["local_written"] += node.get("Local Written Blocks", 0)

        # Recurse into child plans
        if "Plans" in node:
            for child in node["Plans"]:
                _walk_plan(child)

    # Start from the root Plan node
    if "Plan" in plan:
        _walk_plan(plan["Plan"])

    return stats


def _extract_rows_returned(plan: dict) -> int:
    """
    Extract the number of rows returned from the plan.

    This is typically in the root Plan node's "Actual Rows" field.
    """
    if "Plan" in plan:
        root_plan = plan["Plan"]
        # Actual Rows is the number of rows returned by the query
        return root_plan.get("Actual Rows", 0)
    return 0


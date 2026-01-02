# bench/artifacts.py
# Export benchmark results to JSON and CSV formats.
#
# CONTRACT:
# - Create timestamped result directories
# - Write metadata, summaries, raw results, and EXPLAIN plans
# - Use consistent, readable formats (JSON with indent, CSV with headers)

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from bench.analyze import Summary
from bench.config import BenchConfig, SeedConfig
from bench.measure import Measurement


# ----------------------------
# Result directory creation
# ----------------------------

def create_result_directory(base_path: Path) -> Path:
    """
    Create a timestamped result directory.

    Format: results/YYYY-MM-DD_HHMM/
    Returns the Path to the created directory.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    result_dir = base_path / timestamp
    result_dir.mkdir(parents=True, exist_ok=True)

    # Create plans subdirectory structure (will be created per variant)
    # We don't create it here, but in write_plan_file()

    return result_dir


# ----------------------------
# Metadata export
# ----------------------------

def write_metadata(
    result_dir: Path,
    seed_cfg: SeedConfig,
    bench_cfg: BenchConfig,
    variant: str,
    queries: List[str],
) -> None:
    """
    Write metadata.json with configuration snapshot.

    Includes timestamp, seed config, bench config, variant, and query list.
    """
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "seed_config": {
            "seed": seed_cfg.seed,
            "rows": seed_cfg.rows,
        },
        "bench_config": {
            "runs": bench_cfg.runs,
            "warmup": bench_cfg.warmup,
            "variants": list(bench_cfg.variants),
            "queries": list(bench_cfg.queries),
        },
        "variant": variant,
        "queries": queries,
    }

    metadata_path = result_dir / "metadata.json"
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)


# ----------------------------
# Summary CSV export
# ----------------------------

def write_summary_csv(result_dir: Path, summaries: List[Summary]) -> None:
    """
    Write summary.csv with aggregated statistics.

    Columns:
    - variant, query
    - p50_ms, p95_ms, p99_ms, min_ms, max_ms, mean_ms
    - scan_type, index_used, estimated_rows, actual_rows
    - buffer stats (shared_hit, shared_read, etc.)
    """
    csv_path = result_dir / "summary.csv"

    fieldnames = [
        "variant",
        "query",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "min_ms",
        "max_ms",
        "mean_ms",
        "scan_type",
        "index_used",
        "estimated_rows",
        "actual_rows",
        "shared_hit",
        "shared_read",
        "shared_dirtied",
        "shared_written",
        "temp_read",
        "temp_written",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for summary in summaries:
            row = {
                "variant": summary.variant,
                "query": summary.query,
                "p50_ms": summary.percentiles.p50,
                "p95_ms": summary.percentiles.p95,
                "p99_ms": summary.percentiles.p99,
                "min_ms": summary.percentiles.min,
                "max_ms": summary.percentiles.max,
                "mean_ms": summary.percentiles.mean,
                "scan_type": summary.planner_stats.get("scan_type"),
                "index_used": summary.planner_stats.get("index_used"),
                "estimated_rows": summary.planner_stats.get("estimated_rows", 0),
                "actual_rows": summary.planner_stats.get("actual_rows", 0),
                "shared_hit": summary.buffer_stats.get("shared_hit", 0),
                "shared_read": summary.buffer_stats.get("shared_read", 0),
                "shared_dirtied": summary.buffer_stats.get("shared_dirtied", 0),
                "shared_written": summary.buffer_stats.get("shared_written", 0),
                "temp_read": summary.buffer_stats.get("temp_read", 0),
                "temp_written": summary.buffer_stats.get("temp_written", 0),
            }
            writer.writerow(row)


# ----------------------------
# Raw results export
# ----------------------------

def write_raw_results(
    result_dir: Path,
    all_measurements: Dict[str, Dict[str, List[Measurement]]],
) -> None:
    """
    Write raw_results.json with all measurement data.

    Structure:
    {
      "variant_name": {
        "query_name": [
          { measurement data for each run }
        ]
      }
    }
    """
    # Convert measurements to JSON-serializable format
    raw_data: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for variant, queries in all_measurements.items():
        raw_data[variant] = {}
        for query, measurements in queries.items():
            raw_data[variant][query] = [
                {
                    "execution_time_ms": m.execution_time_ms,
                    "planning_time_ms": m.planning_time_ms,
                    "rows_returned": m.rows_returned,
                    "buffers": m.buffers,
                    # Include full plan (can be large)
                    "plan": m.plan,
                }
                for m in measurements
            ]

    raw_path = result_dir / "raw_results.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)


# ----------------------------
# Individual plan export
# ----------------------------

def write_plan_file(
    result_dir: Path,
    variant: str,
    query: str,
    plan: dict,
) -> None:
    """
    Write an individual EXPLAIN plan to plans/<variant>/<query>.json.

    Creates the variant subdirectory if it doesn't exist.
    """
    plans_dir = result_dir / "plans" / variant
    plans_dir.mkdir(parents=True, exist_ok=True)

    plan_path = plans_dir / f"{query}.json"
    with plan_path.open("w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2)


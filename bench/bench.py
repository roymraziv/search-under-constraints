#!/usr/bin/env python3
# bench/bench.py
# CLI entrypoint for the benchmark runner.
#
# Commands:
#   seed - Generate and load dataset
#   run  - Execute benchmark runs

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Dict, List
from dotenv import load_dotenv

from bench.analyze import Summary, summarize_measurements
from bench.artifacts import (
    create_result_directory,
    write_metadata,
    write_plan_file,
    write_raw_results,
    write_summary_csv,
)
from bench.config import BenchConfig, SeedConfig, load_bench_config, load_seed_config
from bench.db import connect, exec_file, exec_sql, fetch_value
from bench.loader import copy_products
from bench.measure import Measurement, execute_query_with_timing, measure_query
from bench.queries import build_query_params, get_query_scenario, list_queries
from bench.variants import apply_variant, get_variant, revert_variant


# ----------------------------
# Configuration helpers
# ----------------------------

load_dotenv()

def get_database_url() -> str:
    """Get database URL from environment variable."""
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable not set. "
            "Set it to: postgresql://user:password@host:port/database"
        )
    return url


def get_config_path(env_var: str, default: Path) -> Path:
    """Get config path from environment or use default."""
    env_path = os.getenv(env_var)
    if env_path:
        return Path(env_path)
    return default


# ----------------------------
# Session configuration
# ----------------------------

def configure_session(conn, bench_cfg: BenchConfig) -> None:
    """Apply session configuration from bench config."""
    session = bench_cfg.session

    # Set timezone
    exec_sql(conn, f"SET timezone = '{session.timezone}'")

    # Set statement timeout (0 means no timeout)
    if session.statement_timeout_ms > 0:
        exec_sql(conn, f"SET statement_timeout = {session.statement_timeout_ms}")

    # Set JIT if specified
    if session.jit is not None:
        exec_sql(conn, f"SET jit = '{session.jit}'")


# ----------------------------
# Seed command
# ----------------------------

def cmd_seed(args: argparse.Namespace) -> None:
    """Generate and load dataset into database."""
    # Load seed config
    seed_config_path = get_config_path("SEED_CONFIG", Path("config/seed.yaml"))
    seed_cfg = load_seed_config(seed_config_path)

    # Override from CLI args
    if args.rows is not None:
        seed_cfg = replace(seed_cfg, rows=args.rows)
    if args.seed is not None:
        seed_cfg = replace(seed_cfg, seed=args.seed)

    print(f"Loading {seed_cfg.rows} rows with seed {seed_cfg.seed}...")

    # Connect to database
    db_url = get_database_url()
    with connect(db_url) as conn:
        # Reset schema
        schema_path = Path("sql/schema/00_schema.sql")
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        print("Resetting schema...")
        exec_file(conn, schema_path)

        # Generate and load data
        print("Generating and loading data...")
        load_stats = copy_products(conn, seed_cfg)

        print(f"✓ Loaded {load_stats.rows} rows in {load_stats.seconds:.2f} seconds")
        print(f"  Rate: {load_stats.rows / load_stats.seconds:.0f} rows/sec")


# ----------------------------
# Run command
# ----------------------------

def cmd_run(args: argparse.Namespace) -> None:
    """Execute benchmark runs."""
    # Load configs
    bench_config_path = get_config_path("BENCH_CONFIG", Path("config/bench.yaml"))
    seed_config_path = get_config_path("SEED_CONFIG", Path("config/seed.yaml"))

    bench_cfg = load_bench_config(bench_config_path)
    seed_cfg = load_seed_config(seed_config_path)

    # Override from CLI args
    if args.variant is not None:
        bench_cfg = replace(bench_cfg, variants=[args.variant])
    if args.runs is not None:
        bench_cfg = replace(bench_cfg, runs=args.runs)

    # Connect to database
    db_url = get_database_url()
    with connect(db_url) as conn:
        # Configure session
        configure_session(conn, bench_cfg)

        # Create result directory
        results_base = Path("results")
        result_dir = create_result_directory(results_base)
        print(f"Results will be written to: {result_dir}")

        # Collect all measurements
        all_measurements: Dict[str, Dict[str, List[Measurement]]] = {}
        all_summaries: List[Summary] = []

        # Run benchmarks for each variant
        for variant_name in bench_cfg.variants:
            print(f"\n{'='*60}")
            print(f"Variant: {variant_name}")
            print(f"{'='*60}")

            try:
                variant = get_variant(variant_name)

                # Apply variant
                print(f"Applying variant {variant_name}...")
                apply_variant(conn, variant)

                # Run ANALYZE
                print("Running ANALYZE...")
                exec_sql(conn, "ANALYZE products")

                # Warmup queries
                if bench_cfg.warmup > 0:
                    print(f"Warming up ({bench_cfg.warmup} runs per query)...")
                    for query_name in bench_cfg.queries:
                        scenario = get_query_scenario(query_name)
                        params = build_query_params(
                            scenario=scenario,
                            seed_cfg=seed_cfg,
                            bench_cfg=bench_cfg,
                        )
                        for _ in range(bench_cfg.warmup):
                            execute_query_with_timing(conn, scenario.sql_path, params)

                # Benchmark queries
                print(f"Running benchmarks ({bench_cfg.runs} runs per query)...")
                variant_measurements: Dict[str, List[Measurement]] = {}

                for query_name in bench_cfg.queries:
                    print(f"  Query: {query_name}")
                    scenario = get_query_scenario(query_name)
                    params = build_query_params(
                        scenario=scenario,
                        seed_cfg=seed_cfg,
                        bench_cfg=bench_cfg,
                    )

                    measurements = []
                    for run_num in range(bench_cfg.runs):
                        measurement = measure_query(conn, scenario, params)
                        measurements.append(measurement)

                        # Write individual plan file (use first run's plan)
                        if run_num == 0:
                            write_plan_file(result_dir, variant_name, query_name, measurement.plan)

                    variant_measurements[query_name] = measurements

                    # Create summary for this variant + query
                    summary = summarize_measurements(measurements, variant_name, query_name)
                    all_summaries.append(summary)

                    print(f"    p50: {summary.percentiles.p50:.2f}ms, "
                          f"p95: {summary.percentiles.p95:.2f}ms, "
                          f"p99: {summary.percentiles.p99:.2f}ms")

                all_measurements[variant_name] = variant_measurements

            except Exception as e:
                print(f"ERROR in variant {variant_name}: {e}")
                raise
            finally:
                # Always revert variant, even on error
                try:
                    print(f"Reverting variant {variant_name}...")
                    revert_variant(conn, variant)
                except Exception as e:
                    print(f"WARNING: Failed to revert variant {variant_name}: {e}")

        # Query actual row count from database (may differ from config file)
        actual_row_count = fetch_value(conn, "SELECT COUNT(*) FROM products")
        seed_cfg = replace(seed_cfg, rows=actual_row_count)

        # Write artifacts
        print(f"\nWriting results to {result_dir}...")

        # Write metadata (use first variant for metadata)
        if bench_cfg.variants:
            write_metadata(result_dir, seed_cfg, bench_cfg, bench_cfg.variants[0], bench_cfg.queries)

        # Write summary CSV
        write_summary_csv(result_dir, all_summaries)

        # Write raw results
        write_raw_results(result_dir, all_measurements)

        print(f"✓ Results written to {result_dir}")


# ----------------------------
# CLI setup
# ----------------------------

def main() -> None:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="PostgreSQL indexing benchmark runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    subparsers.required = True

    # Seed command
    seed_parser = subparsers.add_parser("seed", help="Generate and load dataset")
    seed_parser.add_argument(
        "--rows",
        type=int,
        help="Number of rows to generate (overrides config)",
    )
    seed_parser.add_argument(
        "--seed",
        type=int,
        help="Random seed (overrides config)",
    )
    seed_parser.set_defaults(func=cmd_seed)

    # Run command
    run_parser = subparsers.add_parser("run", help="Execute benchmark runs")
    run_parser.add_argument(
        "--variant",
        type=str,
        help="Run specific variant only (overrides config)",
    )
    run_parser.add_argument(
        "--runs",
        type=int,
        help="Number of runs per query (overrides config)",
    )
    run_parser.set_defaults(func=cmd_run)

    # Parse args and execute
    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


# bench/queries.py
# Query scenario registry and deterministic parameter selection.
#
# CONTRACT:
# - Query text is stored in committed .sql files under sql/queries/
# - Query shapes are fixed. We bind parameters; we do not generate SQL dynamically.
# - Parameter values (tokens, pagination offsets, keyset anchors) are deterministic.
#
# This module defines:
# - QueryScenario: name + SQL file path + parameter binding
# - list_queries / get_query_scenario: discovery like variants.py
# - build_query_params: deterministic params for each scenario

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from bench.config import BenchConfig, SeedConfig


# ----------------------------
# Query scenario type
# ----------------------------

@dataclass(frozen=True)
class QueryScenario:
    # Name derived from file stem or directory name convention.
    name: str

    # Path to the .sql file implementing the fixed query shape.
    sql_path: Path

    # Whether this query expects the generated column search_text to exist.
    # Used for preflight validation in bench.py (optional).
    requires_search_text: bool = False

    # Whether this query expects the generated column search_vector to exist.
    # Used for preflight validation in bench.py (optional).
    requires_search_vector: bool = False


# ----------------------------
# Discovery
# ----------------------------

_DEFAULT_QUERIES_DIR = Path("sql") / "queries"


def list_queries(queries_dir: Path = _DEFAULT_QUERIES_DIR) -> List[str]:
    """
    Discover query files and return scenario names, sorted.

    Convention:
    - scenario name == filename without extension
      e.g. sql/queries/Q1_name_substring.sql -> "Q1_name_substring"
    """
    if not queries_dir.exists():
        raise FileNotFoundError(f"Queries directory not found: {queries_dir}")
    if not queries_dir.is_dir():
        raise ValueError(f"Queries path is not a directory: {queries_dir}")

    names: List[str] = []
    for child in queries_dir.iterdir():
        if child.is_file() and child.suffix == ".sql":
            names.append(child.stem)

    names.sort()
    return names


def get_query_scenario(name: str, queries_dir: Path = _DEFAULT_QUERIES_DIR) -> QueryScenario:
    """
    Resolve query scenario from filename and attach any known requirements.

    Requirements are encoded here (not in SQL files) because they are metadata
    about the scenario.
    """
    path = queries_dir / f"{name}.sql"
    if not path.exists():
        available = list_queries(queries_dir)
        raise KeyError(f"Unknown query '{name}'. Available: {available}")
    if not path.is_file():
        raise ValueError(f"Query path exists but is not a file: {path}")

    # Small explicit mapping for known dependencies.
    # Keep this tight and auditable.
    requires_search_text = name in {"Q3_search_text_substring", "Q4_search_plus_filter"}
    requires_search_vector = name in {"Q7_fts_search", "Q8_fts_substring"}

    return QueryScenario(
        name=name,
        sql_path=path,
        requires_search_text=requires_search_text,
        requires_search_vector=requires_search_vector,
    )


# ----------------------------
# Deterministic parameters per scenario
# ----------------------------

def build_query_params(
    *,
    scenario: QueryScenario,
    seed_cfg: SeedConfig,
    bench_cfg: BenchConfig,
) -> Mapping[str, Any]:
    """
    Return a dict of named parameters to bind for the given scenario.

    Important:
    - Params must be deterministic and stable.
    - DO NOT pick random tokens unless you can prove and log determinism.

    This function expects your SQL files to use named placeholders like:
      %(pattern)s, %(category)s, %(offset)s, %(limit)s, %(last_name)s, %(last_id)s
    """

    name = scenario.name

    # Q1 — Selective substring on name (use a relatively rare-ish token pattern)
    # Your spec example was '%worc%'. We'll keep that exact token for comparability.
    if name == "Q1_name_substring":
        return {"pattern": "%worc%"}  # used by: WHERE name ILIKE %(pattern)s

    # Q2 — Non-selective substring on name (high frequency token)
    # Your spec example was '%chicken%'. We'll keep that exact token.
    if name == "Q2_name_common":
        return {"pattern": "%chicken%"}

    # Q3 — Multi-field substring (search_text) for keyword-style search
    if name == "Q3_search_text_substring":
        return {"pattern": "%organic%"}

    # Q4 — Search + filter
    if name == "Q4_search_plus_filter":
        # Category should be one of your canonical categories (exact match).
        # We pick the modal/hot category from your distribution to stress selectivity.
        # Deterministic choice: max-weight category.
        category = _argmax_key(seed_cfg.distributions.categories)
        return {"pattern": "%organic%", "category": category}

    # Q5 — OFFSET pagination
    if name == "Q5_offset_pagination":
        return {"offset": bench_cfg.pagination.offset, "limit": bench_cfg.pagination.limit}

    # Q6 — Keyset pagination
    if name == "Q6_keyset_pagination":
        # We need a deterministic (last_name, last_id) anchor.
        #
        # Hard rule for determinism:
        # - do NOT query the database here (that adds hidden dependencies)
        # - derive anchor from generator logic, but without generating all rows
        #
        # However, keyset anchor MUST correspond to a plausible tuple (name,id) in your dataset,
        # otherwise you may get skewed results (e.g., always first page).
        #
        # Best compromise:
        # - pick a deterministic row index "anchor_index"
        # - compute that row's deterministic UUID
        # - compute that row's base_name (same logic as generator), but WITHOUT importing generator here
        #
        # Problem: generator's near-duplicate + injection could alter final name.
        # If we attempt to match the final name exactly without DB lookup, we risk mismatch.
        #
        # So for Q6 we should anchor by a value that definitely exists.
        #
        # Recommendation (simple and correct): anchor at the very beginning:
        # last_name = '' and last_id = '00000000-0000-0000-0000-000000000000'
        # This yields the "first page" deterministically.
        #
        # If you want "deep page" keyset tests, we should add a precomputed anchor table
        # during seed (store N anchors), or allow one DB lookup step inside cmd_run preflight.
        return {
            "last_name": "",
            "last_id": "00000000-0000-0000-0000-000000000000",
            "limit": bench_cfg.pagination.limit,
        }

    # Q7 — Full-text search
    if name == "Q7_fts_search":
        # Use same token as Q3 for comparability: "organic"
        # to_tsquery format: simple word (PostgreSQL handles conversion)
        return {"query": "organic"}

    # Q8 — Selective full-text search
    if name == "Q8_fts_substring":
        # Use same selective term as Q1 for comparability: "worc"
        # to_tsquery format: simple word (PostgreSQL handles conversion)
        return {"query": "worc"}

    raise KeyError(f"No param builder for scenario: {name}")


def _argmax_key(weights: Mapping[str, float]) -> str:
    """
    Return the key with the maximum weight. Deterministic tie-break: lexical.
    """
    if not weights:
        raise ValueError("weights mapping is empty")
    # Sort by (-weight, key) so ties break consistently.
    return sorted(weights.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

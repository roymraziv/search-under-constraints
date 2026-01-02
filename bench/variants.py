# bench/variants.py
# Variant registry + helpers.
#
# A "variant" is the independent variable in your experiment:
# - it changes schema/index state (indexes, generated columns, extensions)
# - it is applied and reverted while keeping the dataset constant (Method 2)
#
# CONTRACT:
# - Variant definition lives on disk under sql/variants/<name>/{up.sql,down.sql}
# - Registry should be deterministic: same repo contents => same variant list
# - Apply/revert must be safe to call repeatedly (files should use IF EXISTS where appropriate)

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import psycopg

from bench.db import exec_file


# ----------------------------
# Variant type
# ----------------------------

@dataclass(frozen=True)
class Variant:
    # Name is derived from directory name: e.g. "V1_btree"
    name: str

    # Paths to the variant's scripts.
    # Using Path keeps it OS-safe and avoids stringly-typed paths.
    up_sql: Path
    down_sql: Path


# ----------------------------
# Registry discovery
# ----------------------------

_DEFAULT_VARIANTS_DIR = Path("sql") / "variants"


def list_variants(variants_dir: Path = _DEFAULT_VARIANTS_DIR) -> List[str]:
    """
    Discover variant directories and return their names, sorted.

    Why discovery matters:
    - avoids duplicated lists in Python and YAML
    - guarantees the registry reflects repo state
    - makes it easy to add variants without touching code

    We sort names so listing is deterministic across platforms/filesystems.
    """
    if not variants_dir.exists():
        raise FileNotFoundError(f"Variants directory not found: {variants_dir}")
    if not variants_dir.is_dir():
        raise ValueError(f"Variants path is not a directory: {variants_dir}")

    names: List[str] = []
    for child in variants_dir.iterdir():
        # Only directories are variants.
        if not child.is_dir():
            continue

        # A valid variant directory must contain up.sql and down.sql.
        up = child / "up.sql"
        down = child / "down.sql"
        if up.is_file() and down.is_file():
            names.append(child.name)

    names.sort()
    return names


def get_variant(name: str, variants_dir: Path = _DEFAULT_VARIANTS_DIR) -> Variant:
    """
    Resolve a Variant by name. Raises if missing or invalid structure.

    This is the function bench.py should use when converting bench.yaml variant names
    into concrete script paths.
    """
    vdir = variants_dir / name
    if not vdir.exists():
        # Give a helpful error showing what's available.
        available = list_variants(variants_dir)
        raise KeyError(f"Unknown variant '{name}'. Available: {available}")

    if not vdir.is_dir():
        raise ValueError(f"Variant path exists but is not a directory: {vdir}")

    up = vdir / "up.sql"
    down = vdir / "down.sql"
    if not up.is_file():
        raise FileNotFoundError(f"Variant '{name}' missing up.sql: {up}")
    if not down.is_file():
        raise FileNotFoundError(f"Variant '{name}' missing down.sql: {down}")

    return Variant(name=name, up_sql=up, down_sql=down)


# ----------------------------
# Apply / revert helpers
# ----------------------------

def apply_variant(conn: psycopg.Connection, variant: Variant) -> None:
    """
    Apply a variant by executing its up.sql.

    Design notes:
    - We don't wrap in an explicit BEGIN/COMMIT here because:
        - your up.sql files can include their own transactions
        - psycopg connection context manager will commit on success
    - The up.sql itself should be responsible for being safe (IF NOT EXISTS where appropriate).
    """
    exec_file(conn, variant.up_sql)


def revert_variant(conn: psycopg.Connection, variant: Variant) -> None:
    """
    Revert a variant by executing its down.sql.

    down.sql must return the DB to canonical schema state:
    - drop indexes created in up
    - drop generated columns created in up (if any)
    - do not drop the base table (products)
    """
    exec_file(conn, variant.down_sql)


# ----------------------------
# Optional: light sanity checks (useful later)
# ----------------------------

def ensure_variant_scripts_are_idempotent(variant: Variant) -> None:
    """
    Cheap static sanity check.

    This does NOT fully prove idempotency, but it catches common footguns:
    - down.sql missing IF EXISTS
    - up.sql missing IF NOT EXISTS for indexes

    You can call this in CI or a preflight command.
    """
    up_text = variant.up_sql.read_text(encoding="utf-8")
    down_text = variant.down_sql.read_text(encoding="utf-8")

    # Heuristic checks only.
    if "CREATE INDEX" in up_text and "IF NOT EXISTS" not in up_text:
        # Not always required, but recommended for re-runs during development.
        raise ValueError(f"{variant.name}/up.sql: CREATE INDEX without IF NOT EXISTS")

    if "DROP INDEX" in down_text and "IF EXISTS" not in down_text:
        raise ValueError(f"{variant.name}/down.sql: DROP INDEX without IF EXISTS")


def validate_all_variants(variants_dir: Path = _DEFAULT_VARIANTS_DIR) -> None:
    """
    Validate structure and run static sanity checks for all variants.

    Useful as a dev command (or future CI).
    """
    for name in list_variants(variants_dir):
        v = get_variant(name, variants_dir)
        ensure_variant_scripts_are_idempotent(v)

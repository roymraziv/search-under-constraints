# bench/config.py
# Typed config loading + validation for Search Under Constraints.
# This module should be pure: load YAML -> validate -> return dataclasses.
# No DB calls, no filesystem writes beyond reading config files.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import yaml


# ----------------------------
# Dataclasses
# ----------------------------

@dataclass(frozen=True)
class TimestampConfig:
    base_utc: datetime
    step_seconds: int


@dataclass(frozen=True)
class RareTokenConfig:
    prefix: str
    start: int
    end: int
    width: int


@dataclass(frozen=True)
class DistributionConfig:
    brands: Mapping[str, float]
    categories: Mapping[str, float]


@dataclass(frozen=True)
class TokenInjectionConfig:
    common_row_rate: float
    rare_row_rate: float
    fields: Mapping[str, float]  # e.g. {"name": 0.25, "description": 0.75}


@dataclass(frozen=True)
class DescriptionTokensConfig:
    min: int
    max: int


@dataclass(frozen=True)
class DuplicateConfig:
    near_duplicate_row_rate: float


@dataclass(frozen=True)
class TemplateConfig:
    name_patterns: Sequence[str]


@dataclass(frozen=True)
class SeedConfig:
    seed: int
    rows: int
    timestamps: TimestampConfig
    distributions: DistributionConfig
    token_injection: TokenInjectionConfig
    description_tokens: DescriptionTokensConfig
    duplicates: DuplicateConfig
    rare_tokens: RareTokenConfig
    templates: TemplateConfig


@dataclass(frozen=True)
class SessionConfig:
    timezone: str = "UTC"
    statement_timeout_ms: int = 0
    jit: Optional[str] = None  # e.g. "off" or "on"


@dataclass(frozen=True)
class PaginationConfig:
    offset: int = 100
    limit: int = 25


@dataclass(frozen=True)
class BenchConfig:
    runs: int
    warmup: int
    variants: Sequence[str]
    queries: Sequence[str]
    session: SessionConfig
    pagination: PaginationConfig


# ----------------------------
# Public API
# ----------------------------

def load_seed_config(path: Path) -> SeedConfig:
    raw = _load_yaml(path)
    _require_keys(raw, ["seed", "rows", "timestamps", "distributions", "token_injection",
                        "description_tokens", "duplicates", "rare_tokens", "templates"], ctx="seed.yaml")

    seed = _as_int(raw["seed"], "seed")
    rows = _as_int(raw["rows"], "rows")
    if rows <= 0:
        raise ValueError("rows must be > 0")

    ts_raw = _as_dict(raw["timestamps"], "timestamps")
    _require_keys(ts_raw, ["base_utc", "step_seconds"], ctx="timestamps")

    base_utc = _parse_utc_datetime(ts_raw["base_utc"], field="timestamps.base_utc")
    step_seconds = _as_int(ts_raw["step_seconds"], "timestamps.step_seconds")
    if step_seconds <= 0:
        raise ValueError("timestamps.step_seconds must be > 0")

    dist_raw = _as_dict(raw["distributions"], "distributions")
    _require_keys(dist_raw, ["brands", "categories"], ctx="distributions")

    brands = _as_float_map(dist_raw["brands"], "distributions.brands")
    categories = _as_float_map(dist_raw["categories"], "distributions.categories")
    _validate_probability_map(brands, "distributions.brands")
    _validate_probability_map(categories, "distributions.categories")

    inj_raw = _as_dict(raw["token_injection"], "token_injection")
    _require_keys(inj_raw, ["common_row_rate", "rare_row_rate", "fields"], ctx="token_injection")

    common_row_rate = _as_float(inj_raw["common_row_rate"], "token_injection.common_row_rate")
    rare_row_rate = _as_float(inj_raw["rare_row_rate"], "token_injection.rare_row_rate")
    _validate_rate(common_row_rate, "token_injection.common_row_rate")
    _validate_rate(rare_row_rate, "token_injection.rare_row_rate")

    fields = _as_float_map(inj_raw["fields"], "token_injection.fields")
    _validate_probability_map(fields, "token_injection.fields")
    allowed_fields = {"name", "description", "brand", "category"}
    unknown = set(fields.keys()) - allowed_fields
    if unknown:
        raise ValueError(f"token_injection.fields contains unknown field(s): {sorted(unknown)} "
                         f"(allowed: {sorted(allowed_fields)})")

    desc_raw = _as_dict(raw["description_tokens"], "description_tokens")
    _require_keys(desc_raw, ["min", "max"], ctx="description_tokens")
    desc_min = _as_int(desc_raw["min"], "description_tokens.min")
    desc_max = _as_int(desc_raw["max"], "description_tokens.max")
    if desc_min <= 0 or desc_max <= 0:
        raise ValueError("description_tokens min/max must be > 0")
    if desc_min > desc_max:
        raise ValueError("description_tokens.min must be <= description_tokens.max")

    dup_raw = _as_dict(raw["duplicates"], "duplicates")
    _require_keys(dup_raw, ["near_duplicate_row_rate"], ctx="duplicates")
    near_dup_rate = _as_float(dup_raw["near_duplicate_row_rate"], "duplicates.near_duplicate_row_rate")
    _validate_rate(near_dup_rate, "duplicates.near_duplicate_row_rate")

    rare_raw = _as_dict(raw["rare_tokens"], "rare_tokens")
    _require_keys(rare_raw, ["prefix", "start", "end", "width"], ctx="rare_tokens")
    prefix = _as_str(rare_raw["prefix"], "rare_tokens.prefix")
    start = _as_int(rare_raw["start"], "rare_tokens.start")
    end = _as_int(rare_raw["end"], "rare_tokens.end")
    width = _as_int(rare_raw["width"], "rare_tokens.width")
    if start <= 0 or end <= 0:
        raise ValueError("rare_tokens.start/end must be > 0")
    if start > end:
        raise ValueError("rare_tokens.start must be <= rare_tokens.end")
    if width <= 0:
        raise ValueError("rare_tokens.width must be > 0")

    tmpl_raw = _as_dict(raw["templates"], "templates")
    _require_keys(tmpl_raw, ["name_patterns"], ctx="templates")
    name_patterns = _as_str_list(tmpl_raw["name_patterns"], "templates.name_patterns")
    if not name_patterns:
        raise ValueError("templates.name_patterns must have at least 1 pattern")

    return SeedConfig(
        seed=seed,
        rows=rows,
        timestamps=TimestampConfig(base_utc=base_utc, step_seconds=step_seconds),
        distributions=DistributionConfig(brands=brands, categories=categories),
        token_injection=TokenInjectionConfig(
            common_row_rate=common_row_rate,
            rare_row_rate=rare_row_rate,
            fields=fields,
        ),
        description_tokens=DescriptionTokensConfig(min=desc_min, max=desc_max),
        duplicates=DuplicateConfig(near_duplicate_row_rate=near_dup_rate),
        rare_tokens=RareTokenConfig(prefix=prefix, start=start, end=end, width=width),
        templates=TemplateConfig(name_patterns=name_patterns),
    )


def load_bench_config(path: Path) -> BenchConfig:
    raw = _load_yaml(path)
    _require_keys(raw, ["runs", "warmup", "variants", "queries"], ctx="bench.yaml")

    runs = _as_int(raw["runs"], "runs")
    warmup = _as_int(raw["warmup"], "warmup")
    if runs <= 0:
        raise ValueError("runs must be > 0")
    if warmup < 0:
        raise ValueError("warmup must be >= 0")

    variants = _as_str_list(raw["variants"], "variants")
    queries = _as_str_list(raw["queries"], "queries")
    if not variants:
        raise ValueError("variants must be a non-empty list")
    if not queries:
        raise ValueError("queries must be a non-empty list")

    session_raw = raw.get("session", {}) or {}
    session_raw = _as_dict(session_raw, "session")
    session = SessionConfig(
        timezone=_as_str(session_raw.get("timezone", "UTC"), "session.timezone"),
        statement_timeout_ms=_as_int(session_raw.get("statement_timeout_ms", 0), "session.statement_timeout_ms"),
        jit=(None if session_raw.get("jit") is None else _as_str(session_raw["jit"], "session.jit")),
    )
    if session.statement_timeout_ms < 0:
        raise ValueError("session.statement_timeout_ms must be >= 0")
    if session.jit is not None and session.jit not in {"on", "off"}:
        raise ValueError("session.jit must be 'on', 'off', or omitted")

    pagination_raw = raw.get("pagination", {}) or {}
    pagination_raw = _as_dict(pagination_raw, "pagination")
    pagination = PaginationConfig(
        offset=_as_int(pagination_raw.get("offset", 100), "pagination.offset"),
        limit=_as_int(pagination_raw.get("limit", 25), "pagination.limit"),
    )
    if pagination.offset < 0:
        raise ValueError("pagination.offset must be >= 0")
    if pagination.limit <= 0:
        raise ValueError("pagination.limit must be > 0")

    return BenchConfig(
        runs=runs,
        warmup=warmup,
        variants=variants,
        queries=queries,
        session=session,
        pagination=pagination,
    )


# ----------------------------
# YAML + validation helpers
# ----------------------------

def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    if not path.is_file():
        raise ValueError(f"Config path is not a file: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Top-level YAML must be a mapping/object: {path}")
    return data


def _require_keys(d: Mapping[str, Any], keys: Sequence[str], *, ctx: str) -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        raise ValueError(f"Missing required key(s) in {ctx}: {missing}")


def _as_dict(v: Any, field: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise TypeError(f"{field} must be a mapping/object")
    return v


def _as_int(v: Any, field: str) -> int:
    if isinstance(v, bool):
        raise TypeError(f"{field} must be an int (got bool)")
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    raise TypeError(f"{field} must be an int")


def _as_float(v: Any, field: str) -> float:
    if isinstance(v, bool):
        raise TypeError(f"{field} must be a float (got bool)")
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip())
        except ValueError:
            pass
    raise TypeError(f"{field} must be a float")


def _as_str(v: Any, field: str) -> str:
    if not isinstance(v, str):
        raise TypeError(f"{field} must be a string")
    s = v.strip()
    if not s:
        raise ValueError(f"{field} must be a non-empty string")
    return s


def _as_str_list(v: Any, field: str) -> List[str]:
    if not isinstance(v, list):
        raise TypeError(f"{field} must be a list")
    out: List[str] = []
    for i, item in enumerate(v):
        if not isinstance(item, str):
            raise TypeError(f"{field}[{i}] must be a string")
        s = item.strip()
        if not s:
            raise ValueError(f"{field}[{i}] must be a non-empty string")
        out.append(s)
    return out


def _as_float_map(v: Any, field: str) -> Dict[str, float]:
    if not isinstance(v, dict):
        raise TypeError(f"{field} must be a mapping of string -> float")
    out: Dict[str, float] = {}
    for k, val in v.items():
        if not isinstance(k, str) or not k.strip():
            raise TypeError(f"{field} keys must be non-empty strings")
        out[k.strip()] = _as_float(val, f"{field}.{k}")
    return out


def _validate_rate(x: float, field: str) -> None:
    if x < 0.0 or x > 1.0:
        raise ValueError(f"{field} must be between 0.0 and 1.0 (got {x})")


def _validate_probability_map(m: Mapping[str, float], field: str) -> None:
    if not m:
        raise ValueError(f"{field} must be non-empty")
    for k, v in m.items():
        if v < 0.0:
            raise ValueError(f"{field}.{k} must be >= 0 (got {v})")
    total = sum(m.values())
    # Allow slight float drift (YAML floats). We want "weights sum to ~1.0".
    if total <= 0.0:
        raise ValueError(f"{field} weights must sum to > 0")
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"{field} weights must sum to 1.0 (got {total})")


def _parse_utc_datetime(v: Any, *, field: str) -> datetime:
    s = _as_str(v, field)
    # Accept ISO-8601 with Z suffix (e.g., 2025-01-01T00:00:00Z)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"{field} must be ISO-8601 datetime (got {v})") from e

    if dt.tzinfo is None:
        # Treat naive timestamps as UTC to keep config ergonomic.
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt

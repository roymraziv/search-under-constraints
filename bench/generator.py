# bench/generator.py
# Deterministic dataset generator for the canonical `products` table.
#
# CONTRACT:
# - No DB access.
# - No reading YAML.
# - No wall-clock time.
# - Every row is reproducible from (seed_config, row_index) ONLY.
# - Vocabulary is hardcoded in this file (per project constraints).
#
# Output rows must match schema column order used by COPY:
#   (id, name, brand, category, description, created_at, updated_at)

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import random
from typing import Iterator, Sequence

from bench.config import SeedConfig


# ----------------------------
# Hardcoded vocabulary (committed to repo)
# ----------------------------
# These are intentionally not in YAML to guarantee reproducibility.
# YAML controls distributions and injection rates only.

COMMON_TOKENS: tuple[str, ...] = (
    "organic",
    "chicken",
    "gluten",
    "natural",
    "spicy",
    "classic",
    "premium",
    "fresh",
    "healthy",
    "seasoned",
)

# Words used to generate product names and filler descriptions.
# Keep these stable; changes here change the dataset.
ADJECTIVES: tuple[str, ...] = (
    "smoky", "crispy", "zesty", "hearty", "sweet", "savory", "tangy", "bright",
    "roasted", "toasted", "herbed", "sliced", "chunky", "creamy", "bold",
)

NOUNS: tuple[str, ...] = (
    "chips", "soup", "sauce", "tea", "coffee", "broth", "granola", "mix",
    "bites", "snacks", "spread", "berries", "greens", "protein", "vitamins",
)

QUALIFIERS: tuple[str, ...] = (
    "family size", "single serve", "value pack", "low sodium", "no sugar",
    "extra hot", "limited batch", "stone ground", "farm style",
)

# Extra filler tokens for descriptions (helps make 20–60 token descriptions without repeating COMMON_TOKENS too much).
FILLER_TOKENS: tuple[str, ...] = (
    "crafted", "selected", "quality", "ingredients", "from", "trusted", "sources",
    "packed", "for", "everyday", "meals", "quick", "snacking", "great", "taste",
    "balanced", "flavor", "kitchen", "pantry", "ready", "to", "enjoy",
)


# ----------------------------
# Output row type
# ----------------------------

@dataclass(frozen=True)
class ProductRow:
    # Keep field order identical to COPY column list.
    id: str
    name: str
    brand: str
    category: str
    description: str
    created_at: datetime
    updated_at: datetime


# ----------------------------
# Weighted picker
# ----------------------------
# We precompute cumulative weights once and then do O(log n) picks deterministically.

class WeightedPicker:
    def __init__(self, items: Sequence[str], weights: Sequence[float]) -> None:
        # Validate lengths match (should already be validated in config, but this is cheap safety).
        if len(items) != len(weights):
            raise ValueError("items and weights must be same length")
        if not items:
            raise ValueError("items must be non-empty")

        # Build cumulative distribution: [w1, w1+w2, w1+w2+w3, ...]
        total = 0.0
        cdf: list[float] = []
        for w in weights:
            total += float(w)
            cdf.append(total)

        # We expect weights sum to 1.0 (config enforces), but we don’t rely on exact equality.
        self._items = list(items)
        self._cdf = cdf
        self._total = total

    def pick(self, rng: random.Random) -> str:
        # Draw uniform [0, total) and find first CDF threshold greater than x.
        x = rng.random() * self._total

        # Manual binary search to avoid importing bisect (either is fine).
        lo, hi = 0, len(self._cdf) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if x <= self._cdf[mid]:
                hi = mid
            else:
                lo = mid + 1
        return self._items[lo]


# ----------------------------
# Deterministic primitives
# ----------------------------

def _row_rng(seed: int, row_index: int) -> random.Random:
    """
    Create a per-row RNG that is deterministic and independent of generation order.

    Why:
    - If you rely on a single global RNG, results depend on iteration order.
    - With per-row RNG, row N is always the same even if you parallelize later.
    """
    # Use a stable hash to derive an integer seed for this row.
    # blake2b is stable, fast, and deterministic.
    h = hashlib.blake2b(digest_size=8)
    h.update(str(seed).encode("utf-8"))
    h.update(b":")
    h.update(str(row_index).encode("utf-8"))
    derived = int.from_bytes(h.digest(), byteorder="big", signed=False)
    return random.Random(derived)


def deterministic_uuid(seed: int, row_index: int) -> str:
    """
    Deterministic UUID via md5(seed + row_index), formatted as UUID string.

    Requirement from your spec: md5(seed + row_index).
    """
    m = hashlib.md5()  # nosec - not for security, only determinism
    m.update(str(seed).encode("utf-8"))
    m.update(b":")
    m.update(str(row_index).encode("utf-8"))
    hex32 = m.hexdigest()  # 32 hex chars

    # Format as UUID v4-like string layout (8-4-4-4-12).
    # It’s not a "true" UUIDv4, but it’s a valid UUID string.
    return f"{hex32[0:8]}-{hex32[8:12]}-{hex32[12:16]}-{hex32[16:20]}-{hex32[20:32]}"


def deterministic_timestamp(base_utc: datetime, step_seconds: int, row_index: int) -> datetime:
    """
    Deterministic timestamp: base + index * step_seconds, always UTC (base_utc is normalized in config).
    """
    return base_utc + timedelta(seconds=row_index * step_seconds)


def rare_token(cfg: SeedConfig, rng: random.Random) -> str:
    """
    Pick a deterministic rare token in the configured range.
    Example: rare-000001 .. rare-020000
    """
    start = cfg.rare_tokens.start
    end = cfg.rare_tokens.end
    n = rng.randint(start, end)  # inclusive
    return f"{cfg.rare_tokens.prefix}{n:0{cfg.rare_tokens.width}d}"


def _choose_injection_field(cfg: SeedConfig, rng: random.Random) -> str:
    """
    Choose which field to inject into, based on cfg.token_injection.fields weights.
    """
    fields = list(cfg.token_injection.fields.keys())
    weights = list(cfg.token_injection.fields.values())
    picker = WeightedPicker(fields, weights)
    return picker.pick(rng)


# ----------------------------
# Name + description builders (pure functions)
# ----------------------------

def base_name_for_index(cfg: SeedConfig, row_index: int) -> str:
    """
    Generate a base product name deterministically from row_index.
    This is used both for normal generation and to create duplicates without storing prior rows.
    """
    rng = _row_rng(cfg.seed, row_index)

    # Choose a pattern from templates deterministically.
    pattern = cfg.templates.name_patterns[rng.randrange(0, len(cfg.templates.name_patterns))]

    # Fill tokens deterministically from our vocab.
    adj = ADJECTIVES[rng.randrange(0, len(ADJECTIVES))]
    noun = NOUNS[rng.randrange(0, len(NOUNS))]
    qualifier = QUALIFIERS[rng.randrange(0, len(QUALIFIERS))]

    # “brandish” is a flavor word; we’ll derive it from adjectives to avoid coupling to brand.
    brandish = ADJECTIVES[(rng.randrange(0, len(ADJECTIVES)))]

    # Replace placeholders. Keep set limited and explicit (fail fast if pattern is unknown).
    out = pattern
    out = out.replace("{adj}", adj)
    out = out.replace("{noun}", noun)
    out = out.replace("{qualifier}", qualifier)
    out = out.replace("{brandish}", brandish)

    # If template still contains braces, it had an unknown token.
    if "{" in out or "}" in out:
        raise ValueError(f"Unknown template placeholder in name pattern: {pattern}")

    return out


def maybe_make_near_duplicate(cfg: SeedConfig, row_index: int, base_name: str, rng: random.Random) -> str:
    """
    With near_duplicate_row_rate, produce a near-duplicate name derived from some earlier row’s base name.

    Deterministic method:
    - pick a previous index < row_index
    - reuse that previous base name (computed purely)
    - apply a small deterministic suffix/prefix mutation
    """
    rate = cfg.duplicates.near_duplicate_row_rate
    if row_index == 0:
        return base_name  # no prior row to duplicate

    if rng.random() >= rate:
        return base_name

    source_idx = rng.randrange(0, row_index)
    source_name = base_name_for_index(cfg, source_idx)

    # Apply a small deterministic perturbation:
    # Choose 1 of a few stable transforms.
    transform = rng.randrange(0, 3)
    if transform == 0:
        return f"{source_name} {QUALIFIERS[rng.randrange(0, len(QUALIFIERS))]}"
    if transform == 1:
        return f"{ADJECTIVES[rng.randrange(0, len(ADJECTIVES))]} {source_name}"
    return f"{source_name} - limited"

# row index not used?
def build_description(cfg: SeedConfig, row_index: int, rng: random.Random) -> str:
    """
    Build a 20–60 token description deterministically.

    Strategy:
    - choose target length in [min,max]
    - start with some filler tokens
    - optionally inject common/rare tokens (later step may also inject into description)
    """
    min_t = cfg.description_tokens.min
    max_t = cfg.description_tokens.max
    target = rng.randint(min_t, max_t)

    tokens: list[str] = []

    # Add stable filler tokens deterministically.
    while len(tokens) < target:
        # Alternate between filler and nouns/adjectives to keep variety.
        if len(tokens) % 5 == 0:
            tokens.append(ADJECTIVES[rng.randrange(0, len(ADJECTIVES))])
        elif len(tokens) % 5 == 1:
            tokens.append(NOUNS[rng.randrange(0, len(NOUNS))])
        else:
            tokens.append(FILLER_TOKENS[rng.randrange(0, len(FILLER_TOKENS))])

    # Join into a description string.
    return " ".join(tokens)


def inject_token_into_text(text: str, token: str, rng: random.Random) -> str:
    """
    Deterministically inject a token into an existing text by inserting it at a token boundary.
    """
    parts = text.split()
    if not parts:
        return token

    # Choose an insertion position between tokens (0..len).
    pos = rng.randrange(0, len(parts) + 1)
    parts.insert(pos, token)
    return " ".join(parts)


# ----------------------------
# Main public generator
# ----------------------------

def generate_products(cfg: SeedConfig) -> Iterator[ProductRow]:
    """
    Yield ProductRow objects for indices [0..rows-1].

    Determinism guarantee:
    - Each row i depends only on (cfg, i).
    - No global RNG state.
    - No hidden external input.
    """
    # Precompute weighted pickers for brand/category using config weights.
    brand_items = list(cfg.distributions.brands.keys())
    brand_weights = list(cfg.distributions.brands.values())
    brand_picker = WeightedPicker(brand_items, brand_weights)

    category_items = list(cfg.distributions.categories.keys())
    category_weights = list(cfg.distributions.categories.values())
    category_picker = WeightedPicker(category_items, category_weights)

    # Main generation loop.
    for i in range(cfg.rows):
        rng = _row_rng(cfg.seed, i)

        pid = deterministic_uuid(cfg.seed, i)

        # Base timestamps
        created = deterministic_timestamp(cfg.timestamps.base_utc, cfg.timestamps.step_seconds, i)
        # For now updated_at = created_at (you can later add deterministic updates in a write-overhead phase)
        updated = created

        # Weighted brand/category selection
        brand = brand_picker.pick(rng)
        category = category_picker.pick(rng)

        # Base name and near-duplicate logic
        base_name = base_name_for_index(cfg, i)
        name = maybe_make_near_duplicate(cfg, i, base_name, rng)

        # Description construction
        description = build_description(cfg, i, rng)

        # Token injection events:
        # 1) common tokens injected on some fraction of rows
        if rng.random() < cfg.token_injection.common_row_rate:
            token = COMMON_TOKENS[rng.randrange(0, len(COMMON_TOKENS))]
            field = _choose_injection_field(cfg, rng)
            if field == "name":
                name = inject_token_into_text(name, token, rng)
            elif field == "description":
                description = inject_token_into_text(description, token, rng)
            elif field == "brand":
                # brand is a single token; inject by appending a stable suffix token
                brand = f"{brand} {token}"
            elif field == "category":
                category = f"{category} {token}"

        # 2) rare tokens injected on some fraction of rows
        if rng.random() < cfg.token_injection.rare_row_rate:
            token = rare_token(cfg, rng)
            field = _choose_injection_field(cfg, rng)
            if field == "name":
                name = inject_token_into_text(name, token, rng)
            elif field == "description":
                description = inject_token_into_text(description, token, rng)
            elif field == "brand":
                brand = f"{brand} {token}"
            elif field == "category":
                category = f"{category} {token}"

        yield ProductRow(
            id=pid,
            name=name,
            brand=brand,
            category=category,
            description=description,
            created_at=created,
            updated_at=updated,
        )

# bench/analyze.py
# Statistical analysis of benchmark measurements.
#
# CONTRACT:
# - Compute percentiles (p50/p95/p99) from execution times
# - Extract planner statistics from EXPLAIN plans
# - Generate summaries grouped by variant + query

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from bench.measure import Measurement


# ----------------------------
# Data structures
# ----------------------------

@dataclass(frozen=True)
class Percentiles:
    """Statistical percentiles and summary stats."""
    p50: float
    p95: float
    p99: float
    min: float
    max: float
    mean: float


@dataclass(frozen=True)
class Summary:
    """Summary statistics for a variant + query combination."""
    variant: str
    query: str
    percentiles: Percentiles
    planner_stats: dict
    buffer_stats: dict


# ----------------------------
# Percentile calculation
# ----------------------------

def compute_percentiles(times: List[float]) -> Percentiles:
    """
    Compute percentiles and summary statistics from a list of execution times.

    Uses manual calculation (sort + index) to avoid numpy dependency.
    Times should be in milliseconds.

    Returns Percentiles with p50, p95, p99, min, max, and mean.
    """
    if not times:
        raise ValueError("times list cannot be empty")

    sorted_times = sorted(times)
    n = len(sorted_times)

    # Percentile calculation using linear interpolation
    def _percentile(p: float) -> float:
        """Calculate p-th percentile (0.0 to 1.0)."""
        if n == 1:
            return sorted_times[0]
        # Linear interpolation between adjacent values
        index = (n - 1) * p
        lower = int(index)
        upper = lower + 1
        weight = index - lower

        if upper >= n:
            return sorted_times[-1]

        return sorted_times[lower] * (1 - weight) + sorted_times[upper] * weight

    p50 = _percentile(0.50)
    p95 = _percentile(0.95)
    p99 = _percentile(0.99)
    min_val = sorted_times[0]
    max_val = sorted_times[-1]
    mean_val = sum(sorted_times) / n

    return Percentiles(
        p50=p50,
        p95=p95,
        p99=p99,
        min=min_val,
        max=max_val,
        mean=mean_val,
    )


# ----------------------------
# Planner statistics extraction
# ----------------------------

# Node types that wrap other operations (not actual scans)
WRAPPER_NODES = {
    "Limit",
    "Gather",
    "Gather Merge",
    "Sort",
    "Materialize",
    "Memoize",
    "Append",
    "Merge Append",
}


def _find_first_scan_node(node: dict, path: List[str] | None = None) -> tuple[str, List[str]]:
    """
    Recursively find the first non-wrapper scan node.
    
    Returns:
        (node_type, path) where path is list of node types from root to scan
    """
    if path is None:
        path = []
    
    node_type = node.get("Node Type")
    if node_type:
        path.append(node_type)
    
    # If this is a real scan (not a wrapper), return it
    if node_type and node_type not in WRAPPER_NODES:
        return node_type, path
    
    # Otherwise, recurse into children
    if "Plans" in node and node["Plans"]:
        # Take first child (most plans have one main child)
        return _find_first_scan_node(node["Plans"][0], path)
    
    # Fallback: return root node type if no children
    return node_type or "Unknown", path


def extract_planner_stats(plan: dict) -> dict:
    """
    Extract planner decisions from an EXPLAIN plan.

    Returns a dict with:
    - scan_type: The primary scan type (Seq Scan, Index Scan, etc.)
    - index_used: Name of index if used, None otherwise
    - estimated_rows: Planner's row estimate
    - actual_rows: Actual rows returned
    - plan_nodes: List of all plan node types in the tree
    """
    if "Plan" not in plan:
        return {
            "scan_type": None,
            "index_used": None,
            "estimated_rows": 0,
            "actual_rows": 0,
            "plan_nodes": [],
        }

    root_plan = plan["Plan"]
    root_node_type = root_plan.get("Node Type", None)
    estimated_rows = root_plan.get("Plan Rows", 0)
    actual_rows = root_plan.get("Actual Rows", 0)

    # If root is a wrapper, find underlying scan and format path
    if root_node_type in WRAPPER_NODES:
        underlying_scan, path = _find_first_scan_node(root_plan)
        # Format as "Limit > Index Scan" or "Limit > Gather Merge > Sort > Seq Scan"
        scan_type = " > ".join(path) if len(path) > 1 else root_node_type
    else:
        scan_type = root_node_type

    # Collect all node types and find index name in the plan tree
    plan_nodes = []
    index_found = []

    def _collect_nodes(node: dict) -> None:
        """Recursively collect all node types and find index name."""
        node_type = node.get("Node Type")
        if node_type:
            plan_nodes.append(node_type)

        # Check if this node has an index name
        if "Index Name" in node:
            index_found.append(node["Index Name"])

        if "Plans" in node:
            for child in node["Plans"]:
                _collect_nodes(child)

    _collect_nodes(root_plan)
    
    # Use first index found (most plans have one index)
    index_used = index_found[0] if index_found else None

    return {
        "scan_type": scan_type,
        "index_used": index_used,
        "estimated_rows": estimated_rows,
        "actual_rows": actual_rows,
        "plan_nodes": plan_nodes,
    }


# ----------------------------
# Measurement summarization
# ----------------------------

def summarize_measurements(
    measurements: List[Measurement],
    variant: str,
    query: str,
) -> Summary:
    """
    Summarize a list of measurements for a single variant + query.

    Computes percentiles, extracts planner stats, and aggregates buffer stats.
    """
    if not measurements:
        raise ValueError("measurements list cannot be empty")

    # Extract execution times
    execution_times = [m.execution_time_ms for m in measurements]

    # Compute percentiles
    percentiles = compute_percentiles(execution_times)

    # Extract planner stats from the first measurement's plan
    # (All measurements should have similar plans for the same query)
    planner_stats = extract_planner_stats(measurements[0].plan)

    # Aggregate buffer stats across all measurements
    buffer_stats = _aggregate_buffer_stats(measurements)

    return Summary(
        variant=variant,
        query=query,
        percentiles=percentiles,
        planner_stats=planner_stats,
        buffer_stats=buffer_stats,
    )


def _aggregate_buffer_stats(measurements: List[Measurement]) -> dict:
    """
    Aggregate buffer statistics across multiple measurements.

    Returns mean values for each buffer metric.
    """
    if not measurements:
        return {}

    n = len(measurements)
    aggregated = {
        "shared_hit": 0.0,
        "shared_read": 0.0,
        "shared_dirtied": 0.0,
        "shared_written": 0.0,
        "temp_read": 0.0,
        "temp_written": 0.0,
        "local_hit": 0.0,
        "local_read": 0.0,
        "local_written": 0.0,
    }

    for m in measurements:
        for key in aggregated:
            aggregated[key] += m.buffers.get(key, 0)

    # Compute means
    for key in aggregated:
        aggregated[key] = aggregated[key] / n

    return aggregated


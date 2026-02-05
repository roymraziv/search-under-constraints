# Benchmark CLI Usage Guide

## 1. Introduction

**search-under-constraints** is a PostgreSQL indexing benchmark system that compares different indexing strategies for search-heavy relational workloads. This guide explains how to use the CLI tool to run benchmarks and analyze results.

### What the CLI Tool Does

The benchmark runner:
- Generates deterministic test datasets
- Applies different indexing strategies (variants)
- Executes fixed query patterns
- Captures performance metrics and EXPLAIN plans
- Exports results for analysis

### Prerequisites

- **Python 3.10+** (check with `python3 --version`)
- **PostgreSQL 16.4** (via Docker)
- **Docker & Docker Compose** (for running PostgreSQL)
- **pip** (Python package manager)

---

## 2. Installation & Setup

### Step 1: Install Python Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `psycopg[binary]` - PostgreSQL adapter
- `pyyaml` - YAML config parsing
- `python-dotenv` - Environment variable loading
- `numpy` - Statistical calculations

### Step 2: Start PostgreSQL

```bash
cd infra
docker-compose up -d
cd ..
```

This starts PostgreSQL in a Docker container with:
- Database: `app_db`
- User: `app`
- Password: `app_pw`
- Port: `5432`

Verify it's running:
```bash
docker ps | grep pg_local
```

### Step 3: Configure Environment Variables

Create a `.env` file in the project root:

```bash
DATABASE_URL=postgresql://app:app_pw@localhost:5432/app_db
```

Optional overrides:
```
# SEED_CONFIG=config/seed.yaml
# BENCH_CONFIG=config/bench.yaml
```

### Step 4: Verify Installation

Quick test to ensure everything works:

```bash
# Test database connection
docker exec -it pg_local psql -U app -d app_db -c "SELECT version();"

# Test CLI (should show help)
python3 -m bench.bench --help
```

---

## 3. Running the CLI

### Important: Module Execution

**Always use module execution:**

```bash
python3 -m bench.bench <command>
```

**Do NOT use:**
```bash
python3 bench/bench.py <command>  # ❌ This will fail
```

### Why the `-m` Flag?

The `-m` flag tells Python to run `bench.bench` as a module, which properly resolves package imports. Running `bench/bench.py` directly causes import errors because Python doesn't recognize `bench` as a package.

---

## 4. Commands Reference

### 4.1 Seed Command

**Purpose:** Generate and load a deterministic dataset into PostgreSQL.

**Syntax:**
```bash
python3 -m bench.bench seed [--rows N] [--seed N]
```

**Options:**
- `--rows N`: Override number of rows to generate (default: from `config/seed.yaml`)
- `--seed N`: Override random seed for determinism (default: from `config/seed.yaml`)

**Examples:**

Quick test with 1,000 rows:
```bash
python3 -m bench.bench seed --rows 1000 --seed 12345
```

Full dataset from config (100,000 rows):
```bash
python3 -m bench.bench seed
```

**What It Does:**
1. Resets the database schema (drops and recreates `products` table)
2. Generates deterministic product data using the configured seed
3. Loads data via PostgreSQL COPY (fast bulk insert)
4. Prints load statistics (rows, time, rate)

**Expected Output:**
```
Loading 1000 rows with seed 12345...
Resetting schema...
Generating and loading data...
✓ Loaded 1000 rows in 0.15 seconds
  Rate: 6666 rows/sec
```

### 4.2 Run Command

**Purpose:** Execute benchmark runs against one or more index variants.

**Syntax:**
```bash
python3 -m bench.bench run [--variant NAME] [--runs N]
```

**Options:**
- `--variant NAME`: Run a specific variant only (overrides `config/bench.yaml`)
- `--runs N`: Override number of runs per query (overrides `config/bench.yaml`)

**Examples:**

Test single variant with 5 runs:
```bash
python3 -m bench.bench run --variant V0_baseline_none --runs 5
```

Run all variants from config:
```bash
python3 -m bench.bench run
```

**What It Does:**
1. For each variant:
   - Applies the variant (creates indexes/generated columns)
   - Runs `ANALYZE` to update statistics
   - Warms up queries (if warmup > 0)
   - Executes benchmark runs
   - Captures EXPLAIN (ANALYZE, BUFFERS) plans
   - Reverts the variant
2. Computes statistics (p50, p95, p99 percentiles)
3. Exports results to `results/` directory

**Expected Output:**
```
Results will be written to: results/2026-01-02_1650

============================================================
Variant: V0_baseline_none
============================================================
Applying variant V0_baseline_none...
Running ANALYZE...
Warming up (5 runs per query)...
Running benchmarks (5 runs per query)...
  Query: Q1_name_substring
    p50: 0.32ms, p95: 0.33ms, p99: 0.33ms
  Query: Q2_name_common
    p50: 0.33ms, p95: 0.34ms, p99: 0.34ms
  ...
Reverting variant V0_baseline_none...
Writing results to results/2026-01-02_1650...
✓ Results written to results/2026-01-02_1650
```

---

## 5. Configuration Files

### 5.1 `config/seed.yaml`

Controls dataset generation parameters.

**Key Settings:**

| Setting | Description | Default |
|---------|-------------|---------|
| `seed` | Random seed for determinism | `12345` |
| `rows` | Number of rows to generate | `100000` |
| `distributions.brands` | Brand name weights | Weighted distribution |
| `distributions.categories` | Category weights | Weighted distribution |
| `token_injection.common_row_rate` | Rate of common token injection | `0.35` |
| `token_injection.rare_row_rate` | Rate of rare token injection | `0.03` |
| `description_tokens.min` | Min tokens per description | `20` |
| `description_tokens.max` | Max tokens per description | `60` |

**When to Modify:**
- Changing dataset size (rows)
- Adjusting data distributions
- Modifying token injection rates for different selectivity patterns

**Example:**
```yaml
seed: 12345
rows: 1000000  # Generate 1M rows instead of 100k
```

### 5.2 `config/bench.yaml`

Controls benchmark execution parameters.

**Key Settings:**

| Setting | Description | Default |
|---------|-------------|---------|
| `runs` | Number of benchmark runs per query | `50` |
| `warmup` | Warmup runs before benchmarking | `5` |
| `variants` | List of variants to test | All variants |
| `queries` | List of queries to run | All queries |
| `pagination.offset` | OFFSET for pagination queries | `100` |
| `pagination.limit` | LIMIT for pagination queries | `25` |

**When to Modify:**
- Testing subset of variants
- Testing subset of queries
- Adjusting run count for faster/slower benchmarks
- Changing pagination parameters

**Example:**
```yaml
variants:
  - V0_baseline_none
  - V2_trgm_name
  # Only test these two variants

runs: 10  # Fewer runs for faster testing
```

### 5.3 `.env` File

Environment variables for database connection.

**Required:**
```
DATABASE_URL=postgresql://user:password@host:port/database
```

**Default (matches docker-compose.yml):**
```
DATABASE_URL=postgresql://app:app_pw@localhost:5432/app_db
```

**Optional Overrides:**
```
SEED_CONFIG=config/seed.yaml
BENCH_CONFIG=config/bench.yaml
```

---

## 6. Variants Explained

Index variants are the independent variable in the experiment. Each variant applies different indexing strategies to the same dataset.

| Variant | Description | Indexes Created |
|---------|-------------|-----------------|
| **V0_baseline_none** | No indexes (baseline) | None (primary key only) |
| **V1_btree** | B-tree indexes | `btree(name)`, `btree(category, name)`, `btree(name, id)` |
| **V2_trgm_name** | GIN trigram on name | `gin(name gin_trgm_ops)` |
| **V3_trgm_search_text** | GIN trigram on search_text | `gin(search_text gin_trgm_ops)` |
| **V4_fts_search_vector** | GIN full-text search | `gin(search_vector)` |

**Note:** All variants use the same base table. The `search_text` column exists in all variants (defined in base schema). Variants only differ in which indexes are created.

---

## 7. Queries Explained

Fixed query patterns that test different search scenarios.

| Query | Description | Tests |
|-------|-------------|-------|
| **Q1_name_substring** | Selective substring search on `name` | `WHERE name ILIKE '%worc%'` |
| **Q2_name_common** | Common token search on `name` | `WHERE name ILIKE '%chicken%'` |
| **Q3_search_text_substring** | Multi-field search on `search_text` | `WHERE search_text ILIKE '%organic%'` |
| **Q4_search_plus_filter** | Search + category filter | `WHERE search_text ILIKE '%organic%' AND category = 'Snacks'` |
| **Q5_offset_pagination** | OFFSET-based pagination | `ORDER BY name OFFSET 100 LIMIT 25` |
| **Q6_keyset_pagination** | Keyset-based pagination | `WHERE (name, id) > (last_name, last_id) ORDER BY name, id LIMIT 25` |
| **Q7_fts_search** | Full-text search on `search_vector` | `WHERE search_vector @@ to_tsquery('english', 'organic')` |
| **Q8_fts_substring** | Selective full-text search on `search_vector` | `WHERE search_vector @@ to_tsquery('english', 'worc')` |

**Query Characteristics:**
- **Q1**: High selectivity (rare token)
- **Q2**: Low selectivity (common token)
- **Q3/Q4**: Multi-field search patterns
- **Q5/Q6**: Pagination comparison (OFFSET vs keyset)
- **Q7/Q8**: Full-text search using PostgreSQL's FTS (tests V4's GIN index on search_vector)
  - **Q7**: Non-selective FTS search with `to_tsquery()` (common term: "organic")
  - **Q8**: Selective FTS search with `to_tsquery()` (rare term: "worc", comparable to Q1)

---

## 8. Results Interpretation

### 8.1 Result Directory Structure

Results are written to timestamped directories:

```
results/
  2026-01-02_1650/
    metadata.json          # Configuration snapshot
    summary.csv           # Aggregated statistics
    raw_results.json      # All measurements
    plans/
      V0_baseline_none/
        Q1_name_substring.json
        Q2_name_common.json
        ...
```

**Directory Format:** `YYYY-MM-DD_HHMM`

### 8.2 Reading `summary.csv`

The summary CSV contains aggregated statistics for quick comparison.

**Columns:**

| Column | Description |
|--------|-------------|
| `variant` | Index variant name |
| `query` | Query name |
| `p50_ms`, `p95_ms`, `p99_ms` | Percentiles (median, 95th, 99th) |
| `min_ms`, `max_ms`, `mean_ms` | Min, max, average execution time |
| `scan_type` | Planner's scan type (Seq Scan, Index Scan, etc.) |
| `index_used` | Index name if used (empty if no index) |
| `estimated_rows` | Planner's row estimate |
| `actual_rows` | Actual rows returned |
| `shared_hit`, `shared_read` | Buffer cache statistics |

**Example Row:**
```csv
V0_baseline_none,Q1_name_substring,0.319,0.3294,0.33068,0.308,0.331,0.3182,Seq Scan,,1,0,0.0,0.0,...
```

**Interpretation:**
- Query Q1 on V0 took ~0.32ms (p50)
- Used Seq Scan (no index)
- Planner estimated 1 row, returned 0
- All data from cache (shared_hit > 0, shared_read = 0)

**Quick Analysis Commands:**

```bash
# View summary in table format
column -t -s, results/2026-01-02_1650/summary.csv

# Compare specific query across variants
grep "Q1_name_substring" results/*/summary.csv

# Find queries that used indexes
grep -v ",," results/2026-01-02_1650/summary.csv | cut -d, -f1,2,9
```

### 8.3 Understanding EXPLAIN Plans

Individual EXPLAIN plans are in `plans/<variant>/<query>.json`.

**Key Fields:**

| Field | Description |
|-------|-------------|
| `Plan.Node Type` | Operation type (Seq Scan, Index Scan, Bitmap Heap Scan) |
| `Plan.Actual Total Time` | Execution time in milliseconds |
| `Plan.Actual Rows` | Rows returned |
| `Plan.Rows Removed by Filter` | Rows scanned but filtered out |
| `Plan.Shared Hit Blocks` | Blocks read from cache |
| `Plan.Shared Read Blocks` | Blocks read from disk |
| `Planning Time` | Query planning time |
| `Execution Time` | Total execution time |

**Example Plan Analysis:**

```json
{
  "Plan": {
    "Node Type": "Seq Scan",
    "Actual Rows": 0,
    "Rows Removed by Filter": 1000,
    "Shared Hit Blocks": 120,
    "Shared Read Blocks": 0
  },
  "Execution Time": 0.331
}
```

**Interpretation:**
- Full table scan (Seq Scan)
- Scanned all 1000 rows, found 0 matches
- All data from cache (good - no disk I/O)
- Fast execution (0.33ms)

**What Good vs Bad Plans Look Like:**

- **Good:** Index Scan with low Actual Rows, high Shared Hit Blocks
- **Bad:** Seq Scan on large table, high Rows Removed by Filter, Shared Read Blocks > 0

---

## 9. Common Workflows

### 9.1 Quick Test Workflow

For rapid validation and testing:

```bash
# 1. Start database
cd infra && docker-compose up -d && cd ..

# 2. Quick seed (1000 rows)
python3 -m bench.bench seed --rows 1000

# 3. Test single variant
python3 -m bench.bench run --variant V0_baseline_none --runs 5

# 4. Check results
ls -la results/$(ls -t results/ | head -1)
cat results/$(ls -t results/ | head -1)/summary.csv
```

**Time:** ~1-2 minutes

### 9.2 Full Benchmark Workflow

For complete experiments:

```bash
# 1. Seed full dataset (100k rows from config)
python3 -m bench.bench seed

# 2. Run all variants from config
python3 -m bench.bench run

# 3. Analyze results
LATEST=$(ls -t results/ | head -1)
cat "results/$LATEST/summary.csv"
```

**Time:** ~10-30 minutes (depending on dataset size and run count)

### 9.3 Comparing Specific Variants

To compare only certain variants:

**Option 1: Modify `config/bench.yaml`**
```yaml
variants:
  - V0_baseline_none
  - V2_trgm_name
  - V3_trgm_search_text
```

**Option 2: Run variants separately and compare results**
```bash
# Run V0
python3 -m bench.bench run --variant V0_baseline_none

# Run V2
python3 -m bench.bench run --variant V2_trgm_name

# Compare results
diff results/2026-01-02_1650/summary.csv results/2026-01-02_1700/summary.csv
```

---

## 10. Troubleshooting

### 10.1 Common Errors

#### "No module named 'bench'"

**Problem:** Running script directly instead of as module.

**Solution:**
```bash
# ❌ Wrong
python3 bench/bench.py seed

# ✅ Correct
python3 -m bench.bench seed
```

#### "DATABASE_URL environment variable not set"

**Problem:** Missing `.env` file or environment variable.

**Solution:**
```bash
# Create .env file in project root
echo "DATABASE_URL=postgresql://app:app_pw@localhost:5432/app_db" > .env
```

#### "Connection refused" or "could not connect to server"

**Problem:** PostgreSQL not running.

**Solution:**
```bash
# Start PostgreSQL
cd infra && docker-compose up -d && cd ..

# Verify it's running
docker ps | grep pg_local

# Check logs if issues persist
docker logs pg_local
```

#### "column does not exist" or "relation does not exist"

**Problem:** Schema out of sync (e.g., after schema changes).

**Solution:**
```bash
# Re-seed the database
python3 -m bench.bench seed
```

#### "generation expression is not immutable"

**Problem:** Schema issue with generated columns (should be fixed in current version).

**Solution:** Ensure you're using the latest schema. If issue persists, check `sql/schema/00_schema.sql` uses `COALESCE` and `||` operators.

### 10.2 Debugging Tips

**Check PostgreSQL Status:**
```bash
docker ps | grep pg_local
docker logs pg_local
```

**Verify Database Connection:**
```bash
docker exec -it pg_local psql -U app -d app_db -c "SELECT version();"
```

**Check Table Exists and Row Count:**
```bash
docker exec -it pg_local psql -U app -d app_db -c "\dt"
docker exec -it pg_local psql -U app -d app_db -c "SELECT COUNT(*) FROM products;"
```

**View Recent Results:**
```bash
ls -t results/ | head -1
cat results/$(ls -t results/ | head -1)/summary.csv
```

**Check Query Plans:**
```bash
# View a specific plan
cat results/2026-01-02_1650/plans/V0_baseline_none/Q1_name_substring.json | python3 -m json.tool
```

---

## 11. Best Practices

1. **Always warm up before benchmarking**
   - Warmup runs ensure caches are populated
   - Configurable via `warmup` in `bench.yaml`

2. **Use consistent seeds for reproducibility**
   - Same seed + same row count = same dataset
   - Document seeds used in experiments

3. **Start with small datasets for testing**
   - Use `--rows 1000` for quick validation
   - Scale up to full dataset for final runs

4. **Check EXPLAIN plans to understand planner decisions**
   - Verify indexes are being used
   - Look for unexpected scan types
   - Compare estimated vs actual rows

5. **Compare percentiles, not just means**
   - p95 and p99 show worst-case performance
   - Means can hide outliers

6. **Run multiple variants for fair comparison**
   - Same dataset, same queries, only indexes differ
   - Ensures fair experimental design

7. **Document your configuration**
   - Note any config overrides
   - Record seed values used
   - Save metadata.json for reference

---

## 12. Advanced Usage

### Custom Configuration Overrides

Override config values via CLI:

```bash
# Override rows and seed
python3 -m bench.bench seed --rows 500000 --seed 99999

# Override runs per query
python3 -m bench.bench run --runs 100
```

### Running Subsets of Queries

Modify `config/bench.yaml`:

```yaml
queries:
  - Q1_name_substring
  - Q2_name_common
  # Only run these two queries
```

### Analyzing Specific Variants

Extract data for specific variant:

```bash
# Extract V0 results from summary
grep "V0_baseline_none" results/2026-01-02_1650/summary.csv

# Compare two variants side-by-side
grep -E "(V0_baseline_none|V2_trgm_name)" results/2026-01-02_1650/summary.csv
```

### Exporting Results for External Analysis

Results are in standard formats:

- **CSV**: Import into Excel, pandas, R
- **JSON**: Parse with any JSON library
- **EXPLAIN plans**: PostgreSQL standard format

**Example with pandas:**
```python
import pandas as pd
df = pd.read_csv('results/2026-01-02_1650/summary.csv')
df[df['variant'] == 'V0_baseline_none']
```

---

## Quick Reference

### Essential Commands

```bash
# Setup
pip install -r requirements.txt
cd infra && docker-compose up -d && cd ..
echo "DATABASE_URL=postgresql://app:app_pw@localhost:5432/app_db" > .env

# Quick test
python3 -m bench.bench seed --rows 1000
python3 -m bench.bench run --variant V0_baseline_none --runs 5

# Full benchmark
python3 -m bench.bench seed
python3 -m bench.bench run

# View results
cat results/$(ls -t results/ | head -1)/summary.csv
```

### File Locations

- **CLI tool**: `bench/bench.py`
- **Configs**: `config/seed.yaml`, `config/bench.yaml`
- **SQL**: `sql/schema/`, `sql/variants/`, `sql/queries/`
- **Results**: `results/YYYY-MM-DD_HHMM/`

---

## Getting Help

If you encounter issues:

1. Check the **Troubleshooting** section above
2. Review PostgreSQL logs: `docker logs pg_local`
3. Verify configuration files are valid YAML
4. Ensure database is running: `docker ps | grep pg_local`
5. Check environment variables: `cat .env`

For more details on the project architecture and design, see the project documentation.

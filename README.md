# search-under-constraints

A PostgreSQL indexing benchmark that measures how different index strategies perform under search-heavy workloads.

## What This Does

This project generates a deterministic dataset of product rows, applies five different indexing strategies one at a time, and runs eight fixed query patterns against each one. For every query execution it captures EXPLAIN (ANALYZE, BUFFERS) plans, latency percentiles (p50, p95, p99), and buffer cache statistics. The goal is to compare how B-tree, trigram, and full-text search indexes behave across different search patterns and table sizes (100k and 1M rows).

## Prerequisites

- Python 3.10+
- Docker and Docker Compose
- pip

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Start PostgreSQL via Docker:

```bash
cd infra && docker-compose up -d && cd ..
```

Create a `.env` file in the project root:

```
DATABASE_URL=postgresql://app:app_pw@localhost:5432/app_db
```

Verify the database is running:

```bash
docker exec -it pg_local psql -U app -d app_db -c "SELECT version();"
```

**Important:** Always run the CLI as a module with `python3 -m bench.bench`. Running `python3 bench/bench.py` directly will fail due to package import resolution.

## Commands

### seed

Generates and loads a deterministic dataset into PostgreSQL using COPY.

```bash
python3 -m bench.bench seed [--rows N] [--seed N]
```

`--rows` overrides the row count from `config/seed.yaml`. `--seed` overrides the random seed.

Example (quick test with 1,000 rows):

```bash
python3 -m bench.bench seed --rows 1000 --seed 12345
```

### run

Executes benchmark runs against one or more index variants.

```bash
python3 -m bench.bench run [--variant NAME] [--runs N]
```

`--variant` runs a single variant instead of all configured variants. `--runs` overrides the number of runs per query.

Example (test one variant with 5 runs):

```bash
python3 -m bench.bench run --variant V0_baseline_none --runs 5
```

To run the full benchmark across all variants:

```bash
python3 -m bench.bench run
```

## Index Variants

Each variant applies a different indexing strategy to the same base table. Only the indexes differ.

| Variant | Strategy | Indexes |
|---------|----------|---------|
| V0 | Baseline | None (primary key only) |
| V1 | B-tree | `btree(name)`, `btree(category, name)`, `btree(name, id)` |
| V2 | Trigram on name | `gin(name gin_trgm_ops)` |
| V3 | Trigram on search_text | `gin(search_text gin_trgm_ops)` |
| V4 | Full-text search | `gin(search_vector)` |

`search_text` and `search_vector` are generated columns defined in the base schema (`sql/schema/00_schema.sql`). They concatenate name, brand, category, and description into a single text field and a tsvector field respectively.

## Query Patterns

Eight fixed queries that cover different search scenarios:

| Query | Pattern | What It Tests |
|-------|---------|---------------|
| Q1_name_ilike_selective | `name ILIKE '%worc%'` | ILIKE on name with a rare token |
| Q2_name_ilike_common | `name ILIKE '%chicken%'` | ILIKE on name with a common token |
| Q3_search_text_ilike_common | `search_text ILIKE '%organic%'` | ILIKE on the generated search_text column with a common token |
| Q4_search_text_ilike_filtered | `search_text ILIKE '%organic%' AND category = ...` | Same as Q3 with a category filter |
| Q5_pagination_offset | `ORDER BY name OFFSET ... LIMIT 25` | Pagination using OFFSET |
| Q6_pagination_keyset | `WHERE (name, id) > (...) ORDER BY name, id LIMIT 25` | Pagination using keyset |
| Q7_fts_common | `search_vector @@ to_tsquery('organic')` | Full-text search with a common token |
| Q8_fts_selective | `search_vector @@ to_tsquery('worc')` | Full-text search with a rare token |

## Configuration

**`config/seed.yaml`** controls data generation: row count, random seed, brand/category distributions, and token injection rates that determine how often common and rare terms appear in the dataset.

**`config/bench.yaml`** controls benchmark execution: number of runs per query, warmup count, which variants and queries to include, and pagination parameters.

## Results

Each benchmark run writes results to a timestamped directory under `results/`:

```
results/2026-02-05_1752/
  metadata.json       # configuration snapshot
  summary.csv         # aggregated statistics per variant/query
  raw_results.json    # all individual measurements
  plans/
    V0_baseline_none/
      Q1_name_ilike_selective.json
      ...
```

`summary.csv` contains p50/p95/p99 latencies, scan types, index usage, planner row estimates vs actuals, and buffer cache hit/read counts for every variant/query combination.

## Troubleshooting

**"No module named 'bench'"**: You ran the script directly. Use `python3 -m bench.bench` instead of `python3 bench/bench.py`.

**"DATABASE_URL environment variable not set"**: Create a `.env` file in the project root with `DATABASE_URL=postgresql://app:app_pw@localhost:5432/app_db`.

**"Connection refused"**: PostgreSQL is not running. Start it with `cd infra && docker-compose up -d && cd ..` and verify with `docker ps | grep pg_local`.

## What I Learned

I documented my expectations going in, the observed behavior, key deviations from those expectations, and what it changed about my mental model of how PostgreSQL uses indexes. See [WHAT_I_LEARNED.md](WHAT_I_LEARNED.md).

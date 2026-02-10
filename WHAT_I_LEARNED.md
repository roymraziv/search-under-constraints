## Overall Expectations
- I expected that for common-name substring searches, the planner would frequently favor sequential scans over index access, since low selectivity would require touching a large percentage of the table and sequential I/O would be cheaper per page than traversing large index posting lists.
- I expected the planner to tolerate sequential scans more often at smaller table sizes (100k rows), where the absolute cost of scanning the table is relatively low and index access provides less marginal benefit.
- I expected that at larger table sizes (1M rows), index usage would become more sensitive to token selectivity, with the planner remaining unlikely to choose index scans for low-cardinality or high-frequency terms despite the increased table size.

---

## Expectations by Query
### Selective Substring Queries
- I expect the planner to favor index-backed access for selective substring searches, since high selectivity allows the index to significantly reduce the number of heap pages that must be visited compared to a full table scan.
- I expect trigram indexes to remain effective at larger table sizes (1M rows), where the cost of sequential scanning grows linearly while index access continues to benefit from small posting lists and limited heap access.
- I expect schemas without trigram indexes to perform poorly for these queries, particularly at larger scales, as the planner will be unable to optimize beyond sequential scans despite high selectivity.
### Non-Selective Substring Queries
- I expect the planner to favor sequential scans over index-based access for common substring terms, since low selectivity requires touching a large portion of the table and the overhead of traversing large trigram posting lists and heap pages outweighs any index benefit.
- I expect limited performance differentiation across index strategies for these queries, as the planner will consistently chooses sequential scans regardless of available indexes.
- I also expect planner row estimates to be less reliable for high-frequency terms, which may further reinforce the preference for sequential scans.
### Search + Filter Queries
- I expect the planner to favor B-tree index access for ordering when a suitable index exists, since a B-tree can deliver rows in sorted order and eliminate the need to materialize and explicitly sort the result set.
- I expect schemas without supporting B-tree indexes to perform inefficiently for these queries, as the database must sort the result set explicitly after filtering, incurring additional memory and overhead.
- I expect both OFFSET and keyset pagination to lose their performance advantages in schemas without appropriate ordering indexes, since the database cannot efficiently jump to the correct position in the ordered result set or terminate early.

### Tokenized Full-Text Search Queries
- I expect schemas without a supporting GIN index on the search_vector to perform sub-optimally, as the database must fall back to sequential scans and evaluate each row to determine whether it matches the full-text search criteria.
- I expect highly selective full-text search queries with a supporting GIN index to be effective compared to other search strategies, since tokenization and normalization are performed ahead of time and the planner can rely on the index to identify matching rows directly rather than scanning the full table.

---
## Index / Schema Strategy Expectations
### Baseline (Primary Key Only)
- I expect both selective and non-selective search queries to perform poorly on this schema, since the planner has no usable index to exploit and cannot optimize beyond a sequential scan regardless of selectivity.
- I expect non-selective search queries to behave similarly to selective ones in this schema, as the lack of supporting indexes forces the planner to scan the table in full and makes selectivity largely irrelevant to execution strategy.

### Trigram Index Variants
- I expect the planner to favor trigram index–backed access for selective substring search queries, since high selectivity allows the index to substantially reduce the number of heap pages visited compared to a full table scan.
- I expect trigram indexes to provide little to no benefit for either keyset or OFFSET-based pagination queries, as these queries depend primarily on ordering semantics rather than fuzzy text matching, which trigram indexes do not support.
- I expect the planner’s willingness to use trigram indexes to be highly dependent on token frequency, with diminishing returns as selectivity decreases.

### Generated Columns
- I expect queries against the generated search_text column to exhibit similar read performance to equivalent substring searches on individual text columns, since the planner ultimately relies on the same trigram index mechanics and operator classes.
- I expect the primary benefit of the generated column to be query simplification and multi-field search support, rather than improvements in planner behavior or execution performance.
- I also expect this schema to incur higher write and maintenance costs due to the need to recompute and index the generated column on row updates.

### Full Text Search
- I expect the presence of a supporting GIN index to be the primary differentiating factor for full-text search performance, since the planner can rely on the index to identify matching rows directly, at the cost of additional write-time overhead to maintain the search_vector and its index.
- I expect the planner to still favor sequential scans for full-text searches on non-selective terms, as large posting lists would require visiting a significant portion of the table and make index-backed access less efficient than scanning the table sequentially.

---

## Observed Behavior
- The planner consistently chose Gather > Seq Scan on all queries without a supporting index at both 100k and 1M rows. Without an index whose operator class matches the query's operators, there is no alternative access path.
- Whenever a query's operator class had a supporting index, the planner chose Bitmap Heap Scan every time — even for low-selectivity terms like "organic" that match ~3.5% of the table. The planner never fell back to a sequential scan when an operator-class-compatible index existed.
- The fastest query was Q8 (selective FTS term "worc") on V4 (GIN index on search_vector), completing in ~0.003 ms with near-zero shared_read. The index resolved to zero matching rows almost entirely from cache.
- The slowest queries were ILIKE searches on search_text without a trigram index on that column — Q3 on V0 (~2,039 ms) and Q3 on V2 (~2,221 ms), both falling back to sequential scans across the full concatenated text column.
- Planner row estimates varied significantly. V0 Q3 estimated 70,667 rows versus 34,730 actual (2x overestimate). V1 Q3 estimated 10,096 versus 34,730 actual (3.4x underestimate). V3 Q3, with the trigram index providing better statistics, estimated 40,405 versus 34,730 actual so much closer. Index statistics meaningfully improved planner accuracy.
- P99 latency diverged more from P50 on the 1M row table. V4 Q3, for instance, had a min of 711 ms and a max of 897 ms. V3 Q8 ranged from 69 ms to 115 ms. At smaller table sizes the variance was tighter, suggesting that cache pressure at 1M rows introduces more run-to-run variability.
- A small number of queries on V0 triggered non-zero shared_written (dirty page flushes to disk), specifically Q2 (~3 shared_written) and Q3. These long-running sequential scans occupied buffer pages long enough for the background writer to evict and flush them during execution.
- Indexed queries with high selectivity (V2 Q1, V4 Q8) achieved near-100% shared buffer hit ratios with essentially zero shared_read. Unindexed queries on the 1M table showed hit ratios around 10–12%, with ~268k shared_read pages per execution, nearly the entire table read from disk on each run.

### Key Deviations from Expectations
- Q7 (FTS for "organic") on V4 scaled far worse than expected. The GIN index provided a 13.2x speedup at 100k rows but only 1.9x at 1M rows. The planner still chose the index at 1M, but the speedup collapsed because the bitmap had to visit ~3.5% of the heap via random I/O, exceeding shared buffer capacity.
- Q3 on V3 exhibited a steeper scaling factor (1M time / 100k time) than Q3 on schemas without the trigram index. The indexed Bitmap Heap Scan plan's cost grew super-linearly as the ~3.5% matching rows (~34,730 tuples) exceeded cache at 1M, while the sequential scan plans on V0 and V2 scaled more linearly due to sequential I/O. V3's absolute time was still faster, but its rate of degradation with table size was worse.
- At 100k rows, only V3 (trigram on search_text) showed non-zero shared_read. The trigram index introduced random heap access that could not be fully satisfied from cache even at 100k, while other schemas' sequential scans at that scale had working sets small enough for shared buffers and OS cache to absorb all I/O.
- The planner never chose a trigram index for pagination queries (Q5/Q6) on any schema, confirming that trigram indexes do not support ordering semantics and provide no benefit for sorted access patterns.


### Why the System Behaved this Way
- Q7's scaling degradation comes down to cache capacity. At 100k rows, the GIN index on search_vector returns ~3,500 matching tids, and the corresponding heap pages fit comfortably in shared buffers — most reads are cache hits. At 1M rows, the same ~3.5% selectivity means ~35,000 matches spread across ~31,000 heap pages of random I/O. That working set exceeds cache, so the indexed plan pays for disk reads that a sequential scan would avoid entirely via prefetching and OS readahead.
- Q3 on V3 follows the same mechanism. The trigram index returns a bitmap covering ~3.5% of the table. At smaller table sizes, the bitmap is exact and the heap pages are cached. At 1M rows, the bitmap may become lossy (falling back to page-level granularity and forcing recheck), the heap working set spills out of cache, and per-match cost increases. Sequential scan avoids all of this — its cost grows linearly with table size because the I/O pattern is sequential and predictable.
- V3's shared_read at 100k rows exists because the trigram index forces a random-access pattern that cannot be fully satisfied from shared buffers even at that scale. Other schemas at 100k use sequential scans with working sets small enough for OS readahead and shared buffers to absorb all I/O, resulting in zero shared_read.

### What this Changed About my Mental Model
- An index existing does not guarantee it improves scaling. The planner will use an operator-class-compatible index, but the indexed plan can scale worse than a sequential scan when selectivity is low and the heap working set exceeds cache.
- The planner's decision to use an index is not the same as the index being beneficial at scale. The planner optimizes for estimated cost at the current table size, not for how that cost grows as the table grows. An index that wins at 100k rows may barely help at 1M.
- Sequential scans have a hidden advantage: linear, predictable I/O that benefits from OS readahead and prefetching. This makes them surprisingly competitive for low-selectivity queries even at large table sizes, because their cost grows proportionally to table size without the cache cliffs that random-access plans hit.
- Buffer cache capacity is the critical inflection point. The same indexed plan went from 13x faster at 100k rows to 1.9x faster at 1M rows purely because of cache pressure — the index, the query, and the selectivity did not change. Understanding where that inflection occurs for a given workload is more important than knowing whether an index exists.

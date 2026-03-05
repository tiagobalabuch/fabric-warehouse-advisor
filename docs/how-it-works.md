# How It Works

The advisor runs a **7-phase pipeline** that collects metadata, analyses
query patterns, estimates cardinality, and produces scored recommendations.
Everything runs inside a single Fabric Spark notebook session.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fabric Spark Notebook                        │
│                                                                 │
│  DataClusteringAdvisor.run()                                    │
│    │                                                            │
│    ├── Phase 1: Metadata        ──► sys.tables/columns/types    │
│    ├── Phase 2: Clustering      ──► sys.indexes/index_columns   │
│    ├── Phase 3: Row Counts      ──► COUNT_BIG(*) per table      │
│    ├── Phase 4: Query Patterns  ──► queryinsights.*              │
│    ├── Phase 5: Predicates      ──► regex parser                │
│    ├── Phase 6: Cardinality     ──► APPROX_COUNT_DISTINCT       │
│    └── Phase 7: Scoring         ──► composite score + reports   │
│                                                                 │
│  All SQL runs via T-SQL passthrough (no data transfer to Spark) │
└─────────────────────────────────────────────────────────────────┘
```

## Phase 1: Metadata Collection

Reads the warehouse's system catalog views via T-SQL passthrough:

- `sys.tables` — user tables
- `sys.schemas` — schema names
- `sys.columns` — column metadata (name, type, length, precision)
- `sys.types` — data type names

Produces a DataFrame with one row per column across all user tables.

If `table_names` is specified in the config, the metadata is filtered
immediately to include only the requested tables.

## Phase 2: Current Clustering Configuration

Reads the current `CLUSTER BY` configuration:

- `sys.indexes` — index definitions
- `sys.index_columns` — columns in each index, filtered by
  `data_clustering_ordinal > 0`

This phase also emits **early warnings** for potentially sub-optimal
clustering choices:

- `char`/`varchar` columns with `max_length > 32` (only the first 32
  characters produce column statistics)
- `decimal`/`numeric` with `precision > 18` (predicates won't push down
  to storage)
- Unsupported data types (`bit`, `varbinary`, `uniqueidentifier`)
- Tables exceeding `max_clustering_columns` clustered columns

## Phase 3: Row Counts

Counts rows per table using T-SQL passthrough:

```sql
SELECT COUNT_BIG(*) AS cnt FROM [schema].[table]
```

This runs **inside the SQL engine** — Fabric Warehouse resolves
`COUNT_BIG(*)` from columnstore metadata, so it completes almost
instantly even on billion-row tables. No data is transferred to Spark.

Tables below `min_row_count` are excluded from further analysis.

## Phase 4: Query Pattern Analysis

Reads `queryinsights.frequently_run_queries` (enabled by default on
every Fabric Warehouse) and categorises queries into:

- **WHERE queries** — contain a `WHERE` clause and reference large tables
- **Full-scan queries** — reference large tables but have no `WHERE` clause

Both types are valuable signals:

- WHERE queries identify specific columns used for filtering
- Full-scan queries indicate tables that would benefit from any
  clustering (reducing I/O even without a specific predicate)

Queries are filtered by `min_query_runs` and internal system queries
(like `COUNT_BIG(*)` from Phase 3) are excluded automatically.

## Phase 5: Predicate Extraction

Parses the SQL text of WHERE queries using a regex-based heuristic to
identify which columns appear in predicates.

The parser:

1. Extracts `WHERE` clauses (everything between `WHERE` and
   `ORDER BY`/`GROUP BY`/`HAVING`/`UNION`/`;`/end-of-string)
2. Finds identifiers in the clause text
3. Matches them against known columns from Phase 1
4. Detects comparison operators (`=`, `>`, `<`, `BETWEEN`, `IN`, `LIKE`, etc.)

The parser handles:

- Bracket-quoted identifiers: `[schema].[table].[column]`
- Two-part and three-part names
- Multiple WHERE clauses in a single query (e.g., subqueries)

Results are aggregated into a dictionary of
`(schema, table, column) → total_weighted_hits`, weighted by each
query's `number_of_runs`.

## Phase 6: Cardinality Estimation

Estimates distinct value counts for candidate columns using T-SQL
passthrough:

```sql
SELECT COUNT_BIG(*) AS total,
       APPROX_COUNT_DISTINCT([column]) AS distinct_cnt
FROM [schema].[table]
```

`APPROX_COUNT_DISTINCT` runs server-side using HyperLogLog — it's
fast and accurate enough for classification without transferring any
data to Spark.

Cardinality is estimated for:

- All columns that appeared in WHERE predicates (Phase 5)
- All columns currently in a `CLUSTER BY` (Phase 2)
- All supported-type columns on full-scan tables, using a **batch query**
  that estimates multiple columns in a single T-SQL call

If T-SQL passthrough fails for any reason, the advisor falls back to
reading through the Spark connector (slower, but reliable).

## Phase 7: Scoring & Recommendations

Combines all signals into a composite score per column and groups
results into per-table recommendations. See [Scoring](scoring.md)
for the detailed formulas.

This phase produces:

- A sorted list of `ColumnScore` objects
- A list of `TableRecommendation` objects (grouped by table)
- Three report formats: text, Markdown, HTML
- An optional per-column CTAS DDL (when `generate_ctas=True`)

## Data Flow

```
Phase 1 (metadata) ──────────────┐
Phase 2 (clustering) ────────────┤
Phase 3 (row counts) ────────────┼──► Phase 7 (scoring)
Phase 4 (queries) ──► Phase 5 ───┤        │
Phase 6 (cardinality) ───────────┘        ▼
                                    AdvisorResult
                                    ├── all_scores
                                    ├── recommendations
                                    ├── scores_df
                                    ├── text_report
                                    ├── markdown_report
                                    └── html_report
```

## Performance Characteristics

| Phase | Method | Data Transfer | Speed |
|-------|--------|---------------|-------|
| 1. Metadata | T-SQL passthrough | Metadata only (~KB) | Instant |
| 2. Clustering | T-SQL passthrough | Metadata only (~KB) | Instant |
| 3. Row Counts | T-SQL `COUNT_BIG(*)` | None (resolved from metadata) | Instant |
| 4. Query Patterns | T-SQL passthrough | Query text only (~KB-MB) | Fast |
| 5. Predicates | Local regex | None (in-memory) | Instant |
| 6. Cardinality | T-SQL `APPROX_COUNT_DISTINCT` | None (computed server-side) | Fast |
| 7. Scoring | Local computation | None (in-memory) | Instant |

The entire pipeline typically completes in **under 2 minutes** even for
warehouses with hundreds of tables, because no user data is ever
transferred to Spark.

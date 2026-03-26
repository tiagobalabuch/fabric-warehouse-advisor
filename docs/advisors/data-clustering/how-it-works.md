# How It Works

The advisor runs a **multi-phase pipeline** that validates the warehouse edition, collects metadata, analyses query patterns, estimates cardinality, and produces scored recommendations.  Everything runs inside a single Fabric notebook session.

## Architecture Overview

```text

  DataClusteringAdvisor.run()                                  
  │                                                            
  ├─ Phase 0: Edition → DATABASEPROPERTYEX (gate check)        
  ├─ Phase 1: Metadata        → sys.tables/columns/types       
  ├─ Phase 2: Clustering      → sys.indexes/index_columns      
  ├─ Phase 3: Row Counts      → COUNT_BIG(*) per table         
  ├─ Phase 4: Query Patterns  → queryinsights.*                
  ├─ Phase 5: Predicates      → regex parser                   
  ├─ Phase 6: Cardinality     → APPROX_COUNT_DISTINCT (batch)  
  └─ Phase 7: Scoring         → composite score + reports
       
```
!!! note 
  All SQL runs via T-SQL passthrough (no data transfer to      
  Spark).  A configurable phase_delay between phases reduces   
  HTTP 429 throttling from the Fabric control-plane API.    

## Phase 0: Edition Detection

Runs a gating check to determine whether the connected Fabric item is a
**DataWarehouse** or a **SQL Analytics Endpoint** (Lakehouse):

If the edition is not `DataWarehouse`, the advisor **aborts immediately**
with a clear error message — data clustering is only supported on Fabric
Data Warehouse, not on SQL Analytics Endpoints.

## Phase 1: Metadata Collection

Reads the warehouse's system catalog views via T-SQL passthrough:

- `sys.tables` — user tables
- `sys.schemas` — schema names
- `sys.columns` — column metadata (name, type, length, precision)
- `sys.types` — data type names

Produces a DataFrame with one row per column across all user tables.

**Scope filtering** is applied immediately in this phase:

- If `schema_names` is configured, only matching schemas are kept
- If `table_names` is configured, only matching tables are kept (supports
  both `"table_name"` and `"schema.table_name"` formats)

If no tables match the configured scope filters, the advisor **exits
early** — skipping Phases 2–7 and returning an empty result.  This
avoids unnecessary SQL round-trips.

## Phase 2: Current Clustering Configuration

Reads the current `CLUSTER BY` configuration:

- `sys.indexes` — index definitions
- `sys.index_columns` — columns in each index, filtered by
  `data_clustering_ordinal > 0`

The same scope filters from Phase 1 are applied so only selected tables
are inspected.

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

This runs **inside the SQL engine** — one query per table. No user data
is transferred to Spark, only the resulting count (~KB per table).

Tables below `min_row_count` (default: 1,000,000) are excluded from
further analysis.

## Phase 4: Query Pattern Analysis

Reads `queryinsights.frequently_run_queries` (enabled by default on
every Fabric Warehouse) and categorises queries into:

- **WHERE queries** — contain a `WHERE` clause and reference large tables
- **Full-scan queries** — reference large tables but have no `WHERE` clause

Both types are valuable signals:

- WHERE queries identify specific columns used for filtering
- Full-scan queries indicate tables that would benefit from any clustering (reducing I/O even without a specific predicate)

Queries are filtered by `min_query_runs` and internal system queries
(like `COUNT_BIG(*)` from Phase 3) are excluded automatically.

Full-scan query activity is tracked per table with weighted run counts
(each query's `number_of_runs` is summed) — this feeds into the scoring
in Phase 7.

## Phase 5: Predicate Extraction

Parses the SQL text of WHERE queries using a regex-based heuristic to
identify which columns appear in predicates.

The parser:

1. Extracts `WHERE` clauses (everything between `WHERE` and
   `ORDER BY`/`GROUP BY`/`HAVING`/`UNION`/`;`/`end of the query text`)
2. Finds identifiers in the clause text
3. Matches them against known columns from Phase 1
4. Detects comparison operators (`=`, `>`, `<`, `BETWEEN`, `IN`, `LIKE`, etc.)

The parser handles:

- Bracket-quoted identifiers: `[schema].[table].[column]`
- Two-part and three-part names
- Multiple WHERE clauses in a single query (e.g., subqueries)

## Phase 6: Cardinality Estimation

Estimates distinct value counts for candidate columns using **batched**
T-SQL passthrough — one query per table covering all candidate columns:

```sql
SELECT COUNT_BIG(*) AS total,
       APPROX_COUNT_DISTINCT([col1]) AS col1_distinct,
       APPROX_COUNT_DISTINCT([col2]) AS col2_distinct,
       ...
FROM [schema].[table]
```

`APPROX_COUNT_DISTINCT` runs inside the SQL engine using HyperLogLog — it's
fast and accurate enough for classification without transferring any
data to Spark.

### Candidate Selection

Columns are batched by table.  Three sources contribute candidates:

1. **Predicate columns** — columns that appeared in WHERE predicates (Phase 5)
2. **Currently clustered columns** — columns already in a `CLUSTER BY` (Phase 2)
3. **Full-scan table columns** — for tables identified as full-scan in
   Phase 4, all columns with supported data types are included.  Data type
   eligibility is evaluated by the `data_type_support` module

### Parallel Execution

Cardinality estimation supports parallel execution controlled by
`max_parallel_tables` (default: 4).  Each table gets its own thread
running a single batched query.  Higher values reduce wall-clock time
but increase concurrent SQL sessions on the warehouse.  Set to `1` to
disable parallelism.

### Failure Handling

If cardinality estimation fails for a table (e.g., query timeout or
transient error), the advisor logs a warning and continues. Failed
tables are tracked and a summary is printed:

```text
⚠ Cardinality estimation failed for 2 table(s): dbo.Orders, dbo.LineItems
  These tables may receive lower scores due to missing cardinality data.
```

The advisor never aborts due to cardinality failures — scoring proceeds
with whatever data was successfully collected.

## Phase 7: Scoring & Recommendations

Combines all signals into a composite score per column and groups
results into per-table recommendations. See [Scoring](scoring.md) for the detailed formulas.

This phase produces:

- A sorted list of `ColumnScore` objects
- A list of `TableRecommendation` objects (grouped by table)
- Three report formats: **text**, **Markdown**, **HTML**
- An optional per-column CTAS DDL (when `generate_ctas=True`)

The HTML report includes workspace metadata (workspace name, capacity SKU) when available from the REST client.

### Saving Reports

The `DataClusteringResult` object returned by `advisor.run()` includes a
`.save()` method:

```python
result.save("report.html")                  # default: HTML
result.save("report.md", format="md")       # Markdown
result.save("report.txt", format="txt")     # plain text
```

## Phase Tracking

All phases are timed using a `PhaseTracker` that records each phase's
name, elapsed time, and status (completed / skipped / failed).  At the
end of the run, a summary table is printed:

```text
Phase Summary
─────────────────────────────────────────────
Phase 0: Edition detection     0.42s   (1%)
Phase 1: Metadata              1.23s   (4%)
Phase 2: Current clustering    0.87s   (3%)
Phase 3: Row counts            2.15s   (7%)
Phase 4: Query patterns        3.41s  (11%)
Phase 5: Predicate columns     0.12s   (0%)
Phase 6: Cardinality          18.76s  (60%)
Phase 7: Scoring & reports     4.32s  (14%)
─────────────────────────────────────────────
Total                         31.28s
```

## Performance Characteristics

| Phase | Method | Data Transfer | Speed |
|-------|--------|---------------|-------|
| 0. Edition | T-SQL `DATABASEPROPERTYEX` | Metadata only (~bytes) | Instant |
| 1. Metadata | T-SQL passthrough | Metadata only (~KB) | Instant |
| 2. Clustering | T-SQL passthrough | Metadata only (~KB) | Instant |
| 3. Row Counts | T-SQL `COUNT_BIG(*)` | Count per table (~KB) | Fast |
| 4. Query Patterns | T-SQL passthrough | Query text only (~KB-MB) | Fast |
| 5. Predicates | Local regex | None (in-memory) | Instant |
| 6. Cardinality | T-SQL `APPROX_COUNT_DISTINCT` (batched) | None (computed server-side) | Fast (parallel) |
| 7. Scoring | Local computation | None (in-memory) | Instant |

No user data is ever transferred to Spark — only metadata, counts, and
aggregates.

Overall execution time depends primarily on the **number of tables** and
the **number of columns per table**, since Phases 3 and 6 issue one or
more T-SQL queries per table.  Phase 6 is typically the longest phase
due to the `APPROX_COUNT_DISTINCT` computation — parallel execution
(`max_parallel_tables`) significantly reduces wall-clock time for
warehouses with many tables.

A configurable `phase_delay` (default: 1 second) is inserted between
phases to reduce HTTP 429 throttling from the Fabric control-plane API.
Set to `0` to disable.

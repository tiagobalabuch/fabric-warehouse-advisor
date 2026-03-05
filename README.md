# Fabric Warehouse Data Clustering Advisor

A PySpark **library** (installable wheel) that assesses and recommends which
tables and columns should use **Data Clustering** in Microsoft Fabric Warehouse.

It runs entirely inside a **Fabric Spark notebook** — the Synapse SQL connector
comes pre-installed in the Fabric runtime, Query Insights is enabled by default
on every warehouse, and no Lakehouse or data source attachment is required.

## Installation

### Build the wheel

```bash
pip install build
python -m build          # produces dist/fabric_data_clustering_advisor-0.2.0-py3-none-any.whl
```

### Install in a Fabric Notebook

Upload the `.whl` file to your Lakehouse **Files** area and install it in the
first notebook cell:

```python
%pip install /lakehouse/default/Files/fabric_data_clustering_advisor-0.2.0-py3-none-any.whl
```

Or attach it via the Fabric **Environment** resource and it will be
pre-installed on every Spark session.

## Quick Start

```python
from fabric_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

# Only warehouse_name is required when running in the same workspace.
# For cross-workspace access, workspace_id and warehouse_id are also required.
config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",

    # --- Cross-workspace (required only when targeting a different workspace) ---
    # workspace_id="<workspace-guid>",      # mandatory for cross-workspace
    # warehouse_id="<warehouse-item-guid>",  # mandatory for cross-workspace

    # --- Scope to specific tables (optional) ---
    # table_names=["dbo.Orders", "FactSales"],  # empty = all tables

    # --- Override any defaults you need ---
    # min_row_count=1_000_000,
    # large_table_rows=50_000_000,
    # min_predicate_hits=2,
    # min_query_runs=2,
    # generate_ctas=False,      # set True to generate CTAS DDL output
    # verbose=False,             # set True for detailed debug output
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()
```

### Working with the results

```python
# Printable text report (auto-printed during run)
print(result.text_report)

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)

# Spark DataFrame with per-column scores
result.scores_df.show()

# Save reports to files
result.save("/lakehouse/default/Files/reports/advisor_report.html")          # HTML
result.save("/lakehouse/default/Files/reports/advisor_report.md", "md")      # Markdown
result.save("/lakehouse/default/Files/reports/advisor_report.txt", "txt")    # Plain text

# Save scores to a Lakehouse Delta table
result.scores_df.write.mode("overwrite").format("delta").saveAsTable(
    "default.clustering_advisor_scores"
)

# Python objects for programmatic access
for rec in result.recommendations:
    print(rec.cluster_by_ddl)
```

## How It Works

The advisor runs **7 phases** to produce scored recommendations:

| Phase | What it does |
|-------|-------------|
| 1. **Metadata Collection** | Reads `sys.tables`, `sys.schemas`, `sys.columns`, `sys.types` via the Fabric Spark connector |
| 2. **Current Clustering** | Reads `sys.indexes` / `sys.index_columns` to show what's already clustered |
| 3. **Row Counts** | Counts rows per table; filters out small tables below `min_row_count` |
| 4. **Query Patterns** | Reads `queryinsights.frequently_run_queries`; tracks WHERE and full-scan queries on large tables |
| 5. **Predicate Extraction** | Parses SQL text with regex to identify columns used in `WHERE` filters |
| 6. **Cardinality Estimation** | Uses T-SQL `APPROX_COUNT_DISTINCT()` pushed down to the SQL engine (no data transferred to Spark) |
| 7. **Scoring** | Combines all signals into a 0–100 composite score, applies cardinality penalties, and generates per-column CTAS DDL |

### Scoring Breakdown (default weights — all configurable)

The raw composite score is the sum of four weighted factors:

| Factor | Max Points | Config Parameter |
|--------|-----------|------------------|
| Table size | 30 | `score_weight_table_size` |
| Predicate frequency | 30 | `score_weight_predicate_freq` |
| Cardinality | 25 | `score_weight_cardinality` |
| Data type support | 15 | `score_weight_data_type` |

#### Cardinality Penalty

After computing the raw composite, a **penalty multiplier** is applied based
on cardinality classification — because a column that is unsuitable for
clustering should never score high regardless of how large or frequently
queried the table is:

| Cardinality | Multiplier | Effect |
|-------------|-----------|--------|
| High | 1.0× | No penalty — ideal for clustering |
| Medium | 1.0× | No penalty — suitable for clustering |
| Low | 0.35× | Heavy penalty — too few distinct values |
| Unknown | 0.70× | Moderate penalty — could not be estimated |

For example, a column with a raw score of 77 but Low cardinality receives
a final score of `77 × 0.35 = 26`, which falls well below the default
threshold of 40.

#### Already-Clustered Column Validation

Columns that are already part of a `CLUSTER BY` are evaluated with the
same scoring logic.  If a clustered column has low cardinality, an
unsupported data type, or scores below the threshold, the report raises
an explicit **warning** recommending its removal.

## Configuration Reference

All parameters are fields of `DataClusteringAdvisorConfig` with defaults:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `warehouse_name` | `str` | `""` | **Required.** Fabric Warehouse name. |
| `workspace_id` | `str` | `""` | Workspace GUID for cross-workspace access. |
| `warehouse_id` | `str` | `""` | Warehouse item GUID for cross-workspace access. |
| `min_row_count` | `int` | `1_000_000` | Minimum rows for a table to be analysed. |
| `large_table_rows` | `int` | `50_000_000` | Row-count threshold for full table-size score. |
| `min_predicate_hits` | `int` | `2` | Minimum WHERE hits for a column to be a candidate. |
| `min_query_runs` | `int` | `2` | Minimum query executions to count as "frequent". |
| `low_cardinality_upper` | `float` | `0.001` | Ratio below which cardinality is "Low". |
| `high_cardinality_lower` | `float` | `0.05` | Ratio at/above which cardinality is "High". |
| `low_cardinality_abs_max` | `int` | `50` | Distinct count always classified as "Low". |
| `cardinality_sample_fraction` | `float` | `1.0` | Fraction of table to sample (1.0 = full read). |
| `score_weight_table_size` | `int` | `30` | Max points for table size. |
| `score_weight_predicate_freq` | `int` | `30` | Max points for predicate frequency. |
| `score_weight_cardinality` | `int` | `25` | Max points for cardinality. |
| `score_weight_data_type` | `int` | `15` | Max points for data-type support. |
| `max_clustering_columns` | `int` | `3` | Warn when a table already exceeds this many clustered columns. |
| `min_recommendation_score` | `int` | `40` | Minimum score to surface a recommendation. |
| `generate_ctas` | `bool` | `False` | Generate per-column CTAS DDL. Set `True` to include DDL in the report. |
| `table_names` | `list[str]` | `[]` | List of tables to analyse. Use `"schema.table"` or `"table"`. Empty = all tables. |
| `verbose` | `bool` | `False` | Print structured debug output for each phase. |

> **Note:** Score weights must sum to 100. The config validates this at runtime.

## Package Structure

```
src/fabric_data_clustering_advisor/
├── __init__.py            # Public API exports
├── config.py              # DataClusteringAdvisorConfig dataclass
├── advisor.py             # DataClusteringAdvisor orchestrator class
├── data_type_support.py   # Data-type eligibility rules
├── warehouse_reader.py    # Spark connector wrappers for sys views
├── predicate_parser.py    # SQL text → predicate column extraction
├── scoring.py             # Composite scoring + DDL generation
└── report.py              # Text, Markdown & HTML report generators
```

## Data Type Support Reference

| Category | Data Type | Clustering Supported |
|----------|-----------|---------------------|
| Exact numerics | `bigint`, `int`, `smallint`, `decimal`*, `numeric`* | Yes |
| Exact numerics | `bit` | No |
| Approximate numerics | `float`, `real` | Yes |
| Date/time | `date`, `datetime2`, `time` | Yes |
| Character strings | `char`, `varchar` (not MAX) | Yes** |
| LOB types | `varchar(max)`, `varbinary(max)` | No |
| Binary strings | `varbinary`, `uniqueidentifier` | No |

\* `decimal`/`numeric` with precision > 18: predicates won't push down to storage.
\** `char`/`varchar`: only the first 32 characters are used for column statistics.

## Best Practices

- Data clustering works best on **large tables**
- Choose columns with **mid-to-high cardinality** used in **WHERE** filters
- Batch DML operations (≥ 1 M rows) for optimal clustering quality
- **Equality join conditions** (`=` joins) do **NOT** benefit from clustering
- Don't cluster on more columns than necessary
- Column order in `CLUSTER BY` doesn't affect row storage
- For `char`/`varchar`, only the first 32 characters produce column statistics
- For `decimal` with precision > 18, predicates won't push down to storage
- For `char`/`varchar`, only the first 32 characters produce column statistics
- For `decimal` with precision > 18, predicates won't push down to storage

## License

MIT

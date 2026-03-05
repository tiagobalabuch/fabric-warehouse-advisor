# Fabric Warehouse Data Clustering Advisor

A PySpark **library** (installable wheel) that assesses and recommends which
tables and columns should use **Data Clustering** in Microsoft Fabric Warehouse.

It runs entirely inside a **Fabric Spark notebook** — the Synapse SQL connector
comes pre-installed in the Fabric runtime, Query Insights is enabled by default
on every warehouse, and no Lakehouse or data source attachment is required.

## Installation

### Option A: Download Pre-Built Wheel (Recommended)

Download the latest `.whl` file from
[**GitHub Releases**](https://github.com/tiagobalabuch/fabric-warehouse-data-clustering-advisor/releases/latest),
then install it in your Fabric notebook:

```python
%pip install /lakehouse/default/Files/fabric_data_clustering_advisor-0.2.0-py3-none-any.whl
```

### Option B: Build from Source

```bash
pip install build
python -m build          # produces dist/fabric_data_clustering_advisor-0.2.0-py3-none-any.whl
```

### Install in Fabric

Upload the `.whl` file to your Lakehouse **Files** area and use `%pip install`,
or attach it via a Fabric **Environment** resource so it's pre-installed on
every Spark session.

See [Getting Started](docs/getting-started.md) for detailed instructions.

## Quick Start

```python
from fabric_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)
```

## How It Works

The advisor runs **7 phases** — all using T-SQL passthrough (no data
transferred to Spark):

| Phase | What it does |
|-------|-------------|
| 1. **Metadata** | Reads `sys.tables`, `sys.columns`, `sys.types` |
| 2. **Clustering** | Reads `sys.indexes` / `sys.index_columns` for current `CLUSTER BY` |
| 3. **Row Counts** | `COUNT_BIG(*)` per table; filters small tables |
| 4. **Query Patterns** | Reads `queryinsights.frequently_run_queries` |
| 5. **Predicates** | Regex extraction of WHERE-clause columns |
| 6. **Cardinality** | `APPROX_COUNT_DISTINCT()` pushed down to SQL engine |
| 7. **Scoring** | 0–100 composite score with cardinality penalties |

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, first run, working with results |
| [Configuration](docs/configuration.md) | Full parameter reference with defaults |
| [How It Works](docs/how-it-works.md) | Deep dive into the 7-phase pipeline |
| [Scoring](docs/scoring.md) | Scoring formula, cardinality penalties, worked examples |
| [Reports](docs/reports.md) | Text, Markdown, and HTML report formats |
| [Cross-Workspace](docs/cross-workspace.md) | Analysing warehouses in other workspaces |
| [Data Type Reference](docs/data-type-reference.md) | Supported types and limitations |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and solutions |

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

## License

MIT — see [LICENSE](LICENSE) for details.

# Fabric Warehouse Advisor

A modular Python **advisory framework** for **Microsoft Fabric Warehouse**.
Each advisor module analyses a different aspect of warehouse health and
produces scored recommendations with rich reports.

**Available Advisors:**

* **Data Clustering Advisor** — assesses which tables and columns should use
  Data Clustering (scored 0–100).
* **Performance Check Advisor** — scans for data-type anti-patterns, caching
  misconfigurations, stale statistics, and V-Order issues (findings-based).

It runs entirely inside a **Fabric Notebook**. The Microsoft Fabric Data Warehouse connector comes pre-installed in the Fabric runtime, and Query Insights is enabled by default on every warehouse. A Lakehouse is required only when the solution is installed from a wheel file stored in OneLake.

## Installation

### Option A: Download Pre-Built Wheel (Recommended)

Download the latest `.whl` file from
[**GitHub Releases**](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/latest),
then install it in your Fabric notebook:

```python
%pip install /lakehouse/default/Files/fabric_warehouse_advisor-1.0.3-py3-none-any.whl
```

### Option B: Build from Source

```bash
pip install build
python -m build          # produces dist/fabric_warehouse_advisor-1.0.3-py3-none-any.whl
```

### Install in Fabric

Upload the `.whl` file to your Lakehouse **Files** area and use `%pip install`,
or attach it via a Fabric **Environment** resource so it's pre-installed on
every Spark session.

See [Getting Started](docs/getting-started.md) for detailed instructions.

## Quick Start

### Data Clustering

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)
```

=== "Performance Check"

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()

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
| [Advisors Overview](docs/advisors/index.md) | Comparison of all available advisors |
| **Data Clustering** | |
| [DC — How It Works](docs/advisors/data-clustering/how-it-works.md) | 7-phase pipeline deep dive |
| [DC — Configuration](docs/advisors/data-clustering/configuration.md) | Full parameter reference |
| [DC — Scoring](docs/advisors/data-clustering/scoring.md) | Scoring formula, penalties, examples |
| [DC — Reports](docs/advisors/data-clustering/reports.md) | Text, Markdown, and HTML output |
| [DC — Data Type Reference](docs/advisors/data-clustering/data-type-reference.md) | Supported types |
| **Performance Check** | |
| [PC — How It Works](docs/advisors/performance-check/how-it-works.md) | 5-phase pipeline deep dive |
| [PC — Configuration](docs/advisors/performance-check/configuration.md) | Full parameter reference |
| [PC — Check Categories](docs/advisors/performance-check/checks.md) | All checks with severity and fixes |
| [PC — Reports](docs/advisors/performance-check/reports.md) | Text, Markdown, and HTML output |
| **Shared** | |
| [Cross-Workspace](docs/cross-workspace.md) | Analysing warehouses in other workspaces |
| [Troubleshooting](docs/troubleshooting.md) | Common issues and solutions |

## Package Structure

```
src/fabric_warehouse_advisor/
├── __init__.py                        # Top-level public API & re-exports
├── core/
│   ├── __init__.py
│   ├── warehouse_reader.py            # Spark connector wrappers for sys views
│   ├── predicate_parser.py            # SQL text → predicate column extraction
│   └── report.py                      # save_report() utility
└── advisors/
    ├── __init__.py
    ├── data_clustering/
    │   ├── __init__.py                # Data Clustering Advisor exports
    │   ├── config.py                  # DataClusteringAdvisorConfig dataclass
    │   ├── advisor.py                 # DataClusteringAdvisor orchestrator
    │   ├── data_type_support.py       # Data-type eligibility rules
    │   ├── scoring.py                 # Composite scoring + DDL generation
    │   └── report.py                  # Text, Markdown & HTML report generators
    └── performance_check/
        ├── __init__.py                # Performance Check Advisor exports
        ├── config.py                  # PerformanceCheckConfig dataclass
        ├── advisor.py                 # PerformanceCheckAdvisor orchestrator
        ├── findings.py                # Finding & CheckSummary dataclasses
        ├── report.py                  # Text, Markdown & HTML report generators
        └── checks/
            ├── warehouse_type.py      # Edition detection
            ├── data_types.py          # Column data-type anti-patterns
            ├── caching.py             # Result cache & cold start analysis
            ├── vorder.py              # V-Order optimization state
            └── statistics.py          # Statistics health & staleness
```

## License

MIT — see [LICENSE](LICENSE) for details.

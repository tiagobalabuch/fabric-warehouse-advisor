# Getting Started

This guide walks you through installing the **Fabric Warehouse Advisor**
and running your first analysis.

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Microsoft Fabric Workspace** | With at least one Fabric Warehouse or Lakehouse SQL Endpoint |
| **Fabric Notebook** | The advisors run inside a Fabric Spark notebook |
| **Warehouse / SQL Endpoint access** | The identity running the notebook (your Entra ID / service principal) must have at least **Read access** on the target Warehouse / SQL Endpoint |
| **Same tenant** | The target Warehouse / SQL Endpoint must be in the same Fabric tenant |
| **Python 3.9+** | Only needed if building from source (not required on Fabric) |

!!! note "Prerequisite Note"
    A Lakehouse is only required if you plan to upload the `.whl` file to the Lakehouse Files section.
    The Microsoft Fabric Data Warehouse connector is pre-installed in the
    Fabric Spark runtime, and Query Insights is enabled by default on every Fabric Warehouse.

## Getting the Wheel

### Option A: Download Pre-Built (Recommended)

Download the latest `.whl` file from the
[GitHub Releases](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/latest)
page — no build tools required.

### Option B: Build from Source

On your local machine (or in any Python 3.9+ environment):

```bash
pip install build
python -m build
```

This produces two files in `dist/`:

- `fabric_warehouse_advisor-1.0.0-py3-none-any.whl` — the installable wheel
- `fabric_warehouse_advisor-1.0.0.tar.gz` — source distribution

You only need the `.whl` file for Fabric.

## Installing in Fabric

### Option A: Per-Notebook Install (Recommended)

1. Upload the `.whl` file to your Lakehouse **Files** area (see the [official documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/load-data-lakehouse#upload-files) for detailed upload instructions).
2. In the first cell of your notebook, run:

```python
%pip install /lakehouse/default/Files/fabric_warehouse_advisor-1.0.0-py3-none-any.whl
```

This is the quickest way to get up and running.

### Option B: Fabric Environment

For a more permanent setup, you can attach the library to a Fabric Environment (see the [official documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/create-and-use-environment)):

1. In your Fabric Workspace, create or open an **Environment** resource
2. Under **Libraries**, upload the `.whl` file
3. Publish the Environment
4. Attach the Environment to your notebook (Settings → Environment)

With this approach the library is pre-installed on every session that uses
the Environment — no `%pip install` needed.

## Running an Advisor

### Data Clustering Advisor

Analyses your warehouse and recommends which tables and columns should
use `CLUSTER BY`:

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()
```

See the full [Data Clustering documentation](advisors/data-clustering/index.md)
for configuration options, scoring details, and report formats.

### Performance Check Advisor

Detects performance anti-patterns across data types, caching, V-Order,
and statistics:

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()
```

See the full [Performance Check documentation](advisors/performance-check/index.md)
for configuration options, check categories, and report formats.

## Working with Results

Both advisors return a result object with pre-formatted reports in three
formats.

### Viewing Reports

```python
# Rich HTML report (recommended in Fabric notebooks)
displayHTML(result.html_report)

# Plain text
print(result.text_report)

# Markdown
print(result.markdown_report)
```

### Saving Reports

```python
# Save as HTML (default)
result.save("/lakehouse/default/Files/reports/report.html")

# Save as Markdown
result.save("/lakehouse/default/Files/reports/report.md", "md")

# Save as plain text
result.save("/lakehouse/default/Files/reports/report.txt", "txt")
```

### Data Clustering — Exploring Scores

The Data Clustering advisor also provides a Spark DataFrame:

```python
result.scores_df.show()

# Persist to Delta for tracking over time
result.scores_df.write.mode("overwrite").format("delta").saveAsTable(
    "yourschema.clustering_advisor_scores"
)
```

### Performance Check — Exploring Findings

The Performance Check advisor provides structured findings:

```python
# Summary counts by severity
print(f"Critical: {result.critical_count}")
print(f"High:     {result.high_count}")
print(f"Medium:   {result.medium_count}")
print(f"Low:      {result.low_count}")
print(f"Info:     {result.info_count}")

# Iterate findings (sorted by severity)
for f in result.findings:
    print(f"[{f.level}] {f.object_name}: {f.message}")

# Filter to actionable findings only (excludes INFO)
for f in result.findings:
    if f.is_actionable:
        print(f"[{f.level}] {f.object_name}: {f.message}")
        if f.recommendation:
            print(f"  → {f.recommendation}")
```

## Next Steps

- [Data Clustering Advisor](advisors/data-clustering/index.md) — full documentation
- [Performance Check Advisor](advisors/performance-check/index.md) — full documentation
- [Cross-Workspace](cross-workspace.md) — analyse warehouses in other workspaces
- [Troubleshooting](troubleshooting.md) — common issues and solutions

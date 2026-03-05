# Getting Started

This guide walks you through building, installing, and running the
**Fabric Warehouse Data Clustering Advisor** for the first time.

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Microsoft Fabric Workspace** | With at least one Fabric Warehouse |
| **Fabric Spark Notebook** | The advisor runs inside a PySpark notebook |
| **Query Insights** | Enabled by default on every Fabric Warehouse — no action needed |
| **Python 3.9+** | Only needed if building from source (not required on Fabric) |

> **Note:** No Lakehouse attachment or external data source is required.
> The Synapse SQL connector is pre-installed in the Fabric Spark runtime.

## Getting the Wheel

### Option A: Download Pre-Built (Recommended)

Download the latest `.whl` file from the
[GitHub Releases](https://github.com/tiagobalabuch/fabric-warehouse-data-clustering-advisor/releases/latest)
page — no build tools required.

### Option B: Build from Source

On your local machine (or in any Python 3.9+ environment):

```bash
pip install build
python -m build
```

This produces two files in `dist/`:

- `fabric_data_clustering_advisor-0.2.0-py3-none-any.whl` — the installable wheel
- `fabric_data_clustering_advisor-0.2.0.tar.gz` — source distribution

You only need the `.whl` file for Fabric.

## Installing in Fabric

### Option A: Per-Notebook Install

1. Upload the `.whl` file to your Lakehouse **Files** area
2. In the first cell of your notebook, run:

```python
%pip install /lakehouse/default/Files/fabric_data_clustering_advisor-0.2.0-py3-none-any.whl
```

### Option B: Fabric Environment (Recommended)

1. In your Fabric Workspace, create or open an **Environment** resource
2. Under **Libraries**, upload the `.whl` file
3. Publish the Environment
4. Attach the Environment to your notebook (under Settings → Environment)

With this approach, the library is pre-installed on every Spark session that
uses the Environment — no `%pip install` needed.

## Quick Start

```python
from fabric_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

# Minimal configuration — only warehouse_name is required
config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()
```

The advisor will:

1. Read metadata from your warehouse's system catalog views
2. Analyse query patterns from Query Insights
3. Estimate column cardinality
4. Score every candidate column
5. Print a text report and return an `AdvisorResult` object

## Working with Results

```python
# The text report is auto-printed during run().
# You can also print it again:
print(result.text_report)

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)

# Spark DataFrame with per-column scores
result.scores_df.show()
```

### Saving Reports

```python
# Save as HTML (default)
result.save("/lakehouse/default/Files/reports/advisor_report.html")

# Save as Markdown
result.save("/lakehouse/default/Files/reports/advisor_report.md", "md")

# Save as plain text
result.save("/lakehouse/default/Files/reports/advisor_report.txt", "txt")
```

### Persisting Scores to Delta

```python
result.scores_df.write.mode("overwrite").format("delta").saveAsTable(
    "yourschema.clustering_advisor_scores"
)
```

### Programmatic Access

```python
for rec in result.recommendations:
    print(f"{rec.schema_name}.{rec.table_name}: {rec.cluster_by_ddl}")
    for col in rec.recommended_columns:
        print(f"  {col.column_name} — score {col.composite_score}, {col.recommendation}")
```

## Next Steps

- [Configuration Reference](configuration.md) — tune every parameter
- [How It Works](how-it-works.md) — understand the 7-phase pipeline
- [Scoring](scoring.md) — learn how scores are calculated
- [Cross-Workspace Usage](cross-workspace.md) — analyse warehouses in other workspaces
- [Reports](reports.md) — text, Markdown, and HTML output formats

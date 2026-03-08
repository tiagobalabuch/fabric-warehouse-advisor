# Getting Started

This guide walks you through installing and running the
**Fabric Warehouse Data Clustering Advisor** for the first time.

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Microsoft Fabric Workspace** | With at least one Fabric Warehouse |
| **Fabric Notebook** | The advisor runs inside a notebook |
| **Warehouse access** | The identity running the notebook (your Entra ID / service principal) must have at least **Read access** on the target warehouse |
| **Same tenant** | The target warehouse must be in the same Fabric tenant |
| **Python 3.9+** | Only needed if building from source (not required on Fabric) |

> **Note:** No Lakehouse attachment or external data source is required.
> The Microsoft Fabric Data Warehouse connector is pre-installed in the
> Fabric Spark runtime, and Query Insights is enabled by default on every
> Fabric Warehouse.

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

- `fabric_warehouse_data_clustering_advisor-0.3.0-py3-none-any.whl` — the installable wheel
- `fabric_warehouse_data_clustering_advisor-0.3.0.tar.gz` — source distribution

You only need the `.whl` file for Fabric.

## Installing in Fabric

### Option A: Per-Notebook Install (Recommended)

1. Upload the `.whl` file to your Lakehouse **Files** area (see the [official documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/load-data-lakehouse) for detailed upload instructions).
2. In the first cell of your notebook, run:

```python
%pip install /lakehouse/default/Files/fabric_warehouse_data_clustering_advisor-0.3.0-py3-none-any.whl
```

This is the quickest way to get up and running.

### Option B: Fabric Environment

For a more permanent setup, you can attach the library to a Fabric Environment:

1. In your Fabric Workspace, create or open an **Environment** resource
2. Under **Libraries**, upload the `.whl` file
3. Publish the Environment
4. Attach the Environment to your notebook (Settings → Environment)

With this approach the library is pre-installed on every session that uses
the Environment — no `%pip install` needed.

## Running the Advisor

Once installed, you only need three lines to analyse a warehouse:

```python
from fabric_warehouse_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()
```

The advisor will run a 7-phase pipeline (see [How It Works](how-it-works.md))
and return an `AdvisorResult` object containing scores, recommendations, and
ready-to-use reports.

## Working with Results

### Viewing Reports

```python
# The text report is auto-printed during run().
# You can also display the rich HTML report:
displayHTML(result.html_report)

# Or print the text version again:
print(result.text_report)
```

### Exploring Scores

```python
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

- [Configuration](configuration.md) — tune every parameter
- [How It Works](how-it-works.md) — understand the 7-phase pipeline
- [Scoring](scoring.md) — learn how scores are calculated
- [Cross-Workspace](cross-workspace.md) — analyse warehouses in other workspaces
- [Reports](reports.md) — text, Markdown, and HTML output formats

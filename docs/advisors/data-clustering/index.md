# Data Clustering Advisor

The Data Clustering Advisor helps you answer a deceptively simple question:
**which columns in my Fabric Warehouse should I cluster?**

Data Clustering is one of the most impactful performance levers in
Microsoft Fabric Warehouse — it controls how data is physically organized on OneLake, which directly affects query speed and resource consumption.
But choosing the *right* columns to cluster on isn't always obvious.
That's where this advisor comes in.

## What it does

The advisor analyses your **actual query patterns** (via Query Insights),
combines them with **table metadata** and **column cardinality estimates**,
and scores every candidate column from 0 to 100. You get a clear report
telling you exactly what to cluster and why.

## Quick Start

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

config = DataClusteringConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)
```

## Working with Results

### Exploring Scores

```python
# Spark DataFrame with per-column scores
result.scores_df.show()
```

### Programmatic Access

```python
for rec in result.recommendations:
    print(f"{rec.schema_name}.{rec.table_name}: {rec.cluster_by_ddl}")
    for col in rec.recommended_columns:
        print(f"  {col.column_name} — score {col.composite_score}, {col.recommendation}")
```

### Saving Reports

```python
result.save("/lakehouse/default/Files/reports/clustering_report.html")
result.save("/lakehouse/default/Files/reports/clustering_report.md", "md")
result.save("/lakehouse/default/Files/reports/clustering_report.txt", "txt")
```

### Persisting Scores to Delta

```python
result.scores_df.write.mode("overwrite").format("delta").saveAsTable(
    "yourschema.clustering_advisor_scores"
)
```

## Documentation

| Document | Description |
|----------|-------------|
| [How It Works](how-it-works.md) | Deep dive into the 7-phase pipeline |
| [Configuration](configuration.md) | Full parameter reference with defaults |
| [Scoring](scoring.md) | Scoring formula, cardinality penalties, worked examples |
| [Reports](reports.md) | Text, Markdown, and HTML report formats |
| [Data Type Reference](data-type-reference.md) | Supported types and limitations |

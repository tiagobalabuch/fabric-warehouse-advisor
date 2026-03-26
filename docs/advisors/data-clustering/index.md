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

# To experience all features and interactive capabilities, save the report and open it in a web browser
result.save("/lakehouse/default/Files/reports/report.html")
# Rich HTML report
displayHTML(result.html_report)
```

!!! warning
    Execution time and CU consumption of the Data Clustering Advisor vary based on data volume, and column count. For optimal performance and minimal impact, we recommend running the Advisor during low-usage periods and outside peak concurrency windows.

## Working with Results

!!! tip "Web Browser is recommended"
    The best way to visualize the report is to save it as `HTML`, which provides the full experience with rich features and interactivity.
    
### Exploring Scores

```python
# Spark DataFrame with per-column scores
display(result.scores_df)
```

### Saving Reports

```python
result.save("/lakehouse/default/Files/reports/dataclustering_report.html")
result.save("/lakehouse/default/Files/reports/dataclustering_report.md", "md")
result.save("/lakehouse/default/Files/reports/dataclustering_report.txt", "txt")
```

### Persisting data to Delta table

```python
result.scores_df.write.mode("overwrite").format("delta").saveAsTable(
    "yourschema.data_clustering_advisor_scores"
)
```

## Documentation

| Document | Description |
|----------|-------------|
| [How It Works](how-it-works.md) | Detailed analysis of each phase in the pipeline execution lifecycle |
| [Configuration](configuration.md) | Full parameter reference with defaults |
| [Scoring](scoring.md) | Scoring formula, cardinality penalties, worked examples |
| [Reports](reports.md) | HTML, Text and Markdown report formats |
| [Data Type Reference](data-type-reference.md) | Supported types and limitations |

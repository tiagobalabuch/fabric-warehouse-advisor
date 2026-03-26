# Reports

The advisor produces reports in three formats. All formats contain
the same information — choose whichever fits your workflow.

## Report Formats

| Format | Method | Best For |
|--------|--------|----------|
| **Text** | `result.text_report` | `print()` in a notebook cell; quick console overview |
| **Markdown** | `result.markdown_report` | Saving as `.md`; rendering in GitHub, wikis, documentation |
| **HTML** | `result.html_report` | Saving as `.html`; Use a Web Browser for rich visual display or the `displayHTML()` in Fabric notebooks |

!!! tip "Web Browser is recommended"
    The best way to visualize the report is to save it as `HTML`, which provides the full experience with rich features and interactivity.

## Viewing Reports

### HTML Report (Recommended)

The HTML report renders natively in a Fabric notebook cell:

```python
displayHTML(result.html_report)
```

It includes:
- Sidebar with tab-based navigation across tables, DDL, and best practices
- Summary cards (tables analysed, recommendations, etc.)
- Per-table sections with sortable score tables
- Column header tooltips explaining each metric
- Visual score bars
- Color-coded recommendation badges
- Collapsible DDL sections
- Best practices reference

### Markdown Report

```python
print(result.markdown_report)
```

### Text Report

The text report is not automatically printed at the end of `advisor.run()`.
To print it:

```python
print(result.text_report)
```

## Saving Reports

Use the `result.save()` method or the standalone `save_report()` function:

```python
# Via DataClusteringResult
result.save("/lakehouse/default/Files/reports/report.html")           # HTML (default)
result.save("/lakehouse/default/Files/reports/report.md", "md")       # Markdown
result.save("/lakehouse/default/Files/reports/report.txt", "txt")     # Plain text

# Via standalone function
from fabric_warehouse_advisor import save_report

save_report(result.html_report, "/path/to/report.html", format="html")
save_report(result.markdown_report, "/path/to/report.md", format="md")
```

The format parameter accepts `"html"`, `"md"`, or `"txt"`. When omitted,
it is inferred from the file extension.

For HTML format, if the content doesn't already contain `<html>` tags,
the save function wraps it in a minimal HTML document with UTF-8 encoding
and a title.

Parent directories are created automatically.

## Report Sections

All three formats include the following sections:

### Executive Summary

- Total tables analysed
- Tables with recommendations
- Tables already clustered
- Score threshold used

### Per-Table Recommendations

For each table the report includes:

- Table name, schema, and row count
- Current `CLUSTER BY` columns (if any)
- Warnings for sub-optimal existing clustering
- Suggested CTAS DDL (when `generate_ctas=True`)

Each table also contains a column-level detail table with:

- Column name and data type
- Predicate hits (weighted by query runs)
- Approximate distinct count, cardinality ratio, and percentage
- Cardinality classification (High/Medium/Low)
- Composite score (with visual bar in HTML)
- Recommendation label
- Optimization warnings (if applicable)

### All Suggested DDL

A consolidated section with every CTAS statement from all tables,
plus an explanatory note about how Fabric applies data clustering.

### Best Practices

A reference section with key recommendations:

- Data clustering is most effective on large tables
- Choose mid-to-high cardinality columns used in WHERE filters
- Batch ingestion (≥ 1M rows per DML) for optimal quality
- Equality JOINs do NOT benefit from data clustering
- Column order in CLUSTER BY doesn't affect row storage
- `char`/`varchar` first 32 character limit for statistics
- `decimal` precision > 18 predicate pushdown limitation

## Customizing Report Output

The `DataClusteringResult` object exposes several attributes beyond the
pre-formatted reports:

| Attribute | Type | Description |
|-----------|------|-------------|
| `recommendations` | `list[TableRecommendation]` | Per-table recommendations with nested `ColumnScore` objects. |
| `all_scores` | `list[ColumnScore]` | Flat list of every scored column across all tables. |
| `scores_df` | `DataFrame` | Spark DataFrame with the detailed scores — useful for custom queries, joins, or saving to a Lakehouse table. |
| `captured_at` | `str` | ISO-8601 UTC timestamp of when the advisor run completed. |

You can work with the Spark DataFrame for further analysis:

```python
# Show top candidates
display(result.scores_df.orderBy("composite_score", ascending=False))

# Save scores to a Lakehouse table
result.scores_df.write.mode("overwrite").saveAsTable("data_clustering_scores")
```

This allows you to build custom reports, dashboards, or integrations.

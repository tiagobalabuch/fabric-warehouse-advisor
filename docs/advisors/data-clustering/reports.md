# Reports

The advisor produces reports in three formats. All formats contain
the same information — choose whichever fits your workflow.

## Report Formats

| Format | Method | Best For |
|--------|--------|----------|
| **Text** | `result.text_report` | `print()` in a notebook cell; quick console overview |
| **Markdown** | `result.markdown_report` | Saving as `.md`; rendering in GitHub, wikis, documentation |
| **HTML** | `result.html_report` | `displayHTML()` in Fabric notebooks; rich visual display |

## Viewing Reports

### Text Report

The text report is not automatically printed at the end of `advisor.run()`.
To print it:

```python
print(result.text_report)
```

### HTML Report (Recommended)

The HTML report renders natively in a Fabric notebook cell:

```python
displayHTML(result.html_report)
```

It includes:
- Summary cards (tables analysed, recommendations, etc.)
- Per-table sections with sortable score tables
- Visual score bars
- Color-coded recommendation badges
- Collapsible DDL sections
- Best practices reference

### Markdown Report

```python
print(result.markdown_report)
```

## Saving Reports

Use the `result.save()` method or the standalone `save_report()` function:

```python
# Via AdvisorResult
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

## Customising Report Output

Reports are generated from Python dataclasses (`TableRecommendation`,
`ColumnScore`) that you can also access directly:

```python
for rec in result.recommendations:
    print(f"Table: {rec.schema_name}.{rec.table_name}")
    print(f"  Rows: {rec.row_count:,}")
    print(f"  Current CLUSTER BY: {rec.currently_clustered_columns}")
    print(f"  Warnings: {rec.warnings}")
    for col in rec.recommended_columns:
        print(f"  - {col.column_name}: score={col.composite_score}, {col.recommendation}")
```

This allows you to build custom reports, dashboards, or integrations.

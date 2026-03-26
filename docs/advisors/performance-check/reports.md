# Report Formats

The advisor produces reports in three formats. All formats contain the same information — choose whichever fits your workflow.

## Report Formats

| Format | Method | Best For |
|--------|--------|----------|
| **Text** | `result.text_report` | `print()` in a notebook cell; quick console overview |
| **Markdown** | `result.markdown_report` | Saving as `.md`; rendering in GitHub, wikis, documentation |
| **HTML** | `result.html_report` | `displayHTML()` in Fabric notebooks; rich visual display |

!!! tip "Web Browser is recommended"
    The best way to visualize the report is to save it as `HTML`, which provides the full experience with rich features and interactivity.

## Viewing Reports

### HTML Report (Recommended)

The HTML report renders natively in a Fabric notebook cell:

```python
displayHTML(result.html_report)
```

Self-contained HTML page (no external dependencies).
Features:

- **Sidebar navigation** — tabbed sidebar with category tabs grouped
  under section dividers:
    - **Query Performance** — Caching, Statistics, Query Regression
    - **Storage & Layout** — V-Order, Data Types, Collation
    - **Workload Management** — Custom SQL Pools
- **Workspace metadata** — workspace display name and capacity SKU shown in the sidebar when available.
- **Info stat cards** — Edition, Tables, Columns
- **Severity stat cards** — colour-coded clickable filters for
  Critical / High / Medium / Low / Info / Total
- **Sortable columns** — click Level, Object, or Rule headers to sort
- **Finding tables** — one row per finding with Level, Object, Rule,
  Finding, Recommendation columns
- **Collapsible groups** — checks with > 10 hits are collapsed into
  a single row with an expandable object pill grid
- **Warn-box callouts** — for warehouse-wide or workspace-wide checks
  (Query Regression, V-Order, Custom SQL Pools)
- **SQL code blocks** — dark-themed blocks for fix scripts

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
# Via PerformanceCheckResult
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

## Programmatic Access

The `PerformanceCheckResult` object exposes findings and summary data.

### Result Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `findings` | `list[Finding]` | All findings from every check. |
| `summary` | `CheckSummary` | Aggregated summary object. |
| `warehouse_edition` | `str` | Detected edition (`DataWarehouse` or `LakeWarehouse`). |
| `text_report` | `str` | Pre-formatted text report. |
| `markdown_report` | `str` | Markdown report. |
| `html_report` | `str` | HTML report. |
| `captured_at` | `str` | ISO-8601 timestamp (UTC) of the run. |
| `has_critical` | `bool` | `True` if any CRITICAL-level finding exists. |
| `critical_count` | `int` | Number of CRITICAL findings. |
| `high_count` | `int` | Number of HIGH findings. |
| `medium_count` | `int` | Number of MEDIUM findings. |
| `low_count` | `int` | Number of LOW findings. |
| `info_count` | `int` | Number of INFO findings. |

You can work with the Spark DataFrame for further analysis:

```python
# Show top candidates
display(result.findings)

```

See the [Finding dataclass](index.md#output-model) for the full field reference.


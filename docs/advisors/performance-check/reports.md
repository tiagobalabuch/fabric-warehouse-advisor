# Report Formats

The advisor produces reports in three formats. All formats contain the same information — choose whichever fits your workflow.

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

Self-contained HTML page (no external dependencies) styled with the
**Fabric blue** (`#0078d4`) design language. Features:

- **Summary cards** — grid of Warehouse, Edition, Tables, Columns
- **Severity cards** — colour-coded Critical / High / Medium / Low / Info / Total
- **Category sections** — each with a severity badge bar
- **Finding tables** — one row per finding with Level, Object, Finding,
  Recommendation columns
- **Collapsible groups** — blocks for checks with > 10 hits
- **SQL code blocks** — dark-themed blocks

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

## Programmatic Access

All findings are also available as structured data:

```python
# Iterate all findings
for finding in result.findings:
    print(f"[{finding.level}] {finding.category} - {finding.object_name}: {finding.message}")

# Filter by severity
critical = [f for f in result.findings if f.is_critical]

# Group by category
data_type_issues = [f for f in result.findings if f.category == "data_types"]

# Summary counts
print(f"Critical: {result.critical_count}")
print(f"High:     {result.high_count}")
print(f"Medium:   {result.medium_count}")
print(f"Low:      {result.low_count}")
print(f"Info:     {result.info_count}")
```

See the [Finding dataclass](index.md#output-model) for the full field reference.

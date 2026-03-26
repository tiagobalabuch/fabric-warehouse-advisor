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

- **Sidebar navigation** — tabbed sidebar with section groups:
    - **Workspace & Platform** — Workspace Roles, Network Isolation, OneLake Settings, Sensitivity Labels
    - **Item Security** — SQL Audit Settings, Item Permissions
    - **OneLake Security** — OneLake Data Access Roles, OneLake Security Sync
    - **SQL Security** — Schema Permissions, Custom Roles, RLS, CLS, DDM
    - **Cross-Reference** — Role Alignment
- **Auth mode banner** — when running against a Lakehouse SQL Endpoint, the
  detected access mode (User Identity or Delegated Identity) is shown in
  the sidebar header
- **Auth mode explainer blocks** — contextual alert boxes inside affected
  tab panes (e.g. "SQL RLS is inactive in User Identity mode")
- **Summary cards** — colour-coded Critical / High / Medium / Low / Info / Total
- **Severity badge bars** — each category tab shows its severity distribution
- **Sortable columns** — click Level, Object, or Rule column headers to sort
- **Severity filters** — filter the table by severity level
- **Finding tables** — one row per finding with Level, Object, Rule,
  Finding, and Recommendation columns
- **Object pills** — when a single check yields > 10 findings, a compact
  expandable group with object pills replaces individual rows
- **SQL code blocks** — dark-themed blocks with ready-to-run fixes
- **Collapsible groups** — expandable blocks for checks with many hits
- **Warn-box callouts** — highlighted callouts for critical/high findings

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
# Via SecurityCheckResult
result.save("/lakehouse/default/Files/reports/security_report.html")           # HTML (default)
result.save("/lakehouse/default/Files/reports/security_report.md", "md")       # Markdown
result.save("/lakehouse/default/Files/reports/security_report.txt", "txt")     # Plain text

# Via standalone function
from fabric_warehouse_advisor import save_report

save_report(result.html_report, "/path/to/security_report.html", format="html")
save_report(result.markdown_report, "/path/to/security_report.md", format="md")
```

The format parameter accepts `"html"`, `"md"`, or `"txt"`. When omitted,
it is inferred from the file extension.

For HTML format, if the content doesn't already contain `<html>` tags,
the save function wraps it in a minimal HTML document with UTF-8 encoding
and a title.

Parent directories are created automatically.

## Programmatic Access

All findings are also available as structured data.

### Result Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `findings` | `list[Finding]` | All findings from every check. |
| `summary` | `CheckSummary` | Aggregated summary with helper methods. |
| `text_report` | `str` | Pre-formatted plain-text report. |
| `markdown_report` | `str` | Markdown report. |
| `html_report` | `str` | Self-contained HTML report. |
| `captured_at` | `str` | ISO-8601 timestamp (UTC). |
| `has_critical` | `bool` | `True` if any CRITICAL finding exists. |
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

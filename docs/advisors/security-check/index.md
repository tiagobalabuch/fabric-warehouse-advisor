# Security Check Advisor

The Security Check Advisor scans your Fabric Warehouse for common
security misconfigurations and produces actionable findings — covering
permissions, roles, Row-Level Security, Column-Level Security, and
Dynamic Data Masking.

## What it checks

| Category | What it detects |
|----------|----------------|
| **Schema Permissions** | Grants to the `public` role, direct user grants, overly broad schema-wide privileges |
| **Custom Roles** | Excessive `db_owner` membership, empty (unused) roles, users without any role |
| **Row-Level Security** | Disabled RLS policies, unsupported BLOCK predicates, tables without RLS coverage |
| **Column-Level Security** | Sensitive columns (by name pattern) lacking DENY SELECT protection |
| **Dynamic Data Masking** | Excessive UNMASK grants, weak `default()` masking on short string columns |

## Quick Start

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = SecurityCheckAdvisor(spark, config)
result = advisor.run()

# Rich HTML report
displayHTML(result.html_report)
```

## Output Model

Like the Performance Check advisor, the Security Check advisor produces
**findings** at five severity levels:

| Level | Meaning |
|-------|---------|
| **CRITICAL** | Immediate action required — significant security risk |
| **HIGH** | Important issue — should be addressed soon |
| **MEDIUM** | Worth reviewing — potential security improvement |
| **LOW** | Minor concern — fix when convenient |
| **INFO** | Informational — current state is healthy or for awareness |

Each finding includes:

- **Object name** — the specific warehouse, schema, table, column, or role affected
- **Message** — one-line summary of the issue
- **Detail** — context (current state, impact)
- **Recommendation** — actionable guidance
- **SQL fix** — ready-to-run T-SQL statement (when applicable)

## Working with Results

### Summary Counts

```python
# Summary counts by severity
print(f"Critical: {result.critical_count}")
print(f"High:     {result.high_count}")
print(f"Medium:   {result.medium_count}")
print(f"Low:      {result.low_count}")
print(f"Info:     {result.info_count}")
```

### Iterating Findings

```python
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

### Filtering by Category or Level

```python
# Only critical findings
critical = [f for f in result.findings if f.is_critical]
display(critical)

# Only permissions findings
from fabric_warehouse_advisor.advisors.security_check.findings import CATEGORY_PERMISSIONS

perm_findings = result.summary.findings_by_category(CATEGORY_PERMISSIONS)
display(perm_findings)
```

### Saving Reports

```python
result.save("/lakehouse/default/Files/reports/security_report.html")
result.save("/lakehouse/default/Files/reports/security_report.md", "md")
result.save("/lakehouse/default/Files/reports/security_report.txt", "txt")
```

## Documentation

| Document | Description |
|----------|-------------|
| [How It Works](how-it-works.md) | The multi-phase pipeline |
| [Configuration](configuration.md) | Full parameter reference |
| [Checks Reference](checks.md) | Deep dive into each check category |
| [Reports](reports.md) | Text, Markdown, and HTML report formats |

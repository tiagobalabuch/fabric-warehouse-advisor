# Security Check Advisor

The Security Check Advisor scans your Fabric Warehouse or SQL Analytics Endpoint for security misconfigurations and produces actionable
findings — covering workspace roles, network isolation, OneLake security, SQL permissions, Row-Level Security, Column-Level Security,
Dynamic Data Masking, and more.

## What it checks

The advisor runs **15 phases** grouped into four layers:

### Workspace & Platform

| Category | What it detects |
|----------|----------------|
| **Workspace Roles** | Excessive admins, EntireTenant access, service-principal admin grants |
| **Network Isolation** | Inbound / outbound public access policy (Allow vs Deny) |
| **OneLake Settings** | Diagnostics logging, immutability policies |
| **Sensitivity Labels** | Missing Microsoft Purview sensitivity label on the warehouse item |

### Item Security

| Category | What it detects |
|----------|----------------|
| **SQL Audit Settings** | Auditing disabled, short retention, missing audit action groups |
| **Item Permissions** | EntireTenant item access, excessive ReadData sharing, write grants outside workspace roles |

### OneLake Security

| Category | What it detects |
|----------|----------------|
| **OneLake Data Access Roles** | DefaultReader covering all paths with custom roles, ReadWrite + RLS/CLS conflicts, wildcard paths, empty roles, excessive roles, multi-role CLS conflicts |
| **OneLake Security Sync** | Stale `ols_` sync roles, missing sync for OneLake roles |

### SQL Security

| Category | What it detects |
|----------|----------------|
| **Schema Permissions** | Grants to the `public` role, direct user grants, overly broad schema-wide privileges |
| **Custom Roles** | Excessive `db_owner` membership, empty (unused) roles, users without any role |
| **Row-Level Security** | Disabled RLS policies, unsupported BLOCK predicates, tables without RLS coverage |
| **Column-Level Security** | Sensitive columns (by name pattern) lacking DENY SELECT protection |
| **Dynamic Data Masking** | Excessive UNMASK grants, weak `default()` masking on short string columns |

### Cross-Reference

| Category | What it detects |
|----------|----------------|
| **Role Alignment** | Workspace Viewer with `db_owner`, high-privilege DB roles without matching workspace role |

!!! note "Auth Mode Awareness"
    For SQL Analytics Endpoints, the advisor detects the access mode (User Identity vs Delegated Identity) and adjusts which checks are active. In **User Identity mode**, SQL RLS, CLS, and custom roles are inactive — the advisor produces INFO findings instead of
    running them, and highlights OneLake security roles as the primary access control mechanism.

## Quick Start

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = SecurityCheckAdvisor(spark, config)
result = advisor.run()

# To experience all features and interactive capabilities, save the report and open it in a web browser
result.save("/lakehouse/default/Files/reports/report.html")
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

!!! tip "Web Browser is recommended"
    The best way to visualize the report is to save it as `HTML`, which provides the full experience with rich features and interactivity.
    
### Exploring Findings

```python
# Spark DataFrame with findings
display(result.findings)
```

### Saving Reports

```python
result.save("/lakehouse/default/Files/reports/security_report.html")
result.save("/lakehouse/default/Files/reports/security_report.md", "md")
result.save("/lakehouse/default/Files/reports/security_report.txt", "txt")
```

### Persisting data to Delta table

```python
result.findings.write.mode("overwrite").format("delta").saveAsTable(
    "yourschema.security_advisor"
)
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

# Available category constants:
# CATEGORY_PERMISSIONS, CATEGORY_ROLES, CATEGORY_RLS, CATEGORY_CLS,
# CATEGORY_DDM, CATEGORY_WORKSPACE_ROLES, CATEGORY_NETWORK,
# CATEGORY_SQL_AUDIT, CATEGORY_ITEM_PERMISSIONS,
# CATEGORY_SENSITIVITY_LABELS, CATEGORY_ROLE_ALIGNMENT,
# CATEGORY_AUTH_MODE, CATEGORY_ONELAKE_DATA_ACCESS,
# CATEGORY_ONELAKE_SETTINGS, CATEGORY_ONELAKE_SECURITY_SYNC
```

## Documentation

| Document | Description |
|----------|-------------|
| [How It Works](how-it-works.md) | The multi-phase pipeline |
| [Configuration](configuration.md) | Full parameter reference |
| [Checks Reference](checks.md) | Deep dive into each check category |
| [Reports](reports.md) | Text, Markdown, and HTML report formats |

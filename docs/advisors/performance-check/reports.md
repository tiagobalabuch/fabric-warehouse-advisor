# Report Formats

The Performance Check advisor generates reports in three formats — all
from the same `PerformanceCheckResult` object.

```python
result = advisor.run()

print(result.text_report)       # Plain text with box drawing
print(result.markdown_report)   # Markdown tables & details tags
print(result.html_report)       # Self-contained HTML page
```

## Saving Reports

```python
result.save("performance_check_report.txt")
---

## Plain Text Report

Uses Unicode box drawing for a clean terminal-friendly layout.

```
╔════════════════════════════════════════════════════════════════════════╗
║          Fabric Warehouse Performance Check Report                   ║
╚════════════════════════════════════════════════════════════════════════╝

  Warehouse : SalesWarehouse
  Edition   : DataWarehouse
  Tables    : 12
  Columns   : 87

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SUMMARY
    ❌ CRITICAL : 2
    🔴 HIGH     : 3
    🟡 MEDIUM   : 4
    🔵 LOW      : 2
    ✅ INFO     : 8
    Total      : 19 findings
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━ DATA TYPES (1 critical, 3 warnings, 2 info) ━━━

  ❌ [dbo].[FactSales].[Description] VARCHAR(MAX) column detected.
    Data type: VARCHAR(MAX) (1,250,000 rows). The engine allocates
    maximum potential memory during sort/hash operations...
    → Determine the actual maximum length with:
      SELECT MAX(LEN([Description])) FROM [dbo].[FactSales]
    SQL: ALTER TABLE [dbo].[FactSales] ALTER COLUMN [Description]
         VARCHAR(<actual_max>) NOT NULL;
```

**Grouping:** When the same check fires on more than 5 objects, the
text report collapses them into a single summary line with a count and
a truncated list of affected objects.

---

## Markdown Report

Emits GitHub-flavoured Markdown with emoji severity icons, tables,
and collapsible `<details>` sections for large finding groups.

````markdown
# 🔍 Fabric Warehouse Performance Check Report

| Property | Value |
|----------|-------|
| Warehouse | `SalesWarehouse` |
| Edition | `DataWarehouse` |
| Tables Analysed | 12 |
| Columns Analysed | 87 |

## Summary

| Level | Count |
|-------|-------|
| ❌ Critical | **2** |
| 🔴 High | **3** |
| 🟡 Medium | **4** |
| 🔵 Low | **2** |
| ✅ Info | 8 |
| **Total** | **19** |

## Data Types — ❌ 1 critical

- ❌ **`[dbo].[FactSales].[Description]`** — VARCHAR(MAX) column detected.
  > Data type: VARCHAR(MAX) (1,250,000 rows)...
  - **Recommendation:** Determine the actual maximum length...
  ```sql
  ALTER TABLE [dbo].[FactSales]
  ALTER COLUMN [Description] VARCHAR(<actual_max>) NOT NULL;
  ```

<details>
<summary>Affected objects (12)</summary>

- `[dbo].[FactSales].[Notes]`
- `[dbo].[DimProduct].[LongDescription]`
- ...

</details>
````

**Large groups:** When a check fires on more than 5 objects, the
Markdown report uses a collapsible `<details>` block with a shared
recommendation and SQL fix.

---

## HTML Report

Self-contained HTML page (no external dependencies) styled with the
**Fabric blue** (`#0078d4`) design language. Features:

- **Summary cards** — grid of Warehouse, Edition, Tables, Columns
- **Severity cards** — colour-coded Critical / High / Medium / Low / Info / Total
- **Category sections** — each with a severity badge bar
- **Finding tables** — one row per finding with Level, Object, Finding,
  Recommendation columns
- **Collapsible groups** — `<details>` blocks for checks with > 10 hits
- **SQL code blocks** — dark-themed `<pre class="sql">` blocks

### Visual Style

| Element | Style |
|---------|-------|
| Header | Fabric blue `#0078d4` underline |
| Critical badge | Red `#d32f2f` pill |
| High badge | Orange-red `#e65100` pill |
| Medium badge | Amber `#f57c00` pill |
| Low badge | Blue `#1565c0` pill |
| Info badge | Green `#388e3c` pill |
| Finding rows | Left border colour matches severity |
| SQL blocks | Dark background, `Cascadia Code` font |
| Cards | White with subtle box shadow |

### Displaying the results

```python

displayHTML(result.html_report)

```

---

## Programmatic Access

All findings are also available as structured data:

```python
# Iterate all findings
for finding in result.findings:
    print(finding.level, finding.category, finding.object_name, finding.message)

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

See the [Finding dataclass](index.md#output-model) for the full
field reference.

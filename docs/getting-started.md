# Getting Started

This guide walks you through installing the **Fabric Warehouse Advisor**
and running your first analysis.

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Microsoft Fabric Workspace** | With at least one Fabric Warehouse or SQL Analytics Endpoint |
| **Fabric Notebook** | The advisors run inside a Fabric Spark notebook |
| **Warehouse / SQL Analytics Endpoint access** | The identity running the notebook (your Entra ID / service principal) must have at least **Read access** on the target Warehouse / SQL Analytics Endpoint |
| **Same tenant** | The target Warehouse / SQL Analytics Endpoint must be in the same Fabric tenant |

!!! info
    A Lakehouse is only required if you plan to upload the `.whl` file to the Lakehouse Files section or save the report.
    The Microsoft Fabric Data Warehouse connector is pre-installed in the
    Fabric Spark runtime, and Query Insights is enabled by default on every Fabric Warehouse.

## Installing in Fabric

### Option A: Per-Notebook Install (Recommended)

Run PIP install 

```python
%pip install fabric-warehouse-advisor
```
This is the quickest way to get up and running.

For version information, dependencies, and release notes, see the [details](https://pypi.org/project/fabric-warehouse-advisor/).

### Option B: Download Pre-Built

!!! note "Download the `whl` file"
    Download the latest `.whl` file from the [GitHub Releases](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/latest) page — no build tools required.

1. Upload the `.whl` file to your Lakehouse **Files** area (see the [official documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/load-data-lakehouse#upload-files) for detailed upload instructions).
2. In the first cell of your notebook, run:

```python
%pip install /lakehouse/default/Files/fabric_warehouse_advisor-X.Y.Z-py3-none-any.whl
```

### Option C: Fabric Environment

!!! note "Download the `whl` file"
    Download the latest `.whl` file from the [GitHub Releases](https://github.com/tiagobalabuch/fabric-warehouse-advisor/releases/latest) page — no build tools required.

1. In your Fabric Workspace, create or open an **Environment** resource
2. Under **Libraries**, upload the `.whl` file
3. Publish the Environment
4. Attach the Environment to your notebook (Settings → Environment)

For a more permanent setup, you can attach the library to a Fabric Environment (see the [official documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/create-and-use-environment)):

With this approach the library is pre-installed on every session that uses the Environment — no `%pip install` needed.

## Running an Advisor

### Data Clustering Advisor

Analyses your warehouse and recommends which tables and columns should
use `CLUSTER BY`:

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

config = DataClusteringConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()
```

See the full [Data Clustering documentation](advisors/data-clustering/index.md)
for configuration options, scoring details, and report formats.

### Performance Check Advisor

Detects performance anti-patterns across custom SQL Pools, data types, caching, V-Order, and statistics:

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()
```

See the full [Performance Check documentation](advisors/performance-check/index.md) for configuration options, check categories, and report formats.

### Security Check Advisor

Analyses your warehouse security posture across OneLake Security permissions, roles, RLS, CLS, and Dynamic Data Masking:

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = SecurityCheckAdvisor(spark, config)
result = advisor.run()
```

See the full [Security Check documentation](advisors/security-check/index.md)
for configuration options, check categories, and report formats.

## Working with Results

Both advisors return a result object with pre-formatted reports in three
formats.

### Viewing Reports

```python
# Rich HTML report (recommended in Fabric notebooks)
displayHTML(result.html_report)

# Plain text
print(result.text_report)

# Markdown
print(result.markdown_report)
```

### Saving Reports

!!! warning "Lakehouse is required"
    The best way to visualize the report is to save it as `HTML`, which provides the full experience with rich features and interactivity.

```python
# Save as HTML (default)
result.save("/lakehouse/default/Files/reports/report.html")

# Save as Markdown
result.save("/lakehouse/default/Files/reports/report.md", "md")

# Save as plain text
result.save("/lakehouse/default/Files/reports/report.txt", "txt")
```

## Next Steps

- [Data Clustering Advisor](advisors/data-clustering/index.md) — full documentation
- [Performance Check Advisor](advisors/performance-check/index.md) — full documentation
- [Security Check Advisor](advisors/security-check/index.md) — full documentation
- [Cross-Workspace](cross-workspace.md) — analyse warehouses in other workspaces
- [Troubleshooting](troubleshooting.md) — common issues and solutions

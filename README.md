# Fabric Warehouse Advisor

A modular Python **advisory framework** for **Microsoft Fabric Warehouse**.
Each advisor module analyses a different aspect of warehouse health and
produces scored recommendations with rich reports.

## Available Advisors

| Document | What it does |
|---------|-------------|
| [**Getting Started**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/getting-started/) | Installation, first run, working with results | 
| [**Advisors Overview**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/) | Comparison of all available advisors |
| [**Data Clustering**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/data-clustering/) | Analyzes query patterns, table metadata, and column cardinality to identify and score the best candidate columns for data clustering, optimizing physical data organization on OneLake for better query speed. |
| [**Performance Check**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/performance-check/) | Identifies common performance pitfalls in Fabric Warehouses and Lakehouse SQL Endpoints by auditing Custom SQL Pools, data types, caching status, V-Order optimization, statistics health, and query performance regressions. |
| [**Security Check**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/security-check/) | Scans for security misconfigurations and OneLake Security settings, including schema permissions, custom roles, Row-Level Security (RLS), Column-Level Security (CLS), and Dynamic Data Masking, delivering actionable insights with concrete SQL remediation guidance | 

It runs entirely inside a **Fabric Notebook**. Spark connector for Microsoft Fabric Data Warehouse comes pre-installed in the Fabric runtime, and Query Insights is enabled by default on every Data Warehouse. A Lakehouse is required only when the solution is installed from a wheel file stored in OneLake.

## Screenshots

Each advisor produces a rich, interactive HTML report with light and dark themes.

### Data Clustering

<p>
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/data-clustering-light.png" alt="Data Clustering - Light" width="49%">
  
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/data-clustering-dark.png" alt="Data Clustering - Dark" width="49%">
</p>

### Security Check

<p>
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/security-check-light.png" alt="Security Check - Light" width="49%">

  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/security-check-dark.png" alt="Security Check - Dark" width="49%">
</p>

### Performance Check

<p>
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/performance-check-light.png" alt="Performance Check - Light" width="49%">
  
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/performance-check-dark.png" alt="Performance Check - Dark" width="49%">
</p>


## Acknowledgements

Report icons provided by [Flaticon](https://www.flaticon.com/):

- [Cyber security icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/cyber-security)
- [Performance icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/performance)
- [Graph icons created by Karacis - Flaticon](https://www.flaticon.com/free-icons/graph)
- [Idea icons created by berkahicon - Flaticon](https://www.flaticon.com/free-icons/idea)
- [Warning icons created by Hilmy Abiyyu A. - Flaticon](https://www.flaticon.com/free-icons/warning)

## License

MIT — see [LICENSE](LICENSE) for details.

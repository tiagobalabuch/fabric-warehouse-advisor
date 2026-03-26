# Data Type Reference

Not all SQL data types are supported for Data Clustering in Microsoft Fabric Warehouse. The advisor evaluates each candidate column's data
type and factors this into the score.

## Support Matrix

| Category | Data Type | Clustering Supported | Notes |
|----------|-----------|---------------------|-------|
| **Exact numerics** | `bigint` | Yes | Fully supported |
| | `int` | Yes | Fully supported |
| | `smallint` | Yes | Fully supported |
| | `decimal(p)` where p ≤ 18 | Yes | Fully supported |
| | `numeric(p)` where p ≤ 18 | Yes | Fully supported |
| | `decimal(p)` where p > 18 | Yes (with warnings) | Predicates won't push down to storage |
| | `numeric(p)` where p > 18 | Yes (with warnings) | Predicates won't push down to storage |
| | `bit` | **No** | Not supported |
| **Approximate numerics** | `float` | Yes | Fully supported |
| | `real` | Yes | Fully supported |
| **Date/time** | `date` | Yes | Fully supported |
| | `datetime2` | Yes | Fully supported |
| | `time` | Yes | Fully supported |
| **Character strings** | `char(n)` where n ≤ 32 | Yes | Fully supported |
| | `varchar(n)` where n ≤ 32 | Yes | Fully supported |
| | `char(n)` where n > 32 | Yes (with warnings) | Only first 32 characters produce stats |
| | `varchar(n)` where n > 32 | Yes (with warnings) | Only first 32 characters produce stats |
| **LOB types** | `varchar(max)` | **No** | LOB type — cannot be clustered |
| | `varbinary(max)` | **No** | LOB type — cannot be clustered |
| **Binary / other** | `varbinary` | **No** | Not supported |
| | `uniqueidentifier` | **No** | Not supported |

## Impact on Scoring

The data type assessment affects the score in two ways:

### 1. Data Type Factor (up to 15 points by default)

| Assessment | Score |
|-----------|-------|
| Fully supported | 100% of weight (15 points) |
| Supported with warnings | 65% of weight (≈10 points) |
| Not supported | 0 points |

### 2. Recommendation Label

Columns with unsupported data types are labelled "Not recommended (unsupported data type)" regardless of their composite score. If such a
column is currently in a `CLUSTER BY`, it receives "Already clustered — NOT RECOMMENDED (unsupported data type)".

## Warnings Explained

### `decimal`/`numeric` with precision > 18

```
Warning: precision=22 > 18 prevents predicate pushdown to storage.
```

While these types *can* be clustered, the query engine cannot push predicates down to the V-Order storage layer when precision exceeds 18.
This means queries filtering on such columns won't benefit from the columnar segment elimination that makes clustering effective.

**Recommendation:** If possible, reduce precision to ≤ 18. If the full
precision is required, clustering this column will have limited benefit.

### `char`/`varchar` with max_length > 32

```
Warning: max_length=100 > 32; only the first 32 characters are used
for column statistics.
```

Fabric Warehouse only considers the first 32 characters of string
columns for clustering statistics. If your filtering predicates depend
on characters beyond position 32, segment elimination won't be
effective.

**Recommendation:** This is usually fine for columns like short codes (`CountryCode`, `Currency`), but less effective for long natural-language columns or identifiers that share a common prefix.

## How the Advisor Detects Data Types

Column data types are read from `sys.columns` joined with `sys.types` in Phase 1 (Metadata Collection). The relevant fields are:

- `data_type` — the type name (e.g., `int`, `varchar`)
- `max_length` — storage length in bytes (−1 for MAX types)
- `precision` — numeric precision (relevant for `decimal`/`numeric`)

These are passed to `assess_data_type()` which returns a
`DataTypeAssessment` with:

- `is_supported` — whether clustering is allowed
- `support_label` — human-readable label ("Yes", "Yes (with warnings)", "No")
- `optimization_flag` — "OK" or a warning message
- `detail` — full explanation string

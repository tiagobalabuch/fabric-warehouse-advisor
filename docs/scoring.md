# Scoring

The advisor assigns each candidate column a **composite score from 0 to
100** by combining four weighted factors and applying a cardinality
penalty.

## Factor Breakdown

The default weights (all configurable):

| Factor | Max Points | Config Parameter | What it measures |
|--------|-----------|------------------|------------------|
| Table size | 30 | `score_weight_table_size` | How large the table is — clustering benefits scale with table size |
| Predicate frequency | 30 | `score_weight_predicate_freq` | How often the column appears in WHERE predicates |
| Cardinality | 25 | `score_weight_cardinality` | Whether the column has enough distinct values for effective clustering |
| Data type support | 15 | `score_weight_data_type` | Whether the data type is fully/partially/not supported |

> **Note:** Weights must sum to 100. This is validated at runtime.

## Table Size Scoring

Points are assigned based on row count relative to `large_table_rows`
(default: 50M):

| Condition | Points (% of weight) |
|-----------|---------------------|
| ≥ 10× large_table_rows (500M+) | 100% |
| ≥ 1× large_table_rows (50M+) | 75% |
| ≥ 0.1× large_table_rows (5M+) | 40% |
| ≥ 0.01× large_table_rows (500K+) | 15% |
| Below 0.01× | 0% |

## Predicate Frequency Scoring

Points based on weighted predicate hit count (weighted by
`number_of_runs` per query):

| Predicate Hits | Points (% of weight) |
|---------------|---------------------|
| ≥ 20 | 100% |
| ≥ 10 | 80% |
| ≥ 5 | 55% |
| ≥ 2 | 30% |
| 1 | 15% |
| 0 | 0% |

## Cardinality Scoring

Points based on cardinality classification:

| Cardinality Level | Points (% of weight) |
|-------------------|---------------------|
| High | 100% |
| Medium | 65% |
| Low | 10% |
| Unknown | 0% |

See [Configuration](configuration.md#cardinality-classification) for how
levels are determined.

## Data Type Scoring

| Condition | Points (% of weight) |
|-----------|---------------------|
| Fully supported, no warnings | 100% |
| Supported with warnings (e.g., `decimal(>18)`, `varchar(>32)`) | 65% |
| Not supported | 0% |

See [Data Type Reference](data-type-reference.md) for the full support matrix.

## Cardinality Penalty Multiplier

After computing the raw composite score (sum of the four factors), a
**penalty multiplier** is applied based on cardinality classification:

| Cardinality | Multiplier | Effect |
|-------------|-----------|--------|
| **High** | 1.0× | No penalty — ideal for clustering |
| **Medium** | 1.0× | No penalty — suitable for clustering |
| **Low** | 0.35× | Heavy penalty — too few distinct values |
| **Unknown** | 0.70× | Moderate penalty — could not be estimated |

### Why a penalty multiplier?

Without it, a Low-cardinality column on a huge, frequently-queried table
could score 77/100 — because cardinality is only 25% of the total
weight. Clustering such a column would be ineffective and wasteful.

With the penalty:

```
Raw score:  77  (30 table-size + 30 predicate + 2 cardinality + 15 data-type)
Penalty:    × 0.35
Final:      26  → well below the default threshold of 40
```

This ensures that cardinality — the most fundamental requirement for
effective clustering — has a decisive impact on the final recommendation.

## Recommendation Labels

After scoring, each column receives a label:

| Label | Condition |
|-------|-----------|
| **RECOMMENDED** | Score ≥ `min_recommendation_score + 20` (default: 60+) |
| **Consider** | Score ≥ `min_recommendation_score` (default: 40–59) |
| **Not recommended (low score)** | Score < `min_recommendation_score` |
| **Not recommended (low cardinality)** | Cardinality classified as Low |
| **Not recommended (unsupported data type)** | Data type cannot be clustered |
| **Already clustered** | Column is in an existing `CLUSTER BY` and scores well |
| **Already clustered — NOT RECOMMENDED** | Column is clustered but has low cardinality, unsupported type, or low score |

## Already-Clustered Column Validation

Columns that are already part of a `CLUSTER BY` are **not excluded** from
scoring. They go through the same evaluation pipeline and receive the
same score/label as any other column. This allows the advisor to:

- Confirm that existing clustering choices are effective
- Flag clustered columns that have **low cardinality** — suggesting they
  should be removed from `CLUSTER BY`
- Flag clustered columns with **unsupported data types** or **low scores**
- Generate explicit warnings in the report

## CTAS DDL Generation

When `generate_ctas=True`, the advisor generates one `CREATE TABLE ...
AS SELECT` statement per recommended column:

```sql
CREATE TABLE [schema].[table_clustered]
WITH (CLUSTER BY ([column_name]))
AS SELECT * FROM [schema].[table];
```

Only columns meeting **all** of these criteria get DDL:

- Not already clustered
- Supported data type
- Score ≥ `min_recommendation_score`
- Cardinality classified as Medium or High

The DDL appears in each per-table section and in a consolidated "All DDL"
section at the end of every report format.

## Worked Example

Consider a column `OrderDate` on a 200M-row table that appears in WHERE
predicates 12 times, has High cardinality, and is a `date` type:

| Factor | Calculation | Points |
|--------|------------|--------|
| Table size | 200M ≥ 10 × 50M → 100% of 30 | 30 |
| Predicate frequency | 12 hits ≥ 10 → 80% of 30 | 24 |
| Cardinality | High → 100% of 25 | 25 |
| Data type | `date` fully supported → 100% of 15 | 15 |
| **Raw composite** | | **94** |
| Penalty | High cardinality → 1.0× | **94** |
| **Label** | 94 ≥ 60 | **RECOMMENDED** |

Now compare with a `StatusCode` column on the same table — 8 predicate
hits, only 5 distinct values, `int` type:

| Factor | Calculation | Points |
|--------|------------|--------|
| Table size | 200M → 100% of 30 | 30 |
| Predicate frequency | 8 hits ≥ 5 → 55% of 30 | 16 |
| Cardinality | Low (5 ≤ 50 abs max) → 10% of 25 | 2 |
| Data type | `int` fully supported → 100% of 15 | 15 |
| **Raw composite** | | **63** |
| Penalty | Low cardinality → 0.35× | **22** |
| **Label** | 22 < 40 | **Not recommended (low cardinality)** |

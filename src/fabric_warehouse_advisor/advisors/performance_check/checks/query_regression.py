"""
Performance Check — Query Regression Detection
================================================
Compares recent query performance against a historical baseline
using ``queryinsights.exec_requests_history``.

A query shape (identified by ``query_hash``) is flagged when the
median elapsed time in the recent window is significantly worse
than its baseline median.

Both windows are computed from the same view so there is no
overlap between baseline and recent periods.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    LEVEL_INFO,
    LEVEL_MEDIUM,
    CATEGORY_QUERY_REGRESSION,
)


# -- SQL ----------------------------------------------------------------
# Fabric's SQL endpoint requires PERCENTILE_CONT with OVER() rather than
# the WITHIN GROUP (ORDER BY ...) aggregate form.  We use a subquery with
# the window function and then group to get one row per query_hash.

# Schemas whose queries should be excluded from regression analysis.
_EXCLUDED_SCHEMAS = [
    "queryinsights.",
    "sys.",
]


def _build_exclusion_clause() -> str:
    """Build a SQL AND clause that filters out commands touching excluded schemas."""
    if not _EXCLUDED_SCHEMAS:
        return ""
    parts = [f"command NOT LIKE '%{schema}%'" for schema in _EXCLUDED_SCHEMAS]
    return "      AND " + "\n      AND ".join(parts)


_BASELINE_QUERY = """
SELECT
    query_hash,
    COUNT(*)                                                        AS baseline_execs,
    CAST(MAX(pct_median) AS decimal(18,2))                          AS baseline_median_ms
FROM (
    SELECT
        query_hash,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_elapsed_time_ms)
            OVER (PARTITION BY query_hash)                          AS pct_median
    FROM queryinsights.exec_requests_history
    WHERE start_time >= DATEADD(day, -30, GETUTCDATE())
      AND start_time <  DATEADD(day, -{{lookback_days}}, GETUTCDATE())
{exclusions}
) sub
GROUP BY query_hash
HAVING COUNT(*) >= {{min_execs}}
""".format(exclusions=_build_exclusion_clause())

_RECENT_QUERY = """
SELECT
    query_hash,
    COUNT(*)                                                        AS recent_execs,
    CAST(MAX(pct_median) AS decimal(18,2))                          AS recent_median_ms,
    LEFT(MAX(command), 200)                                         AS query_text_preview
FROM (
    SELECT
        query_hash,
        command,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_elapsed_time_ms)
            OVER (PARTITION BY query_hash)                          AS pct_median
    FROM queryinsights.exec_requests_history
    WHERE start_time >= DATEADD(day, -{{lookback_days}}, GETUTCDATE())
{exclusions}
) sub
GROUP BY query_hash
HAVING COUNT(*) >= {{min_execs}}
""".format(exclusions=_build_exclusion_clause())


# -- Public API ---------------------------------------------------------

def check_query_regression(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Detect query shapes whose recent performance regressed.

    Compares the median elapsed time in the last *N* days (recent)
    against the preceding period (baseline) within the 30-day Query
    Insights retention window.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session with Fabric connectivity.
    warehouse : str
        Warehouse name.
    config : PerformanceCheckConfig
        Configuration with regression thresholds.

    Returns
    -------
    list[Finding]
        One finding per regressed query hash, graduated by severity.
    """
    findings: List[Finding] = []

    lookback = config.regression_lookback_days
    min_execs = config.regression_min_executions
    factor_warn = config.regression_factor_warning
    factor_crit = config.regression_factor_critical

    baseline_sql = _BASELINE_QUERY.format(
        lookback_days=int(lookback),
        min_execs=int(min_execs),
    )
    recent_sql = _RECENT_QUERY.format(
        lookback_days=int(lookback),
        min_execs=int(min_execs),
    )

    try:
        baseline_df = read_warehouse_query(
            spark, warehouse, baseline_sql,
            config.workspace_id, config.warehouse_id,
        )
        recent_df = read_warehouse_query(
            spark, warehouse, recent_sql,
            config.workspace_id, config.warehouse_id,
        )
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_MEDIUM,
            category=CATEGORY_QUERY_REGRESSION,
            check_name="regression_check_error",
            object_name=warehouse,
            message="Query regression check could not be executed.",
            detail=str(exc)[:300],
            recommendation=(
                "Verify that queryinsights.exec_requests_history is accessible "
                "and that the Spark session has sufficient permissions."
            ),
        ))
        return findings

    # Join in PySpark to avoid connector issues with complex CTEs
    baseline_rows = {row["query_hash"]: row for row in baseline_df.collect()}
    recent_rows = recent_df.collect()

    matched = []
    for r in recent_rows:
        b = baseline_rows.get(r["query_hash"])
        if b is None:
            continue
        baseline_ms = float(b["baseline_median_ms"] or 0)
        recent_ms = float(r["recent_median_ms"] or 0)
        if baseline_ms <= 0:
            continue
        factor = recent_ms / baseline_ms
        if factor >= factor_warn:
            matched.append((r, b, factor))

    if not matched:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_QUERY_REGRESSION,
            check_name="no_regression_detected",
            object_name=warehouse,
            message="No query regressions detected.",
            detail=(
                f"Compared last {lookback} days against prior baseline "
                f"(min {min_execs} executions, ≥{factor_warn}x threshold)."
            ),
        ))
        return findings

    # Sort by regression factor descending
    matched.sort(key=lambda x: x[2], reverse=True)

    for r, b, factor in matched:
        q_hash = str(r["query_hash"] or "unknown")
        preview = str(r["query_text_preview"] or "")[:200]
        baseline_ms = float(b["baseline_median_ms"] or 0)
        recent_ms = float(r["recent_median_ms"] or 0)
        baseline_execs = int(b["baseline_execs"] or 0)
        recent_execs = int(r["recent_execs"] or 0)

        level = LEVEL_CRITICAL if factor >= factor_crit else LEVEL_HIGH

        findings.append(Finding(
            level=level,
            category=CATEGORY_QUERY_REGRESSION,
            check_name="query_regression_detected",
            object_name=q_hash,
            message=(
                f"Query regressed {factor:.1f}x "
                f"(baseline: {baseline_ms:,.0f} ms → recent: {recent_ms:,.0f} ms, "
                f"{recent_execs} recent executions)"
            ),
            detail=preview if preview else "",
            recommendation=(
                "Review query execution plan for changes. "
                "Check if statistics are stale or schema has changed. "
                "Consider running UPDATE STATISTICS on referenced tables."
            ),
        ))

    return findings

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
    CATEGORY_QUERY_REGRESSION,
)


# -- SQL ----------------------------------------------------------------

_REGRESSION_QUERY = """
WITH baseline AS (
    SELECT
        query_hash,
        COUNT(*)                                                    AS baseline_execs,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_elapsed_time_ms) AS baseline_median_ms
    FROM queryinsights.exec_requests_history
    WHERE start_time >= DATEADD(day, -30, GETUTCDATE())
      AND start_time <  DATEADD(day, -{lookback_days}, GETUTCDATE())
    GROUP BY query_hash
    HAVING COUNT(*) >= {min_execs}
),
recent AS (
    SELECT
        query_hash,
        COUNT(*)                                                    AS recent_execs,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_elapsed_time_ms) AS recent_median_ms,
        MAX(command)                                                AS last_command
    FROM queryinsights.exec_requests_history
    WHERE start_time >= DATEADD(day, -{lookback_days}, GETUTCDATE())
    GROUP BY query_hash
    HAVING COUNT(*) >= {min_execs}
)
SELECT
    r.query_hash,
    LEFT(r.last_command, 200)                                      AS query_text_preview,
    b.baseline_execs,
    r.recent_execs,
    CAST(b.baseline_median_ms AS decimal(12,2))                    AS baseline_median_ms,
    CAST(r.recent_median_ms  AS decimal(12,2))                     AS recent_median_ms,
    CAST(r.recent_median_ms / NULLIF(b.baseline_median_ms, 0)
         AS decimal(10,2))                                         AS regression_factor
FROM recent r
JOIN baseline b ON b.query_hash = r.query_hash
WHERE r.recent_median_ms > b.baseline_median_ms * {factor_threshold}
ORDER BY regression_factor DESC
"""


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

    query = _REGRESSION_QUERY.format(
        lookback_days=int(lookback),
        min_execs=int(min_execs),
        factor_threshold=float(factor_warn),
    )

    try:
        df = read_warehouse_query(
            spark, warehouse, query,
            config.workspace_id, config.warehouse_id,
        )
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_QUERY_REGRESSION,
            check_name="regression_check_error",
            object_name=warehouse,
            message="Query regression check could not be executed.",
            detail=str(exc)[:300],
        ))
        return findings

    rows = df.collect()

    if not rows:
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

    for row in rows:
        q_hash = str(row["query_hash"] or "unknown")
        preview = str(row["query_text_preview"] or "")[:200]
        baseline_ms = float(row["baseline_median_ms"] or 0)
        recent_ms = float(row["recent_median_ms"] or 0)
        factor = float(row["regression_factor"] or 0)
        baseline_execs = int(row["baseline_execs"] or 0)
        recent_execs = int(row["recent_execs"] or 0)

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

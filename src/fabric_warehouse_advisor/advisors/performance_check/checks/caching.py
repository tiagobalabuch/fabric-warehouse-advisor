"""
Performance Check — Caching Analysis
======================================
Checks result-set caching configuration and analyses cold-start
patterns from ``queryinsights.exec_requests_history``.

Checks
------
* Is result set caching enabled on the warehouse?
* Cold-start detection (``data_scanned_remote_storage_mb > 0``)
* Result-set cache hit ratio analysis
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_CRITICAL,
    CATEGORY_CACHING,
)


# -- SQL ----------------------------------------------------------------

_RESULT_CACHE_STATUS_QUERY = """
    SELECT name, is_result_set_caching_on
    FROM sys.databases
    WHERE database_id = DB_ID()
"""

_CACHE_HIT_ANALYSIS_QUERY = """
    SELECT
        result_cache_hit,
        COUNT(*) AS query_count,
        AVG(data_scanned_remote_storage_mb) AS avg_remote_mb,
        SUM(CASE WHEN data_scanned_remote_storage_mb > 0 THEN 1 ELSE 0 END) AS cold_start_count
    FROM queryinsights.exec_requests_history
    WHERE submit_time >= DATEADD(HOUR, -{lookback_hours}, GETUTCDATE())
    GROUP BY result_cache_hit
"""

_COLD_START_DETAIL_QUERY = """
    SELECT TOP 20
        command,
        data_scanned_remote_storage_mb,
        total_elapsed_time_ms,
        submit_time
    FROM queryinsights.exec_requests_history
    WHERE submit_time >= DATEADD(HOUR, -{lookback_hours}, GETUTCDATE())
        AND data_scanned_remote_storage_mb > 0
"""


# -- Public API ---------------------------------------------------------

def check_caching(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Analyse result-set caching status and cold-start patterns.

    Returns
    -------
    list[Finding]
        Findings related to caching configuration and behaviour.
    """
    findings: List[Finding] = []

    # --- Result set caching status ---
    if config.result_cache_check:
        findings.extend(_check_result_cache_status(spark, warehouse, config))

    # --- Cold start / cache hit analysis ---
    if config.cold_start_analysis:
        findings.extend(_check_cold_start(spark, warehouse, config))

    return findings


# -- Internal checks ----------------------------------------------------

def _check_result_cache_status(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Check if result set caching is enabled."""
    findings: List[Finding] = []
    try:
        df = read_warehouse_query(
            spark, warehouse, _RESULT_CACHE_STATUS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
        if not rows:
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_CACHING,
                check_name="result_cache_status_unknown",
                object_name=warehouse,
                message="Could not determine result set caching status.",
                detail="The sys.databases query returned no rows.",
            ))
            return findings

        row = rows[0]
        is_enabled = row["is_result_set_caching_on"]

        if is_enabled:
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_CACHING,
                check_name="result_cache_enabled",
                object_name=warehouse,
                message="Result set caching is ENABLED.",
                detail=(
                    "Result set caching persists query results so subsequent "
                    "identical queries return instantly. This is the recommended "
                    "configuration for analytical workloads."
                ),
            ))
        else:
            findings.append(Finding(
                level=LEVEL_CRITICAL,
                category=CATEGORY_CACHING,
                check_name="result_cache_disabled",
                object_name=warehouse,
                message="Result set caching is DISABLED.",
                detail=(
                    "With result set caching disabled, every query execution "
                    "fully recomputes results even if the underlying data "
                    "has not changed. This increases CU consumption and latency."
                ),
                recommendation=(
                    "Enable result set caching unless you have a specific reason "
                    "to keep it off (e.g. real-time data requirements)."
                ),
                sql_fix=(
                    f"ALTER DATABASE [{warehouse}]\n"
                    f"SET RESULT_SET_CACHING ON;"
                ),
            ))
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CACHING,
            check_name="result_cache_check_failed",
            object_name=warehouse,
            message=f"Could not check result set caching: {exc}",
        ))
    return findings


def _check_cold_start(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Analyse cold-start patterns and cache hit ratios."""
    findings: List[Finding] = []
    lookback = config.cold_start_lookback_hours

    try:
        query = _CACHE_HIT_ANALYSIS_QUERY.format(lookback_hours=lookback)
        df = read_warehouse_query(
            spark, warehouse, query,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()

        if not rows:
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_CACHING,
                check_name="no_query_history",
                object_name=warehouse,
                message=f"No query history found in the last {lookback} hours.",
                detail="queryinsights.exec_requests_history has no recent records.",
            ))
            return findings

        # Parse cache hit stats
        total_queries = 0
        cache_hits = 0   # result_cache_hit = 2
        cache_creates = 0  # result_cache_hit = 1
        cache_miss = 0     # result_cache_hit = 0
        total_cold_starts = 0

        for row in rows:
            rch = row["result_cache_hit"]
            count = row["query_count"]
            cold = row["cold_start_count"]
            total_queries += count
            total_cold_starts += cold
            if rch == 2:
                cache_hits = count
            elif rch == 1:
                cache_creates = count
            elif rch == 0:
                cache_miss = count

        # Cache hit ratio
        if total_queries > 0:
            hit_ratio = cache_hits / total_queries

            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_CACHING,
                check_name="cache_hit_summary",
                object_name=warehouse,
                message=(
                    f"Cache analysis ({lookback}h): {total_queries} queries, "
                    f"{cache_hits} hits ({hit_ratio:.0%} hit ratio)."
                ),
                detail=(
                    f"Cache hits (reused): {cache_hits} | "
                    f"Cache creates: {cache_creates} | "
                    f"Not cacheable: {cache_miss}"
                ),
            ))

            if hit_ratio < config.cache_hit_ratio_warning_threshold:
                findings.append(Finding(
                    level=LEVEL_MEDIUM,
                    category=CATEGORY_CACHING,
                    check_name="low_cache_hit_ratio",
                    object_name=warehouse,
                    message=(
                        f"Cache hit ratio is {hit_ratio:.0%} "
                        f"(threshold: {config.cache_hit_ratio_warning_threshold:.0%})."
                    ),
                    detail=(
                        "A low cache hit ratio means most queries cannot reuse "
                        "cached results. This can happen when queries use "
                        "non-deterministic functions, VARCHAR(MAX) output columns, "
                        "window functions, or when underlying data changes frequently."
                    ),
                    recommendation=(
                        "Review the result set caching disqualification rules. "
                        "Common fixes: avoid GETDATE() in queries, remove "
                        "VARCHAR(MAX) columns from output, reduce ORDER BY on "
                        "non-output columns."
                    ),
                ))

        # Cold start analysis
        if total_cold_starts > 0:
            cold_pct = total_cold_starts / total_queries if total_queries > 0 else 0
            level = LEVEL_MEDIUM if cold_pct > 0.3 else LEVEL_LOW
            findings.append(Finding(
                level=level,
                category=CATEGORY_CACHING,
                check_name="cold_start_detected",
                object_name=warehouse,
                message=(
                    f"{total_cold_starts} queries ({cold_pct:.0%}) fetched data "
                    f"from remote storage (cold start)."
                ),
                detail=(
                    "Non-zero data_scanned_remote_storage_mb indicates data was "
                    "fetched from OneLake rather than local cache. This is normal "
                    "for first-run queries but should decrease on subsequent runs."
                ),
                recommendation=(
                    "Don't judge query performance on the first execution. "
                    "Check data_scanned_remote_storage_mb to distinguish cold "
                    "start from actual performance issues."
                ),
            ))

    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CACHING,
            check_name="cold_start_check_failed",
            object_name=warehouse,
            message=f"Could not analyse cache/cold-start patterns: {exc}",
            detail="Ensure Query Insights is enabled on the warehouse.",
        ))

    return findings

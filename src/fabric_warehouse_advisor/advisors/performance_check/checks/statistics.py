"""
Performance Check — Statistics Health
======================================
Analyses the health of query optimizer statistics in the warehouse.

Checks
------
* Is proactive statistics refresh enabled?
* Are auto-create and auto-update statistics enabled?
* Stale statistics (older than threshold)
* Row count drift (actual vs. statistics estimate)
* Tables with no statistics at all
* Orphaned / non-actionable statistics objects
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query, get_table_row_counts
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_STATISTICS,
)


# -- SQL ----------------------------------------------------------------

_DATABASE_STATS_CONFIG_QUERY = """
    SELECT
        name,
        is_auto_create_stats_on,
        is_auto_update_stats_on
    FROM sys.databases
    WHERE database_id = DB_ID()
"""

_PROACTIVE_REFRESH_SCOPED_CONFIG_QUERY = """
    SELECT name, value
    FROM sys.database_scoped_configurations
    WHERE name = 'PROACTIVE_STATS_COLLECTION'
"""

_PROACTIVE_REFRESH_QUERY = """
    SELECT
        name,
        is_proactive_statistics_refresh_on
    FROM sys.databases
    WHERE database_id = DB_ID()
"""

_ALL_STATISTICS_QUERY = """
    SELECT
        SCHEMA_NAME(o.schema_id)      AS schema_name,
        o.name                        AS table_name,
        c.name                        AS column_name,
        s.name                        AS stats_name,
        s.stats_id,
        s.auto_created,
        s.user_created,
        s.stats_generation_method_desc,
        STATS_DATE(s.object_id, s.stats_id) AS stats_update_date
    FROM sys.stats AS s
    INNER JOIN sys.objects AS o
        ON o.object_id = s.object_id
    LEFT JOIN sys.stats_columns AS sc
        ON s.object_id = sc.object_id
        AND s.stats_id = sc.stats_id
    LEFT JOIN sys.columns AS c
        ON sc.object_id = c.object_id
        AND c.column_id = sc.column_id
    WHERE o.type = 'U'
"""

_STATS_HEADER_QUERY = """
    DBCC SHOW_STATISTICS ('{schema}.{table}', '{stats_name}') WITH STAT_HEADER
"""


# -- Public API ---------------------------------------------------------

def check_statistics(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    row_counts: Dict[Tuple[str, str], int] | None = None,
) -> List[Finding]:
    """Analyse statistics health across all user tables.

    Parameters
    ----------
    row_counts : dict, optional
        Pre-computed ``(schema, table) -> row_count``. If not provided,
        row counts are fetched from ``sys.partitions``.

    Returns
    -------
    list[Finding]
        Findings related to statistics health.
    """
    findings: List[Finding] = []

    # --- Database-level settings ---
    findings.extend(_check_stats_config(spark, warehouse, config))

    if config.proactive_refresh_check:
        findings.extend(_check_proactive_refresh(spark, warehouse, config))

    # --- Per-table statistics analysis ---
    findings.extend(_check_stats_health(spark, warehouse, config, row_counts))

    return findings


# -- Internal checks ----------------------------------------------------

def _check_stats_config(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Check auto-create and auto-update statistics settings."""
    findings: List[Finding] = []

    try:
        df = read_warehouse_query(
            spark, warehouse, _DATABASE_STATS_CONFIG_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
        if not rows:
            return findings

        row = rows[0]
        auto_create = row["is_auto_create_stats_on"]
        auto_update = row["is_auto_update_stats_on"]

        if auto_create:
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_STATISTICS,
                check_name="auto_create_stats_on",
                object_name=warehouse,
                message="Auto-create statistics is ENABLED.",
                detail="The engine automatically creates statistics at query time when needed.",
            ))
        else:
            findings.append(Finding(
                level=LEVEL_CRITICAL,
                category=CATEGORY_STATISTICS,
                check_name="auto_create_stats_off",
                object_name=warehouse,
                message="Auto-create statistics is DISABLED.",
                detail=(
                    "The query optimizer cannot automatically create statistics. "
                    "This forces the optimizer to make poor cardinality estimates, "
                    "leading to sub-optimal execution plans."
                ),
                recommendation="Enable auto-create statistics for the warehouse.",
            ))

        if auto_update:
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_STATISTICS,
                check_name="auto_update_stats_on",
                object_name=warehouse,
                message="Auto-update statistics is ENABLED.",
                detail=(
                    "The engine automatically refreshes stale statistics when "
                    "significant data changes are detected."
                ),
            ))
        else:
            findings.append(Finding(
                level=LEVEL_CRITICAL,
                category=CATEGORY_STATISTICS,
                check_name="auto_update_stats_off",
                object_name=warehouse,
                message="Auto-update statistics is DISABLED.",
                detail=(
                    "Statistics will not be refreshed automatically after data "
                    "changes. Stale statistics cause the optimizer to choose "
                    "poor execution plans."
                ),
                recommendation="Enable auto-update statistics for the warehouse.",
            ))
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_STATISTICS,
            check_name="stats_config_check_failed",
            object_name=warehouse,
            message=f"Could not check statistics configuration: {exc}",
        ))

    return findings


def _check_proactive_refresh(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Check if proactive statistics refresh is enabled."""
    findings: List[Finding] = []

    is_proactive = _read_proactive_flag(spark, warehouse, config)
    if is_proactive is None:
        # Neither approach could determine the setting
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_STATISTICS,
            check_name="proactive_refresh_check_skipped",
            object_name=warehouse,
            message=(
                "Could not determine proactive statistics refresh status."
            ),
            detail=(
                "The 'is_proactive_statistics_refresh_on' column and the "
                "'PROACTIVE_STATS_COLLECTION' scoped configuration are not "
                "available on this warehouse. This feature may not be "
                "supported on the current Fabric SKU or warehouse edition."
            ),
        ))
        return findings

    if is_proactive:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_STATISTICS,
            check_name="proactive_refresh_on",
            object_name=warehouse,
            message="Proactive statistics refresh is ENABLED.",
            detail=(
                "Fabric proactively refreshes histogram statistics "
                "(_WA_Sys_*) after data changes, reducing the likelihood "
                "of query delays caused by synchronous stats updates."
            ),
        ))
    else:
        findings.append(Finding(
            level=LEVEL_MEDIUM,
            category=CATEGORY_STATISTICS,
            check_name="proactive_refresh_off",
            object_name=warehouse,
            message="Proactive statistics refresh is DISABLED.",
            detail=(
                "Without proactive refresh, statistics are only updated "
                "synchronously at query time. This can add latency to "
                "the first query after a data load."
            ),
            recommendation=(
                "Enable proactive statistics refresh to pre-populate "
                "statistics after data changes."
            ),
            sql_fix=(
                "ALTER DATABASE CURRENT\n"
                "SET PROACTIVE_STATS_COLLECTION = ON;"
            ),
        ))

    return findings


def _read_proactive_flag(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> bool | None:
    """Try to read the proactive stats collection setting.

    Tries ``sys.database_scoped_configurations`` first (more widely
    available), then falls back to the ``sys.databases`` column.

    Returns ``True``/``False`` if the value could be read, or ``None``
    if neither approach is supported.
    """
    # Attempt 1: database_scoped_configurations
    try:
        df = read_warehouse_query(
            spark, warehouse, _PROACTIVE_REFRESH_SCOPED_CONFIG_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
        if rows:
            value = rows[0]["value"]
            # value is typically a string '0' or '1', or an int
            return str(value).strip() not in ("0", "OFF", "False", "false", "")
    except Exception:
        pass

    # Attempt 2: sys.databases column (may not exist on all editions)
    try:
        df = read_warehouse_query(
            spark, warehouse, _PROACTIVE_REFRESH_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
        if rows:
            return bool(rows[0]["is_proactive_statistics_refresh_on"])
    except Exception:
        pass

    return None


def _check_stats_health(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    row_counts: Dict[Tuple[str, str], int] | None = None,
) -> List[Finding]:
    """Check individual statistics objects for staleness and row drift."""
    findings: List[Finding] = []

    # Fetch all statistics
    try:
        stats_df = read_warehouse_query(
            spark, warehouse, _ALL_STATISTICS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        stats_rows = stats_df.collect()
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_STATISTICS,
            check_name="stats_health_check_failed",
            object_name=warehouse,
            message=f"Could not read statistics catalog: {exc}",
        ))
        return findings

    # Fetch actual row counts if not provided
    if row_counts is None:
        row_counts = _fetch_row_counts(spark, warehouse, config)

    # Group stats by table for summary
    from collections import defaultdict
    from datetime import datetime, timezone, timedelta

    tables_with_stats: set = set()
    stale_threshold = timedelta(days=config.stale_stats_threshold_days)
    now = datetime.now(timezone.utc)

    stats_by_table: dict = defaultdict(list)
    for srow in stats_rows:
        schema = srow["schema_name"]
        table = srow["table_name"]

        # Apply scope filters
        if config.schema_names:
            if schema.lower() not in [s.lower() for s in config.schema_names]:
                continue
        if config.table_names:
            if not _matches_table_filter(schema, table, config.table_names):
                continue

        tables_with_stats.add((schema, table))
        stats_by_table[(schema, table)].append(srow)

        column = srow["column_name"] or "(unknown)"
        stats_name = srow["stats_name"]
        update_date = srow["stats_update_date"]
        auto_created = srow["auto_created"]
        user_created = srow["user_created"]
        stats_method = srow["stats_generation_method_desc"] or ""

        fqn = f"[{schema}].[{table}].[{column}]"

        # --- Stale statistics ---
        if update_date is not None:
            try:
                if hasattr(update_date, 'tzinfo') and update_date.tzinfo is None:
                    update_date = update_date.replace(tzinfo=timezone.utc)
                age = now - update_date
                if age > stale_threshold:
                    days_old = age.days
                    findings.append(Finding(
                        level=LEVEL_HIGH,
                        category=CATEGORY_STATISTICS,
                        check_name="stale_statistics",
                        object_name=fqn,
                        message=(
                            f"Statistics '{stats_name}' are {days_old} days old "
                            f"(threshold: {config.stale_stats_threshold_days} days)."
                        ),
                        detail=(
                            f"Last updated: {update_date.strftime('%Y-%m-%d %H:%M UTC')}. "
                            f"Auto-created: {bool(auto_created)}. "
                            f"User-created: {bool(user_created)}."
                        ),
                        recommendation=(
                            f"Update statistics to reflect current data distribution."
                        ),
                        sql_fix=(
                            f"UPDATE STATISTICS [{schema}].[{table}] "
                            f"({stats_name}) WITH FULLSCAN;"
                        ),
                    ))
            except (TypeError, ValueError):
                pass  # Could not parse date; skip staleness check

    # --- Row count drift ---
    for (schema, table), stats_list in stats_by_table.items():
        actual_rows = row_counts.get((schema, table))
        if actual_rows is None or actual_rows <= 0:
            continue

        # Check every histogram stat for row count drift
        histogram_stats = [
            s for s in stats_list
            if (s["stats_name"] or "").startswith("_WA_Sys_") or s["user_created"]
        ]
        if not histogram_stats:
            continue

        for stat in histogram_stats:
            stats_name = stat["stats_name"]
            column = stat["column_name"] or "(unknown)"

            try:
                header_query = _STATS_HEADER_QUERY.format(
                    schema=schema, table=table, stats_name=stats_name,
                )
                header_df = read_warehouse_query(
                    spark, warehouse, header_query,
                    config.workspace_id, config.warehouse_id,
                )
                header_rows = header_df.collect()
                if header_rows:
                    header = header_rows[0]
                    stats_rows_est = header["Rows"]
                    if stats_rows_est is not None and stats_rows_est > 0:
                        drift_pct = abs(actual_rows - stats_rows_est) / stats_rows_est * 100
                        if drift_pct > config.row_drift_pct_threshold:
                            level = LEVEL_CRITICAL if drift_pct > 50 else LEVEL_HIGH
                            fqn = f"[{schema}].[{table}].[{column}]"
                            findings.append(Finding(
                                level=level,
                                category=CATEGORY_STATISTICS,
                                check_name="row_count_drift",
                                object_name=fqn,
                                message=(
                                    f"Row count drift: {drift_pct:.1f}% "
                                    f"(threshold: {config.row_drift_pct_threshold}%)."
                                ),
                                detail=(
                                    f"Actual rows: {actual_rows:,} | "
                                    f"Stats estimate: {int(stats_rows_est):,} | "
                                    f"Drift: {drift_pct:.1f}%. "
                                    f"Statistics: {stats_name}."
                                ),
                                recommendation=(
                                    "Update statistics so the optimizer has accurate "
                                    "row count estimates for plan costing."
                                ),
                                sql_fix=(
                                    f"UPDATE STATISTICS [{schema}].[{table}] "
                                    f"({stats_name}) WITH FULLSCAN;"
                                ),
                            ))
            except Exception:
                pass  # DBCC may fail for some stats types; skip silently

    # --- Tables without any statistics ---
    if config.tables_without_stats_check:
        all_tables = set(row_counts.keys()) if row_counts else set()
        tables_no_stats = all_tables - tables_with_stats
        for (schema, table) in tables_no_stats:
            rc = row_counts.get((schema, table), 0) if row_counts else 0
            if rc > 0:
                findings.append(Finding(
                    level=LEVEL_HIGH,
                    category=CATEGORY_STATISTICS,
                    check_name="no_statistics",
                    object_name=f"[{schema}].[{table}]",
                    message=f"Table has {rc:,} rows but no statistics.",
                    detail=(
                        "No auto-created or user-created statistics found. "
                        "The optimizer will generate statistics on first query, "
                        "which may cause cold-start latency."
                    ),
                    recommendation=(
                        "Consider running a representative query to trigger "
                        "auto-statistics creation, or create statistics manually "
                        "on frequently filtered/joined columns."
                    ),
                ))

    return findings


# -- Helpers -------------------------------------------------------------

def _fetch_row_counts(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> Dict[Tuple[str, str], int]:
    """Fetch actual row counts via the shared warehouse_reader helper."""
    try:
        rc_df = get_table_row_counts(
            spark, warehouse,
            workspace_id=config.workspace_id,
            warehouse_id=config.warehouse_id,
            verbose=config.verbose,
        )
        return {
            (row["schema_name"], row["table_name"]): row["row_count"]
            for row in rc_df.collect()
            if row["row_count"] is not None and row["row_count"] >= 0
        }
    except Exception:
        return {}


def _matches_table_filter(
    schema: str, table: str, table_names: list[str]
) -> bool:
    """Check if a table matches the user-specified filter list."""
    for entry in table_names:
        parts = entry.replace("[", "").replace("]", "").split(".")
        if len(parts) == 2:
            if parts[0].strip().lower() == schema.lower() and parts[1].strip().lower() == table.lower():
                return True
        else:
            if parts[0].strip().lower() == table.lower():
                return True
    return False

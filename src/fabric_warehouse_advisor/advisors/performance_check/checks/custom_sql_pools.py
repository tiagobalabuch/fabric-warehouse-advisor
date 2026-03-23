"""
Performance Check — Custom SQL Pools
======================================
Analyses the Custom SQL Pools configuration for a Fabric Warehouse
workspace via the REST API, and monitors pool pressure via
``queryinsights.sql_pool_insights``.

Checks
------
* Custom SQL Pools enabled/disabled awareness
* Resource allocation sum ≠ 100%
* Low resource allocation per pool (risk on SKU downscale)
* Single pool dominance (≥ 90% to one pool)
* Empty classifier values
* Read optimization heuristic for reporting pools
* Pool count near the 8-pool limit
* Pool under pressure (from Query Insights)
* Unclassified traffic (app names not matching any classifier)
* Known Fabric app patterns not classified
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ....core.fabric_rest_client import FabricRestClient, FabricRestError
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
)

CATEGORY_CUSTOM_SQL_POOLS = "custom_sql_pools"

# -- Well-known Fabric application name patterns -------------------------

_KNOWN_PATTERNS = {
    "Fabric Pipelines": r"^Data Integration-to[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
    "Power BI": r"^(PowerBIPremium-DirectQuery|Mashup Engine(?: \(PowerBIPremium-Import\))?)",
    "SQL Query Editor": r"DMS_user",
}

# -- Heuristics for read-heavy pool detection ----------------------------

_READ_POOL_NAME_HINTS = re.compile(
    r"powerbi|power.bi|report|dashboard|analytics|directquery|bi",
    re.IGNORECASE,
)

_READ_CLASSIFIER_HINTS = re.compile(
    r"PowerBI|DirectQuery|Mashup Engine|report|dashboard|analytics",
    re.IGNORECASE,
)

# -- SQL -----------------------------------------------------------------

_POOL_PRESSURE_QUERY = """
    SELECT
        sql_pool_name,
        COUNT(*) AS pressure_events,
        MIN([timestamp]) AS first_pressure,
        MAX([timestamp]) AS last_pressure,
        MAX(max_resource_percentage) AS allocated_pct
    FROM queryinsights.sql_pool_insights
    WHERE is_pool_under_pressure = 1
      AND [timestamp] > DATEADD(HOUR, -{lookback_hours}, GETUTCDATE())
    GROUP BY sql_pool_name
"""

_DISTINCT_PROGRAM_NAMES_QUERY = """
    SELECT DISTINCT program_name
    FROM queryinsights.exec_requests_history
    WHERE submit_time >= DATEADD(HOUR, -{lookback_hours}, GETUTCDATE())
      AND program_name IS NOT NULL
      AND program_name <> ''
"""

_MAX_POOL_COUNT = 8


# -- Public API -----------------------------------------------------------

def check_custom_sql_pools(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    rest_client: Optional[FabricRestClient] = None,
) -> List[Finding]:
    """Analyse Custom SQL Pools configuration and runtime pressure.

    Parameters
    ----------
    spark : SparkSession
        Active PySpark session.
    warehouse : str
        Warehouse name (for Query Insights T-SQL queries).
    config : PerformanceCheckConfig
        Advisor configuration.
    rest_client : FabricRestClient or None
        REST client for the Fabric API.  If ``None``, the check emits
        an INFO finding and returns early.

    Returns
    -------
    list[Finding]
        Findings related to Custom SQL Pools.
    """
    findings: List[Finding] = []

    # ── Fetch pool configuration via REST API ──────────────────────
    if rest_client is None or not config.workspace_id:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="custom_pools_skipped",
            object_name=warehouse,
            message=(
                "Custom SQL Pools check skipped — REST API client "
                "or workspace ID not available."
            ),
        ))
        return findings

    pool_config: Optional[Dict[str, Any]] = None
    try:
        pool_config = rest_client.get_sql_pools_configuration(
            config.workspace_id,
        )
    except FabricRestError as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="custom_pools_api_error",
            object_name=warehouse,
            message=f"Could not retrieve Custom SQL Pools configuration: {exc}",
            detail=(
                "The SQL pools configuration API may not be available "
                "for this workspace or the caller lacks permission."
            ),
        ))
        return findings

    if pool_config is None:
        return findings

    # ── Check 1: Feature enabled? ──────────────────────────────────
    enabled = pool_config.get("customSQLPoolsEnabled", False)
    pools: List[Dict[str, Any]] = pool_config.get("customSQLPools", [])

    if not enabled:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="custom_pools_not_enabled",
            object_name=warehouse,
            message=(
                "Custom SQL Pools are not enabled. The workspace uses "
                "default autonomous workload management (SELECT vs non-SELECT)."
            ),
            recommendation=(
                "Consider enabling Custom SQL Pools if multiple applications "
                "share this workspace and compete for compute resources."
            ),
        ))
        return findings

    if not pools:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="custom_pools_enabled_no_pools",
            object_name=warehouse,
            message=(
                "Custom SQL Pools are enabled but no pools are configured."
            ),
        ))
        return findings

    # ── Configuration checks (REST-only) ───────────────────────────
    findings.extend(_check_resource_sum(pools, warehouse))
    findings.extend(_check_resource_imbalance(pools, warehouse, config))
    findings.extend(_check_single_pool_dominance(pools, warehouse, config))
    findings.extend(_check_empty_classifiers(pools, warehouse))
    findings.extend(_check_read_optimization(pools, warehouse))
    findings.extend(_check_pool_count(pools, warehouse))

    # ── Runtime checks (T-SQL + REST) ──────────────────────────────
    findings.extend(
        _check_pool_pressure(spark, warehouse, config)
    )
    findings.extend(
        _check_unclassified_traffic(spark, warehouse, config, pools)
    )
    findings.extend(
        _check_known_apps_unclassified(spark, warehouse, config, pools)
    )

    return findings


# -- Configuration checks ------------------------------------------------

def _check_resource_sum(
    pools: List[Dict[str, Any]],
    warehouse: str,
) -> List[Finding]:
    """Check 3: Resource percentages should sum to 100%."""
    findings: List[Finding] = []
    total_pct = sum(p.get("maxResourcePercentage", 0) for p in pools)

    if total_pct != 100:
        direction = "Under-allocation" if total_pct < 100 else "Over-allocation"
        detail = (
            f"Unused capacity is wasted."
            if total_pct < 100
            else f"Over-allocation may lead to unexpected resource contention."
        )
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="resource_sum_mismatch",
            object_name=warehouse,
            message=(
                f"Total resource allocation is {total_pct}% instead of 100%. "
                f"{direction} detected."
            ),
            detail=detail,
            recommendation=(
                "Adjust pool percentages to sum to exactly 100% "
                "for optimal resource utilization."
            ),
        ))
    return findings


def _check_resource_imbalance(
    pools: List[Dict[str, Any]],
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Check 4: Flag pools with very low resource allocation."""
    findings: List[Finding] = []
    threshold = config.pool_min_resource_pct_warning

    for pool in pools:
        pct = pool.get("maxResourcePercentage", 0)
        name = pool.get("name", "(unnamed)")
        if pct <= threshold:
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_CUSTOM_SQL_POOLS,
                check_name="resource_allocation_imbalance",
                object_name=f"Pool: {name}",
                message=(
                    f"Pool \"{name}\" is allocated only {pct}% of resources."
                ),
                detail=(
                    f"Pools with very low allocation risk the error "
                    f"'The assigned SQL pool for this query has no resources "
                    f"and must be reconfigured' when the capacity SKU is "
                    f"downscaled."
                ),
                recommendation=(
                    f"Ensure this pool has sufficient resources for its "
                    f"workload. Consider increasing allocation or merging "
                    f"with another pool."
                ),
            ))
    return findings


def _check_single_pool_dominance(
    pools: List[Dict[str, Any]],
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Check 10: Flag if one pool holds ≥ threshold% of resources."""
    findings: List[Finding] = []
    if len(pools) < 2:
        return findings

    threshold = config.pool_dominance_threshold_pct
    for pool in pools:
        pct = pool.get("maxResourcePercentage", 0)
        name = pool.get("name", "(unnamed)")
        if pct >= threshold:
            findings.append(Finding(
                level=LEVEL_LOW,
                category=CATEGORY_CUSTOM_SQL_POOLS,
                check_name="single_pool_dominance",
                object_name=f"Pool: {name}",
                message=(
                    f"Pool \"{name}\" holds {pct}% of resources while "
                    f"other pools exist."
                ),
                detail=(
                    f"Other pools may experience resource starvation under "
                    f"concurrent workloads."
                ),
                recommendation=(
                    "Review if the remaining pools have sufficient capacity "
                    "for their workloads."
                ),
            ))
    return findings


def _check_empty_classifiers(
    pools: List[Dict[str, Any]],
    warehouse: str,
) -> List[Finding]:
    """Check 5: Flag pools with empty classifier values."""
    findings: List[Finding] = []

    for pool in pools:
        name = pool.get("name", "(unnamed)")
        classifier = pool.get("classifier", {})
        values = classifier.get("value", [])
        non_empty = [v for v in values if v and v.strip()]

        if not non_empty:
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_CUSTOM_SQL_POOLS,
                check_name="empty_classifier",
                object_name=f"Pool: {name}",
                message=(
                    f"Pool \"{name}\" has no classifier values configured."
                ),
                detail=(
                    "Queries cannot be routed to this pool unless it is "
                    "the default pool."
                ),
                recommendation=(
                    "Add one or more application name values or regex "
                    "patterns as classifiers for this pool."
                ),
            ))
    return findings


def _check_read_optimization(
    pools: List[Dict[str, Any]],
    warehouse: str,
) -> List[Finding]:
    """Check 6: Heuristic — flag reporting pools not optimized for reads."""
    findings: List[Finding] = []

    for pool in pools:
        name = pool.get("name", "(unnamed)")
        optimize_for_reads = pool.get("optimizeForReads", False)

        if optimize_for_reads:
            continue

        # Heuristic 1: pool name suggests read-heavy workload
        name_match = bool(_READ_POOL_NAME_HINTS.search(name))

        # Heuristic 2: classifier values suggest read-heavy traffic
        classifier = pool.get("classifier", {})
        values = classifier.get("value", [])
        classifier_match = any(
            _READ_CLASSIFIER_HINTS.search(v) for v in values if v
        )

        if name_match or classifier_match:
            hint = (
                f"pool name \"{name}\""
                if name_match
                else f"classifier values {values}"
            )
            findings.append(Finding(
                level=LEVEL_LOW,
                category=CATEGORY_CUSTOM_SQL_POOLS,
                check_name="read_optimization_missing",
                object_name=f"Pool: {name}",
                message=(
                    f"Pool \"{name}\" appears to serve read-heavy traffic "
                    f"(based on {hint}) but is not optimized for reads."
                ),
                recommendation=(
                    "Enable 'Optimize for Reads' on pools that serve "
                    "read-heavy workloads such as Power BI, reporting, "
                    "or analytics queries."
                ),
            ))
    return findings


def _check_pool_count(
    pools: List[Dict[str, Any]],
    warehouse: str,
) -> List[Finding]:
    """Check 8: Flag if pool count is near the 8-pool limit."""
    findings: List[Finding] = []
    count = len(pools)

    if count >= _MAX_POOL_COUNT:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="pool_count_at_limit",
            object_name=warehouse,
            message=(
                f"{count}/{_MAX_POOL_COUNT} custom SQL pools are configured. "
                f"You have reached the maximum."
            ),
            recommendation=(
                "Plan consolidation if you need additional pools in the future."
            ),
        ))
    elif count == _MAX_POOL_COUNT - 1:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="pool_count_near_limit",
            object_name=warehouse,
            message=(
                f"{count}/{_MAX_POOL_COUNT} custom SQL pools are configured. "
                f"Only one slot remains."
            ),
            recommendation=(
                "Plan consolidation if you need additional pools in the future."
            ),
        ))
    return findings


# -- Runtime checks (T-SQL) ----------------------------------------------

def _check_pool_pressure(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Check 6 (runtime): Query sql_pool_insights for pressure events."""
    findings: List[Finding] = []
    lookback = config.pool_pressure_lookback_hours

    try:
        query = _POOL_PRESSURE_QUERY.format(lookback_hours=lookback)
        df = read_warehouse_query(
            spark, warehouse, query,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="pool_pressure_check_failed",
            object_name=warehouse,
            message=f"Could not query pool pressure data: {exc}",
        ))
        return findings

    crit_threshold = config.pool_pressure_critical_threshold
    high_threshold = config.pool_pressure_high_threshold

    for row in rows:
        pool_name = row["sql_pool_name"] or "(unnamed)"
        events = row["pressure_events"] or 0
        first_ts = row["first_pressure"]
        last_ts = row["last_pressure"]
        alloc_pct = row["allocated_pct"]

        if events >= crit_threshold:
            level = LEVEL_CRITICAL
        elif events >= high_threshold:
            level = LEVEL_HIGH
        else:
            level = LEVEL_MEDIUM

        lookback_label = _format_lookback(lookback)

        findings.append(Finding(
            level=level,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="pool_under_pressure",
            object_name=f"Pool: {pool_name}",
            message=(
                f"Pool \"{pool_name}\" experienced resource pressure "
                f"{events} time(s) in the last {lookback_label}."
            ),
            detail=(
                f"First: {first_ts}, Last: {last_ts}. "
                f"Allocated: {alloc_pct}%."
            ),
            recommendation=(
                "Consider increasing the resource percentage for this pool "
                "or redistributing workloads across pools."
            ),
            sql_fix=(
                f"-- Monitor pressure for this pool:\n"
                f"SELECT [timestamp], sql_pool_name, max_resource_percentage,\n"
                f"       is_pool_under_pressure\n"
                f"FROM queryinsights.sql_pool_insights\n"
                f"WHERE sql_pool_name = '{pool_name}'\n"
                f"  AND is_pool_under_pressure = 1\n"
                f"  AND [timestamp] > DATEADD(HOUR, -{lookback}, GETUTCDATE())\n"
                f"ORDER BY [timestamp] DESC;"
            ),
        ))
    return findings


def _check_unclassified_traffic(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    pools: List[Dict[str, Any]],
) -> List[Finding]:
    """Check 7: Detect application names not matching any classifier."""
    findings: List[Finding] = []

    # Check if there is a default pool — if yes, unclassified traffic
    # still has a routing path, so we lower the severity.
    has_default = any(p.get("isDefault", False) for p in pools)

    # Gather all configured classifier values and their type
    classifier_type = ""
    all_classifier_values: List[str] = []
    for pool in pools:
        classifier = pool.get("classifier", {})
        ctype = classifier.get("type", "")
        if ctype:
            classifier_type = ctype
        all_classifier_values.extend(
            v for v in classifier.get("value", []) if v and v.strip()
        )

    if not all_classifier_values:
        return findings

    lookback = config.pool_pressure_lookback_hours

    try:
        query = _DISTINCT_PROGRAM_NAMES_QUERY.format(lookback_hours=lookback)
        df = read_warehouse_query(
            spark, warehouse, query,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
    except Exception:
        return findings

    observed_names = [row["program_name"] for row in rows if row["program_name"]]
    unmatched = _find_unmatched_names(
        observed_names, all_classifier_values, classifier_type,
    )

    if unmatched:
        top = unmatched[:10]
        remaining = len(unmatched) - len(top)
        names_display = ", ".join(f'"{n}"' for n in top)
        if remaining > 0:
            names_display += f" ... and {remaining} more"

        level = LEVEL_LOW if has_default else LEVEL_MEDIUM
        findings.append(Finding(
            level=level,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="unclassified_traffic",
            object_name=warehouse,
            message=(
                f"{len(unmatched)} application name(s) in recent query "
                f"history do not match any pool classifier."
            ),
            detail=f"Unmatched: {names_display}.",
            recommendation=(
                "Add classifiers for these application names or ensure "
                "a default pool is configured to catch unmatched traffic."
                if not has_default
                else "These queries route to the default pool. Consider "
                "creating dedicated pools if they need isolated resources."
            ),
            sql_fix=(
                "-- Find distinct application names and their pools:\n"
                "SELECT DISTINCT program_name, sql_pool_name\n"
                "FROM queryinsights.exec_requests_history\n"
                "ORDER BY program_name;"
            ),
        ))
    return findings


def _check_known_apps_unclassified(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    pools: List[Dict[str, Any]],
) -> List[Finding]:
    """Check 9: Known Fabric app patterns not classified."""
    findings: List[Finding] = []

    all_classifier_values: List[str] = []
    classifier_type = ""
    for pool in pools:
        classifier = pool.get("classifier", {})
        ctype = classifier.get("type", "")
        if ctype:
            classifier_type = ctype
        all_classifier_values.extend(
            v for v in classifier.get("value", []) if v and v.strip()
        )

    lookback = config.pool_pressure_lookback_hours

    try:
        query = _DISTINCT_PROGRAM_NAMES_QUERY.format(lookback_hours=lookback)
        df = read_warehouse_query(
            spark, warehouse, query,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
    except Exception:
        return findings

    observed_names = [row["program_name"] for row in rows if row["program_name"]]

    for app_label, known_regex in _KNOWN_PATTERNS.items():
        # Check if any observed name matches this known pattern
        traffic_present = any(
            re.search(known_regex, name) for name in observed_names
        )
        if not traffic_present:
            continue

        # Check if any classifier already covers this pattern
        already_classified = _is_pattern_covered(
            known_regex, all_classifier_values, classifier_type,
        )
        if already_classified:
            continue

        findings.append(Finding(
            level=LEVEL_MEDIUM,
            category=CATEGORY_CUSTOM_SQL_POOLS,
            check_name="known_apps_unclassified",
            object_name=warehouse,
            message=(
                f"Traffic from {app_label} is detected but not routed "
                f"to a dedicated pool."
            ),
            detail=(
                f"Recommended classifier regex: {known_regex}"
            ),
            recommendation=(
                f"Consider creating a dedicated pool for {app_label} "
                f"traffic using the regex pattern above."
            ),
        ))
    return findings


# -- Helpers --------------------------------------------------------------

def _find_unmatched_names(
    observed: List[str],
    classifier_values: List[str],
    classifier_type: str,
) -> List[str]:
    """Return observed names that don't match any classifier value."""
    unmatched: List[str] = []
    is_regex = "regex" in classifier_type.lower()

    for name in observed:
        matched = False
        for cv in classifier_values:
            if is_regex:
                try:
                    if re.search(cv, name):
                        matched = True
                        break
                except re.error:
                    if name == cv:
                        matched = True
                        break
            else:
                if name.lower() == cv.lower():
                    matched = True
                    break
        if not matched:
            unmatched.append(name)
    return unmatched


def _is_pattern_covered(
    known_regex: str,
    classifier_values: List[str],
    classifier_type: str,
) -> bool:
    """Check if a known regex pattern is already covered by classifiers."""
    is_regex = "regex" in classifier_type.lower()

    for cv in classifier_values:
        if is_regex:
            # If the classifier is regex-based, check for substring overlap
            # (exact match or the classifier regex encompasses the known one)
            if cv == known_regex:
                return True
            # Simple heuristic: check if core distinguishing tokens overlap
            try:
                # If the classifier regex matches the same kind of strings,
                # consider it covered.  We check a few representative values.
                known_compiled = re.compile(known_regex)
                cv_compiled = re.compile(cv)
                # Check if the classifiers share significant pattern overlap
                if known_regex in cv or cv in known_regex:
                    return True
            except re.error:
                pass
        else:
            # For exact application names, check if any key tokens match
            cv_lower = cv.lower()
            if "powerbi" in known_regex.lower() and "powerbi" in cv_lower:
                return True
            if "data integration" in known_regex.lower() and "data integration" in cv_lower:
                return True
            if "dms_user" in known_regex.lower() and "dms_user" in cv_lower:
                return True
    return False


def _format_lookback(hours: int) -> str:
    """Format a lookback period in hours to a human-readable string."""
    if hours < 24:
        return f"{hours} hour(s)"
    days = hours // 24
    remaining_hours = hours % 24
    if remaining_hours == 0:
        return f"{days} day(s)"
    return f"{days} day(s) and {remaining_hours} hour(s)"

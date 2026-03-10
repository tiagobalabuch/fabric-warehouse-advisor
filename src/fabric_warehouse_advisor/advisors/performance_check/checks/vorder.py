"""
Performance Check — V-Order Optimization
==========================================
Checks the V-Order write-time optimization state on the warehouse.

V-Order applies special sorting, row group distribution, dictionary
encoding, and compression to Parquet files, enabling lightning-fast
reads under Microsoft Fabric compute engines.

**Applies to DataWarehouse only.**
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_MEDIUM,
    LEVEL_CRITICAL,
    CATEGORY_VORDER,
)


# -- SQL ----------------------------------------------------------------

_VORDER_STATUS_QUERY = """
    SELECT [name], [is_vorder_enabled]
    FROM sys.databases
    WHERE database_id = DB_ID()
"""


# -- Public API ---------------------------------------------------------

def check_vorder(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    edition: str = "DataWarehouse",
) -> List[Finding]:
    """Check the V-Order optimization state.

    Parameters
    ----------
    edition : str
        The detected warehouse edition. V-Order checks are only
        meaningful for ``"DataWarehouse"``.

    Returns
    -------
    list[Finding]
        Findings related to V-Order configuration.
    """
    findings: List[Finding] = []

    if edition != "DataWarehouse":
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_VORDER,
            check_name="vorder_not_applicable",
            object_name=warehouse,
            message="V-Order check skipped — not applicable to SQL Analytics Endpoints.",
            detail=f"Detected edition: {edition}.",
        ))
        return findings

    try:
        df = read_warehouse_query(
            spark, warehouse, _VORDER_STATUS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()

        if not rows:
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_VORDER,
                check_name="vorder_status_unknown",
                object_name=warehouse,
                message="Could not determine V-Order status.",
                detail="The sys.databases query returned no rows.",
            ))
            return findings

        for row in rows:
            db_name = row["name"]
            is_enabled = row["is_vorder_enabled"]

            if is_enabled:
                findings.append(Finding(
                    level=LEVEL_INFO,
                    category=CATEGORY_VORDER,
                    check_name="vorder_enabled",
                    object_name=db_name,
                    message=f"V-Order is ENABLED on '{db_name}'.",
                    detail=(
                        "V-Order applies special sorting, dictionary encoding, "
                        "and compression to Parquet files. This is the recommended "
                        "configuration for read-heavy analytical workloads."
                    ),
                ))
            elif config.vorder_warn_if_disabled:
                findings.append(Finding(
                    level=LEVEL_CRITICAL,
                    category=CATEGORY_VORDER,
                    check_name="vorder_disabled",
                    object_name=db_name,
                    message=f"V-Order is DISABLED on '{db_name}'.",
                    detail=(
                        "V-Order is disabled. New Parquet files will not benefit "
                        "from V-Order optimizations. This is an irreversible "
                        "setting — once disabled, it cannot be re-enabled. "
                        "Power BI Direct Lake mode depends on V-Order. "
                        "Read performance may be degraded by 10-50%."
                    ),
                    recommendation=(
                        "CAUTION: Disabling V-Order is irreversible. If V-Order "
                        "is already disabled, consider whether this warehouse is "
                        "used for staging (acceptable) or analytics (problematic). "
                        "A common pattern: staging warehouse with V-Order OFF, "
                        "then move processed data into a V-Order-enabled warehouse "
                        "for reporting."
                    ),
                ))

    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_VORDER,
            check_name="vorder_check_failed",
            object_name=warehouse,
            message=f"Could not check V-Order status: {exc}",
        ))

    return findings

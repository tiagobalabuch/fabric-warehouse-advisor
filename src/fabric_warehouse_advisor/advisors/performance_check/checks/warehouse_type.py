"""
Performance Check — Warehouse Type Detection
==============================================
Detects whether the connected Fabric item is a **DataWarehouse**
or a **LakeWarehouse** (SQL Analytics Endpoint of a Lakehouse).

This is the gating check: some subsequent checks only apply to
DataWarehouse (e.g. V-Order, Data Clustering references).
"""

from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..findings import (
    Finding,
    LEVEL_INFO,
    CATEGORY_WAREHOUSE_TYPE,
)


# -- SQL -----------------------------------------------------------------

_EDITION_QUERY = (
    "SELECT CONVERT(varchar(100), "
    "DATABASEPROPERTYEX(DB_NAME(), 'Edition')) AS edition"
)


# -- Public API -----------------------------------------------------------

def detect_warehouse_edition(
    spark: SparkSession,
    warehouse: str,
    workspace_id: str = "",
    warehouse_id: str = "",
    sql_endpoint_id: str = "",
) -> Tuple[str, List[Finding]]:
    """Detect the Fabric item edition.

    Returns
    -------
    tuple[str, list[Finding]]
        The edition string (``"DataWarehouse"`` or ``"LakeWarehouse"``)
        and a list containing a single informational finding.
    """
    findings: List[Finding] = []

    try:
        row = read_warehouse_query(
            spark, warehouse, _EDITION_QUERY, workspace_id, warehouse_id,
        ).collect()[0]
        edition = row["edition"]
    except Exception as exc:
        edition = "Unknown"
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WAREHOUSE_TYPE,
            check_name="edition_detection_failed",
            object_name=warehouse,
            message=f"Could not detect warehouse edition: {exc}",
            detail="Defaulting to Unknown. Some checks may be skipped.",
            recommendation="Ensure the connected Spark session has access to the warehouse.",
        ))
        return edition, findings

    if edition == "DataWarehouse":
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WAREHOUSE_TYPE,
            check_name="edition_detected",
            object_name=warehouse,
            message="Fabric Data Warehouse detected.",
            detail="Edition: DataWarehouse. All performance checks are applicable.",
        ))
    elif edition == "LakeWarehouse":
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WAREHOUSE_TYPE,
            check_name="edition_detected",
            object_name=warehouse,
            message="Lakehouse SQL Analytics Endpoint detected.",
            detail=(
                "Edition: LakeWarehouse. Some checks (V-Order, Data Clustering) "
                "are not applicable to SQL Analytics Endpoints."
            ),
        ))
    else:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WAREHOUSE_TYPE,
            check_name="edition_detected",
            object_name=warehouse,
            message=f"Unknown edition detected: {edition}",
            detail="Some checks may not be applicable.",
        ))

    return edition, findings

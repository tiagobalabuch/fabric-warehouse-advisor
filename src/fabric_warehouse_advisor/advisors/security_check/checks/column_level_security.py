"""
Security Check — Column-Level Security  (SEC-004)
====================================================
Analyses ``sys.database_permissions`` filtered to column-scoped grants
and checks for sensitive columns that lack DENY SELECT.

Checks
------
* Sensitive columns (by name pattern) with no DENY SELECT protection
* Columns with DENY SELECT already in place (informational)
"""

from __future__ import annotations

from typing import List, Set, Tuple

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_CLS,
)


# -- SQL ----------------------------------------------------------------

_COLUMN_PERMISSIONS_QUERY = """
    SELECT
        dp.class_desc,
        dp.permission_name,
        dp.state_desc,
        SCHEMA_NAME(o.schema_id)   AS schema_name,
        o.name                     AS table_name,
        c.name                     AS column_name,
        pr.name                    AS grantee_name
    FROM sys.database_permissions AS dp
    INNER JOIN sys.objects AS o
        ON dp.major_id = o.object_id
    INNER JOIN sys.columns AS c
        ON dp.major_id = c.object_id
        AND dp.minor_id = c.column_id
    INNER JOIN sys.database_principals AS pr
        ON dp.grantee_principal_id = pr.principal_id
    WHERE dp.minor_id > 0
"""

_ALL_COLUMNS_QUERY = """
    SELECT
        SCHEMA_NAME(o.schema_id)  AS schema_name,
        o.name                    AS table_name,
        c.name                    AS column_name
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o
        ON c.object_id = o.object_id
    WHERE o.type = 'U'
"""


# -- Public API ---------------------------------------------------------

def check_column_level_security(
    spark: SparkSession,
    warehouse: str,
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Analyse Column-Level Security coverage.

    Returns
    -------
    list[Finding]
        Findings related to CLS (DENY SELECT on columns).
    """
    findings: List[Finding] = []

    try:
        perms_df = read_warehouse_query(
            spark, warehouse, _COLUMN_PERMISSIONS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        perm_rows = perms_df.collect()

        cols_df = read_warehouse_query(
            spark, warehouse, _ALL_COLUMNS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        col_rows = cols_df.collect()
    except Exception:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_CLS,
            check_name="cls_query_failed",
            object_name=warehouse,
            message="Unable to query column permission metadata.",
            detail="The current user may lack VIEW DEFINITION permission.",
            recommendation="Ensure the executing identity has VIEW DEFINITION on the warehouse.",
        ))
        return findings

    # Build set of columns protected by DENY SELECT
    protected_columns: Set[Tuple[str, str, str]] = set()
    for r in perm_rows:
        if r["state_desc"] == "DENY" and r["permission_name"] == "SELECT":
            protected_columns.add((
                r["schema_name"], r["table_name"], r["column_name"],
            ))

    # Build sensitive column list from patterns
    sensitive_patterns = config.sensitive_column_patterns
    if not sensitive_patterns:
        # No patterns configured — nothing to check
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CLS,
            check_name="cls_no_patterns",
            object_name=warehouse,
            message="No sensitive column patterns configured — CLS check skipped.",
            detail="Set sensitive_column_patterns in SecurityCheckConfig to enable this check.",
        ))
        return findings

    # Find columns matching sensitive patterns
    sensitive_unprotected: list[Tuple[str, str, str]] = []
    sensitive_protected: list[Tuple[str, str, str]] = []
    total_sensitive = 0

    for r in col_rows:
        schema = r["schema_name"]
        table = r["table_name"]
        col = r["column_name"]

        # Apply table_names filter
        if config.table_names:
            qualified = f"{schema}.{table}"
            if not any(
                t == table or t == qualified
                for t in config.table_names
            ):
                continue

        col_lower = col.lower()
        is_sensitive = any(
            _like_match(col_lower, pattern.lower())
            for pattern in sensitive_patterns
        )
        if not is_sensitive:
            continue

        total_sensitive += 1
        col_key = (schema, table, col)
        if col_key in protected_columns:
            sensitive_protected.append(col_key)
        else:
            sensitive_unprotected.append(col_key)

    # Report unprotected sensitive columns
    for schema, table, col in sensitive_unprotected:
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_CLS,
            check_name="sensitive_column_unprotected",
            object_name=f"{warehouse}.[{schema}].[{table}].[{col}]",
            message=f"Sensitive column [{col}] has no DENY SELECT protection.",
            detail=(
                f"Column [{schema}].[{table}].[{col}] matches a sensitive name "
                f"pattern but has no column-level DENY SELECT grant."
            ),
            recommendation=(
                f"Add a DENY SELECT on the column for roles that should not see it."
            ),
            sql_fix=(
                f"DENY SELECT ON [{schema}].[{table}] ([{col}]) "
                f"TO [<restricted_role>];"
            ),
        ))

    # Summary
    if sensitive_protected:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CLS,
            check_name="sensitive_columns_protected",
            object_name=warehouse,
            message=f"{len(sensitive_protected)} sensitive column(s) protected by DENY SELECT.",
            detail=(
                f"Out of {total_sensitive} column(s) matching sensitive patterns, "
                f"{len(sensitive_protected)} already have column-level restrictions."
            ),
        ))

    if not sensitive_unprotected and not sensitive_protected:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_CLS,
            check_name="no_sensitive_columns_found",
            object_name=warehouse,
            message="No columns matching sensitive name patterns were found.",
            detail=(
                f"Checked {len(col_rows)} column(s) against "
                f"{len(sensitive_patterns)} pattern(s)."
            ),
        ))

    return findings


# -- Helpers -------------------------------------------------------------

def _like_match(value: str, pattern: str) -> bool:
    """Simple SQL LIKE-style match with ``%`` and ``_`` wildcards."""
    import re
    regex = "^"
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "%":
            regex += ".*"
        elif ch == "_":
            regex += "."
        else:
            regex += re.escape(ch)
        i += 1
    regex += "$"
    return bool(re.match(regex, value))

"""
Security Check — Row-Level Security  (SEC-003)
=================================================
Analyses ``sys.security_policies`` and ``sys.security_predicates``
to assess RLS coverage.

Checks
------
* Tables with no RLS policy
* RLS policies that are disabled (``is_enabled = 0``)
* BLOCK predicates (not supported in Fabric — only FILTER)
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
    CATEGORY_RLS,
)


# -- SQL ----------------------------------------------------------------

_SECURITY_POLICIES_QUERY = """
    SELECT
        sp.name                                AS policy_name,
        sp.is_enabled,
        SCHEMA_NAME(sp.schema_id)              AS policy_schema,
        pred.predicate_type_desc,
        SCHEMA_NAME(t.schema_id)               AS table_schema,
        t.name                                 AS table_name,
        pred.predicate_definition
    FROM sys.security_policies AS sp
    INNER JOIN sys.security_predicates AS pred
        ON sp.object_id = pred.object_id
    INNER JOIN sys.objects AS t
        ON pred.target_object_id = t.object_id
"""

_ALL_USER_TABLES_QUERY = """
    SELECT
        SCHEMA_NAME(schema_id) AS schema_name,
        name                   AS table_name
    FROM sys.objects
    WHERE type = 'U'
"""


# -- Public API ---------------------------------------------------------

def check_row_level_security(
    spark: SparkSession,
    warehouse: str,
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Analyse Row-Level Security coverage.

    Returns
    -------
    list[Finding]
        Findings related to RLS policies.
    """
    findings: List[Finding] = []

    try:
        policies_df = read_warehouse_query(
            spark, warehouse, _SECURITY_POLICIES_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        policy_rows = policies_df.collect()

        tables_df = read_warehouse_query(
            spark, warehouse, _ALL_USER_TABLES_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        table_rows = tables_df.collect()
    except Exception:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_RLS,
            check_name="rls_query_failed",
            object_name=warehouse,
            message="Unable to query RLS metadata.",
            detail="The current user may lack VIEW DEFINITION permission.",
            recommendation=f"Ensure the executing identity has VIEW DEFINITION on the {config.item_label}.",
        ))
        return findings

    # Build set of all user tables
    all_tables: Set[Tuple[str, str]] = set()
    for r in table_rows:
        tbl = (r["schema_name"], r["table_name"])
        # Apply table_names filter if configured
        if config.table_names:
            qualified = f"{tbl[0]}.{tbl[1]}"
            if not any(
                t == tbl[1] or t == qualified
                for t in config.table_names
            ):
                continue
        all_tables.add(tbl)

    # Tables covered by at least one RLS FILTER predicate
    covered_tables: Set[Tuple[str, str]] = set()

    for r in policy_rows:
        schema = r["table_schema"]
        table = r["table_name"]
        policy_name = r["policy_name"]
        is_enabled = r["is_enabled"]
        pred_type = r["predicate_type_desc"]

        tbl_key = (schema, table)
        if config.table_names and tbl_key not in all_tables:
            continue

        # Check disabled policies
        if not is_enabled:
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_RLS,
                check_name="rls_policy_disabled",
                object_name=f"{warehouse}.[{schema}].[{table}]",
                message=f"RLS policy [{policy_name}] exists but is DISABLED.",
                detail=(
                    "A disabled policy provides no protection — all rows "
                    "are visible to all users."
                ),
                recommendation=f"Enable the policy: ALTER SECURITY POLICY [{policy_name}] WITH (STATE = ON);",
                sql_fix=f"ALTER SECURITY POLICY [{policy_name}] WITH (STATE = ON);",
            ))
        else:
            if pred_type == "FILTER":
                covered_tables.add(tbl_key)

        # Check BLOCK predicates (not supported in Fabric)
        if pred_type == "BLOCK":
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_RLS,
                check_name="rls_block_predicate",
                object_name=f"{warehouse}.[{schema}].[{table}]",
                message=f"BLOCK predicate found on policy [{policy_name}].",
                detail=(
                    "Microsoft Fabric Warehouse supports only FILTER predicates. "
                    "BLOCK predicates may be silently ignored."
                ),
                recommendation=(
                    "Convert the BLOCK predicate to application-level validation "
                    "or remove it to avoid confusion."
                ),
            ))

    # Tables without any RLS coverage
    uncovered = all_tables - covered_tables
    if uncovered:
        for schema, table in sorted(uncovered):
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_RLS,
                check_name="no_rls_policy",
                object_name=f"{warehouse}.[{schema}].[{table}]",
                message=f"Table [{schema}].[{table}] has no RLS policy.",
                detail="All rows are visible to all users with SELECT access.",
                recommendation=(
                    "If this table contains tenant-specific or user-specific data, "
                    "consider adding a FILTER predicate via a security policy."
                ),
            ))

    # INFO summary if everything is covered
    if covered_tables and not [f for f in findings if f.is_actionable]:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_RLS,
            check_name="rls_healthy",
            object_name=warehouse,
            message="Row-Level Security is properly configured.",
            detail=f"{len(covered_tables)} table(s) protected by active FILTER predicates.",
        ))

    return findings

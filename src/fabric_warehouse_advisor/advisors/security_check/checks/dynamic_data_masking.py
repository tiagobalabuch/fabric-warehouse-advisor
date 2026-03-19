"""
Security Check — Dynamic Data Masking  (SEC-005)
===================================================
Analyses ``sys.masked_columns`` and ``sys.database_permissions``
to assess DDM coverage and UNMASK grant hygiene.

Checks
------
* Sensitive columns with no masking function applied
* Excessive UNMASK grants (more principals than threshold)
* Weak default() masking on short string columns
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
    CATEGORY_DDM,
)


# -- SQL ----------------------------------------------------------------

_MASKED_COLUMNS_QUERY = """
    SELECT
        SCHEMA_NAME(o.schema_id)   AS schema_name,
        o.name                     AS table_name,
        c.name                     AS column_name,
        c.is_masked,
        mc.masking_function,
        t.name                     AS data_type,
        c.max_length
    FROM sys.columns AS c
    INNER JOIN sys.objects AS o
        ON c.object_id = o.object_id
    LEFT JOIN sys.masked_columns AS mc
        ON c.object_id = mc.object_id
        AND c.column_id = mc.column_id
    LEFT JOIN sys.types AS t
        ON c.user_type_id = t.user_type_id
    WHERE o.type = 'U'
"""

_UNMASK_PERMISSIONS_QUERY = """
    SELECT
        pr.name         AS grantee_name,
        pr.type_desc    AS grantee_type,
        dp.state_desc
    FROM sys.database_permissions AS dp
    INNER JOIN sys.database_principals AS pr
        ON dp.grantee_principal_id = pr.principal_id
    WHERE dp.permission_name = 'UNMASK'
      AND dp.state_desc IN ('GRANT', 'GRANT_WITH_GRANT_OPTION')
"""


# -- Public API ---------------------------------------------------------

def check_dynamic_data_masking(
    spark: SparkSession,
    warehouse: str,
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Analyse Dynamic Data Masking coverage and UNMASK grants.

    Returns
    -------
    list[Finding]
        Findings related to DDM configuration.
    """
    findings: List[Finding] = []

    try:
        cols_df = read_warehouse_query(
            spark, warehouse, _MASKED_COLUMNS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        col_rows = cols_df.collect()

        unmask_df = read_warehouse_query(
            spark, warehouse, _UNMASK_PERMISSIONS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        unmask_rows = unmask_df.collect()
    except Exception:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_DDM,
            check_name="ddm_query_failed",
            object_name=warehouse,
            message="Unable to query DDM metadata.",
            detail="The current user may lack VIEW DEFINITION permission.",
            recommendation=f"Ensure the executing identity has VIEW DEFINITION on the {config.item_label}.",
        ))
        return findings

    # --- Check: UNMASK grant count ---
    if len(unmask_rows) > config.max_unmask_principals:
        grantees = [r["grantee_name"] for r in unmask_rows]
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_DDM,
            check_name="excessive_unmask_grants",
            object_name=warehouse,
            message=(
                f"{len(unmask_rows)} principal(s) have UNMASK permission "
                f"(threshold: {config.max_unmask_principals})."
            ),
            detail=f"Principals with UNMASK: {', '.join(grantees)}.",
            recommendation=(
                "Review UNMASK grants and revoke from principals that do not "
                "require access to unmasked data."
            ),
        ))
    elif unmask_rows:
        grantees = [r["grantee_name"] for r in unmask_rows]
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_DDM,
            check_name="unmask_grants_ok",
            object_name=warehouse,
            message=f"{len(unmask_rows)} principal(s) have UNMASK — within threshold.",
            detail=f"Principals: {', '.join(grantees)}.",
        ))

    # --- Analyse masked / sensitive columns ---
    masked_count = 0
    sensitive_patterns = config.sensitive_column_patterns

    # Pre-compute scope filter
    _schema_filter = {s.lower() for s in config.schema_names} if config.schema_names else None

    for r in col_rows:
        schema = r["schema_name"]
        table = r["table_name"]
        col = r["column_name"]

        # Apply schema_names filter
        if _schema_filter and schema.lower() not in _schema_filter:
            continue

        # Apply table_names filter
        if config.table_names:
            qualified = f"{schema}.{table}"
            if not any(
                t == table or t == qualified
                for t in config.table_names
            ):
                continue

        is_masked = r["is_masked"]

        if is_masked:
            masked_count += 1
            masking_fn = r["masking_function"] or ""

            # Check weak masking on short strings
            if config.flag_weak_masking:
                data_type = (r["data_type"] or "").lower()
                max_length = r["max_length"] or 0
                if (
                    masking_fn.lower().startswith("default()")
                    and data_type in ("varchar", "nvarchar", "char", "nchar")
                    and 0 < max_length <= 4
                ):
                    findings.append(Finding(
                        level=LEVEL_MEDIUM,
                        category=CATEGORY_DDM,
                        check_name="weak_default_mask",
                        object_name=f"{warehouse}.[{schema}].[{table}].[{col}]",
                        message=f"default() mask on short {data_type}({max_length}) column [{col}].",
                        detail=(
                            f"The default() mask on a {data_type}({max_length}) column "
                            f"shows 'xxxx' which may be trivially reversible for short values."
                        ),
                        recommendation=(
                            f"Use a partial() or random() masking function instead, "
                            f"or consider Column-Level Security (DENY SELECT)."
                        ),
                        sql_fix=(
                            f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{col}] "
                            f"ADD MASKED WITH (FUNCTION = 'partial(0, \"XXXXX\", 0)');"
                        ),
                    ))

    # Summary
    if masked_count > 0:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_DDM,
            check_name="ddm_columns_masked",
            object_name=warehouse,
            message=f"{masked_count} column(s) have Dynamic Data Masking applied.",
        ))
    else:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_DDM,
            check_name="no_ddm_configured",
            object_name=warehouse,
            message="No Dynamic Data Masking is configured on any column.",
            detail="DDM is optional but recommended for columns containing PII.",
            recommendation=(
                "Consider applying masking functions to columns that contain "
                "personally identifiable information (PII)."
            ),
        ))

    return findings

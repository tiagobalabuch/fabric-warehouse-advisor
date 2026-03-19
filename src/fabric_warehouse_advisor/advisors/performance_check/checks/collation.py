"""
Performance Check — Collation Mismatch
========================================
Checks whether column-level collation deviates from the database
default collation.

Mismatched collation can cause implicit conversions in joins and
comparisons, preventing predicate push-down, degrading performance,
and leading to unexpected sort behaviour.
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_HIGH,
    CATEGORY_COLLATION,
)


# -- SQL ----------------------------------------------------------------

_DATABASE_COLLATION_QUERY = """
    SELECT collation_name
    FROM sys.databases
    WHERE database_id = DB_ID()
"""

_COLUMN_COLLATION_QUERY = """
    SELECT
        SCHEMA_NAME(t.schema_id) AS schema_name,
        t.name                   AS table_name,
        c.name                   AS column_name,
        c.collation_name         AS column_collation
    FROM sys.columns c
    INNER JOIN sys.tables t
        ON c.object_id = t.object_id
    WHERE t.type = 'U'
      AND c.collation_name IS NOT NULL
"""


# -- Public API ---------------------------------------------------------

def check_collation(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
) -> List[Finding]:
    """Compare column collation against the database default collation.

    Returns
    -------
    list[Finding]
        Findings for columns whose collation differs from the database.
    """
    findings: List[Finding] = []

    # --- Get database default collation ---
    try:
        db_df = read_warehouse_query(
            spark, warehouse, _DATABASE_COLLATION_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        db_rows = db_df.collect()
        if not db_rows:
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_COLLATION,
                check_name="collation_check_skipped",
                object_name=warehouse,
                message="Could not determine database collation.",
            ))
            return findings
        db_collation = db_rows[0]["collation_name"]
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_COLLATION,
            check_name="collation_check_failed",
            object_name=warehouse,
            message=f"Could not read database collation: {exc}",
        ))
        return findings

    # --- Get column collations ---
    try:
        col_df = read_warehouse_query(
            spark, warehouse, _COLUMN_COLLATION_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        col_rows = col_df.collect()
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_COLLATION,
            check_name="collation_check_failed",
            object_name=warehouse,
            message=f"Could not read column collations: {exc}",
        ))
        return findings

    mismatch_count = 0
    columns_in_scope = 0

    # Pre-compute scope filter
    _schema_filter = {s.lower() for s in config.schema_names} if config.schema_names else None

    for row in col_rows:
        schema = row["schema_name"]
        table = row["table_name"]
        column = row["column_name"]
        col_collation = row["column_collation"]

        # Apply scope filters
        if _schema_filter and schema.lower() not in _schema_filter:
            continue
        if config.table_names:
            if not _matches_table_filter(schema, table, config.table_names):
                continue

        columns_in_scope += 1

        if col_collation and col_collation != db_collation:
            mismatch_count += 1
            fqn = f"[{schema}].[{table}].[{column}]"
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_COLLATION,
                check_name="collation_mismatch",
                object_name=fqn,
                message=(
                    f"Column collation '{col_collation}' differs from "
                    f"database collation '{db_collation}'."
                ),
                detail=(
                    f"Database collation: {db_collation}. "
                    f"Column collation: {col_collation}. "
                    f"Mismatched collation forces implicit conversions in "
                    f"joins and WHERE clauses, which can prevent predicate "
                    f"push-down and degrade query performance."
                ),
                recommendation=(
                    f"Align the column collation with the database default "
                    f"unless a different collation is explicitly required."
                ),
                sql_fix=(
                    f"ALTER TABLE [{schema}].[{table}] "
                    f"ALTER COLUMN [{column}] "
                    f"-- <current_type> COLLATE {db_collation};"
                ),
            ))

    if columns_in_scope == 0 and (config.schema_names or config.table_names):
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_COLLATION,
            check_name="collation_check_skipped",
            object_name=warehouse,
            message="No tables match the configured scope filters — collation check skipped.",
        ))
    elif mismatch_count == 0:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_COLLATION,
            check_name="collation_consistent",
            object_name=warehouse,
            message=(
                f"All column collations match the database default "
                f"'{db_collation}'."
            ),
        ))

    return findings


# -- Helpers -------------------------------------------------------------

def _matches_table_filter(
    schema: str, table: str, table_names: list[str]
) -> bool:
    """Check if a table matches the user-specified filter list."""
    for entry in table_names:
        parts = entry.replace("[", "").replace("]", "").split(".")
        if len(parts) == 2:
            if (parts[0].strip().lower() == schema.lower()
                    and parts[1].strip().lower() == table.lower()):
                return True
        else:
            if parts[0].strip().lower() == table.lower():
                return True
    return False

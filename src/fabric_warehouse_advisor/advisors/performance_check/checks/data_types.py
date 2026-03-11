"""
Performance Check — Data Type Analysis
========================================
Scans column metadata for data type anti-patterns that hurt performance
in Fabric Warehouse's columnar Delta Parquet storage engine.

Checks
------
* ``VARCHAR(MAX)`` usage (memory spill risk)
* ``CHAR(n)`` where ``VARCHAR(n)`` would be more efficient
* Oversized ``VARCHAR(n)`` declarations (≥ threshold)
* ``DECIMAL`` / ``NUMERIC`` with excessive precision
* ``FLOAT`` / ``REAL`` on monetary-sounding columns
* ``BIGINT`` where ``INT`` / ``SMALLINT`` would suffice
* Date/time data stored as strings
* Nullable columns that could potentially be ``NOT NULL``
"""

from __future__ import annotations

import re
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import PerformanceCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_DATA_TYPES,
)


# -- SQL ----------------------------------------------------------------

_COLUMN_METADATA_QUERY = """
    SELECT
        c.TABLE_SCHEMA,
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.ORDINAL_POSITION,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION,
        c.NUMERIC_SCALE,
        c.IS_NULLABLE,
        c.COLUMN_DEFAULT
    FROM INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN INFORMATION_SCHEMA.TABLES t
        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
        AND c.TABLE_NAME = t.TABLE_NAME
    WHERE t.TABLE_TYPE = 'BASE TABLE'
        AND c.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
"""


# -- Heuristic name patterns -------------------------------------------

_DATE_NAME_PATTERN = re.compile(
    r"(^|_)(date|dt|time|timestamp|created|modified|updated|expired|start|end)"
    r"($|_|at$|on$)",
    re.IGNORECASE,
)

_MONEY_NAME_PATTERN = re.compile(
    r"(^|_)(amount|price|cost|total|balance|salary|wage|fee|revenue|"
    r"income|profit|loss|tax|discount|charge|payment|budget|rate|"
    r"margin|commission|gross|net)($|_)",
    re.IGNORECASE,
)

_SMALL_RANGE_NAME_PATTERN = re.compile(
    r"(^|_)(year|month|day|week|quarter|hour|minute|second|"
    r"day_of_week|day_of_year|fiscal_year|fiscal_month|"
    r"age|count|qty|quantity|num|number_of|flag|status|"
    r"level|tier|rank|priority|sequence|version)($|_)",
    re.IGNORECASE,
)


# -- Public API ---------------------------------------------------------

def check_data_types(
    spark: SparkSession,
    warehouse: str,
    config: PerformanceCheckConfig,
    row_counts: Dict[Tuple[str, str], int] | None = None,
) -> Tuple[List[Finding], int, int]:
    """Analyse column data types for performance anti-patterns.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    warehouse : str
        Warehouse or Lakehouse SQL Endpoint name.
    config : PerformanceCheckConfig
        Advisor configuration.
    row_counts : dict, optional
        Pre-computed ``(schema, table) -> row_count`` mapping.
        Used to enrich findings with table size context.

    Returns
    -------
    tuple[list[Finding], int, int]
        (findings, tables_analyzed, columns_analyzed)
    """
    findings: List[Finding] = []

    df = read_warehouse_query(
        spark, warehouse, _COLUMN_METADATA_QUERY,
        config.workspace_id, config.warehouse_id,
    )

    rows = df.collect()

    tables_seen: set = set()
    columns_count = 0

    for row in rows:
        schema = row["TABLE_SCHEMA"]
        table = row["TABLE_NAME"]
        column = row["COLUMN_NAME"]
        data_type = (row["DATA_TYPE"] or "").strip().lower()
        max_length = row["CHARACTER_MAXIMUM_LENGTH"]  # -1 for MAX
        precision = row["NUMERIC_PRECISION"]
        scale = row["NUMERIC_SCALE"]
        is_nullable = row["IS_NULLABLE"]  # "YES" or "NO"

        # Apply scope filters
        if config.schema_names:
            if schema.lower() not in [s.lower() for s in config.schema_names]:
                continue
        if config.table_names:
            if not _matches_table_filter(schema, table, config.table_names):
                continue

        tables_seen.add((schema, table))
        columns_count += 1

        fqn = f"[{schema}].[{table}].[{column}]"
        rc = row_counts.get((schema, table), 0) if row_counts else 0
        size_note = f" ({rc:,} rows)" if rc > 0 else ""

        # --- VARCHAR(MAX) ---
        if config.varchar_max_warning and data_type == "varchar" and max_length == -1:
            findings.append(Finding(
                level=LEVEL_CRITICAL,
                category=CATEGORY_DATA_TYPES,
                check_name="varchar_max_detected",
                object_name=fqn,
                message=f"{data_type.upper()}(MAX) column detected.",
                detail=(
                    f"Data type: {data_type.upper()}(MAX){size_note}. "
                    f"The engine allocates maximum potential memory during sort/hash "
                    f"operations, causing memory spills and slow queries. "
                    f"VARCHAR(MAX) also disqualifies queries from result set caching."
                ),
                recommendation=(
                    f"Determine the actual maximum length with: "
                    f"SELECT MAX(LEN([{column}])) FROM [{schema}].[{table}]. "
                    f"Then resize to an appropriate fixed length."
                ),
                sql_fix=(
                    f"-- After determining actual max length:\n"
                    f"-- ALTER TABLE [{schema}].[{table}] "
                    f"ALTER COLUMN [{column}] VARCHAR(<actual_max>) NOT NULL;"
                ),
            ))

        # --- Oversized VARCHAR ---
        elif (data_type == "varchar"
              and max_length is not None
              and max_length >= config.oversized_varchar_threshold
              and max_length != -1):
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_DATA_TYPES,
                check_name="oversized_varchar",
                object_name=fqn,
                message=f"{data_type.upper()}({max_length}) is excessively large.",
                detail=(
                    f"Data type: {data_type.upper()}({max_length}){size_note}. "
                    f"Statistics and query cost estimation are more accurate when "
                    f"the data type length is closer to the actual data size."
                ),
                recommendation=(
                    f"Check the actual maximum length with: "
                    f"SELECT MAX(LEN([{column}])) FROM [{schema}].[{table}]. "
                    f"Resize to an appropriate length."
                ),
                sql_fix=(
                    f"-- After determining actual max length:\n"
                    f"-- ALTER TABLE [{schema}].[{table}] "
                    f"ALTER COLUMN [{column}] {data_type.upper()}(<actual_max>);"
                ),
            ))

        # --- CHAR vs VARCHAR ---
        if config.char_to_varchar_warning and data_type == "char":
            findings.append(Finding(
                level=LEVEL_LOW,
                category=CATEGORY_DATA_TYPES,
                check_name="char_used_instead_of_varchar",
                object_name=fqn,
                message=f"CHAR({max_length}) could be VARCHAR({max_length}).",
                detail=(
                    f"Data type: CHAR({max_length}){size_note}. "
                    f"CHAR stores fixed-length padded strings, wasting space when "
                    f"actual values are shorter. VARCHAR stores only the actual "
                    f"length plus small overhead, reducing I/O."
                ),
                recommendation=(
                    f"Use VARCHAR({max_length}) unless fixed-length padding is "
                    f"explicitly required (e.g. ISO codes, fixed-format identifiers)."
                ),
                sql_fix=(
                    f"ALTER TABLE [{schema}].[{table}] "
                    f"ALTER COLUMN [{column}] VARCHAR({max_length});"
                ),
            ))

        # --- Decimal/Numeric over-precision ---
        if (data_type in ("decimal", "numeric")
            and precision is not None
            and precision > config.decimal_over_precision_threshold):
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_DATA_TYPES,
                check_name="decimal_over_precision",
                object_name=fqn,
                message=(
                    f"{data_type.upper()}({precision},{scale}) has excessive precision."
                ),
                detail=(
                    f"Data type: {data_type.upper()}({precision},{scale}){size_note}. "
                    f"Over-provisioned precision increases storage per row and "
                    f"can degrade performance. A DECIMAL(38,0) takes more bytes "
                    f"than DECIMAL(10,2)."
                ),
                recommendation=(
                    f"Review the actual data range and use the smallest precision "
                    f"and scale that accommodates your values."
                ),
            ))

        # --- FLOAT/REAL for money ---
        if (config.float_for_money_warning
            and data_type in ("float", "real")
            and _MONEY_NAME_PATTERN.search(column)):
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_DATA_TYPES,
                check_name="float_for_monetary_data",
                object_name=fqn,
                message=f"{data_type.upper()} used for monetary-sounding column.",
                detail=(
                    f"Data type: {data_type.upper()}{size_note}. "
                    f"Column name '{column}' suggests financial data. "
                    f"FLOAT/REAL are approximate types and can introduce "
                    f"rounding errors in financial calculations."
                ),
                recommendation=(
                    f"Use DECIMAL or NUMERIC with appropriate precision and scale "
                    f"for exact financial calculations (e.g. DECIMAL(19,4))."
                ),
            ))

        # --- BIGINT for small-range values ---
        if (config.bigint_where_int_suffices_warning
            and data_type == "bigint"
            and _SMALL_RANGE_NAME_PATTERN.search(column)):
            findings.append(Finding(
                level=LEVEL_LOW,
                category=CATEGORY_DATA_TYPES,
                check_name="bigint_for_small_range",
                object_name=fqn,
                message=f"BIGINT may be oversized for column '{column}'.",
                detail=(
                    f"Data type: BIGINT (8 bytes){size_note}. "
                    f"Column name suggests a small-range value. "
                    f"INT (4 bytes) supports up to ~2.1 billion; "
                    f"SMALLINT (2 bytes) supports up to 32,767."
                ),
                recommendation=(
                    "Use the smallest integer type that accommodates your "
                    "value range: SMALLINT, INT, or BIGINT."
                ),
            ))

        # --- Date stored as string ---
        if (config.datetime_as_string_warning
            and data_type in ("varchar", "char")
            and max_length != -1
            and _DATE_NAME_PATTERN.search(column)):
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_DATA_TYPES,
                check_name="datetime_stored_as_string",
                object_name=fqn,
                message=f"String column '{column}' may store date/time data.",
                detail=(
                    f"Data type: {data_type.upper()}({max_length}){size_note}. "
                    f"Column name suggests temporal data. Storing dates as strings "
                    f"prevents date arithmetic, makes comparisons slower, "
                    f"and increases storage."
                ),
                recommendation=(
                    f"Use DATE, TIME, or DATETIME2(n) for temporal values. "
                    f"These types are more storage-efficient and enable "
                    f"proper date operations and file-level predicate pushdown."
                ),
            ))

        # --- Nullable columns ---
        if (config.nullable_warning
            and is_nullable == "YES"
            and _looks_like_required_column(column)):
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_DATA_TYPES,
                check_name="nullable_column",
                object_name=fqn,
                message=f"Nullable column '{column}' may benefit from NOT NULL.",
                detail=(
                    f"IS_NULLABLE: YES{size_note}. "
                    f"Nullable columns add metadata overhead and can reduce "
                    f"the effectiveness of query optimizations and statistics."
                ),
                recommendation=(
                    "If your data model guarantees this column is never NULL, "
                    "define it as NOT NULL to improve statistics accuracy "
                    "and query optimization."
                ),
            ))

    return findings, len(tables_seen), columns_count


# -- Helpers -------------------------------------------------------------

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


def _looks_like_required_column(column_name: str) -> bool:
    """Heuristic: column names that strongly suggest NOT NULL semantics."""
    name = column_name.lower()
    patterns = [
        r"(^|_)id$",
        r"(^|_)key$",
        r"(^|_)code$",
        r"(^|_)name$",
        r"(^|_)type$",
        r"(^|_)status$",
        r"^pk_",
        r"^sk_",
        r"^fk_",
        r"_sk$",
        r"_pk$",
        r"_fk$",
    ]
    return any(re.search(p, name) for p in patterns)

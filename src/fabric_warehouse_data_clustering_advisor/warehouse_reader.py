"""
Fabric Warehouse Data Clustering Advisor - Warehouse Reader
===================================================
Thin wrapper around the Fabric Spark Data Warehouse connector that
reads system-catalog views and user tables into PySpark DataFrames.

System catalog views (``sys.*``, ``queryinsights.*``) are accessed via
T-SQL query passthrough::

    spark.read.option(Constants.DatabaseName, warehouse).synapsesql("<T-SQL>")

User tables are read with three-part names::

    spark.read.synapsesql("warehouse.schema.table")
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

# Fabric Spark runtime: importing this package registers the
# .synapsesql() extension method on DataFrameReader / DataFrameWriter.
try:
    from com.microsoft.spark.fabric.Constants import Constants as _FabricConstants
except ImportError:
    _FabricConstants = None  # running outside Fabric (e.g. local tests)


# ------------------------------------------------------------------
# Low-level readers
# ------------------------------------------------------------------

def read_warehouse_query(
    spark: SparkSession,
    warehouse: str,
    query: str,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """Execute a T-SQL query against the Fabric Warehouse via the Spark
    connector's query-passthrough mode.

    This is the correct way to read system catalog views (``sys.*``,
    ``INFORMATION_SCHEMA.*``, ``queryinsights.*``) because the
    three-part-name reader only supports user tables.
    """
    if _FabricConstants is None:
        raise RuntimeError(
            "com.microsoft.spark.fabric is not available. "
            "This function must run inside a Fabric Spark session."
        )
    reader = spark.read.option(_FabricConstants.DatabaseName, warehouse)
    if workspace_id:
        reader = reader.option(_FabricConstants.WorkspaceId, workspace_id)
    if warehouse_id:
        reader = reader.option(_FabricConstants.DatawarehouseId, warehouse_id)
    return reader.synapsesql(query)


def read_warehouse_table(
    spark: SparkSession,
    warehouse: str,
    schema: str,
    table_name: str,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """Read a *user* table from Fabric Warehouse via three-part naming."""
    three_part = f"{warehouse}.{schema}.{table_name}"
    reader = spark.read
    if workspace_id:
        reader = reader.option(_FabricConstants.WorkspaceId, workspace_id)
    if warehouse_id:
        reader = reader.option(_FabricConstants.DatawarehouseId, warehouse_id)
    return reader.synapsesql(three_part)


# ------------------------------------------------------------------
# Composed metadata queries  (T-SQL passthrough)
# ------------------------------------------------------------------

def get_full_column_metadata(
    spark: SparkSession,
    warehouse: str,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """
    Returns a DataFrame with one row per column across every user table:

        schema_name | table_name | object_id | column_id | column_name
        | data_type | max_length | precision | scale
        | table_create_date | table_modify_date
    """
    query = """
        SELECT
            s.name          AS schema_name,
            t.name          AS table_name,
            t.object_id,
            c.column_id,
            c.name          AS column_name,
            ty.name         AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            t.create_date   AS table_create_date,
            t.modify_date   AS table_modify_date
        FROM sys.tables  t
        JOIN sys.schemas s  ON t.schema_id    = s.schema_id
        JOIN sys.columns c  ON t.object_id    = c.object_id
        JOIN sys.types   ty ON c.user_type_id = ty.user_type_id
    """
    return read_warehouse_query(spark, warehouse, query, workspace_id, warehouse_id)


def get_current_clustering_config(
    spark: SparkSession,
    warehouse: str,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """
    Returns columns currently participating in CLUSTER BY:

        schema_name | table_name | index_name | column_name
        | clustering_ordinal | data_type | max_length | precision
    """
    query = """
        SELECT
            s.name                       AS schema_name,
            t.name                       AS table_name,
            i.name                       AS index_name,
            c.name                       AS column_name,
            ic.data_clustering_ordinal   AS clustering_ordinal,
            ty.name                      AS data_type,
            c.max_length,
            c.precision
        FROM sys.tables        t
        JOIN sys.schemas       s  ON t.schema_id  = s.schema_id
        JOIN sys.indexes       i  ON t.object_id  = i.object_id
        JOIN sys.index_columns ic ON i.object_id  = ic.object_id
                                  AND i.index_id  = ic.index_id
        JOIN sys.columns       c  ON ic.object_id = c.object_id
                                  AND ic.column_id = c.column_id
        JOIN sys.types         ty ON c.user_type_id = ty.user_type_id
        WHERE ic.data_clustering_ordinal > 0
    """
    return read_warehouse_query(spark, warehouse, query, workspace_id, warehouse_id).orderBy(
        "schema_name", "table_name", "clustering_ordinal"
    )


# ------------------------------------------------------------------
# Row counts  (reads user tables via three-part names)
# ------------------------------------------------------------------

def get_table_row_counts(
    spark: SparkSession,
    warehouse: str,
    full_metadata: DataFrame,
    min_rows: int = 0,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """
    Return row counts for every table referenced in *full_metadata*.

    Uses per-table ``COUNT_BIG(*)`` via T-SQL passthrough so the count
    runs **inside** the SQL engine (no data transferred to Spark).
    Fabric Warehouse resolves ``COUNT_BIG(*)`` from columnstore metadata
    so it is fast even on billion-row tables.

    Returns: schema_name | table_name | row_count
    """
    distinct_tables: List[Tuple[str, str]] = [
        (row.schema_name, row.table_name)
        for row in (
            full_metadata
            .select("schema_name", "table_name")
            .distinct()
            .collect()
        )
    ]

    results = []
    for schema_name, table_name in distinct_tables:
        try:
            q = (
                f"SELECT COUNT_BIG(*) AS cnt "
                f"FROM [{schema_name}].[{table_name}]"
            )
            cnt_row = read_warehouse_query(
                spark, warehouse, q, workspace_id, warehouse_id
            ).collect()[0]
            count = cnt_row["cnt"]
            results.append((schema_name, table_name, count))
        except Exception as exc:
            print(f"  [WARN] Could not count {schema_name}.{table_name}: {exc}")
            results.append((schema_name, table_name, -1))

    schema = StructType([
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("row_count", LongType(), False),
    ])

    row_counts = spark.createDataFrame(results, schema)
    if min_rows > 0:
        row_counts = row_counts.filter(F.col("row_count") >= min_rows)
    return row_counts


# ------------------------------------------------------------------
# Query Insights  (T-SQL passthrough)
# ------------------------------------------------------------------

def get_frequently_run_queries(
    spark: SparkSession,
    warehouse: str,
    min_runs: int = 1,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """
    Read queryinsights.frequently_run_queries and optionally filter by
    minimum number of runs.
    """
    try:
        where = f"WHERE number_of_runs >= {min_runs}" if min_runs > 1 else ""
        query = f"SELECT * FROM queryinsights.frequently_run_queries {where}"
        return read_warehouse_query(spark, warehouse, query, workspace_id, warehouse_id)
    except Exception as exc:
        print(
            f"  [WARN] Could not read queryinsights.frequently_run_queries: "
            f"{exc}\n"
            f"  Make sure Query Insights is enabled on the warehouse."
        )
        return spark.createDataFrame(
            [],
            StructType([
                StructField("query_hash", StringType()),
                StructField("number_of_runs", LongType()),
                StructField("last_run_start_time", StringType()),
                StructField("last_run_command", StringType()),
            ]),
        )


def get_long_running_queries(
    spark: SparkSession,
    warehouse: str,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> DataFrame:
    """Read queryinsights.long_running_queries for additional context."""
    try:
        return read_warehouse_query(
            spark, warehouse,
            "SELECT * FROM queryinsights.long_running_queries",
            workspace_id, warehouse_id,
        )
    except Exception as exc:
        print(f"  [WARN] Could not read long_running_queries: {exc}")
        return spark.createDataFrame(
            [],
            StructType([
                StructField("query_hash", StringType()),
                StructField("last_run_command", StringType()),
            ]),
        )


# ------------------------------------------------------------------
# Cardinality estimation  (T-SQL passthrough — no full-table reads)
# ------------------------------------------------------------------

def estimate_column_cardinality(
    spark: SparkSession,
    warehouse: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    sample_fraction: float = 1.0,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> Tuple[int, int, float]:
    """
    Estimate the cardinality (distinct count) for a single column.

    Pushes the computation to the SQL engine via T-SQL passthrough
    using ``APPROX_COUNT_DISTINCT``, which is orders-of-magnitude
    faster than reading the entire table through the Spark connector.

    *sample_fraction* is accepted for API compatibility but is ignored
    when the T-SQL path succeeds (the SQL engine uses efficient
    columnstore statistics internally).

    Returns (total_rows, approx_distinct, ratio).
    On error returns (-1, -1, -1.0).
    """
    try:
        # T-SQL APPROX_COUNT_DISTINCT — runs on the SQL engine, no data transfer
        q = (
            f"SELECT COUNT_BIG(*) AS total, "
            f"APPROX_COUNT_DISTINCT([{column_name}]) AS distinct_cnt "
            f"FROM [{schema_name}].[{table_name}]"
        )
        row = read_warehouse_query(
            spark, warehouse, q, workspace_id, warehouse_id
        ).collect()[0]

        total = row["total"]
        distinct = row["distinct_cnt"]
        ratio = distinct / total if total > 0 else 0.0
        return (total, distinct, ratio)
    except Exception as exc:
        print(
            f"  [WARN] T-SQL cardinality failed for "
            f"{schema_name}.{table_name}.{column_name}: {exc}"
        )
        # Fallback: read through Spark connector (slow for large tables)
        return _estimate_column_cardinality_spark(
            spark, warehouse, schema_name, table_name, column_name,
            sample_fraction, workspace_id, warehouse_id,
        )


def _estimate_column_cardinality_spark(
    spark: SparkSession,
    warehouse: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    sample_fraction: float = 1.0,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> Tuple[int, int, float]:
    """Fallback: estimate cardinality by reading through the Spark connector."""
    try:
        df = read_warehouse_table(
            spark, warehouse, schema_name, table_name,
            workspace_id, warehouse_id,
        )
        if 0 < sample_fraction < 1.0:
            df = df.sample(fraction=sample_fraction, seed=42)

        stats = df.agg(
            F.count("*").alias("total"),
            F.approx_count_distinct(F.col(column_name)).alias("distinct"),
        ).collect()[0]

        total = stats["total"]
        distinct = stats["distinct"]
        ratio = distinct / total if total > 0 else 0.0
        return (total, distinct, ratio)
    except Exception as exc:
        print(
            f"  [WARN] Spark cardinality fallback also failed for "
            f"{schema_name}.{table_name}.{column_name}: {exc}"
        )
        return (-1, -1, -1.0)


def estimate_batch_column_cardinality(
    spark: SparkSession,
    warehouse: str,
    schema_name: str,
    table_name: str,
    column_names: List[str],
    sample_fraction: float = 1.0,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> Dict[str, Tuple[int, int, float]]:
    """
    Estimate cardinality for multiple columns of a single table in one
    T-SQL query.

    Generates::

        SELECT COUNT_BIG(*) AS total,
               APPROX_COUNT_DISTINCT([col1]) AS d0,
               APPROX_COUNT_DISTINCT([col2]) AS d1, …
        FROM [schema].[table]

    This is executed server-side — no data is transferred to Spark.

    Returns a dict of ``column_name -> (total, approx_distinct, ratio)``.
    """
    if not column_names:
        return {}
    try:
        agg_parts = ["COUNT_BIG(*) AS total"]
        for i, col in enumerate(column_names):
            agg_parts.append(
                f"APPROX_COUNT_DISTINCT([{col}]) AS d{i}"
            )
        select_clause = ",\n               ".join(agg_parts)
        q = (
            f"SELECT {select_clause}\n"
            f"FROM [{schema_name}].[{table_name}]"
        )
        row = read_warehouse_query(
            spark, warehouse, q, workspace_id, warehouse_id
        ).collect()[0]

        total = row["total"]
        result: Dict[str, Tuple[int, int, float]] = {}
        for i, col in enumerate(column_names):
            distinct = row[f"d{i}"]
            ratio = distinct / total if total > 0 else 0.0
            result[col] = (total, distinct, ratio)
        return result
    except Exception as exc:
        print(
            f"  [WARN] T-SQL batch cardinality failed for "
            f"{schema_name}.{table_name}: {exc}"
        )
        # Fallback: read through Spark connector
        return _estimate_batch_cardinality_spark(
            spark, warehouse, schema_name, table_name, column_names,
            sample_fraction, workspace_id, warehouse_id,
        )


def _estimate_batch_cardinality_spark(
    spark: SparkSession,
    warehouse: str,
    schema_name: str,
    table_name: str,
    column_names: List[str],
    sample_fraction: float = 1.0,
    workspace_id: str = "",
    warehouse_id: str = "",
) -> Dict[str, Tuple[int, int, float]]:
    """Fallback: batch cardinality via the Spark connector."""
    if not column_names:
        return {}
    try:
        df = read_warehouse_table(
            spark, warehouse, schema_name, table_name,
            workspace_id, warehouse_id,
        )
        if 0 < sample_fraction < 1.0:
            df = df.sample(fraction=sample_fraction, seed=42)

        agg_exprs = [F.count("*").alias("_total_")]
        for i, col in enumerate(column_names):
            agg_exprs.append(
                F.approx_count_distinct(F.col(col)).alias(f"_d{i}")
            )

        stats = df.agg(*agg_exprs).collect()[0]
        total = stats["_total_"]

        result: Dict[str, Tuple[int, int, float]] = {}
        for i, col in enumerate(column_names):
            distinct = stats[f"_d{i}"]
            ratio = distinct / total if total > 0 else 0.0
            result[col] = (total, distinct, ratio)
        return result
    except Exception as exc:
        print(
            f"  [WARN] Spark batch cardinality fallback also failed for "
            f"{schema_name}.{table_name}: {exc}"
        )
        return {col: (-1, -1, -1.0) for col in column_names}

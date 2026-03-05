"""
Fabric Warehouse Data Clustering Advisor - Scoring & Recommendation Engine
=================================================================
Combines all signals (table size, predicate frequency, cardinality,
data-type support) into a composite score and produces ranked
recommendations for which tables/columns to cluster.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from .data_type_support import DataTypeAssessment, assess_data_type


# ------------------------------------------------------------------
# Data classes
# ------------------------------------------------------------------

@dataclass
class ColumnScore:
    """Detailed score breakdown for a single candidate column."""
    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    max_length: int
    precision: int
    row_count: int
    predicate_hits: int
    approx_distinct: int
    cardinality_ratio: float
    cardinality_level: str
    data_type_supported: bool
    data_type_label: str
    optimization_flag: str
    score_table_size: int
    score_predicate_freq: int
    score_cardinality: int
    score_data_type: int
    composite_score: int
    already_clustered: bool
    recommendation: str


@dataclass
class TableRecommendation:
    """Top-level recommendation for a table."""
    schema_name: str
    table_name: str
    row_count: int
    currently_clustered_columns: List[str]
    recommended_columns: List[ColumnScore]
    cluster_by_ddl: str
    warnings: List[str] = field(default_factory=list)


# ------------------------------------------------------------------
# Scoring functions
# ------------------------------------------------------------------

def _score_table_size(
    row_count: int,
    large_table_rows: int,
    weight: int,
) -> int:
    if row_count <= 0:
        return 0
    if row_count >= large_table_rows * 10:
        return weight
    elif row_count >= large_table_rows:
        return int(weight * 0.75)
    elif row_count >= large_table_rows / 10:
        return int(weight * 0.40)
    elif row_count >= large_table_rows / 100:
        return int(weight * 0.15)
    return 0


def _score_predicate_frequency(
    hits: int,
    weight: int,
) -> int:
    if hits <= 0:
        return 0
    if hits >= 20:
        return weight
    elif hits >= 10:
        return int(weight * 0.80)
    elif hits >= 5:
        return int(weight * 0.55)
    elif hits >= 2:
        return int(weight * 0.30)
    elif hits >= 1:
        return int(weight * 0.15)
    return 0


def _score_cardinality(
    level: str,
    weight: int,
) -> int:
    level_upper = level.upper()
    if level_upper == "HIGH":
        return weight
    elif level_upper in ("MID", "MEDIUM"):
        return int(weight * 0.65)
    elif level_upper == "LOW":
        return int(weight * 0.10)
    return 0


def _score_data_type(
    assessment: DataTypeAssessment,
    weight: int,
) -> int:
    if not assessment.is_supported:
        return 0
    if assessment.optimization_flag == "OK":
        return weight
    return int(weight * 0.65)


def _classify_cardinality(
    approx_distinct: int,
    total_rows: int,
    low_upper: float,
    high_lower: float,
    low_abs_max: int,
) -> str:
    if approx_distinct <= 0 or total_rows <= 0:
        return "Unknown"
    if approx_distinct <= low_abs_max:
        return "Low"
    ratio = approx_distinct / total_rows
    if ratio < low_upper:
        return "Low"
    elif ratio >= high_lower:
        return "High"
    return "Medium"


def _recommendation_label(
    composite: int,
    data_type_supported: bool,
    already_clustered: bool,
    min_score: int,
    cardinality_level: str = "Medium",
) -> str:
    # Evaluate quality first — applies to ALL columns including
    # those already clustered, so we can flag inefficient choices.
    if not data_type_supported:
        if already_clustered:
            return "Already clustered — NOT RECOMMENDED (unsupported data type)"
        return "Not recommended (unsupported data type)"
    if cardinality_level.upper() == "LOW":
        if already_clustered:
            return "Already clustered — NOT RECOMMENDED (low cardinality)"
        return "Not recommended (low cardinality)"
    if already_clustered:
        if composite < min_score:
            return "Already clustered — NOT RECOMMENDED (low score)"
        return "Already clustered"
    if composite >= min_score + 20:
        return "RECOMMENDED"
    elif composite >= min_score:
        return "Consider"
    return "Not recommended (low score)"


# ------------------------------------------------------------------
# Main scoring pipeline
# ------------------------------------------------------------------

def score_all_candidates(
    spark: SparkSession,
    full_metadata: DataFrame,
    row_counts: DataFrame,
    predicate_agg: Dict[Tuple[str, str, str], int],
    cardinality_cache: Dict[Tuple[str, str, str], Tuple[int, int, float]],
    current_clustering: DataFrame,
    *,
    full_scan_tables: set = frozenset(),
    large_table_rows: int = 1_000_000,
    min_predicate_hits: int = 1,
    weight_table_size: int = 30,
    weight_predicate_freq: int = 30,
    weight_cardinality: int = 25,
    weight_data_type: int = 15,
    low_cardinality_upper: float = 0.001,
    high_cardinality_lower: float = 0.05,
    low_cardinality_abs_max: int = 50,
    min_recommendation_score: int = 40,
) -> List[ColumnScore]:
    """
    Score every candidate column and return a sorted list of ColumnScore.
    """
    row_count_map: Dict[Tuple[str, str], int] = {}
    for row in row_counts.collect():
        row_count_map[(row.schema_name, row.table_name)] = row.row_count

    clustered_set: set = set()
    for row in current_clustering.collect():
        clustered_set.add((
            row.schema_name,
            row.table_name,
            row.column_name,
        ))

    metadata_rows = full_metadata.collect()

    scores: List[ColumnScore] = []

    for meta in metadata_rows:
        table_key = (meta.schema_name, meta.table_name)
        rc = row_count_map.get(table_key, 0)
        if rc <= 0:
            continue

        col_key = (meta.schema_name, meta.table_name, meta.column_name)
        hits = predicate_agg.get(col_key, 0)

        is_clustered = col_key in clustered_set
        is_full_scan_candidate = (
            table_key in full_scan_tables and col_key in cardinality_cache
        )
        if (
            hits < min_predicate_hits
            and not is_clustered
            and not is_full_scan_candidate
        ):
            continue

        dt_assessment = assess_data_type(
            meta.data_type, meta.max_length, meta.precision
        )

        card_data = cardinality_cache.get(col_key, (-1, -1, -1.0))
        total, approx_dist, ratio = card_data
        if approx_dist < 0:
            cardinality_level = "Medium"
            approx_dist = 0
            ratio = 0.0
        else:
            cardinality_level = _classify_cardinality(
                approx_dist, total,
                low_cardinality_upper, high_cardinality_lower,
                low_cardinality_abs_max,
            )

        s_size = _score_table_size(rc, large_table_rows, weight_table_size)
        s_pred = _score_predicate_frequency(hits, weight_predicate_freq)
        s_card = _score_cardinality(cardinality_level, weight_cardinality)
        s_dt = _score_data_type(dt_assessment, weight_data_type)
        composite = s_size + s_pred + s_card + s_dt

        # Apply penalty for cardinality levels that make clustering
        # ineffective.  Without this, a low-cardinality column on a
        # huge, frequently-queried table can still score very high
        # because cardinality is only 25% of the total weight.
        if cardinality_level.upper() == "LOW":
            composite = int(composite * 0.35)
        elif cardinality_level.upper() == "UNKNOWN":
            composite = int(composite * 0.70)

        rec_label = _recommendation_label(
            composite, dt_assessment.is_supported,
            is_clustered, min_recommendation_score,
            cardinality_level,
        )

        scores.append(ColumnScore(
            schema_name=meta.schema_name,
            table_name=meta.table_name,
            column_name=meta.column_name,
            data_type=meta.data_type,
            max_length=meta.max_length,
            precision=meta.precision,
            row_count=rc,
            predicate_hits=hits,
            approx_distinct=approx_dist,
            cardinality_ratio=round(ratio, 6),
            cardinality_level=cardinality_level,
            data_type_supported=dt_assessment.is_supported,
            data_type_label=dt_assessment.support_label,
            optimization_flag=dt_assessment.optimization_flag,
            score_table_size=s_size,
            score_predicate_freq=s_pred,
            score_cardinality=s_card,
            score_data_type=s_dt,
            composite_score=composite,
            already_clustered=is_clustered,
            recommendation=rec_label,
        ))

    scores.sort(key=lambda s: (-s.composite_score, s.table_name, s.column_name))
    return scores


# ------------------------------------------------------------------
# Group scores into per-table recommendations
# ------------------------------------------------------------------

def build_table_recommendations(
    scores: List[ColumnScore],
    max_columns: int = 3,
    min_score: int = 40,
    warehouse_name: str = "",
    generate_ctas: bool = True,
) -> List[TableRecommendation]:
    """
    Group scored columns by table and produce per-table recommendations.
    """
    by_table: Dict[Tuple[str, str], List[ColumnScore]] = defaultdict(list)
    for s in scores:
        by_table[(s.schema_name, s.table_name)].append(s)

    recommendations: List[TableRecommendation] = []

    for (schema, table), col_scores in by_table.items():
        current_cols = [
            c.column_name for c in col_scores if c.already_clustered
        ]

        # Warn if the table already has more clustered columns than
        # the recommended maximum.
        warnings: List[str] = []
        if len(current_cols) > max_columns:
            warnings.append(
                f"Warning: [{schema}].[{table}] already has "
                f"{len(current_cols)} clustered columns (recommended "
                f"max is {max_columns})."
            )

        # Flag clustered columns that are inefficient.
        for c in col_scores:
            if c.already_clustered and "NOT RECOMMENDED" in c.recommendation.upper():
                reason = c.recommendation.split("\u2014 ")[-1] if "\u2014" in c.recommendation else c.recommendation
                warnings.append(
                    f"Column [{c.column_name}] is currently clustered but "
                    f"{reason}. Consider removing it from CLUSTER BY."
                )

        # CTAS candidates: mid-to-high cardinality only
        candidates = [
            c for c in col_scores
            if not c.already_clustered
            and c.data_type_supported
            and c.composite_score >= min_score
            and c.cardinality_level.upper() in ("MID", "MEDIUM", "HIGH")
        ]

        # Generate one CTAS per candidate column so the user can choose
        ddl_parts = []
        if generate_ctas:
            for c in candidates:
                ddl_parts.append(
                    f"CREATE TABLE [{schema}].[{table}_clustered]\n"
                    f"WITH (CLUSTER BY ([{c.column_name}]))\n"
                    f"AS SELECT * FROM [{schema}].[{table}];"
                )
        ddl = "\n\n".join(ddl_parts)

        recommendations.append(TableRecommendation(
            schema_name=schema,
            table_name=table,
            row_count=col_scores[0].row_count if col_scores else 0,
            currently_clustered_columns=current_cols,
            recommended_columns=col_scores,
            cluster_by_ddl=ddl,
            warnings=warnings,
        ))

    recommendations.sort(
        key=lambda r: -(
            max((c.composite_score for c in r.recommended_columns), default=0)
        )
    )
    return recommendations


# ------------------------------------------------------------------
# Convert scores to a PySpark DataFrame
# ------------------------------------------------------------------

_SCORE_SCHEMA = StructType([
    StructField("schema_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("data_type", StringType(), True),
    StructField("max_length", IntegerType(), True),
    StructField("precision", IntegerType(), True),
    StructField("row_count", LongType(), True),
    StructField("predicate_hits", IntegerType(), True),
    StructField("approx_distinct", LongType(), True),
    StructField("cardinality_ratio", DoubleType(), True),
    StructField("cardinality_level", StringType(), True),
    StructField("data_type_label", StringType(), True),
    StructField("optimization_flag", StringType(), True),
    StructField("score_table_size", IntegerType(), True),
    StructField("score_predicate", IntegerType(), True),
    StructField("score_cardinality", IntegerType(), True),
    StructField("score_data_type", IntegerType(), True),
    StructField("composite_score", IntegerType(), True),
    StructField("already_clustered", StringType(), True),
    StructField("recommendation", StringType(), True),
])


def scores_to_dataframe(
    spark: SparkSession,
    scores: List[ColumnScore],
) -> DataFrame:
    """Convert list of ColumnScore to a Spark DataFrame."""
    rows = [
        (
            s.schema_name,
            s.table_name,
            s.column_name,
            s.data_type,
            s.max_length,
            s.precision,
            s.row_count,
            s.predicate_hits,
            s.approx_distinct,
            s.cardinality_ratio,
            s.cardinality_level,
            s.data_type_label,
            s.optimization_flag,
            s.score_table_size,
            s.score_predicate_freq,
            s.score_cardinality,
            s.score_data_type,
            s.composite_score,
            "Yes" if s.already_clustered else "No",
            s.recommendation,
        )
        for s in scores
    ]
    return spark.createDataFrame(rows, _SCORE_SCHEMA)

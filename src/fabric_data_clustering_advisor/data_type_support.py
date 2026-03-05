"""
Fabric Warehouse Data Clustering Advisor - Data Type Support
====================================================
Encapsulates the rules for which SQL data types are supported
by Fabric Warehouse Data Clustering, including warnings for
sub-optimal configurations.
"""

from __future__ import annotations

from dataclasses import dataclass

# ------------------------------------------------------------------
# Supported / unsupported data-type sets
# ------------------------------------------------------------------

_ALWAYS_SUPPORTED = frozenset({
    "bigint", "int", "smallint", "decimal", "numeric",
    "float", "real",
    "date", "datetime2", "time",
})

_CONDITIONALLY_SUPPORTED = frozenset({"char", "varchar"})

_NEVER_SUPPORTED = frozenset({"bit", "varbinary", "uniqueidentifier"})


@dataclass(frozen=True)
class DataTypeAssessment:
    """Result of evaluating a column's data type for clustering."""
    is_supported: bool
    support_label: str
    optimization_flag: str
    detail: str


def assess_data_type(
    data_type: str,
    max_length: int,
    precision: int,
) -> DataTypeAssessment:
    """
    Evaluate whether a column with the given SQL data-type metadata
    can be used in a CLUSTER BY clause and flag sub-optimal choices.
    """
    dt = data_type.strip().lower()

    if dt in _ALWAYS_SUPPORTED:
        if dt in ("decimal", "numeric") and precision > 18:
            return DataTypeAssessment(
                is_supported=True,
                support_label="Yes (with warnings)",
                optimization_flag=(
                    f"Warning: precision={precision} > 18 prevents "
                    "predicate pushdown to storage."
                ),
                detail=(
                    f"{dt}({precision}) is supported but predicates won't "
                    "be pushed down. Prefer precision <= 18."
                ),
            )
        if dt in ("decimal", "numeric"):
            return DataTypeAssessment(
                is_supported=True,
                support_label="Yes",
                optimization_flag="OK",
                detail=f"{dt}({precision}) is fully supported.",
            )
        return DataTypeAssessment(
            is_supported=True,
            support_label="Yes",
            optimization_flag="OK",
            detail=f"{dt} is fully supported for data clustering.",
        )

    if dt in _CONDITIONALLY_SUPPORTED:
        if max_length == -1:
            return DataTypeAssessment(
                is_supported=False,
                support_label="No (LOB type)",
                optimization_flag="N/A",
                detail=f"{dt}(max) is a LOB type and cannot be clustered.",
            )
        warnings = []
        if max_length > 32:
            warnings.append(
                f"Warning: max_length={max_length} > 32; only the first "
                "32 characters are used for column statistics."
            )
        if warnings:
            return DataTypeAssessment(
                is_supported=True,
                support_label="Yes (with warnings)",
                optimization_flag="; ".join(warnings),
                detail=(
                    f"{dt}({max_length}) is supported but long prefixes "
                    "may reduce clustering benefit."
                ),
            )
        return DataTypeAssessment(
            is_supported=True,
            support_label="Yes",
            optimization_flag="OK",
            detail=f"{dt}({max_length}) is fully supported.",
        )

    if dt in _NEVER_SUPPORTED:
        return DataTypeAssessment(
            is_supported=False,
            support_label=f"No ({dt} unsupported)",
            optimization_flag="N/A",
            detail=f"{dt} is not supported for data clustering.",
        )

    return DataTypeAssessment(
        is_supported=False,
        support_label="No (unknown type)",
        optimization_flag="N/A",
        detail=f"Data type '{dt}' is not recognized as clustering-eligible.",
    )

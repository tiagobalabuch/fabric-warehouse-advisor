"""
Fabric Warehouse Advisor
========================
A modular PySpark advisory framework for **Microsoft Fabric Warehouse**.

Each advisor module analyses a different aspect of warehouse health and
produces scored recommendations with rich reports.

Available Advisors
------------------
* **Data Clustering Advisor** — assesses which tables and columns should
  use Data Clustering.

Quick start::

    from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

    config = DataClusteringConfig(warehouse_name="MyWarehouse")
    advisor = DataClusteringAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
    result.scores_df.show()
"""

# ── Core infrastructure ────────────────────────────────────────────
from .core.warehouse_reader import (
    read_warehouse_query,
    read_warehouse_table,
    get_full_column_metadata,
    get_current_clustering_config,
    get_table_row_counts,
    get_frequently_run_queries,
    get_long_running_queries,
    estimate_column_cardinality,
    estimate_batch_column_cardinality,
)
from .core.predicate_parser import (
    PredicateHit,
    QueryPredicateSummary,
    extract_predicates_regex,
    parse_showplan_predicates,
    aggregate_predicate_hits,
)
from .core.report import save_report

# ── Data Clustering Advisor ────────────────────────────────────────
from .advisors.data_clustering import (
    DataClusteringAdvisor,
    DataClusteringConfig,
    DataClusteringResult,
    ColumnScore,
    TableRecommendation,
    DataTypeAssessment,
    assess_data_type,
)
from .advisors.data_clustering.report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
)

# ── Performance Check Advisor ──────────────────────────────────────
from .advisors.performance_check import (
    PerformanceCheckAdvisor,
    PerformanceCheckConfig,
    PerformanceCheckResult,
    Finding,
    CheckSummary,
)

# ── Security Check Advisor ─────────────────────────────────────────
from .advisors.security_check import (
    SecurityCheckAdvisor,
    SecurityCheckConfig,
    SecurityCheckResult,
)

__all__ = [
    # Core infrastructure
    "read_warehouse_query",
    "read_warehouse_table",
    "get_full_column_metadata",
    "get_current_clustering_config",
    "get_table_row_counts",
    "get_frequently_run_queries",
    "get_long_running_queries",
    "estimate_column_cardinality",
    "estimate_batch_column_cardinality",
    "PredicateHit",
    "QueryPredicateSummary",
    "extract_predicates_regex",
    "parse_showplan_predicates",
    "aggregate_predicate_hits",
    "generate_text_report",
    "generate_markdown_report",
    "generate_html_report",
    "save_report",
    # Data Clustering Advisor
    "DataClusteringAdvisor",
    "DataClusteringConfig",
    "DataClusteringResult",
    "ColumnScore",
    "TableRecommendation",
    "DataTypeAssessment",
    "assess_data_type",
    # Performance Check Advisor
    "PerformanceCheckAdvisor",
    "PerformanceCheckConfig",
    "PerformanceCheckResult",
    "Finding",
    "CheckSummary",
    # Security Check Advisor
    "SecurityCheckAdvisor",
    "SecurityCheckConfig",
    "SecurityCheckResult",
]

try:
    from importlib import metadata as _metadata  # type: ignore[import]
except ImportError:  # pragma: no cover - Python < 3.8 fallback
    _metadata = None  # type: ignore[assignment]


def _load_version() -> str:
    """
    Load the package version from installed metadata, falling back to pyproject.toml.

    Returns ``"0.0.0"`` and emits a warning when the version cannot be
    determined (e.g. the package is not installed and neither ``tomllib``
    nor ``tomli`` is available).
    """
    import warnings
    # First, try the installed package metadata.
    if _metadata is not None:
        try:
            return _metadata.version("fabric-warehouse-advisor")
        except _metadata.PackageNotFoundError:
            pass

    # Fallback: attempt to read version from pyproject.toml in the project root.
    try:
        import tomllib  # type: ignore[import]
    except ModuleNotFoundError:
        # tomllib is only available in Python 3.11+; try the tomli backport if present.
        try:
            import tomli as tomllib  # type: ignore[import,assignment]
        except ModuleNotFoundError:
            # Neither tomllib nor tomli is available; use a safe default.
            warnings.warn(
                "fabric_warehouse_advisor version could not be determined: "
                "package is not installed and neither tomllib (Python 3.11+) "
                "nor the tomli backport is available. Reporting version as '0.0.0'.",
                stacklevel=2,
            )
            return "0.0.0"

    from pathlib import Path

    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    if pyproject_path.is_file():
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
        return data.get("project", {}).get("version", "0.0.0")

    # As a last resort, return a placeholder version rather than failing import.
    warnings.warn(
        "fabric_warehouse_advisor version could not be determined: "
        "pyproject.toml not found and package metadata unavailable. "
        "Reporting version as '0.0.0'.",
        stacklevel=2,
    )
    return "0.0.0"


__version__ = _load_version()

# Clean up internal helpers from the public namespace.
del _load_version, _metadata

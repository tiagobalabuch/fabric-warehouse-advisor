"""
Fabric Warehouse Data Clustering Advisor
==========================================
A PySpark library that assesses and recommends which tables and columns
should use **Data Clustering** in Microsoft Fabric Warehouse.

Quick start::

    from fabric_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

    config = DataClusteringAdvisorConfig(warehouse_name="MyWarehouse")
    advisor = DataClusteringAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
    result.scores_df.show()
"""

from .config import DataClusteringAdvisorConfig
from .advisor import DataClusteringAdvisor, AdvisorResult
from .scoring import ColumnScore, TableRecommendation
from .data_type_support import DataTypeAssessment, assess_data_type
from .predicate_parser import (
    PredicateHit,
    QueryPredicateSummary,
    extract_predicates_regex,
    parse_showplan_predicates,
    aggregate_predicate_hits,
)
from .report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
    save_report,
)

__all__ = [
    # Core
    "DataClusteringAdvisor",
    "DataClusteringAdvisorConfig",
    "AdvisorResult",
    # Scoring / results
    "ColumnScore",
    "TableRecommendation",
    # Data-type support
    "DataTypeAssessment",
    "assess_data_type",
    # Predicate parsing
    "PredicateHit",
    "QueryPredicateSummary",
    "extract_predicates_regex",
    "parse_showplan_predicates",
    "aggregate_predicate_hits",
    # Reports
    "generate_text_report",
    "generate_markdown_report",
    "generate_html_report",
    "save_report",
]

__version__ = "0.1.0"

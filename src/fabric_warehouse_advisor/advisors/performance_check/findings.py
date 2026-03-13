"""
Fabric Warehouse Advisor — Performance Check Findings
======================================================
Structured output model for all performance checks.

Shared types (:class:`Finding`, :class:`CheckSummary`, severity
constants) now live in :mod:`fabric_warehouse_advisor.core.findings`
and are re-exported here for backward compatibility.

Performance-check-specific **category** constants remain in this
module.
"""

from __future__ import annotations

# Re-export shared types from core
from ...core.findings import (  # noqa: F401
    Finding,
    CheckSummary,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    SEVERITY_ORDER,
)

# Performance-check-specific categories
CATEGORY_WAREHOUSE_TYPE = "warehouse_type"
CATEGORY_DATA_TYPES = "data_types"
CATEGORY_CACHING = "caching"
CATEGORY_STATISTICS = "statistics"
CATEGORY_VORDER = "vorder"
CATEGORY_COLLATION = "collation"
CATEGORY_QUERY_REGRESSION = "query_regression"

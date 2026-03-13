"""
Fabric Warehouse Advisor — Security Check Findings
======================================================
Re-exports shared types from :mod:`fabric_warehouse_advisor.core.findings`
and defines security-check-specific category constants.
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

# Security-check-specific categories
CATEGORY_PERMISSIONS = "permissions"
CATEGORY_ROLES = "roles"
CATEGORY_RLS = "row_level_security"
CATEGORY_CLS = "column_level_security"
CATEGORY_DDM = "dynamic_data_masking"

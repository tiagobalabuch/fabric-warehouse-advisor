"""
Fabric Warehouse Advisor — Security Check Advisor
=====================================================
Analyses security configuration in Microsoft Fabric Warehouses
including schema permissions, custom roles, Row-Level Security,
Column-Level Security, and Dynamic Data Masking.

Quick start::

    from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

    config = SecurityCheckConfig(warehouse_name="MyWarehouse")
    advisor = SecurityCheckAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
"""

from .advisor import SecurityCheckAdvisor, SecurityCheckResult
from .config import SecurityCheckConfig
from .findings import Finding, CheckSummary

__all__ = [
    "SecurityCheckAdvisor",
    "SecurityCheckConfig",
    "SecurityCheckResult",
    "Finding",
    "CheckSummary",
]

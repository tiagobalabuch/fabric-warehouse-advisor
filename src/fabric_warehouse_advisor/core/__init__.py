"""
Fabric Warehouse Advisor — Core Infrastructure
================================================
Shared utilities and base classes used by all advisor modules.
"""

from .findings import Finding, CheckSummary  # noqa: F401
from .fabric_rest_client import FabricRestClient, FabricRestError  # noqa: F401

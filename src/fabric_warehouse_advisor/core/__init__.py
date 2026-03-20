"""
Fabric Warehouse Advisor — Core Infrastructure
================================================
Shared utilities and base classes used by all advisor modules.
"""

from .findings import Finding, CheckSummary  # noqa: F401
from .fabric_rest_client import FabricRestClient, FabricRestError  # noqa: F401
from .phase_tracker import PhaseTracker, PhaseResult, PHASE_COMPLETED, PHASE_SKIPPED, PHASE_FAILED  # noqa: F401
from .scope_resolver import resolve_table_scope, ScopeResult  # noqa: F401

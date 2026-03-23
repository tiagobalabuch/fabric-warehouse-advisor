"""
Security Check — Auth Mode Detection  (SEC-015)
==================================================
Detects whether a SQL analytics endpoint is running in
**User Identity** mode (OneLake security controls table access)
or **Delegated Identity** mode (SQL security controls all objects).

This fundamentally changes which security checks are meaningful:

* **User Identity** — SQL RLS, CLS, DDM, and custom roles are
  inactive; OneLake data access roles are the source of truth.
* **Delegated** — SQL security is active for all objects; OneLake
  roles are not enforced at query time.

.. note::

   This check uses an **undocumented** internal API and is gated
   behind ``edition == "LakeWarehouse"``.
"""

from __future__ import annotations

from typing import List, Tuple

from ....core.fabric_rest_client import FabricRestClient
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    CATEGORY_AUTH_MODE,
)


_MODE_DESCRIPTIONS = {
    "user_identity": (
        "User Identity mode — OneLake security roles control table-level "
        "access. SQL RLS, CLS, DDM, and custom database roles are inactive."
    ),
    "delegated": (
        "Delegated Identity mode — SQL security (permissions, RLS, CLS, DDM) "
        "controls all objects. OneLake data access roles are not enforced "
        "at query time."
    ),
}


def detect_auth_mode(
    rest_client: FabricRestClient,
    sql_endpoint_id: str,
    config: SecurityCheckConfig,
) -> Tuple[str, List[Finding]]:
    """Detect the SQL analytics endpoint access mode.

    Parameters
    ----------
    rest_client : FabricRestClient
        Authenticated Fabric REST client.
    sql_endpoint_id : str
        The SQL analytics endpoint item ID (GUID).
    config : SecurityCheckConfig
        Advisor configuration.

    Returns
    -------
    tuple[str, list[Finding]]
        A tuple of ``(auth_mode, findings)`` where ``auth_mode`` is
        ``"user_identity"``, ``"delegated"``, or ``""`` (unknown).
    """
    findings: List[Finding] = []

    auth_mode = rest_client.get_sql_endpoint_auth_mode(sql_endpoint_id)

    if auth_mode:
        description = _MODE_DESCRIPTIONS.get(auth_mode, auth_mode)
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_AUTH_MODE,
            check_name="auth_mode_detected",
            object_name=config.warehouse_name,
            message=f"SQL endpoint access mode: {auth_mode.replace('_', ' ').title()}.",
            detail=description,
        ))
    else:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_AUTH_MODE,
            check_name="auth_mode_unknown",
            object_name=config.warehouse_name,
            message=(
                "Could not detect the SQL endpoint access mode."
            ),
            detail=(
                "The undocumented auth mode API did not return a "
                "recognisable value. All checks will run at their "
                "default severity."
            ),
            recommendation=(
                "Verify the sql_endpoint_id is correct and the "
                "token has sufficient permissions."
            ),
        ))

    return auth_mode, findings

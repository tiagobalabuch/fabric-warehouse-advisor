"""
Security Check — Item Permissions  (SEC-009)
==============================================
Analyses Fabric item-level permissions via the **Admin API** to detect
overly permissive sharing of the warehouse item.

.. important::

   This check requires **Fabric Administrator** privileges.
   The API endpoint is:
   ``GET /v1/admin/workspaces/{workspaceId}/items/{itemId}/users``

   If the caller is not a Fabric admin the check is gracefully
   skipped with an INFO finding.

Checks
------
* ``EntireTenant`` principal with item permissions  → CRITICAL
* Excessive number of principals with ``ReadData``  → HIGH
* Principals with ``Write`` who are not workspace
  Admin/Member/Contributor                          → MEDIUM
* Summary of item sharing                           → INFO
"""

from __future__ import annotations

from typing import Dict, List, Set

from ....core.fabric_rest_client import FabricRestClient, FabricRestError
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_ITEM_PERMISSIONS,
)


def check_item_permissions(
    rest_client: FabricRestClient,
    workspace_id: str,
    warehouse_id: str,
    config: SecurityCheckConfig,
    workspace_role_principals: Dict[str, str] | None = None,
) -> List[Finding]:
    """Analyse item-level permissions for a warehouse.

    Parameters
    ----------
    rest_client : FabricRestClient
        Authenticated Fabric REST client.
    workspace_id : str
        Target workspace ID.
    warehouse_id : str
        Target warehouse item ID.
    config : SecurityCheckConfig
        Advisor configuration.
    workspace_role_principals : dict[str, str] | None
        Optional mapping of ``principal_id → workspace_role``
        (e.g. ``{"<guid>": "Admin"}``).  Used to detect
        item-level Write grants that duplicate workspace roles.

    Returns
    -------
    list[Finding]
        Findings related to item sharing and permissions.
    """
    findings: List[Finding] = []
    ws_principals = workspace_role_principals or {}

    # ── Fetch item access details via Admin API ──────────────────
    try:
        access_details = rest_client.list_item_access_details(
            workspace_id, warehouse_id,
        )
    except FabricRestError as exc:
        if exc.status_code in (401, 403):
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_ITEM_PERMISSIONS,
                check_name="item_permissions_skipped_no_admin",
                object_name=warehouse_id,
                message=(
                    "Item permissions check skipped — "
                    "Fabric Admin role is required."
                ),
                detail=(
                    f"The Admin API returned HTTP {exc.status_code}. "
                    f"This check requires the caller to be a Fabric "
                    f"Administrator or use a service principal with "
                    f"Tenant.Read.All scope."
                ),
                recommendation=(
                    "Ask a Fabric Administrator to run this advisor, "
                    "or grant Tenant.Read.All to the service principal."
                ),
            ))
            return findings

        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_ITEM_PERMISSIONS,
            check_name="item_permissions_query_failed",
            object_name=warehouse_id,
            message="Unable to retrieve item access details.",
            detail=f"REST API error: {exc}",
            recommendation=(
                "Ensure the caller is a Fabric Administrator and "
                "the token has Tenant.Read.All scope."
            ),
        ))
        return findings

    entries = access_details.get("accessDetails", [])

    if not entries:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ITEM_PERMISSIONS,
            check_name="no_item_permissions_found",
            object_name=warehouse_id,
            message="No item-level permission entries returned.",
        ))
        return findings

    # ── Classify principals ──────────────────────────────────────
    read_data_principals: List[str] = []
    write_principals: List[str] = []
    reshare_principals: List[str] = []
    all_principals: List[str] = []

    # Workspace roles that imply Write
    _WRITE_ROLES: Set[str] = {"Admin", "Member", "Contributor"}

    for entry in entries:
        principal = entry.get("principal", {})
        detail = entry.get("itemAccessDetails", {})

        principal_type = principal.get("type", "")
        display_name = principal.get("displayName", "(unknown)")
        principal_id = principal.get("id", "")

        permissions: List[str] = detail.get("permissions", [])
        additional: List[str] = detail.get("additionalPermissions", [])
        all_perms = permissions + additional

        all_principals.append(display_name)

        # ── Check: EntireTenant principal ────────────────────────
        if principal_type == "EntireTenant":
            findings.append(Finding(
                level=LEVEL_CRITICAL,
                category=CATEGORY_ITEM_PERMISSIONS,
                check_name="entire_tenant_item_access",
                object_name=warehouse_id,
                message=(
                    "Entire tenant has item-level access to the "
                    "warehouse."
                ),
                detail=(
                    f"The '{display_name}' principal grants item "
                    f"permissions [{', '.join(all_perms)}] to every "
                    f"user in the tenant."
                ),
                recommendation=(
                    "Remove the EntireTenant item sharing and "
                    "grant access to specific users or security groups."
                ),
            ))

        # Aggregate permission holders
        if "ReadData" in all_perms:
            read_data_principals.append(display_name)
        if "Write" in permissions:
            write_principals.append(display_name)
            # ── Check: Write permission outside workspace roles ──
            ws_role = ws_principals.get(principal_id, "")
            if ws_role not in _WRITE_ROLES and principal_type != "EntireTenant":
                findings.append(Finding(
                    level=LEVEL_MEDIUM,
                    category=CATEGORY_ITEM_PERMISSIONS,
                    check_name="item_write_outside_workspace_role",
                    object_name=warehouse_id,
                    message=(
                        f"[{display_name}] has item-level Write "
                        f"permission without a workspace role that "
                        f"implies Write."
                    ),
                    detail=(
                        f"Principal '{display_name}' ({principal_type}) "
                        f"was granted Write via item sharing. "
                        f"Workspace role: "
                        f"'{ws_role or 'none'}'."
                    ),
                    recommendation=(
                        "Review whether this principal truly needs "
                        "Write access. Prefer granting workspace roles "
                        "for write-level access to maintain a clear "
                        "access model."
                    ),
                ))
        if "Reshare" in permissions:
            reshare_principals.append(display_name)

    # ── Check: excessive ReadData sharing ────────────────────────
    if len(read_data_principals) > config.max_item_readdata_principals:
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_ITEM_PERMISSIONS,
            check_name="excessive_readdata_sharing",
            object_name=warehouse_id,
            message=(
                f"{len(read_data_principals)} principal(s) have "
                f"ReadData access (threshold: "
                f"{config.max_item_readdata_principals})."
            ),
            detail=(
                f"Principals: {', '.join(read_data_principals[:10])}"
                f"{'…' if len(read_data_principals) > 10 else ''}."
            ),
            recommendation=(
                "Review item sharing and reduce ReadData grants. "
                "Use T-SQL GRANT/DENY for granular data access "
                "instead of broad ReadData sharing."
            ),
        ))

    # ── INFO summary ─────────────────────────────────────────────
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ITEM_PERMISSIONS,
            check_name="item_permissions_healthy",
            object_name=warehouse_id,
            message="Item-level permissions follow best practices.",
            detail=(
                f"{len(all_principals)} principal(s) with item access. "
                f"ReadData: {len(read_data_principals)}, "
                f"Write: {len(write_principals)}, "
                f"Reshare: {len(reshare_principals)}."
            ),
        ))
    else:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ITEM_PERMISSIONS,
            check_name="item_permissions_summary",
            object_name=warehouse_id,
            message=f"Item access summary for warehouse.",
            detail=(
                f"{len(all_principals)} principal(s) with item access. "
                f"ReadData: {len(read_data_principals)}, "
                f"Write: {len(write_principals)}, "
                f"Reshare: {len(reshare_principals)}."
            ),
        ))

    return findings

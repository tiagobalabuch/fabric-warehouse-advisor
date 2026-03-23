"""
Security Check — Workspace Roles  (SEC-006)
=============================================
Analyses Fabric workspace role assignments via the REST API to detect
overly permissive access patterns.

Checks
------
* Excessive workspace Admin count
* ``EntireTenant`` principal with any workspace role
* Service principals with Admin role
"""

from __future__ import annotations

from typing import List

from ....core.fabric_rest_client import FabricRestClient, FabricRestError
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_WORKSPACE_ROLES,
)


def check_workspace_roles(
    rest_client: FabricRestClient,
    workspace_id: str,
    config: SecurityCheckConfig,
    workspace_display_name: str = "",
) -> List[Finding]:
    """Analyse workspace role assignments.

    Parameters
    ----------
    rest_client : FabricRestClient
        Authenticated Fabric REST client.
    workspace_id : str
        Target workspace ID.
    config : SecurityCheckConfig
        Advisor configuration.
    workspace_display_name : str
        Human-readable workspace name for report display.

    Returns
    -------
    list[Finding]
        Findings related to workspace role assignments.
    """
    findings: List[Finding] = []
    obj_name = workspace_display_name or workspace_id

    try:
        assignments = rest_client.get_workspace_role_assignments(workspace_id)
    except FabricRestError as exc:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_WORKSPACE_ROLES,
            check_name="workspace_roles_query_failed",
            object_name=obj_name,
            message="Unable to retrieve workspace role assignments.",
            detail=f"REST API error: {exc}",
            recommendation=(
                "Ensure you have Member or higher workspace role and "
                "the token has Workspace.Read.All scope."
            ),
        ))
        return findings

    if not assignments:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WORKSPACE_ROLES,
            check_name="no_workspace_roles_found",
            object_name=obj_name,
            message="No workspace role assignments returned.",
        ))
        return findings

    # Classify assignments by role
    admins: list[dict] = []
    members: list[dict] = []
    contributors: list[dict] = []
    viewers: list[dict] = []

    for assignment in assignments:
        role = assignment.get("role", "")
        principal = assignment.get("principal", {})

        if role == "Admin":
            admins.append(assignment)
        elif role == "Member":
            members.append(assignment)
        elif role == "Contributor":
            contributors.append(assignment)
        elif role == "Viewer":
            viewers.append(assignment)

        # --- Check: EntireTenant principal (any role) ---
        principal_type = principal.get("type", "")
        display_name = principal.get("displayName", "(unknown)")

        if principal_type == "EntireTenant":
            level = LEVEL_CRITICAL if role in ("Admin", "Member") else LEVEL_HIGH
            findings.append(Finding(
                level=level,
                category=CATEGORY_WORKSPACE_ROLES,
                check_name="entire_tenant_access",
                object_name=obj_name,
                message=(
                    f"Entire tenant has {role} access to the workspace."
                ),
                detail=(
                    f"The '{display_name}' principal grants {role} "
                    f"workspace access to every user in the tenant. "
                    f"This bypasses the principle of least privilege."
                ),
                recommendation=(
                    f"Remove the EntireTenant assignment and grant "
                    f"workspace access to specific security groups."
                ),
            ))

        # --- Check: Service principal with Admin role ---
        if principal_type == "ServicePrincipal" and role == "Admin":
            app_id = (
                principal.get("servicePrincipalDetails", {})
                .get("aadAppId", "")
            ) or "(not available)"
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_WORKSPACE_ROLES,
                check_name="service_principal_admin",
                object_name=obj_name,
                message=(
                    f"Service principal [{display_name}] has Admin role."
                ),
                detail=(
                    f"Service principal '{display_name}' "
                    f"(AppId: {app_id}) has Admin access. "
                    f"Service principals with Admin role can perform "
                    f"any operation in the workspace."
                ),
                recommendation=(
                    f"Review whether [{display_name}] requires Admin. "
                    f"Prefer Contributor or Member for automation scenarios."
                ),
            ))

    # --- Check: excessive workspace Admin count ---
    if len(admins) > config.max_workspace_admins:
        admin_names = [
            a.get("principal", {}).get("displayName", "(unknown)")
            for a in admins
        ]
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_WORKSPACE_ROLES,
            check_name="excessive_workspace_admins",
            object_name=obj_name,
            message=(
                f"Workspace has {len(admins)} Admin(s) "
                f"(threshold: {config.max_workspace_admins})."
            ),
            detail=f"Admins: {', '.join(admin_names)}.",
            recommendation=(
                "Review Admin assignments and downgrade users who do "
                "not require full administrative access to Member or "
                "Contributor."
            ),
        ))
    elif admins:
        admin_names = [
            a.get("principal", {}).get("displayName", "(unknown)")
            for a in admins
        ]
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WORKSPACE_ROLES,
            check_name="workspace_admins_ok",
            object_name=obj_name,
            message=(
                f"Workspace has {len(admins)} Admin(s) — "
                f"within threshold."
            ),
            detail=f"Admins: {', '.join(admin_names)}.",
        ))

    # --- INFO summary ---
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        total = len(admins) + len(members) + len(contributors) + len(viewers)
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_WORKSPACE_ROLES,
            check_name="workspace_roles_healthy",
            object_name=obj_name,
            message="Workspace role assignments follow best practices.",
            detail=(
                f"{total} assignment(s): {len(admins)} Admin, "
                f"{len(members)} Member, {len(contributors)} Contributor, "
                f"{len(viewers)} Viewer."
            ),
        ))

    return findings

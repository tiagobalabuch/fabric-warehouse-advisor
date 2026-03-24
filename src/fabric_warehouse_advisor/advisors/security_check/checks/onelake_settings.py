"""
Security Check — OneLake Settings  (SEC-013)
==============================================
Analyses workspace-level OneLake settings via the REST API to detect
missing diagnostic logging and immutability policies.

Checks
------
* OneLake diagnostic logging disabled
* Diagnostic logging without immutability policy
* Summary of OneLake settings
"""

from __future__ import annotations

from typing import Any, Dict, List

from ....core.fabric_rest_client import FabricRestClient, FabricRestError
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    CATEGORY_ONELAKE_SETTINGS,
)


def check_onelake_settings(
    rest_client: FabricRestClient,
    workspace_id: str,
    config: SecurityCheckConfig,
    workspace_display_name: str = "",
) -> List[Finding]:
    """Analyse workspace-level OneLake settings.

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
        Findings related to OneLake workspace settings.
    """
    findings: List[Finding] = []
    obj_name = workspace_display_name or workspace_id

    # ── Fetch settings ───────────────────────────────────────────
    try:
        settings = rest_client.get_onelake_settings(workspace_id)
    except FabricRestError as exc:
        if exc.status_code in (401, 403):
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_ONELAKE_SETTINGS,
                check_name="onelake_settings_skipped_no_admin",
                object_name=obj_name,
                message=(
                    "OneLake settings check skipped — Admin "
                    "workspace role is required."
                ),
                detail=f"REST API error: {exc}",
                recommendation=(
                    "Request Admin workspace role to inspect "
                    "OneLake settings, or have a workspace Admin "
                    "run this check."
                ),
            ))
            return findings
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_ONELAKE_SETTINGS,
            check_name="onelake_settings_query_failed",
            object_name=obj_name,
            message="Unable to retrieve OneLake settings.",
            detail=f"REST API error: {exc}",
            recommendation=(
                "Ensure you have Admin workspace role and "
                "the token has OneLake.Read.All scope."
            ),
        ))
        return findings

    # ── Diagnostics ──────────────────────────────────────────────
    diagnostics = settings.get("diagnostics", {})
    diag_status = diagnostics.get("status", "Unknown")

    if diag_status.lower() == "disabled":
        findings.append(Finding(
            level=LEVEL_MEDIUM,
            category=CATEGORY_ONELAKE_SETTINGS,
            check_name="onelake_diagnostics_disabled",
            object_name=obj_name,
            message="OneLake diagnostic logging is disabled.",
            detail=(
                "Without diagnostic logs, you have no visibility "
                "into OneLake data access patterns, which limits "
                "auditing and incident response capabilities."
            ),
            recommendation=(
                "Enable OneLake diagnostic logging in the workspace "
                "settings and configure a lakehouse destination for "
                "the logs."
            ),
        ))
    elif diag_status.lower() == "enabled":
        destination = diagnostics.get("destination", {})
        dest_type = destination.get("type", "Unknown")
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ONELAKE_SETTINGS,
            check_name="onelake_diagnostics_enabled",
            object_name=obj_name,
            message=(
                f"OneLake diagnostic logging is enabled "
                f"(destination: {dest_type})."
            ),
        ))

    # ── Immutability policies ────────────────────────────────────
    policies = settings.get("immutabilityPolicies", [])

    if not policies and diag_status.lower() == "enabled":
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_ONELAKE_SETTINGS,
            check_name="no_immutability_policy",
            object_name=obj_name,
            message=(
                "OneLake diagnostic logs have no immutability policy."
            ),
            detail=(
                "Without an immutability policy, diagnostic logs "
                "could be modified or deleted, undermining audit "
                "integrity."
            ),
            recommendation=(
                "Configure an immutability policy with an "
                "appropriate retention period for your diagnostic "
                "logs to protect audit trail integrity."
            ),
        ))
    elif policies:
        for policy in policies:
            scope = policy.get("scope", "Unknown")
            days = policy.get("retentionDays", 0)
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_ONELAKE_SETTINGS,
                check_name="immutability_policy_found",
                object_name=obj_name,
                message=(
                    f"Immutability policy: scope={scope}, "
                    f"retention={days} day(s)."
                ),
            ))

    return findings

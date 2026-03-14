"""
Security Check — Network Isolation  (SEC-007)
===============================================
Analyses the Fabric workspace network communication policy via the
REST API to detect unrestricted public access.

Checks
------
* Inbound public access allowed (default action = Allow)
* Outbound public access unrestricted
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
    CATEGORY_NETWORK,
)


def check_network_isolation(
    rest_client: FabricRestClient,
    workspace_id: str,
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Analyse workspace network communication policy.

    Parameters
    ----------
    rest_client : FabricRestClient
        Authenticated Fabric REST client.
    workspace_id : str
        Target workspace ID.
    config : SecurityCheckConfig
        Advisor configuration.

    Returns
    -------
    list[Finding]
        Findings related to network isolation settings.
    """
    findings: List[Finding] = []

    try:
        policy = rest_client.get_network_communication_policy(workspace_id)
    except FabricRestError as exc:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_NETWORK,
            check_name="network_policy_query_failed",
            object_name=workspace_id,
            message="Unable to retrieve network communication policy.",
            detail=f"REST API error: {exc}",
            recommendation=(
                "Ensure you have Viewer or higher workspace role and "
                "the token has Workspace.Read.All scope."
            ),
        ))
        return findings

    # Parse the policy structure:
    # {
    #   "inbound": {"publicAccessRules": {"defaultAction": "Allow"|"Deny"}},
    #   "outbound": {"publicAccessRules": {"defaultAction": "Allow"|"Deny"}}
    # }
    inbound = policy.get("inbound", {})
    outbound = policy.get("outbound", {})

    inbound_action = (
        inbound.get("publicAccessRules", {}).get("defaultAction", "")
    )
    outbound_action = (
        outbound.get("publicAccessRules", {}).get("defaultAction", "")
    )

    # --- Check: inbound public access ---
    if inbound_action == "Allow":
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_NETWORK,
            check_name="inbound_public_access_allowed",
            object_name=workspace_id,
            message="Inbound public network access is ALLOWED.",
            detail=(
                "The workspace accepts connections from public networks. "
                "Any user with valid credentials can connect from any "
                "IP address, increasing the attack surface."
            ),
            recommendation=(
                "Configure the workspace network policy to Deny inbound "
                "public access and use private endpoints or trusted "
                "service access instead."
            ),
        ))
    elif inbound_action == "Deny":
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_NETWORK,
            check_name="inbound_public_access_denied",
            object_name=workspace_id,
            message="Inbound public network access is DENIED.",
            detail=(
                "The workspace blocks connections from public networks. "
                "Access is restricted to private endpoints and trusted "
                "services."
            ),
        ))
    else:
        findings.append(Finding(
            level=LEVEL_MEDIUM,
            category=CATEGORY_NETWORK,
            check_name="inbound_policy_unknown",
            object_name=workspace_id,
            message=(
                f"Inbound network policy has unexpected value: "
                f"'{inbound_action}'."
            ),
            detail=(
                "The default action could not be interpreted as "
                "Allow or Deny. The API may have returned an "
                "unexpected value."
            ),
            recommendation=(
                "Verify the workspace network settings in the "
                "Fabric portal."
            ),
        ))

    # --- Check: outbound public access ---
    if outbound_action == "Allow":
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_NETWORK,
            check_name="outbound_public_access_allowed",
            object_name=workspace_id,
            message="Outbound public network access is ALLOWED.",
            detail=(
                "The workspace can send data to public endpoints. "
                "While common for legitimate integrations, this could "
                "be a data exfiltration vector if exploited."
            ),
            recommendation=(
                "If the workspace does not need to reach external "
                "public endpoints, consider restricting outbound access."
            ),
        ))
    elif outbound_action == "Deny":
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_NETWORK,
            check_name="outbound_public_access_denied",
            object_name=workspace_id,
            message="Outbound public network access is DENIED.",
            detail=(
                "The workspace blocks outbound connections to public "
                "networks, reducing data exfiltration risk."
            ),
        ))

    # --- INFO summary if no issues ---
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_NETWORK,
            check_name="network_isolation_healthy",
            object_name=workspace_id,
            message="Network isolation is properly configured.",
            detail=(
                f"Inbound: {inbound_action}, Outbound: {outbound_action}."
            ),
        ))

    return findings

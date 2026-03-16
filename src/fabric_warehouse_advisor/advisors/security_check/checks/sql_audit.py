"""
Security Check — SQL Audit Settings  (SEC-008)
================================================
Analyses Fabric warehouse SQL audit settings via the REST API to detect
missing or weak audit configurations.

Checks
------
* SQL auditing disabled
* Short retention period (< threshold days)
* Entire audit category uncovered (no groups enabled)
* Missing individual recommended audit action groups
* Zero retention days (indefinite — informational)
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, NamedTuple, Set

from ....core.fabric_rest_client import FabricRestClient, FabricRestError
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    CATEGORY_SQL_AUDIT,
)


# ------------------------------------------------------------------
# Audit action groups — full Fabric catalogue
# ------------------------------------------------------------------

class _AuditGroup(NamedTuple):
    name: str
    description: str
    recommended: bool


class _AuditCategory(NamedTuple):
    label: str
    groups: List[_AuditGroup]


_CATEGORIES: List[_AuditCategory] = [
    _AuditCategory(
        label="Authentication and authorization events",
        groups=[
            _AuditGroup("SUCCESSFUL_DATABASE_AUTHENTICATION_GROUP", "User logged in", True),
            _AuditGroup("FAILED_DATABASE_AUTHENTICATION_GROUP", "User failed to log in", True),
            _AuditGroup("DATABASE_LOGOUT_GROUP", "User logged out", False),
            _AuditGroup("DATABASE_ROLE_MEMBER_CHANGE_GROUP", "Role member was changed", True),
            _AuditGroup("AUDIT_CHANGE_GROUP", "Audit was changed", True),
            _AuditGroup("DATABASE_PRINCIPAL_IMPERSONATION_GROUP", "User was impersonated", True),
            _AuditGroup("DATABASE_PRINCIPAL_CHANGE_GROUP", "User was changed", True),
        ],
    ),
    _AuditCategory(
        label="Data access and manipulation events",
        groups=[
            _AuditGroup("DATABASE_OBJECT_ACCESS_GROUP", "Object was accessed", False),
            _AuditGroup("SCHEMA_OBJECT_ACCESS_GROUP", "Schema object permission was used", False),
            _AuditGroup("BATCH_COMPLETED_GROUP", "Batch was completed", True),
            _AuditGroup("BATCH_STARTED_GROUP", "Batch was started", False),
        ],
    ),
    _AuditCategory(
        label="Administrative and configuration events",
        groups=[
            _AuditGroup("DATABASE_OBJECT_PERMISSION_CHANGE_GROUP", "Object permission was changed", True),
            _AuditGroup("DATABASE_OBJECT_CHANGE_GROUP", "Object was changed", False),
            _AuditGroup("SCHEMA_OBJECT_CHANGE_GROUP", "Schema was changed", False),
            _AuditGroup("SCHEMA_OBJECT_PERMISSION_CHANGE_GROUP", "Schema object permission was checked", True),
            _AuditGroup("DATABASE_OWNERSHIP_CHANGE_GROUP", "Owner changed", True),
            _AuditGroup("DATABASE_OBJECT_OWNERSHIP_CHANGE_GROUP", "Object owner changed", False),
            _AuditGroup("SCHEMA_OBJECT_OWNERSHIP_CHANGE_GROUP", "Schema object permission was changed", False),
        ],
    ),
]

# Pre-computed sets for fast look-up
_ALL_GROUPS: FrozenSet[str] = frozenset(
    g.name for cat in _CATEGORIES for g in cat.groups
)
_RECOMMENDED_GROUPS: FrozenSet[str] = frozenset(
    g.name for cat in _CATEGORIES for g in cat.groups if g.recommended
)
_GROUP_DESC: Dict[str, str] = {
    g.name: g.description for cat in _CATEGORIES for g in cat.groups
}


# ------------------------------------------------------------------
# Check function
# ------------------------------------------------------------------

def check_sql_audit(
    rest_client: FabricRestClient,
    workspace_id: str,
    warehouse_id: str,
    config: SecurityCheckConfig,
    is_sql_endpoint: bool = False,
) -> List[Finding]:
    """Analyse SQL audit settings for a warehouse or SQL endpoint.

    Parameters
    ----------
    rest_client : FabricRestClient
        Authenticated Fabric REST client.
    workspace_id : str
        Target workspace ID.
    warehouse_id : str
        Target warehouse / SQL endpoint item ID (GUID).
    config : SecurityCheckConfig
        Advisor configuration.
    is_sql_endpoint : bool
        If *True*, use the SQL Endpoint audit settings API instead
        of the Warehouse audit settings API.

    Returns
    -------
    list[Finding]
        Findings related to SQL audit configuration.
    """
    findings: List[Finding] = []
    object_name = config.warehouse_name or warehouse_id
    item = config.item_label

    # ── Fetch settings ────────────────────────────────────────────
    try:
        if is_sql_endpoint:
            audit = rest_client.get_sql_endpoint_audit_settings(
                workspace_id, warehouse_id,
            )
        else:
            audit = rest_client.get_sql_audit_settings(
                workspace_id, warehouse_id,
            )
    except FabricRestError as exc:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_SQL_AUDIT,
            check_name="sql_audit_query_failed",
            object_name=object_name,
            message="Unable to retrieve SQL audit settings.",
            detail=f"REST API error: {exc}",
            recommendation=(
                f"Ensure you have Reader or higher permission on the "
                f"{item} item and the token has Item.Read.All scope."
            ),
        ))
        return findings

    state = audit.get("state", "")
    retention_days = audit.get("retentionDays", 0)
    action_groups: Set[str] = set(audit.get("auditActionsAndGroups", []))

    # ── Check: auditing disabled ──────────────────────────────────
    if state != "Enabled":
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_SQL_AUDIT,
            check_name="sql_audit_disabled",
            object_name=object_name,
            message=f"SQL auditing is disabled on this {item}.",
            detail=(
                f"Current state: '{state}'. Without SQL auditing, "
                f"database activity is not logged and security "
                f"incidents cannot be investigated retroactively."
            ),
            recommendation=(
                "Enable SQL auditing via the Fabric portal or the "
                "Update SQL Audit Settings REST API."
            ),
        ))
        return findings

    # ── Check: retention period ───────────────────────────────────
    if 0 < retention_days < config.min_audit_retention_days:
        findings.append(Finding(
            level=LEVEL_MEDIUM,
            category=CATEGORY_SQL_AUDIT,
            check_name="sql_audit_short_retention",
            object_name=object_name,
            message=(
                f"SQL audit retention is {retention_days} day(s) "
                f"(threshold: {config.min_audit_retention_days})."
            ),
            detail=(
                "A short retention period limits the ability to "
                "investigate past security incidents. Many compliance "
                "frameworks require at least 90 days."
            ),
            recommendation=(
                f"Increase the retention period to at least "
                f"{config.min_audit_retention_days} day(s)."
            ),
        ))
    elif retention_days == 0:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_SQL_AUDIT,
            check_name="sql_audit_indefinite_retention",
            object_name=object_name,
            message="SQL audit retention is set to indefinite (0 days).",
            detail=(
                "Audit logs are retained indefinitely. This provides "
                "maximum investigative coverage but may increase "
                "storage costs over time."
            ),
        ))

    # ── Check: category-level coverage ────────────────────────────
    for cat in _CATEGORIES:
        cat_group_names = {g.name for g in cat.groups}
        cat_recommended = {g.name for g in cat.groups if g.recommended}
        enabled_in_cat = action_groups & cat_group_names

        if not enabled_in_cat:
            # Entire category uncovered → HIGH
            # Fallback: if a future category has no recommended flag,
            # show the first 2 groups as a starting suggestion.
            rec_list = ", ".join(
                f"{g.name} ({g.description})"
                for g in cat.groups if g.recommended
            ) or ", ".join(g.name for g in cat.groups[:2])
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_SQL_AUDIT,
                check_name="sql_audit_category_uncovered",
                object_name=object_name,
                message=(
                    f"No audit groups enabled for "
                    f"'{cat.label}'."
                ),
                detail=(
                    f"None of the {len(cat.groups)} groups in this "
                    f"category are active. Events in this category "
                    f"will not be logged."
                ),
                recommendation=(
                    f"Enable at least the recommended groups: {rec_list}."
                ),
            ))
            continue  # skip individual checks — whole category is missing

        # Individual recommended groups missing within partially-covered category
        missing_rec = cat_recommended - action_groups
        if missing_rec:
            missing_detail = ", ".join(
                f"{g} ({_GROUP_DESC.get(g, '')})"
                for g in sorted(missing_rec)
            )
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_SQL_AUDIT,
                check_name="sql_audit_missing_recommended_group",
                object_name=object_name,
                message=(
                    f"{len(missing_rec)} recommended group(s) missing "
                    f"in '{cat.label}'."
                ),
                detail=f"Missing: {missing_detail}.",
                recommendation=(
                    "Add the missing groups via the Fabric portal or "
                    "the Update SQL Audit Settings REST API."
                ),
            ))

    # ── Check: unknown groups (future-proofing) ───────────────────
    unknown_groups = action_groups - _ALL_GROUPS
    if unknown_groups:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_SQL_AUDIT,
            check_name="sql_audit_unknown_groups",
            object_name=object_name,
            message=(
                f"{len(unknown_groups)} unrecognised audit action group(s) "
                f"detected."
            ),
            detail=(
                f"Groups: {', '.join(sorted(unknown_groups))}. These may be "
                f"new groups added after the advisor was built."
            ),
        ))

    # ── INFO: audit healthy ───────────────────────────────────────
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        enabled_count = len(action_groups & _ALL_GROUPS)
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_SQL_AUDIT,
            check_name="sql_audit_healthy",
            object_name=object_name,
            message="SQL audit settings follow best practices.",
            detail=(
                f"State: {state}, retention: {retention_days} day(s), "
                f"{enabled_count}/{len(_ALL_GROUPS)} groups enabled "
                f"(all recommended groups active)."
            ),
        ))

    return findings

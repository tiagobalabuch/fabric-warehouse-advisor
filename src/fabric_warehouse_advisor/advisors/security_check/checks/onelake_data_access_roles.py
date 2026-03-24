"""
Security Check — OneLake Data Access Roles  (SEC-012)
======================================================
Analyses OneLake data access roles via the REST API to detect
misconfigured or overly permissive security postures.

Checks
------
* DefaultReader role still present alongside custom roles (full access leak)
* ReadWrite roles containing RLS/CLS constraints (invalid configuration)
* Roles with wildcard ``*`` path scope (overly permissive)
* Roles with no members (unused)
* Excessive role count (complexity risk)
* Multi-role CLS/RLS conflict potential
* Summary of all data access roles

.. note::

   This check is gated behind ``edition == "LakeWarehouse"`` and
   requires the Preview API:
   ``GET /v1/workspaces/{wid}/items/{iid}/dataAccessRoles``
"""

from __future__ import annotations

from typing import Any, Dict, List, Set

from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_ONELAKE_DATA_ACCESS,
)


# ── Helpers ───────────────────────────────────────────────────────

def _extract_paths(role: Dict[str, Any]) -> List[str]:
    """Extract granted paths from a role's decision rules."""
    paths: List[str] = []
    for rule in role.get("decisionRules", []):
        for perm in rule.get("permission", []):
            if perm.get("attributeName") == "Path":
                paths.extend(perm.get("attributeValueIncludedIn", []))
    return paths


def _extract_actions(role: Dict[str, Any]) -> Set[str]:
    """Extract granted actions (Read, ReadWrite) from a role."""
    actions: Set[str] = set()
    for rule in role.get("decisionRules", []):
        for perm in rule.get("permission", []):
            if perm.get("attributeName") == "Action":
                actions.update(perm.get("attributeValueIncludedIn", []))
    return actions


def _has_constraints(role: Dict[str, Any]) -> bool:
    """Return True if any decision rule contains RLS or CLS constraints."""
    for rule in role.get("decisionRules", []):
        constraints = rule.get("constraints")
        if constraints:
            if constraints.get("columns") or constraints.get("rows"):
                return True
    return False


def _get_rls_tables(role: Dict[str, Any]) -> List[str]:
    """Return table paths with row-level security constraints."""
    tables: List[str] = []
    for rule in role.get("decisionRules", []):
        constraints = rule.get("constraints") or {}
        for row in constraints.get("rows", []):
            tp = row.get("tablePath", "")
            if tp:
                tables.append(tp)
    return tables


def _get_cls_tables(role: Dict[str, Any]) -> Dict[str, List[str]]:
    """Return {tablePath: [columnNames]} with column-level security."""
    result: Dict[str, List[str]] = {}
    for rule in role.get("decisionRules", []):
        constraints = rule.get("constraints") or {}
        for col in constraints.get("columns", []):
            tp = col.get("tablePath", "")
            cols = col.get("columnNames", [])
            if tp and cols:
                result.setdefault(tp, []).extend(cols)
    return result


def _count_members(role: Dict[str, Any]) -> int:
    """Count total members (Entra + Fabric item) in a role."""
    members = role.get("members", {})
    entra = len(members.get("microsoftEntraMembers", []))
    fabric = len(members.get("fabricItemMembers", []))
    return entra + fabric


def _format_member_summary(role: Dict[str, Any]) -> str:
    """Return a human-readable member summary."""
    members = role.get("members", {})
    entra = members.get("microsoftEntraMembers", [])
    fabric = members.get("fabricItemMembers", [])
    parts = []
    if entra:
        types = {}
        for m in entra:
            t = m.get("objectType", "Unknown")
            types[t] = types.get(t, 0) + 1
        parts.append(", ".join(f"{c} {t}" for t, c in types.items()))
    if fabric:
        access_types = set()
        for f in fabric:
            access_types.update(f.get("itemAccess", []))
        parts.append(f"{len(fabric)} Fabric member(s) [{', '.join(sorted(access_types))}]")
    return "; ".join(parts) if parts else "no members"


# ── Main check ────────────────────────────────────────────────────

def check_onelake_data_access_roles(
    roles: List[Dict[str, Any]],
    item_id: str,
    config: SecurityCheckConfig,
    *,
    user_identity_mode: bool = True,
) -> List[Finding]:
    """Analyse OneLake data access roles for a lakehouse item.

    Parameters
    ----------
    roles : list[dict]
        Pre-fetched role definitions from
        ``FabricRestClient.list_data_access_roles()``.
    item_id : str
        Lakehouse item ID (for finding references).
    config : SecurityCheckConfig
        Advisor configuration.
    user_identity_mode : bool
        When *True* (default), findings are reported at normal
        severity.  When *False* (Delegated Identity mode), all
        findings are downgraded to INFO with a note that OneLake
        roles are not enforced at query time.

    Returns
    -------
    list[Finding]
        Findings related to OneLake data access role configuration.
    """
    findings: List[Finding] = []

    _DELEGATED_NOTE = (
        " [Delegated Identity mode — OneLake roles are not enforced "
        "at SQL query time; shown for informational purposes only.]"
    )

    def _level(base: str) -> str:
        return LEVEL_INFO if not user_identity_mode else base

    def _detail_suffix() -> str:
        return _DELEGATED_NOTE if not user_identity_mode else ""

    if not roles:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ONELAKE_DATA_ACCESS,
            check_name="no_onelake_roles",
            object_name=config.warehouse_name,
            message="No OneLake data access roles found.",
            recommendation=(
                "OneLake security may not be enabled on this item. "
                "Without data access roles, users rely on workspace "
                "and item permissions for data access."
            ),
        ))
        return findings

    # ── Summary ──────────────────────────────────────────────────
    role_names = [r.get("name", "<unnamed>") for r in roles]
    findings.append(Finding(
        level=LEVEL_INFO,
        category=CATEGORY_ONELAKE_DATA_ACCESS,
        check_name="onelake_roles_summary",
        object_name=config.warehouse_name,
        message=f"Found {len(roles)} OneLake data access role(s).",
        detail=f"Roles: {', '.join(role_names)}",
    ))

    # Classify roles
    default_reader = None
    default_readwriter = None
    custom_roles: List[Dict[str, Any]] = []

    for role in roles:
        name = role.get("name", "")
        if name == "DefaultReader":
            default_reader = role
        elif name == "DefaultReadWriter":
            default_readwriter = role
        else:
            custom_roles.append(role)

    # ── Check: DefaultReader with custom roles (access leak) ─────
    if (
        config.flag_default_reader_with_custom_roles
        and default_reader
        and custom_roles
    ):
        dr_paths = _extract_paths(default_reader)
        has_wildcard = "*" in dr_paths
        if has_wildcard:
            findings.append(Finding(
                level=_level(LEVEL_HIGH),
                category=CATEGORY_ONELAKE_DATA_ACCESS,
                check_name="default_reader_full_access_with_custom_roles",
                object_name="DefaultReader",
                message=(
                    "DefaultReader role grants full access (all paths) "
                    "while custom roles exist."
                ),
                detail=(
                    f"DefaultReader covers all paths (*). "
                    f"Custom roles: {', '.join(r.get('name', '') for r in custom_roles)}. "
                    f"Users in both DefaultReader and a restrictive "
                    f"custom role retain full access via DefaultReader."
                    + _detail_suffix()
                ),
                recommendation=(
                    "Remove users from the DefaultReader role or "
                    "restrict its path scope. Users in DefaultReader "
                    "bypass any custom role restrictions because "
                    "OneLake security uses a UNION (most-permissive) "
                    "model across roles."
                ),
            ))

    # ── Per-role analysis ────────────────────────────────────────
    for role in roles:
        name = role.get("name", "<unnamed>")
        paths = _extract_paths(role)
        actions = _extract_actions(role)
        has_rls_cls = _has_constraints(role)
        member_count = _count_members(role)
        member_summary = _format_member_summary(role)

        # Check: ReadWrite roles with RLS/CLS (invalid per docs)
        if (
            config.flag_readwrite_with_constraints
            and "ReadWrite" in actions
            and has_rls_cls
        ):
            findings.append(Finding(
                level=_level(LEVEL_CRITICAL),
                category=CATEGORY_ONELAKE_DATA_ACCESS,
                check_name="readwrite_role_with_constraints",
                object_name=name,
                message=(
                    f"Role '{name}' has ReadWrite permission with "
                    f"RLS/CLS constraints — this is invalid."
                ),
                detail=(
                    "OneLake security roles with ReadWrite access "
                    "cannot contain row-level or column-level security "
                    "constraints. This configuration will cause errors."
                    + _detail_suffix()
                ),
                recommendation=(
                    "Remove the RLS/CLS constraints from this role "
                    "or change the permission from ReadWrite to Read."
                ),
            ))

        # Check: Wildcard path (overly permissive)
        if (
            config.flag_wildcard_path_roles
            and "*" in paths
            and name not in ("DefaultReader", "DefaultReadWriter")
        ):
            findings.append(Finding(
                level=_level(LEVEL_MEDIUM),
                category=CATEGORY_ONELAKE_DATA_ACCESS,
                check_name="wildcard_path_custom_role",
                object_name=name,
                message=(
                    f"Custom role '{name}' grants access to all "
                    f"paths (*)."
                ),
                detail=(
                    f"Actions: {', '.join(sorted(actions))}. "
                    f"Members: {member_summary}. "
                    f"Granting wildcard path access in a custom role "
                    f"is equivalent to the DefaultReader behaviour."
                    + _detail_suffix()
                ),
                recommendation=(
                    "Restrict the role's path scope to specific "
                    "tables or folders (e.g. 'Tables/schema1/TableA') "
                    "to enforce least-privilege access."
                ),
            ))

        # Check: Roles with no members
        if config.flag_empty_onelake_roles and member_count == 0:
            findings.append(Finding(
                level=_level(LEVEL_MEDIUM),
                category=CATEGORY_ONELAKE_DATA_ACCESS,
                check_name="empty_onelake_role",
                object_name=name,
                message=(
                    f"OneLake role '{name}' has no members."
                ),
                detail=(
                    "A role without members has no effect. It may "
                    "be an incomplete configuration or leftover from "
                    "a previous setup."
                ),
                recommendation=(
                    "Add Microsoft Entra ID members or Fabric item "
                    "members to the role, or remove it if unused."
                ),
            ))

        # Check: RLS/CLS per-role detail (informational)
        rls_tables = _get_rls_tables(role)
        cls_tables = _get_cls_tables(role)
        if rls_tables or cls_tables:
            constraint_parts = []
            if rls_tables:
                constraint_parts.append(
                    f"RLS on {len(rls_tables)} table(s): "
                    f"{', '.join(rls_tables)}"
                )
            if cls_tables:
                for tp, cols in cls_tables.items():
                    col_display = ", ".join(cols) if cols != ["*"] else "*"
                    constraint_parts.append(
                        f"CLS on {tp}: columns [{col_display}]"
                    )
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_ONELAKE_DATA_ACCESS,
                check_name="onelake_role_constraints",
                object_name=name,
                message=(
                    f"Role '{name}' has data-level constraints."
                ),
                detail="; ".join(constraint_parts),
            ))

    # ── Check: Excessive role count ──────────────────────────────
    if len(roles) > config.max_onelake_roles:
        findings.append(Finding(
            level=_level(LEVEL_LOW),
            category=CATEGORY_ONELAKE_DATA_ACCESS,
            check_name="excessive_onelake_roles",
            object_name=config.warehouse_name,
            message=(
                f"Found {len(roles)} OneLake data access roles "
                f"(threshold: {config.max_onelake_roles})."
            ),
            detail=(
                "A large number of roles increases management "
                "complexity and the risk of conflicting permissions. "
                "Role interactions use a UNION model, which can "
                "inadvertently widen access."
            ),
            recommendation=(
                "Review and consolidate roles where possible. "
                "Consider using Entra ID groups to simplify "
                "membership management."
            ),
        ))

    # ── Check: Multi-role CLS conflict potential ─────────────────
    # When a user is in multiple roles with CLS on the same table
    # but different column sets, access is blocked if columns/rows
    # don't align across queries.
    table_cls_by_role: Dict[str, Dict[str, List[str]]] = {}
    for role in roles:
        name = role.get("name", "<unnamed>")
        cls_map = _get_cls_tables(role)
        if cls_map:
            table_cls_by_role[name] = cls_map

    if len(table_cls_by_role) > 1:
        # Find tables with CLS in multiple roles
        table_to_roles: Dict[str, List[str]] = {}
        for rname, cls_map in table_cls_by_role.items():
            for tp in cls_map:
                table_to_roles.setdefault(tp, []).append(rname)

        for tp, rnames in table_to_roles.items():
            if len(rnames) > 1:
                col_sets = {}
                for rn in rnames:
                    cols = frozenset(table_cls_by_role[rn].get(tp, []))
                    col_sets[rn] = cols

                all_same = len(set(col_sets.values())) == 1
                if not all_same:
                    detail_parts = [
                        f"  {rn}: [{', '.join(sorted(cs))}]"
                        for rn, cs in col_sets.items()
                    ]
                    findings.append(Finding(
                        level=_level(LEVEL_HIGH),
                        category=CATEGORY_ONELAKE_DATA_ACCESS,
                        check_name="multi_role_cls_conflict",
                        object_name=tp,
                        message=(
                            f"Potential CLS conflict on '{tp}' across "
                            f"{len(rnames)} roles."
                        ),
                        detail=(
                            "Different column sets across roles may "
                            "block access if a user is a member of "
                            "multiple roles:\n"
                            + "\n".join(detail_parts)
                            + _detail_suffix()
                        ),
                        recommendation=(
                            "Ensure column-level security definitions "
                            "are aligned across roles that may share "
                            "members, or keep restrictive and "
                            "permissive roles mutually exclusive."
                        ),
                    ))

    return findings

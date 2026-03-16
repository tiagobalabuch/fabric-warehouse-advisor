"""
Security Check — Role Alignment  (SEC-011)
============================================
Cross-references Fabric **workspace roles** (REST API) with
**database principals and role memberships** (T-SQL) to detect
permission misalignment.

Checks
------
* Workspace Viewer who is ``db_owner`` in the database  → HIGH
* Workspace Viewer/none with high-privilege DB role      → MEDIUM
* Database ``db_owner`` member with no workspace role
  (item-sharing path)                                    → MEDIUM
* Healthy alignment                                      → INFO
"""

from __future__ import annotations

from typing import Any, Dict, List, Set

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    CATEGORY_ROLE_ALIGNMENT,
)


# -- SQL ----------------------------------------------------------------

_DB_PRINCIPALS_QUERY = """
    SELECT
        p.name            AS principal_name,
        p.type_desc       AS principal_type,
        r.name            AS role_name
    FROM sys.database_principals AS p
    LEFT JOIN sys.database_role_members AS drm
        ON drm.member_principal_id = p.principal_id
    LEFT JOIN sys.database_principals AS r
        ON drm.role_principal_id = r.principal_id
    WHERE p.type_desc IN ('SQL_USER', 'EXTERNAL_USER', 'EXTERNAL_GROUP')
      -- Exclude built-in SQL Server system principals that exist in
      -- every database and should not be compared against workspace roles.
      AND p.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
"""

# Database roles that grant high-level privileges
_HIGH_PRIV_DB_ROLES: Set[str] = {
    "db_owner", "db_securityadmin", "db_ddladmin", "db_accessadmin",
}

# Workspace roles that already imply full write / admin access
_ELEVATED_WS_ROLES: Set[str] = {"Admin", "Member", "Contributor"}


# -- Public API ---------------------------------------------------------

def check_role_alignment(
    spark: SparkSession,
    warehouse: str,
    config: SecurityCheckConfig,
    workspace_role_assignments: List[Dict[str, Any]] | None = None,
) -> List[Finding]:
    """Cross-reference workspace roles with database role memberships.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    warehouse : str
        Warehouse name (for ``synapsesql``).
    config : SecurityCheckConfig
        Advisor configuration.
    workspace_role_assignments : list[dict] | None
        Raw workspace role assignment list from the REST API
        (output of ``get_workspace_role_assignments``).
        If ``None`` or empty the check runs only T-SQL side
        analysis.

    Returns
    -------
    list[Finding]
        Findings related to role mis-alignment.
    """
    findings: List[Finding] = []

    # ── 1. Fetch database principals + role memberships ──────────
    try:
        df = read_warehouse_query(
            spark, warehouse, _DB_PRINCIPALS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        db_rows = df.collect()
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_ROLE_ALIGNMENT,
            check_name="role_alignment_query_failed",
            object_name=warehouse,
            message="Unable to query database principals for alignment.",
            detail=str(exc),
            recommendation="Verify connectivity and permissions.",
        ))
        return findings

    if not db_rows and not workspace_role_assignments:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ROLE_ALIGNMENT,
            check_name="role_alignment_no_data",
            object_name=warehouse,
            message=(
                "No database principals or workspace roles to compare."
            ),
        ))
        return findings

    # Build lookup: principal_name_lower → set of DB role names
    db_user_roles: Dict[str, Set[str]] = {}
    for row in db_rows:
        name = (row["principal_name"] or "").strip()
        role = (row["role_name"] or "").strip()
        key = name.lower()
        db_user_roles.setdefault(key, set())
        if role:
            db_user_roles[key].add(role)

    # ── 2. Build workspace role lookup ───────────────────────────
    # ws_lookup: principal_display_name_lower → workspace_role
    ws_lookup: Dict[str, str] = {}
    if workspace_role_assignments:
        for assignment in workspace_role_assignments:
            principal = assignment.get("principal", {})
            display_name = (
                principal.get("displayName", "")
            ).strip().lower()
            upn = (
                principal.get("userDetails", {}).get(
                    "userPrincipalName", ""
                )
            ).strip().lower()
            role = assignment.get("role", "")
            if display_name:
                ws_lookup[display_name] = role
            if upn:
                ws_lookup[upn] = role

    # ── 3. Cross-reference ───────────────────────────────────────
    checked: Set[str] = set()

    for name_lower, db_roles in db_user_roles.items():
        checked.add(name_lower)
        ws_role = ws_lookup.get(name_lower, "")
        high_db_roles = db_roles & _HIGH_PRIV_DB_ROLES

        if not high_db_roles:
            continue

        roles_str = ", ".join(sorted(high_db_roles))

        # Viewer with high-privilege DB role → dangerous
        if ws_role == "Viewer" and "db_owner" in high_db_roles:
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_ROLE_ALIGNMENT,
                check_name="viewer_with_db_owner",
                object_name=name_lower,
                message=(
                    f"Workspace Viewer [{name_lower}] is a member "
                    f"of db_owner in the database."
                ),
                detail=(
                    f"Workspace role: Viewer. "
                    f"DB roles: {roles_str}. "
                    f"This grants full database control to a "
                    f"user who should have read-only workspace access."
                ),
                recommendation=(
                    "Demote the user from db_owner or upgrade their "
                    "workspace role to match the intended access level."
                ),
                sql_fix=(
                    f"ALTER ROLE db_owner DROP MEMBER [{name_lower}];"
                ),
            ))
        elif ws_role == "Viewer" and high_db_roles:
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_ROLE_ALIGNMENT,
                check_name="viewer_with_high_priv_role",
                object_name=name_lower,
                message=(
                    f"Workspace Viewer [{name_lower}] has "
                    f"high-privilege database role(s)."
                ),
                detail=(
                    f"Workspace role: Viewer. "
                    f"DB roles: {roles_str}. "
                    f"These database roles grant elevated privileges "
                    f"beyond the Viewer workspace role."
                ),
                recommendation=(
                    "Review the database role membership and reduce "
                    "privileges to match the workspace Viewer role."
                ),
            ))
        # No workspace role but high-privilege DB roles (item-sharing path)
        elif not ws_role and high_db_roles:
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_ROLE_ALIGNMENT,
                check_name="no_workspace_role_high_db_priv",
                object_name=name_lower,
                message=(
                    f"[{name_lower}] has high-privilege database "
                    f"role(s) with no workspace role."
                ),
                detail=(
                    f"DB roles: {roles_str}. "
                    f"This user likely accesses the {config.item_label} via "
                    f"item sharing but holds elevated database "
                    f"privileges."
                ),
                recommendation=(
                    "Verify this user's access path. If they use "
                    "item sharing, review whether high-privilege "
                    "database roles are appropriate."
                ),
            ))

    # ── 4. Summary ───────────────────────────────────────────────
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ROLE_ALIGNMENT,
            check_name="role_alignment_healthy",
            object_name=warehouse,
            message=(
                "Workspace roles and database roles are properly "
                "aligned."
            ),
            detail=(
                f"Checked {len(db_user_roles)} database principal(s) "
                f"against {len(ws_lookup)} workspace assignment(s)."
            ),
        ))
    else:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ROLE_ALIGNMENT,
            check_name="role_alignment_summary",
            object_name=warehouse,
            message="Role alignment analysis complete.",
            detail=(
                f"Checked {len(db_user_roles)} database principal(s) "
                f"against {len(ws_lookup)} workspace assignment(s). "
                f"{len(actionable)} issue(s) found."
            ),
        ))

    return findings

"""
Security Check — Custom Roles  (SEC-002)
==========================================
Analyses ``sys.database_principals`` and ``sys.database_role_members``
to detect role hygiene issues.

Checks
------
* Excessive members in the ``db_owner`` fixed role
* Custom roles with zero members (unused)
* Database users who are not a member of any custom role
"""

from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    LEVEL_CRITICAL,
    CATEGORY_ROLES,
)


# -- SQL ----------------------------------------------------------------

_ROLE_MEMBERS_QUERY = """
    SELECT
        r.name             AS role_name,
        r.type_desc        AS role_type,
        m.name             AS member_name,
        m.type_desc        AS member_type
    FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals AS r
        ON drm.role_principal_id = r.principal_id
    INNER JOIN sys.database_principals AS m
        ON drm.member_principal_id = m.principal_id
"""

_ALL_ROLES_QUERY = """
    SELECT
        principal_id,
        name,
        type_desc
    FROM sys.database_principals
    WHERE type_desc = 'DATABASE_ROLE'
      AND is_fixed_role = 0
      AND name NOT IN ('public')
"""

_ALL_USERS_QUERY = """
    SELECT
        p.principal_id,
        p.name,
        p.type_desc
    FROM sys.database_principals AS p
    WHERE p.type_desc IN ('SQL_USER', 'EXTERNAL_USER')
      AND p.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
"""


# -- Public API ---------------------------------------------------------

def check_custom_roles(
    spark: SparkSession,
    warehouse: str,
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Analyse custom database roles and membership.

    Returns
    -------
    list[Finding]
        Findings related to role configuration.
    """
    findings: List[Finding] = []

    try:
        members_df = read_warehouse_query(
            spark, warehouse, _ROLE_MEMBERS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        members_rows = members_df.collect()

        roles_df = read_warehouse_query(
            spark, warehouse, _ALL_ROLES_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        custom_roles = roles_df.collect()

        users_df = read_warehouse_query(
            spark, warehouse, _ALL_USERS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        all_users = users_df.collect()
    except Exception:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_ROLES,
            check_name="roles_query_failed",
            object_name=warehouse,
            message="Unable to query role membership views.",
            detail="The current user may lack VIEW DEFINITION permission.",
            recommendation="Ensure the executing identity has VIEW DEFINITION on the warehouse.",
        ))
        return findings

    # Build lookup structures
    role_member_map: dict[str, list[str]] = {}
    for r in members_rows:
        role_member_map.setdefault(r["role_name"], []).append(r["member_name"])

    users_with_roles = set()
    for r in members_rows:
        if r["member_type"] in ("SQL_USER", "EXTERNAL_USER"):
            users_with_roles.add(r["member_name"])

    # --- Check: db_owner membership ---
    db_owner_members = role_member_map.get("db_owner", [])
    if len(db_owner_members) > config.max_db_owner_members:
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_ROLES,
            check_name="excessive_db_owner_members",
            object_name=warehouse,
            message=f"db_owner has {len(db_owner_members)} members (threshold: {config.max_db_owner_members}).",
            detail=(
                f"Members: {', '.join(db_owner_members)}. "
                f"db_owner bypasses all permission checks and can perform "
                f"any action in the database."
            ),
            recommendation=(
                "Review db_owner membership and remove users who do not "
                "require full administrative access. Use custom roles with "
                "least-privilege grants instead."
            ),
        ))
    elif db_owner_members:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ROLES,
            check_name="db_owner_membership_ok",
            object_name=warehouse,
            message=f"db_owner has {len(db_owner_members)} member(s) — within threshold.",
            detail=f"Members: {', '.join(db_owner_members)}.",
        ))

    # --- Check: empty custom roles ---
    if config.flag_empty_roles:
        for role_row in custom_roles:
            role_name = role_row["name"]
            members = role_member_map.get(role_name, [])
            if not members:
                findings.append(Finding(
                    level=LEVEL_LOW,
                    category=CATEGORY_ROLES,
                    check_name="empty_custom_role",
                    object_name=f"[{role_name}]",
                    message=f"Custom role [{role_name}] has no members.",
                    detail="Unused roles add clutter and may indicate incomplete provisioning.",
                    recommendation=(
                        f"Add members to [{role_name}] or drop it if it is no longer needed."
                    ),
                    sql_fix=f"DROP ROLE [{role_name}];",
                ))

    # --- Check: users without any custom role ---
    if config.flag_users_without_roles:
        for user_row in all_users:
            user_name = user_row["name"]
            if user_name not in users_with_roles:
                findings.append(Finding(
                    level=LEVEL_MEDIUM,
                    category=CATEGORY_ROLES,
                    check_name="user_without_role",
                    object_name=f"[{user_name}]",
                    message=f"User [{user_name}] is not a member of any custom role.",
                    detail=(
                        "Users who are not assigned to roles may rely on "
                        "direct grants or inherit via the public role, "
                        "making access harder to audit."
                    ),
                    recommendation=(
                        f"Assign [{user_name}] to an appropriate custom role "
                        f"that follows the principle of least privilege."
                    ),
                ))

    # --- INFO summary if no issues ---
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_ROLES,
            check_name="roles_healthy",
            object_name=warehouse,
            message="Custom role configuration follows best practices.",
            detail=(
                f"Analysed {len(custom_roles)} custom role(s) and "
                f"{len(all_users)} database user(s)."
            ),
        ))

    return findings

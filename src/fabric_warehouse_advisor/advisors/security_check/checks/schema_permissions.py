"""
Security Check — Schema Permissions  (SEC-001)
=================================================
Analyses ``sys.database_permissions``, ``sys.database_principals``,
and ``sys.database_role_members`` to detect overly broad or risky
permission grants — both explicit and implicit (role-inherited).

Checks
------
* Permissions granted directly to the ``public`` role
* Permissions granted directly to individual users (not via roles)
* Schema-wide grants (``GRANT … ON SCHEMA::`` with broad privileges)
* Implicit high-privilege role membership (``db_owner``, etc.)
* ``GRANT_WITH_GRANT_OPTION`` — delegable permissions
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
    CATEGORY_PERMISSIONS,
)


# -- SQL ----------------------------------------------------------------

_SCHEMA_PERMISSIONS_QUERY = """
    SELECT
        DatabasePrincipal,
        PermissionType,
        PermissionDerivedFrom,
        PrincipalType,
        Authentication,
        Action,
        Permission,
        ObjectType,
        Securable,
        COALESCE(STRING_AGG(ColumnName, ', '), NULL) AS ColumnName
    FROM (
        SELECT
            dbpr.name                         AS DatabasePrincipal,
            CAST('<explicit>' AS VARCHAR(20)) AS PermissionType,
            CAST(NULL AS VARCHAR(128))        AS PermissionDerivedFrom,
            dbpr.type_desc                    AS PrincipalType,
            dbpr.authentication_type_desc     AS Authentication,
            perm.state_desc                   AS Action,
            perm.permission_name              AS Permission,
            obj.type_desc                     AS ObjectType,
            CASE perm.class
                WHEN 0 THEN 'Database::' + DB_NAME()
                WHEN 1 THEN 'Object::' + SCHEMA_NAME(obj.schema_id) + '.'
                            + COALESCE(OBJECT_NAME(perm.major_id), '<UNKNOWN OBJECT>')
                WHEN 3 THEN 'Schema::' + COALESCE(SCHEMA_NAME(perm.major_id), '<UNKNOWN SCHEMA>')
            END                               AS Securable,
            CASE
                WHEN perm.class = 1 AND obj.type = 'U'
                    THEN COALESCE(col.name, 'ALL COLUMNS')
                ELSE NULL
            END                               AS ColumnName
        FROM sys.database_permissions   AS perm
        INNER JOIN sys.database_principals AS dbpr
            ON perm.grantee_principal_id = dbpr.principal_id
        LEFT JOIN sys.objects           AS obj
            ON perm.major_id = obj.object_id AND obj.is_ms_shipped = 0
        LEFT JOIN sys.columns           AS col
            ON perm.major_id = col.object_id AND perm.minor_id = col.column_id

        UNION ALL

        SELECT
            princ_mem.name                    AS DatabasePrincipal,
            CAST('<implicit>' AS VARCHAR(20)) AS PermissionType,
            princ_role.name                   AS PermissionDerivedFrom,
            princ_mem.type_desc               AS PrincipalType,
            princ_mem.authentication_type_desc AS Authentication,
            CASE
                WHEN princ_role.name IN (
                    'db_owner', 'db_ddladmin', 'db_datareader',
                    'db_datawriter', 'db_securityadmin', 'db_accessadmin',
                    'db_backupoperator', 'db_denydatawriter', 'db_denydatareader'
                ) THEN 'IMPLICIT - FDR'
                ELSE pe.state_desc
            END                               AS Action,
            CASE princ_role.name
                WHEN 'db_owner'          THEN 'CONTROL'
                WHEN 'db_ddladmin'       THEN 'CREATE, DROP, ALTER ON ANY OBJECTS'
                WHEN 'db_datareader'     THEN 'SELECT'
                WHEN 'db_datawriter'     THEN 'INSERT, UPDATE, DELETE'
                WHEN 'db_securityadmin'  THEN 'Manage Role Membership and Permissions'
                WHEN 'db_accessadmin'    THEN 'GRANT/REVOKE access to users/roles'
                WHEN 'db_backupoperator' THEN 'Can BACKUP DATABASE'
                WHEN 'db_denydatawriter' THEN 'DENY INSERT, UPDATE, DELETE'
                WHEN 'db_denydatareader' THEN 'DENY SELECT'
                ELSE pe.permission_name
            END                               AS Permission,
            obj.type_desc                     AS ObjectType,
            COALESCE(
                CASE pe.class
                    WHEN 0 THEN 'Database::' + DB_NAME()
                    WHEN 1 THEN 'Object::' + SCHEMA_NAME(obj.schema_id) + '.'
                                + COALESCE(OBJECT_NAME(pe.major_id), '<UNKNOWN_OBJECT>')
                    WHEN 3 THEN 'Schema::' + COALESCE(SCHEMA_NAME(pe.major_id), '<UNKNOWN_SCHEMA>')
                END,
                'Database::' + DB_NAME()
            )                                 AS Securable,
            CASE
                WHEN pe.class = 1 AND obj.type = 'U'
                    THEN COALESCE(col.name, 'ALL COLUMNS')
                ELSE NULL
            END                               AS ColumnName
        FROM sys.database_role_members  AS dbrm
        RIGHT OUTER JOIN sys.database_principals AS princ_role
            ON dbrm.role_principal_id = princ_role.principal_id
        LEFT OUTER JOIN sys.database_principals  AS princ_mem
            ON dbrm.member_principal_id = princ_mem.principal_id
        LEFT JOIN sys.database_permissions AS pe
            ON pe.grantee_principal_id = princ_role.principal_id
        LEFT JOIN sys.objects           AS obj
            ON pe.major_id = obj.object_id AND obj.is_ms_shipped = 0
        LEFT JOIN sys.columns           AS col
            ON pe.major_id = col.object_id AND pe.minor_id = col.column_id
        WHERE princ_role.type = 'R'
          AND princ_mem.name IS NOT NULL
    ) AS CombinedPermissions
    GROUP BY
        DatabasePrincipal, PermissionType, PermissionDerivedFrom,
        PrincipalType, Authentication, Action, Permission,
        ObjectType, Securable
"""

# Fixed database roles considered high-privilege
_HIGH_PRIV_ROLES = {
    "db_owner", "db_securityadmin", "db_ddladmin", "db_accessadmin",
}


# -- Helpers ------------------------------------------------------------

def _parse_securable(securable: str | None) -> str:
    """Extract a readable name from a 'Type::Name' securable string."""
    if not securable:
        return "(unknown)"
    # e.g. "Schema::dbo" -> "dbo", "Database::TPCH10" -> "TPCH10"
    parts = securable.split("::", 1)
    return parts[1] if len(parts) == 2 else securable


def _securable_type(securable: str | None) -> str:
    """Return the type prefix (Database, Schema, Object) or empty."""
    if not securable:
        return ""
    return securable.split("::", 1)[0] if "::" in securable else ""


def _build_revoke_sql(action: str, permission: str, securable: str | None,
                      principal: str) -> str:
    """Build a REVOKE statement from the resolved securable."""
    if not securable or "::" not in securable:
        return ""
    sec_type = _securable_type(securable)
    sec_name = _parse_securable(securable)
    if sec_type == "Schema":
        return f"REVOKE {permission} ON SCHEMA::[{sec_name}] FROM [{principal}];"
    if sec_type == "Object":
        return f"REVOKE {permission} ON [{sec_name}] FROM [{principal}];"
    if sec_type == "Database":
        return f"REVOKE {permission} FROM [{principal}];"
    return ""


# -- Public API ---------------------------------------------------------

def check_schema_permissions(
    spark: SparkSession,
    warehouse: str,
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Analyse schema-level permission grants (explicit and implicit).

    Returns
    -------
    list[Finding]
        Findings related to permission grants.
    """
    findings: List[Finding] = []

    try:
        df = read_warehouse_query(
            spark, warehouse, _SCHEMA_PERMISSIONS_QUERY,
            config.workspace_id, config.warehouse_id,
        )
        rows = df.collect()
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_PERMISSIONS,
            check_name="permissions_query_failed",
            object_name=warehouse,
            message="Unable to query sys.database_permissions.",
            detail=f"Query error: {exc}",
            recommendation="Ensure the executing identity has VIEW DEFINITION on the warehouse.",
        ))
        return findings

    if not rows:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_PERMISSIONS,
            check_name="no_permissions_found",
            object_name=warehouse,
            message="No explicit or implicit permission grants found.",
            detail="All access is controlled through Fabric workspace roles alone.",
        ))
        return findings

    explicit = [r for r in rows if r["PermissionType"] == "<explicit>"]
    implicit = [r for r in rows if r["PermissionType"] == "<implicit>"]

    # --- Check: public role grants (explicit) ---
    if config.flag_public_role_grants:
        public_grants = [
            r for r in explicit
            if r["DatabasePrincipal"] == "public"
            and r["Securable"] is not None
        ]
        for r in public_grants:
            perm = r["Permission"]
            securable = r["Securable"]
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_PERMISSIONS,
                check_name="public_role_grant",
                object_name=securable,
                message=f"{perm} granted to the public role on {securable}.",
                detail=(
                    f"Permission {r['Action']} {perm} on {securable} "
                    f"is granted to the public role — every database user inherits it."
                ),
                recommendation=(
                    "Revoke the grant from public and assign it to a "
                    "specific custom role instead."
                ),
                sql_fix=_build_revoke_sql(r["Action"], perm, securable, "public"),
            ))

    # --- Check: direct user grants (explicit, excluding CONNECT) ---
    if config.flag_direct_user_grants:
        direct_user_grants = [
            r for r in explicit
            if r["PrincipalType"] in ("SQL_USER", "EXTERNAL_USER")
            and r["DatabasePrincipal"] != "dbo"
            and r["Permission"] != "CONNECT"  # CONNECT is universal; handled separately
        ]
        for r in direct_user_grants:
            perm = r["Permission"]
            principal = r["DatabasePrincipal"]
            securable = r["Securable"] or "(unknown)"
            col_info = r["ColumnName"]
            detail_suffix = f" (columns: {col_info})" if col_info else ""
            findings.append(Finding(
                level=LEVEL_MEDIUM,
                category=CATEGORY_PERMISSIONS,
                check_name="direct_user_grant",
                object_name=securable,
                message=f"{perm} granted directly to user [{principal}] on {securable}.",
                detail=(
                    f"Granting permissions directly to individual users is harder "
                    f"to audit and manage at scale than role-based grants."
                    f"{detail_suffix}"
                ),
                recommendation=(
                    f"Create a custom role, grant {perm} to the role, "
                    f"and add [{principal}] as a member."
                ),
            ))

    # --- Check: schema-wide grants (explicit) ---
    if config.flag_schema_wide_grants:
        broad_perms = {"CONTROL", "ALTER", "TAKE OWNERSHIP"}
        schema_wide = [
            r for r in explicit
            if _securable_type(r["Securable"]) == "Schema"
            and r["Permission"] in broad_perms
            and r["DatabasePrincipal"] != "dbo"
        ]
        for r in schema_wide:
            perm = r["Permission"]
            principal = r["DatabasePrincipal"]
            securable = r["Securable"]
            schema_name = _parse_securable(securable)
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_PERMISSIONS,
                check_name="schema_wide_grant",
                object_name=securable,
                message=f"Broad {perm} on schema [{schema_name}] granted to [{principal}].",
                detail=(
                    f"{perm} on an entire schema gives the grantee full control "
                    f"over all current and future objects in that schema."
                ),
                recommendation=(
                    "Scope the grant to specific objects or use a less "
                    "privileged permission (e.g. SELECT, INSERT)."
                ),
                sql_fix=_build_revoke_sql(r["Action"], perm, securable, principal),
            ))

    # --- Check: implicit high-privilege role membership ---
    high_priv = [
        r for r in implicit
        if r["PermissionDerivedFrom"] in _HIGH_PRIV_ROLES
        and r["DatabasePrincipal"] != "dbo"
    ]
    # Deduplicate: one finding per (principal, role) pair
    seen_role_members: set = set()
    for r in high_priv:
        principal = r["DatabasePrincipal"]
        role = r["PermissionDerivedFrom"]
        key = (principal, role)
        if key in seen_role_members:
            continue
        seen_role_members.add(key)

        level = LEVEL_CRITICAL if role == "db_owner" else LEVEL_HIGH
        findings.append(Finding(
            level=level,
            category=CATEGORY_PERMISSIONS,
            check_name="high_privilege_role_member",
            object_name=f"[{role}]",
            message=f"[{principal}] is a member of high-privilege role [{role}].",
            detail=(
                f"User [{principal}] ({r['PrincipalType']}, {r['Authentication']}) "
                f"inherits {r['Permission']} through membership in [{role}]."
            ),
            recommendation=(
                f"Review whether [{principal}] requires [{role}] membership. "
                f"Prefer a custom role with only the specific permissions needed."
            ),
            sql_fix=f"ALTER ROLE [{role}] DROP MEMBER [{principal}];",
        ))

    # --- Check: GRANT_WITH_GRANT_OPTION (delegable permissions) ---
    delegable = [
        r for r in explicit
        if r["Action"] == "GRANT_WITH_GRANT_OPTION"
        and r["DatabasePrincipal"] != "dbo"
    ]
    for r in delegable:
        perm = r["Permission"]
        principal = r["DatabasePrincipal"]
        securable = r["Securable"] or "(unknown)"
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_PERMISSIONS,
            check_name="grant_with_grant_option",
                object_name=securable,
            message=(
                f"[{principal}] has {perm} WITH GRANT OPTION on {securable}."
            ),
            detail=(
                f"GRANT OPTION allows [{principal}] to delegate {perm} to "
                f"other principals, expanding the permission surface "
                f"without administrator intervention."
            ),
            recommendation=(
                f"Remove GRANT OPTION unless [{principal}] explicitly needs "
                f"to delegate this permission."
            ),
            sql_fix=(
                f"REVOKE GRANT OPTION FOR {perm} ON "
                f"{_securable_type(r['Securable'])}::"
                f"[{_parse_securable(r['Securable'])}] FROM [{principal}];"
                if r["Securable"] and "::" in (r["Securable"] or "")
                else ""
            ),
        ))

    # --- Check: connect-only users (potentially stale accounts) ---
    if config.flag_direct_user_grants:
        # Build set of permissions per user from explicit grants
        user_explicit_perms: dict[str, set[str]] = {}
        for r in explicit:
            if (r["PrincipalType"] in ("SQL_USER", "EXTERNAL_USER")
                    and r["DatabasePrincipal"] != "dbo"):
                user_explicit_perms.setdefault(r["DatabasePrincipal"], set()).add(
                    r["Permission"]
                )
        # Users who appear in any implicit (role-membership) row
        users_with_roles = {
            r["DatabasePrincipal"] for r in implicit
            if r["DatabasePrincipal"] != "dbo"
        }
        for principal, perms in user_explicit_perms.items():
            if perms == {"CONNECT"} and principal not in users_with_roles:
                findings.append(Finding(
                    level=LEVEL_LOW,
                    category=CATEGORY_PERMISSIONS,
                    check_name="connect_only_user",
                    object_name=f"Database::{warehouse}",
                    message=(
                        f"[{principal}] has only CONNECT with no other "
                        f"grants or role memberships."
                    ),
                    detail=(
                        f"User [{principal}] holds only the CONNECT permission "
                        f"and has no role memberships or additional grants. "
                        f"This may indicate a stale or incorrectly provisioned account."
                    ),
                    recommendation=(
                        f"Review whether [{principal}] still needs access. "
                        f"If not, revoke CONNECT and remove the user."
                    ),
                    sql_fix=f"DROP USER [{principal}];",
                ))

    # --- INFO summary if no issues ---
    actionable = [f for f in findings if f.is_actionable]
    if not actionable:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_PERMISSIONS,
            check_name="permissions_healthy",
            object_name=warehouse,
            message="Schema permission grants follow best practices.",
            detail=(
                f"Analysed {len(explicit)} explicit and "
                f"{len(implicit)} implicit permission grant(s)."
            ),
        ))

    return findings

"""
Fabric Warehouse Advisor — Security Check Configuration
==========================================================
All configuration values are exposed as fields of the
``SecurityCheckConfig`` dataclass with sensible defaults.

Users create a config instance, override what they need, and pass
it to :class:`SecurityCheckAdvisor`.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SecurityCheckConfig:
    """Configuration for the Security Check Advisor.

    Parameters
    ----------
    warehouse_name : str
        The Fabric Warehouse or Lakehouse SQL Endpoint name.
        **Required** — there is no valid default.

    workspace_id : str
        Optional.  The Fabric Workspace ID (GUID) for cross-workspace
        access.  When empty the advisor assumes same-workspace.

    warehouse_id : str
        Optional.  The Fabric Warehouse item ID (GUID).
        For cross-workspace access together with ``workspace_id``.

    check_schema_permissions : bool
        Enable the schema-level permissions check (SEC-001).

    check_custom_roles : bool
        Enable the custom database roles check (SEC-002).

    check_rls : bool
        Enable the Row-Level Security check (SEC-003).

    check_cls : bool
        Enable the Column-Level Security check (SEC-004).

    check_ddm : bool
        Enable the Dynamic Data Masking check (SEC-005).

    flag_public_role_grants : bool
        Flag permissions granted directly to the ``public`` role.

    flag_direct_user_grants : bool
        Flag permissions granted directly to individual users rather
        than through roles.

    flag_schema_wide_grants : bool
        Flag overly broad schema-wide ``GRANT`` statements.

    max_db_owner_members : int
        Maximum number of members in ``db_owner`` before flagging.

    flag_empty_roles : bool
        Flag custom roles that have zero members.

    flag_users_without_roles : bool
        Flag database users who are not a member of any custom role.

    sensitive_column_patterns : list[str]
        SQL ``LIKE`` patterns for column names that should have CLS
        (e.g. ``"%ssn%"``, ``"%salary%"``).

    max_unmask_principals : int
        Maximum number of principals with ``UNMASK`` permission before
        flagging excessive unmask grants.

    flag_weak_masking : bool
        Flag ``default()`` masking on short string columns where the
        mask may be trivially reversible.

    table_names : list[str]
        Optional list of tables to restrict analysis to.  Each entry
        can be ``"table_name"`` (any schema) or ``"schema.table_name"``.
        Empty means all tables.

    verbose : bool
        If ``True``, print intermediate details for debugging.

    phase_delay : float
        Seconds to pause between phases to reduce HTTP 429 throttling
        from the Fabric control-plane API.  Set to ``0`` to disable.
    """

    # -- Connection --
    warehouse_name: str = ""
    workspace_id: str = ""
    warehouse_id: str = ""

    # -- REST API auth --
    fabric_token: str = ""
    use_notebook_token: bool = True

    # -- Toggle check categories (T-SQL) --
    check_schema_permissions: bool = True
    check_custom_roles: bool = True
    check_rls: bool = True
    check_cls: bool = True
    check_ddm: bool = True

    # -- Toggle check categories (REST API) --
    check_workspace_roles: bool = True
    check_network_isolation: bool = True
    check_sql_audit: bool = True

    # -- SEC-001: Schema Permissions thresholds --
    flag_public_role_grants: bool = True
    flag_direct_user_grants: bool = True
    flag_schema_wide_grants: bool = True

    # -- SEC-002: Custom Roles thresholds --
    max_db_owner_members: int = 2
    flag_empty_roles: bool = True
    flag_users_without_roles: bool = True

    # -- SEC-004: CLS thresholds --
    sensitive_column_patterns: list[str] = field(default_factory=lambda: [
        "%ssn%", "%social_security%",
        "%salary%", "%compensation%",
        "%credit_card%", "%card_number%",
        "%password%", "%secret%",
        "%date_of_birth%", "%dob%",
    ])

    # -- SEC-005: DDM thresholds --
    max_unmask_principals: int = 3
    flag_weak_masking: bool = True

    # -- SEC-006: Workspace Roles thresholds --
    max_workspace_admins: int = 3

    # -- SEC-008: SQL Audit Settings thresholds --
    min_audit_retention_days: int = 90

    # -- SEC-009: Item Permissions thresholds --
    check_item_permissions: bool = True
    max_item_readdata_principals: int = 10

    # -- SEC-010: Sensitivity Labels --
    check_sensitivity_labels: bool = True

    # -- SEC-011: Role Alignment --
    check_role_alignment: bool = True

    # -- Scope filtering --
    table_names: list[str] = field(default_factory=list)

    # -- Output --
    verbose: bool = False

    # -- Throttle protection --
    phase_delay: float = 1.0

    def validate(self) -> None:
        """Raise ``ValueError`` if the configuration is not usable."""
        if not self.warehouse_name or self.warehouse_name == "<your_warehouse_name>":
            raise ValueError(
                "warehouse_name must be set to your actual Fabric "
                "Warehouse or Lakehouse SQL Endpoint name."
            )

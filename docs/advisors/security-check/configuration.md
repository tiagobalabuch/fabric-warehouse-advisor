# Configuration Reference

All parameters are fields of the `SecurityCheckConfig` dataclass.
Create an instance, override the defaults you need, and pass it to
`SecurityCheckAdvisor`.

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    check_rls=True,
    check_ddm=True,
    max_db_owner_members=3,
    verbose=True,
)

advisor = SecurityCheckAdvisor(spark, config)
result = advisor.run()
```

## Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `warehouse_name` | `str` | `""` | **Required.** The Fabric Warehouse or Lakehouse SQL Endpoint name. |
| `workspace_id` | `str` | `""` | Workspace GUID. Only needed for [cross-workspace](../../cross-workspace.md) access. Auto-detected when running inside a Fabric notebook. |
| `warehouse_id` | `str` | `""` | Warehouse item GUID. Only needed for [cross-workspace](../../cross-workspace.md) access. |
| `sql_endpoint_id` | `str` | `""` | SQL Analytics Endpoint item GUID. Used when connecting to a Lakehouse SQL Endpoint instead of a Data Warehouse. If `warehouse_id` is empty and this is set, it is used as the warehouse identifier for cross-workspace connections. |

## Scope Filtering

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schema_names` | `list[str]` | `[]` | Restrict analysis to specific schemas. Empty = all user schemas. |
| `table_names` | `list[str]` | `[]` | Restrict RLS, CLS, and DDM analysis to specific tables. Each entry can be `"table_name"` (any schema) or `"schema.table_name"`. Empty = all tables. |

Examples:

```python
# Only check tables in the 'sales' schema
config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    schema_names=["sales"],
)

# Only check specific tables for RLS / CLS coverage
config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    table_names=["dbo.FactSales", "dbo.DimCustomer"],
)
```

## Check Category Toggles

Each check category can be independently enabled or disabled:

### SQL Checks

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `check_schema_permissions` | `bool` | `True` | Enable the schema-level permissions check. |
| `check_custom_roles` | `bool` | `True` | Enable the custom database roles check. |
| `check_rls` | `bool` | `True` | Enable the Row-Level Security check. |
| `check_cls` | `bool` | `True` | Enable the Column-Level Security check. |
| `check_ddm` | `bool` | `True` | Enable the Dynamic Data Masking check. |

### Workspace & Item Level Checks

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `check_workspace_roles` | `bool` | `True` | Enable the workspace roles check. |
| `check_network_isolation` | `bool` | `True` | Enable the network isolation check. |
| `check_sql_audit` | `bool` | `True` | Enable the SQL audit settings check. |
| `check_item_permissions` | `bool` | `True` | Enable the item permissions check. |
| `check_sensitivity_labels` | `bool` | `True` | Enable the sensitivity labels check. |
| `check_role_alignment` | `bool` | `True` | Enable the role alignment cross-reference check. |
| `check_auth_mode` | `bool` | `True` | Enable auth mode detection for Lakehouse SQL Endpoints. |

### OneLake Security Checks

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `check_onelake_data_access_roles` | `bool` | `True` | Enable the OneLake data access roles check. Lakehouse SQL Endpoints only. |
| `check_onelake_settings` | `bool` | `True` | Enable the OneLake settings check. |
| `check_onelake_security_sync` | `bool` | `True` | Enable the security sync health check. Lakehouse SQL Endpoints only. |

Example — run only the RLS and CLS checks:

```python
config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    check_schema_permissions=False,
    check_custom_roles=False,
    check_rls=True,
    check_cls=True,
    check_ddm=False,
    # Disable REST checks
    check_workspace_roles=False,
    check_network_isolation=False,
    check_sql_audit=False,
    check_item_permissions=False,
    check_sensitivity_labels=False,
    check_role_alignment=False,
    check_onelake_data_access_roles=False,
    check_onelake_settings=False,
    check_onelake_security_sync=False,
)
```

## Schema Permissions Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flag_public_role_grants` | `bool` | `True` | Flag permissions granted directly to the `public` role. |
| `flag_direct_user_grants` | `bool` | `True` | Flag permissions granted directly to individual users rather than through roles. |
| `flag_schema_wide_grants` | `bool` | `True` | Flag overly broad schema-wide `GRANT` statements (`CONTROL`, `ALTER`, `TAKE OWNERSHIP`). |

## Custom Roles Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_db_owner_members` | `int` | `2` | Maximum number of members in `db_owner` before flagging. |
| `flag_empty_roles` | `bool` | `True` | Flag custom roles that have zero members. |
| `flag_users_without_roles` | `bool` | `True` | Flag database users who are not a member of any custom role. |

## Column-Level Security Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sensitive_column_patterns` | `list[str]` | See below | SQL `LIKE` patterns for column names that should have CLS protection. |

Default sensitive patterns:

```python
[
    "%ssn%", "%social_security%",
    "%salary%", "%compensation%",
    "%credit_card%", "%card_number%",
    "%password%", "%secret%",
    "%date_of_birth%", "%dob%",
]
```

Example — add custom patterns:

```python
config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    sensitive_column_patterns=[
        "%ssn%", "%social_security%",
        "%salary%", "%compensation%",
        "%credit_card%", "%card_number%",
        "%password%", "%secret%",
        "%date_of_birth%", "%dob%",
        # Custom additions
        "%national_id%",
        "%bank_account%",
        "%tax_id%",
    ],
)
```

## Dynamic Data Masking Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_unmask_principals` | `int` | `3` | Maximum number of principals with `UNMASK` permission before flagging excessive grants. |
| `flag_weak_masking` | `bool` | `True` | Flag `default()` masking on short string columns (≤ 4 characters) where the mask may be trivially reversible. |

## Workspace Roles Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_workspace_admins` | `int` | `3` | Maximum number of workspace Admin role members before flagging. |

## SQL Audit Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_audit_retention_days` | `int` | `90` | Minimum acceptable audit log retention period in days. Shorter retention is flagged as MEDIUM. |

## Item Permissions Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_item_readdata_principals` | `int` | `10` | Maximum number of principals with direct ReadData item permissions before flagging excessive sharing. |

## OneLake Data Access Roles Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_onelake_roles` | `int` | `20` | Maximum number of OneLake data access roles before flagging. |
| `flag_readwrite_with_constraints` | `bool` | `True` | Flag ReadWrite roles when RLS or CLS constraints are defined (potential bypass). |
| `flag_default_reader_with_custom_roles` | `bool` | `True` | Flag when DefaultReader covers all paths and custom roles exist. |
| `flag_wildcard_path_roles` | `bool` | `True` | Flag roles using `**` wildcard path patterns. |
| `flag_empty_onelake_roles` | `bool` | `True` | Flag OneLake roles with no members assigned. |

## Output

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `verbose` | `bool` | `False` | Print intermediate debug output for each phase. |
| `phase_delay` | `float` | `1.0` | Seconds to pause between phases to reduce HTTP 429 throttling. Set to `0` to disable. |

## Validation

The config is validated automatically when `advisor.run()` is called:

- `warehouse_name` must be set to a non-empty value (not the placeholder
  `"<your_warehouse_name>"`)

If the check fails, a `ValueError` is raised with a descriptive message.

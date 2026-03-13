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
| `workspace_id` | `str` | `""` | Workspace GUID. Only needed for [cross-workspace](../../cross-workspace.md) access. |
| `warehouse_id` | `str` | `""` | Warehouse item GUID. Only needed for [cross-workspace](../../cross-workspace.md) access. |

## Scope Filtering

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_names` | `list[str]` | `[]` | Restrict RLS and CLS analysis to specific tables. Each entry can be `"table_name"` (any schema) or `"schema.table_name"`. Empty = all tables. |

Example:

```python
# Only check specific tables for RLS / CLS coverage
config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    table_names=["dbo.FactSales", "dbo.DimCustomer"],
)
```

## Check Category Toggles

Each check category can be independently enabled or disabled:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `check_schema_permissions` | `bool` | `True` | Enable the schema-level permissions check (SEC-001). |
| `check_custom_roles` | `bool` | `True` | Enable the custom database roles check (SEC-002). |
| `check_rls` | `bool` | `True` | Enable the Row-Level Security check (SEC-003). |
| `check_cls` | `bool` | `True` | Enable the Column-Level Security check (SEC-004). |
| `check_ddm` | `bool` | `True` | Enable the Dynamic Data Masking check (SEC-005). |

Example — run only the RLS and CLS checks:

```python
config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
    check_schema_permissions=False,
    check_custom_roles=False,
    check_rls=True,
    check_cls=True,
    check_ddm=False,
)
```

## Schema Permissions Settings (SEC-001)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flag_public_role_grants` | `bool` | `True` | Flag permissions granted directly to the `public` role. |
| `flag_direct_user_grants` | `bool` | `True` | Flag permissions granted directly to individual users rather than through roles. |
| `flag_schema_wide_grants` | `bool` | `True` | Flag overly broad schema-wide `GRANT` statements (`CONTROL`, `ALTER`, `TAKE OWNERSHIP`). |

## Custom Roles Settings (SEC-002)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_db_owner_members` | `int` | `2` | Maximum number of members in `db_owner` before flagging. |
| `flag_empty_roles` | `bool` | `True` | Flag custom roles that have zero members. |
| `flag_users_without_roles` | `bool` | `True` | Flag database users who are not a member of any custom role. |

## Column-Level Security Settings (SEC-004)

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

## Dynamic Data Masking Settings (SEC-005)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_unmask_principals` | `int` | `3` | Maximum number of principals with `UNMASK` permission before flagging excessive grants. |
| `flag_weak_masking` | `bool` | `True` | Flag `default()` masking on short string columns (≤ 4 characters) where the mask may be trivially reversible. |

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

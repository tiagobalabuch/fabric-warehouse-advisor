# Check Categories

The Security Check advisor runs up to **5 check categories**, each
targeting a different area of warehouse security posture. Every finding
includes a severity level, a human-readable message, and — where
applicable — a ready-to-run T-SQL fix.

---

## 1. Schema Permissions (SEC-001)

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_PERMISSIONS` |
| Config toggle | `check_schema_permissions` |
| Applies to | DataWarehouse, LakeWarehouse |

Analyses `sys.database_permissions` joined with `sys.database_principals`
to detect permission grants that violate least-privilege principles.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `public_role_grant` | HIGH | Permission granted directly to the `public` role | Every database user inherits `public` grants — this effectively removes access control for that permission. |
| `direct_user_grant` | MEDIUM | Permission granted directly to an individual user | Direct grants are harder to audit and manage at scale than role-based grants. |
| `schema_wide_grant` | HIGH | Broad permission (`CONTROL`, `ALTER`, `TAKE OWNERSHIP`) on an entire schema | Applies to all current and future objects in the schema — the grantee has full control. |
| `no_explicit_permissions` | INFO | No explicit grants found | All access is controlled through Fabric workspace roles alone. |
| `permissions_healthy` | INFO | All grants follow best practices | No actionable permission issues detected. |
| `permissions_query_failed` | LOW | Unable to query permission metadata | The executing identity may lack VIEW DEFINITION permission. |

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `flag_public_role_grants` | `True` | Enable / disable public role grant detection |
| `flag_direct_user_grants` | `True` | Enable / disable direct user grant detection |
| `flag_schema_wide_grants` | `True` | Enable / disable broad schema-wide grant detection |

### Example Findings

```
🔴 [MyWarehouse.sales] SELECT granted to the public role.
   Permission GRANT SELECT on SCHEMA is granted to the public role
   — every database user inherits it.
   → Revoke the grant from public and assign it to a specific custom role instead.
   SQL: REVOKE SELECT ON SCHEMA::[sales] FROM [public];

🟠 [MyWarehouse.dbo] INSERT granted directly to user [alice@contoso.com].
   Granting permissions directly to individual users is harder to audit
   and manage at scale than role-based grants.
   → Create a custom role, grant INSERT to the role, and add [alice@contoso.com]
     as a member.
```

---

## 2. Custom Roles (SEC-002)

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_ROLES` |
| Config toggle | `check_custom_roles` |
| Applies to | DataWarehouse, LakeWarehouse |

Analyses `sys.database_principals` and `sys.database_role_members` to
detect role hygiene issues.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `excessive_db_owner_members` | HIGH | `db_owner` has more members than `max_db_owner_members` | `db_owner` bypasses all permission checks and can perform any action in the database. |
| `db_owner_membership_ok` | INFO | `db_owner` membership within threshold | Healthy state. |
| `empty_custom_role` | LOW | Custom role with zero members | Unused roles add clutter and may indicate incomplete provisioning. |
| `user_without_role` | MEDIUM | Database user not a member of any custom role | User may rely on direct grants or the `public` role, making access harder to audit. |
| `roles_query_failed` | LOW | Unable to query role membership views | The executing identity may lack VIEW DEFINITION permission. |

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_db_owner_members` | `2` | Threshold before flagging excessive membership |
| `flag_empty_roles` | `True` | Enable / disable empty role detection |
| `flag_users_without_roles` | `True` | Enable / disable unassigned user detection |

### Example Findings

```
🔴 [MyWarehouse] db_owner has 5 members (threshold: 2).
   Members: admin1, admin2, dev1, dev2, deploy_svc.
   db_owner bypasses all permission checks and can perform any action
   in the database.
   → Review db_owner membership and remove users who do not require full
     administrative access. Use custom roles with least-privilege grants instead.

🟡 [MyWarehouse].[DataReaders] Custom role [DataReaders] has no members.
   Unused roles add clutter and may indicate incomplete provisioning.
   → Add members to [DataReaders] or drop it if it is no longer needed.
   SQL: DROP ROLE [DataReaders];
```

---

## 3. Row-Level Security (SEC-003)

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_RLS` |
| Config toggle | `check_rls` |
| Applies to | DataWarehouse, LakeWarehouse |

Analyses `sys.security_policies` and `sys.security_predicates` to assess
RLS coverage and configuration.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `rls_policy_disabled` | HIGH | RLS policy exists but `is_enabled = 0` | A disabled policy provides no protection — all rows are visible to all users. |
| `rls_block_predicate` | MEDIUM | BLOCK predicate found on a policy | Fabric Warehouse supports only FILTER predicates. BLOCK predicates may be silently ignored. |
| `no_rls_policy` | INFO | Table has no RLS policy | All rows are visible to all users with SELECT access. |
| `rls_healthy` | INFO | All tables covered by active FILTER predicates | Healthy state. |
| `rls_query_failed` | LOW | Unable to query RLS metadata | The executing identity may lack VIEW DEFINITION permission. |

!!! note "Scope filtering"
    When `table_names` is configured, only the specified tables are
    evaluated for RLS coverage. Tables outside the filter are ignored.

### How It Works

**Step 1 — Collect policies**

```sql
SELECT
    sp.name AS policy_name,
    sp.is_enabled,
    pred.predicate_type_desc,
    SCHEMA_NAME(t.schema_id) AS table_schema,
    t.name AS table_name
FROM sys.security_policies AS sp
INNER JOIN sys.security_predicates AS pred
    ON sp.object_id = pred.object_id
INNER JOIN sys.objects AS t
    ON pred.target_object_id = t.object_id
```

**Step 2 — Collect all user tables**

```sql
SELECT SCHEMA_NAME(schema_id) AS schema_name, name AS table_name
FROM sys.objects WHERE type = 'U'
```

**Step 3 — Compare coverage**: tables with active FILTER predicates
are marked as covered; all others are flagged as uncovered.

### Example Findings

```
🔴 [MyWarehouse].[dbo].[FactSales]
   RLS policy [SalesFilter] exists but is DISABLED.
   A disabled policy provides no protection — all rows are visible
   to all users.
   → Enable the policy:
   SQL: ALTER SECURITY POLICY [SalesFilter] WITH (STATE = ON);

🟠 [MyWarehouse].[dbo].[DimEmployee] BLOCK predicate found on policy
   [EmpPolicy]. Microsoft Fabric Warehouse supports only FILTER
   predicates. BLOCK predicates may be silently ignored.
```

---

## 4. Column-Level Security (SEC-004)

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_CLS` |
| Config toggle | `check_cls` |
| Applies to | DataWarehouse, LakeWarehouse |

Checks `sys.database_permissions` (filtered to column-scoped grants)
against a configurable list of sensitive column name patterns to detect
columns that should have DENY SELECT protection but do not.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `sensitive_column_unprotected` | HIGH | Sensitive column (by name pattern) with no DENY SELECT | Users with SELECT on the table can see the sensitive data. |
| `sensitive_columns_protected` | INFO | Sensitive columns already protected by DENY SELECT | Healthy state. |
| `cls_no_patterns` | INFO | No sensitive column patterns configured | CLS check skipped — set `sensitive_column_patterns` to enable. |
| `cls_query_failed` | LOW | Unable to query column permission metadata | The executing identity may lack VIEW DEFINITION permission. |

### Default Sensitive Patterns

| Pattern | Matches (examples) |
|---------|---------------------|
| `%ssn%` | `ssn`, `customer_ssn` |
| `%social_security%` | `social_security_number` |
| `%salary%` | `base_salary`, `salary_amount` |
| `%compensation%` | `total_compensation` |
| `%credit_card%` | `credit_card_number` |
| `%card_number%` | `card_number`, `debit_card_number` |
| `%password%` | `password_hash`, `temp_password` |
| `%secret%` | `client_secret` |
| `%date_of_birth%` | `date_of_birth` |
| `%dob%` | `dob`, `customer_dob` |

### Example Findings

```
🔴 [MyWarehouse].[hr].[Employees].[salary]
   Sensitive column [salary] has no DENY SELECT protection.
   Column [hr].[Employees].[salary] matches a sensitive name pattern
   but has no column-level DENY SELECT grant.
   → Add a DENY SELECT on the column for roles that should not see it.
   SQL: DENY SELECT ON [hr].[Employees] ([salary]) TO [<restricted_role>];
```

---

## 5. Dynamic Data Masking (SEC-005)

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_DDM` |
| Config toggle | `check_ddm` |
| Applies to | DataWarehouse, LakeWarehouse |

Analyses `sys.masked_columns` and `sys.database_permissions` (UNMASK
grants) to assess DDM coverage and hygiene.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `excessive_unmask_grants` | HIGH | More principals have UNMASK than `max_unmask_principals` | Too many UNMASK grants defeat the purpose of masking — data is effectively visible to many users. |
| `unmask_grants_ok` | INFO | UNMASK grant count within threshold | Healthy state. |
| `weak_default_mask` | MEDIUM | `default()` mask on a short string column (≤ 4 chars) | The mask shows `xxxx` which may be trivially reversible for short values. |
| `ddm_columns_masked` | INFO | Count of columns with masking applied | Summary finding. |
| `no_masked_columns` | INFO | No columns have masking applied | No DDM in use. |
| `ddm_query_failed` | LOW | Unable to query DDM metadata | The executing identity may lack VIEW DEFINITION permission. |

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_unmask_principals` | `3` | Threshold before flagging excessive UNMASK grants |
| `flag_weak_masking` | `True` | Enable / disable weak default mask detection |

### How UNMASK Analysis Works

```sql
SELECT
    pr.name AS grantee_name,
    pr.type_desc AS grantee_type,
    dp.state_desc
FROM sys.database_permissions AS dp
INNER JOIN sys.database_principals AS pr
    ON dp.grantee_principal_id = pr.principal_id
WHERE dp.permission_name = 'UNMASK'
  AND dp.state_desc IN ('GRANT', 'GRANT_WITH_GRANT_OPTION')
```

### SQL Fixes

Replace weak default masking with a partial mask:

```sql
ALTER TABLE [schema].[table] ALTER COLUMN [column]
ADD MASKED WITH (FUNCTION = 'partial(0, "XXXXX", 0)');
```

Revoke excessive UNMASK grants:

```sql
REVOKE UNMASK FROM [principal_name];
```

### Example Findings

```
🔴 [MyWarehouse] 7 principal(s) have UNMASK permission (threshold: 3).
   Principals with UNMASK: admin1, admin2, report_svc, analyst1,
   analyst2, dev1, etl_svc.
   → Review UNMASK grants and revoke from principals that do not
     require access to unmasked data.

🟠 [MyWarehouse].[dbo].[Customers].[zip_code]
   default() mask on short varchar(5) column [zip_code].
   The default() mask on a varchar(5) column shows 'xxxx' which may
   be trivially reversible for short values.
   → Use a partial() or random() masking function instead, or consider
     Column-Level Security (DENY SELECT).
   SQL: ALTER TABLE [dbo].[Customers] ALTER COLUMN [zip_code]
        ADD MASKED WITH (FUNCTION = 'partial(0, "XXXXX", 0)');
```

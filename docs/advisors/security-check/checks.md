# Check Categories

The Security Check advisor runs up to **15 check categories**, each
targeting a different area of warehouse security posture. Every finding
includes a severity level, a human-readable message, and — where
applicable — a ready-to-run fix.

Categories are grouped into four layers:

- **Workspace & Platform**
- **Item Security**
- **OneLake Security**
- **SQL Security**
- **Cross-Reference**
- **Detection (auth mode)**

---

## 1. Schema Permissions

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

---

## 2. Custom Roles

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

---

## 3. Row-Level Security

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

---

## 4. Column-Level Security

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

---

## 5. Dynamic Data Masking

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

---

## 6. Workspace Roles

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_WORKSPACE_ROLES` |
| Config toggle | `check_workspace_roles` |
| Applies to | DataWarehouse, LakeWarehouse |

Analyses workspace role assignments to detect overly broad access,
excessive admin membership, and service principal misuse.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `entire_tenant_access` | CRITICAL | Entire tenant has Admin or Member workspace access | Every user in the tenant inherits high-privilege access to the workspace. |
| `entire_tenant_access` | HIGH | Entire tenant has Contributor or Viewer access | Broad access even at lower privilege levels. |
| `service_principal_admin` | MEDIUM | Service principal has the Admin workspace role | Service principals with Admin bypass human approval flows. |
| `excessive_workspace_admins` | HIGH | Admin-role member count exceeds `max_workspace_admins` | Excess admins increase the blast radius of credential compromise. |
| `workspace_admins_ok` | INFO | Admin membership within threshold | Healthy state. |
| `no_workspace_roles_found` | INFO | No workspace role assignments returned | Unexpected — may indicate API permission issue. |
| `workspace_roles_healthy` | INFO | Workspace role assignments follow best practices | No actionable findings. |
| `workspace_roles_query_failed` | LOW | REST API call failed | Token or connectivity issue. |

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_workspace_admins` | `3` | Threshold before flagging excessive admin membership |

---

## 7. Network Isolation

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_NETWORK` |
| Config toggle | `check_network_isolation` |
| Applies to | DataWarehouse, LakeWarehouse |

Inspects the workspace-level network communication policy for inbound
and outbound access rules.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `inbound_public_access_allowed` | HIGH | Inbound public network access is allowed | Anyone on the internet can connect to workspace endpoints. |
| `inbound_public_access_denied` | INFO | Inbound public access is denied | Healthy state. |
| `inbound_policy_unknown` | MEDIUM | Inbound default action is an unexpected value | Policy may not be configured correctly. |
| `outbound_public_access_allowed` | LOW | Outbound public network access is allowed | Data can flow to external endpoints. |
| `outbound_public_access_denied` | INFO | Outbound public access is denied | Healthy state. |
| `network_isolation_healthy` | INFO | Both inbound and outbound policies are properly configured | No actionable findings. |
| `network_policy_query_failed` | LOW | REST API call failed | Token or connectivity issue. |

---

## 8. SQL Audit Settings

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_SQL_AUDIT` |
| Config toggle | `check_sql_audit` |
| Applies to | DataWarehouse, LakeWarehouse |

Evaluates SQL audit configuration: whether auditing is enabled, log
retention, and action group coverage.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `sql_audit_disabled` | HIGH | SQL auditing is disabled | No audit trail for security events. |
| `sql_audit_short_retention` | MEDIUM | Retention period below `min_audit_retention_days` | Logs may be purged before incident investigations complete. |
| `sql_audit_indefinite_retention` | INFO | Retention set to indefinite (0 days) | Healthy configuration. |
| `sql_audit_category_uncovered` | HIGH | No audit groups enabled for an entire audit category | Critical audit events will not be captured. |
| `sql_audit_missing_recommended_group` | MEDIUM | Some recommended groups missing in a partially-covered category | Coverage gap in an otherwise enabled category. |
| `sql_audit_unknown_groups` | INFO | Unrecognised audit action groups detected | Custom or preview groups that are not in the known catalogue. |
| `sql_audit_healthy` | INFO | SQL audit settings follow best practices | No actionable findings. |
| `sql_audit_query_failed` | LOW | REST API call failed | Token or connectivity issue. |

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `min_audit_retention_days` | `90` | Minimum acceptable audit retention period |

---

## 9. Item Permissions

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_ITEM_PERMISSIONS` |
| Config toggle | `check_item_permissions` |
| Applies to | DataWarehouse, LakeWarehouse |

Lists principals with direct item-level permissions on the warehouse or
SQL endpoint and detects overly broad sharing.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `entire_tenant_item_access` | CRITICAL | Entire tenant has item-level access | Every user in the tenant can access the warehouse directly. |
| `excessive_readdata_sharing` | HIGH | More principals have ReadData than `max_item_readdata_principals` | Excessive direct sharing bypasses workspace role governance. |
| `item_write_outside_workspace_role` | MEDIUM | Principal has item Write without a workspace role that implies Write | Write access granted via sharing rather than workspace role. |
| `item_permissions_healthy` | INFO | Item-level permissions follow best practices | No actionable findings. |
| `item_permissions_summary` | INFO | Summary with principal counts | Emitted when actionable findings exist. |
| `no_item_permissions_found` | INFO | No item-level permission entries returned | All access is via workspace roles. |
| `item_permissions_skipped_no_admin` | INFO | Check skipped — Fabric Admin role required | HTTP 401/403 from Admin API. |
| `item_permissions_query_failed` | LOW | REST API call failed | Token or connectivity issue. |

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_item_readdata_principals` | `10` | Threshold before flagging excessive ReadData sharing |

---

## 10. Sensitivity Labels

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_SENSITIVITY_LABELS` |
| Config toggle | `check_sensitivity_labels` |
| Applies to | DataWarehouse, LakeWarehouse |

Checks whether a Microsoft Purview sensitivity label is applied to the
warehouse or SQL endpoint item.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `no_sensitivity_label` | HIGH | No sensitivity label applied to the item | Data classification requirements may not be met; downstream governance policies may not fire. |
| `sensitivity_label_applied` | INFO | Sensitivity label is applied | Healthy state. |

---

## 11. Role Alignment

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_ROLE_ALIGNMENT` |
| Config toggle | `check_role_alignment` |
| Applies to | DataWarehouse, LakeWarehouse |

Cross-references workspace role assignments with SQL
database role membership to detect misalignments.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `viewer_with_db_owner` | HIGH | Workspace Viewer is `db_owner` in the database | Viewer can bypass all SQL permission checks — a privilege escalation path. |
| `viewer_with_high_priv_role` | MEDIUM | Workspace Viewer has high-privilege DB roles (not `db_owner`) | Viewer has broader SQL access than the workspace role implies. |
| `no_workspace_role_high_db_priv` | MEDIUM | Database principal with elevated SQL roles but no workspace role | Orphaned high-privilege access — no workspace governance. |
| `role_alignment_healthy` | INFO | Workspace roles and database roles are properly aligned | No actionable findings. |
| `role_alignment_summary` | INFO | Alignment analysis complete with issue count | Emitted when actionable findings exist. |
| `role_alignment_no_data` | INFO | No database principals or workspace roles to compare | Both sources empty. |
| `role_alignment_query_failed` | LOW | Unable to query database principals | T-SQL query exception. |

---

## 12. OneLake Data Access Roles

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_ONELAKE_DATA_ACCESS` |
| Config toggle | `check_onelake_data_access_roles` |
| Applies to | LakeWarehouse only |

Analyses OneLake data access role definitions for configuration risks.

!!! note "Auth mode sensitivity"
    In **User Identity mode**, OneLake roles control table access and findings are raised at full severity. In **Delegated Identity mode**, OneLake roles are not enforced — all findings are downgraded to INFO.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `readwrite_role_with_constraints` | CRITICAL ‡ | ReadWrite role defined alongside RLS / CLS constraints | ReadWrite bypasses row- and column-level restrictions. |
| `default_reader_full_access_with_custom_roles` | HIGH ‡ | DefaultReader grants wildcard path access while custom roles exist | Custom roles are ineffective if the default role already grants full access. |
| `multi_role_cls_conflict` | HIGH ‡ | Principal can be in two roles with different CLS column sets for the same table | Effective column access is the union, defeating column restrictions. |
| `wildcard_path_custom_role` | MEDIUM ‡ | Custom role uses a wildcard (`*`) path pattern | Broadens access scope beyond what may be intended. |
| `empty_onelake_role` | MEDIUM ‡ | OneLake role has no members | Unused role adds configuration complexity. |
| `excessive_onelake_roles` | LOW ‡ | Role count exceeds `max_onelake_roles` threshold | Large numbers of roles increase operational complexity. |
| `onelake_role_constraints` | INFO | Role has RLS or CLS data-level constraints | Informational. |
| `onelake_roles_summary` | INFO | Summary of all OneLake roles found | Always emitted. |
| `no_onelake_roles` | INFO | No OneLake data access roles found | OneLake security not configured. |

!!! info
    ‡ = Downgraded to INFO in **Delegated Identity mode**.

### Configuration Knobs

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_onelake_roles` | `20` | Threshold before flagging excessive roles |
| `flag_readwrite_with_constraints` | `True` | Detect ReadWrite + RLS/CLS conflict |
| `flag_default_reader_with_custom_roles` | `True` | Detect DefaultReader wildcard + custom roles |
| `flag_wildcard_path_roles` | `True` | Detect wildcard path patterns |
| `flag_empty_onelake_roles` | `True` | Detect roles with no members |

---

## 13. OneLake Settings

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_ONELAKE_SETTINGS` |
| Config toggle | `check_onelake_settings` |
| Applies to | DataWarehouse, LakeWarehouse |

Inspects workspace-level OneLake configuration for diagnostic logging
and immutability policies.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `onelake_diagnostics_disabled` | MEDIUM | OneLake diagnostic logging is disabled | No visibility into data access patterns. |
| `onelake_diagnostics_enabled` | INFO | Diagnostic logging is enabled | Healthy state. |
| `no_immutability_policy` | LOW | No immutability policy on diagnostic logs | Logs could be tampered with or deleted. |
| `immutability_policy_found` | INFO | Immutability policy found with scope and retention | Healthy state. |
| `onelake_settings_skipped_no_admin` | INFO | Check skipped — Admin workspace role required | HTTP 401/403. |
| `onelake_settings_query_failed` | LOW | REST API call failed | Token or connectivity issue. |

---

## 14. Security Sync Health

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_ONELAKE_SECURITY_SYNC` |
| Config toggle | `check_onelake_security_sync` |
| Applies to | LakeWarehouse only |

Verifies that OneLake data access roles are correctly synchronised into
the SQL engine as `ols_`-prefixed database roles.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `security_sync_missing` | HIGH | OneLake roles exist but no `ols_*` sync roles in SQL | Security definitions are not propagated to the SQL endpoint. |
| `stale_sync_role` | MEDIUM | `ols_*` role exists in SQL but no matching OneLake role | Orphaned role — may grant unintended access. |
| `missing_sync_role` | MEDIUM | OneLake role has no corresponding `ols_*` sync role | Role not enforced at the SQL layer. |
| `security_sync_summary` | INFO | Summary of `ols_*` sync roles found | Emitted when sync roles exist. |
| `no_ols_roles` | INFO | No `ols_*` sync roles found | Expected if OneLake security is not enabled. |
| `security_sync_query_failed` | LOW | T-SQL query failed | Permission or connectivity issue. |

---

## 15. Auth Mode Detection

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_AUTH_MODE` |
| Config toggle | `check_auth_mode` |
| Applies to | LakeWarehouse only |

Detects the SQL endpoint’s access mode (User Identity vs Delegated
Identity). The result gates which checks are active in subsequent
phases (see [How it works — Auth Mode Gating](how-it-works.md#auth-mode-gating)).

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `auth_mode_detected` | INFO | Access mode successfully determined | Downstream phases adjust severity accordingly. |
| `auth_mode_unknown` | LOW | Access mode could not be determined | Phases run with default severity — some findings may not be contextually accurate. |

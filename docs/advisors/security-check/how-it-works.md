# How It Works

The Security Check advisor runs a **15-phase pipeline** that analyses
workspace roles, network policies, OneLake security, SQL permissions,
Row-Level Security, Column-Level Security, Dynamic Data Masking, and
cross-references REST API metadata with T-SQL catalog data.

## Architecture Overview

```text
SecurityCheckAdvisor.run()
  |
  ├─ Phase 0a: Edition detection     → synapsesql()                 
  ├─ Phase 0b: Auth mode detection   → REST (LakeWarehouse only)    
  ├─ Phase 1: Workspace Roles        → GET /v1/workspaces/.../roles 
  ├─ Phase 2: Network Isolation      → GET .../communicationPolicy  
  ├─ Phase 3: OneLake Settings       → GET .../workspace settings   
  ├─ Phase 4: SQL Audit Settings     → GET .../settings/sqlAudit    
  ├─ Phase 5: Item Permissions       → Admin permissions API        
  ├─ Phase 6: Sensitivity Labels     → Warehouse/endpoint metadata  
  ├─ Phase 7: OneLake Data Access    → GET .../dataAccessRoles      
  ├─ Phase 8: Schema Permissions     → sys.database_permissions     
  ├─ Phase 9: Custom Roles           → sys.database_principals      
  ├─ Phase 10: Row-Level Security    → sys.security_policies        
  ├─ Phase 11: Column-Level Security → sys.database_permissions     
  ├─ Phase 12: Dynamic Data Masking  → sys.masked_columns           
  ├─ Phase 13: Security Sync Health  → sys.database_principals (ols)
  └─ Phase 14: Role Alignment        → T-SQL + REST combined        

```

## Phase 0a: Edition Detection

Determines whether the target is a **Warehouse** or a
**SQL Analytics Endpoint** (`LakeWarehouse` edition). This gates
subsequent checks — OneLake security phases (7, 13) only run for
SQL Analytics Endpoint, and auth mode detection (Phase 0b) is skipped
for standalone Warehouses.

## Phase 0b: Auth Mode Detection

For SQL Analytics Endpoint only. Calls an internal Fabric API to
determine the access mode:

- **User Identity** — OneLake security roles control table-level access.
  SQL custom roles, RLS, and CLS are not enforced.
- **Delegated Identity** — traditional SQL security model is active.
  All Phase 7 (OneLake Data Access) findings are downgraded to INFO.

The detected mode influences how Phases 7–11 behave (see
*Auth Mode Gating* below).

## Phase 1: Workspace Roles 

Checks include:

- **EntireTenant access** — a workspace role granted to the entire
  tenant is flagged as CRITICAL.
- **Excessive workspace admins** — more admin-role members than
  `max_workspace_admins` (default: 3) is flagged as HIGH.
- **Service principal as admin** — service principals in the Admin role
  are flagged as MEDIUM.

## Phase 2: Network Isolation 

Checks include:

- **Inbound public access allowed** — flagged as HIGH.
- **Outbound public access allowed** — flagged as LOW (informational).

## Phase 3: OneLake Settings

Inspects workspace-level OneLake configuration. This phase is **not**
gated by auth mode — it runs for both Warehouses and SQL Endpoints.

Checks include:

- **OneLake diagnostics disabled** — flagged as MEDIUM.
- **No immutability policy** — flagged as LOW.

## Phase 4: SQL Audit Settings 

Checks include:

- **SQL auditing disabled** — flagged as HIGH.
- **Short audit retention** — retention below `min_audit_retention_days`
  (default: 90) is flagged as MEDIUM.
- **Missing audit action groups** — required action categories not
  covered are flagged as HIGH.

## Phase 5: Item Permissions 

Calls the Admin permissions API to list principals with direct item
permissions on the warehouse or SQL endpoint.

Checks include:

- **EntireTenant item access** — flagged as CRITICAL.
- **Excessive ReadData sharing** — more principals than
  `max_item_readdata_principals` (default: 10) is flagged as HIGH.
- **Write permission outside workspace role** — a principal with
  item-level write access but no corresponding workspace role is flagged
  as MEDIUM.

## Phase 6: Sensitivity Labels

Inspects the warehouse or SQL endpoint metadata for a Microsoft Purview
sensitivity label.

- **No sensitivity label** — flagged as HIGH.
- **Label applied** — reported as INFO for visibility.

### Phase 7: OneLake Data Access Roles

!!! note "Edition gate"
    This phase only runs for **SQL Analytics Endpoint**. For
    standalone Warehouses it is skipped.

Checks include:

- **ReadWrite role with RLS/CLS constraints** — a role granting
  ReadWrite access while RLS or CLS is defined creates a bypass risk
  (CRITICAL in User Identity mode, INFO in Delegated).
- **DefaultReader full-access with custom roles** — the default reader
  role covers all paths while custom roles also exist (HIGH / INFO).
- **Wildcard path roles** — roles with `**` path patterns flagged as
  MEDIUM.
- **Empty OneLake roles** — roles with no members flagged as LOW.
- **Excessive roles** — more roles than `max_onelake_roles` (default:
  20) flagged as MEDIUM.
- **Multi-role CLS conflict** — a principal in multiple roles where one
  bypasses CLS restrictions (HIGH / INFO).

## Phase 8: Schema Permissions

Analyses `sys.database_permissions` joined with `sys.database_principals`
to detect overly broad or risky permission grants.

Three sub-checks:

1. **Public role grants** — any permission granted to the `public` role
   is flagged as HIGH because every database user inherits it.
2. **Direct user grants** — permissions granted to individual
   `SQL_USER` / `EXTERNAL_USER` principals are flagged as MEDIUM.
3. **Schema-wide grants** — broad permissions (`CONTROL`, `ALTER`,
   `TAKE OWNERSHIP`) on an entire schema are flagged as HIGH.

!!! note "User Identity mode"
    In User Identity mode, table-level permission findings are
    downgraded to INFO because SQL permissions are not enforced.

## Phase 9: Custom Roles

Queries `sys.database_principals` and `sys.database_role_members` to
assess role hygiene.

Three sub-checks:

1. **Excessive `db_owner` membership** — more members than
   `max_db_owner_members` (default: 2) raises a HIGH finding.
2. **Empty custom roles** — roles with zero members (LOW).
3. **Users without any custom role** — database users relying on direct
   grants or the `public` role (MEDIUM).

!!! note "User Identity mode"
    In User Identity mode, this check is replaced with a single INFO
    finding stating that SQL custom roles are inactive.

## Phase 10: Row-Level Security

Analyses `sys.security_policies` joined with `sys.security_predicates`
and `sys.objects` to assess RLS coverage.

Three sub-checks:

1. **Disabled policies** — `is_enabled = 0` (HIGH).
2. **BLOCK predicates** — Fabric Warehouse supports only FILTER
   predicates; BLOCK is flagged as MEDIUM.
3. **Tables without RLS** — user tables lacking an active FILTER
   predicate (INFO).

!!! note "User Identity mode"
    In User Identity mode, this check is replaced with a single INFO
    finding stating that SQL RLS is inactive.

## Phase 11: Column-Level Security

Checks `sys.database_permissions` against configurable sensitive column
name patterns.

Two sub-checks:

1. **Sensitive columns without DENY SELECT** — columns matching
   `sensitive_column_patterns` without protection (HIGH).
2. **Protected columns** — columns with DENY SELECT (INFO).

!!! note "User Identity mode"
    In User Identity mode, this check is replaced with a single INFO
    finding stating that SQL CLS is inactive.

## Phase 12: Dynamic Data Masking

Analyses `sys.masked_columns` and UNMASK grants.

Two sub-checks:

1. **Excessive UNMASK grants** — more principals than
   `max_unmask_principals` (default: 3) (HIGH).
2. **Weak default masking** — `default()` on short string columns
   (`max_length ≤ 4`) (MEDIUM).

!!! note
    DDM is a SQL-engine feature enforced at query time regardless of
    auth mode, so this phase runs in both modes.

## Phase 13: Security Sync Health

Queries `sys.database_principals` for `ols_`-prefixed roles that Fabric
creates to synchronise OneLake data access roles into the SQL engine.

!!! note "Edition gate"
    This phase only runs for **SQL Analytics Endpoint**.

Checks include:

- **Security sync missing** — no `ols_` roles exist when OneLake roles
  are defined (HIGH).
- **Stale sync role** — an `ols_` role exists in SQL but no matching
  OneLake role was found (MEDIUM).
- **Missing sync role** — an OneLake role has no corresponding `ols_`
  role in SQL (MEDIUM).

## Phase 14: Role Alignment 

Combines workspace role assignments (from the REST API, fetched in
Phase 1) with SQL database role membership (T-SQL) to detect misalignments.

Checks include:

- **Viewer with `db_owner`** — a workspace Viewer who is a member of
  `db_owner` in the database (HIGH).
- **Viewer with high-privilege role** — a Viewer with a custom role
  granting broad permissions (MEDIUM).
- **No workspace role but high DB privileges** — a database principal
  with elevated SQL roles who has no workspace role assignment (MEDIUM).

## Auth Mode Gating

For SQL Analytics Endpoint, the detected auth mode changes which
checks are active:

| Phase | Check | User Identity | Delegated Identity |
|-------|-------|---------------|-------------------|
| 7 | OneLake Data Access Roles | Full severity | All findings → INFO |
| 9 | Custom Roles | INFO (inactive) | Full check |
| 10 | Row-Level Security | INFO (inactive) | Full check |
| 11 | Column-Level Security | INFO (inactive) | Full check |
| 8 | Schema Permissions | Table-level → INFO | Full severity |

Phases not listed in the table above run identically in both modes.

## Throttle Protection

Each phase is separated by a configurable `phase_delay` (default: 1.0
second) to reduce the risk of HTTP 429 throttling from the Fabric
control-plane API when running multiple queries in quick succession.
Set `phase_delay=0` to disable the delay.

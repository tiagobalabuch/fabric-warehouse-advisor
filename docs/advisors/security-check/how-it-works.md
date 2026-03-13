# How It Works

The Security Check advisor runs a **5-phase pipeline (Phases 1–5)** that
analyses permission grants, role membership, Row-Level Security policies,
Column-Level Security coverage, and Dynamic Data Masking configuration.

## Architecture Overview

```text
┌────────────────────────────────────────────────────────────────┐
│                     Fabric Notebook                            │
│                                                                │
│  ├─ Phase 1: Schema Permissions  → sys.database_permissions    │
│  ├─ Phase 2: Custom Roles        → sys.database_principals     │
│  ├─ Phase 3: Row-Level Security  → sys.security_policies       │
│  ├─ Phase 4: Column-Level Sec.   → sys.database_permissions    │
│  └─ Phase 5: Dynamic Data Mask.  → sys.masked_columns          │
│                                                                │
│  All SQL runs via T-SQL passthrough (no data transferred to    │
│  Spark — only metadata and permission information)             │
└────────────────────────────────────────────────────────────────┘
```

## Phase 1: Schema Permissions (SEC-001)

Analyses `sys.database_permissions` joined with `sys.database_principals`
to detect overly broad or risky permission grants.

```sql
SELECT
    dp.class_desc,
    dp.permission_name,
    dp.state_desc,
    SCHEMA_NAME(dp.major_id) AS schema_name,
    pr.name AS grantee_name,
    pr.type_desc AS grantee_type
FROM sys.database_permissions AS dp
INNER JOIN sys.database_principals AS pr
    ON dp.grantee_principal_id = pr.principal_id
WHERE dp.state_desc IN ('GRANT', 'GRANT_WITH_GRANT_OPTION')
```

Three sub-checks are applied to the result set:

1. **Public role grants** — any permission granted to the `public` role
   is flagged as HIGH because every database user inherits it.
2. **Direct user grants** — permissions granted to individual
   `SQL_USER` / `EXTERNAL_USER` principals (rather than roles) are
   flagged as MEDIUM because they are harder to audit at scale.
3. **Schema-wide grants** — broad permissions (`CONTROL`, `ALTER`,
   `TAKE OWNERSHIP`) on an entire schema are flagged as HIGH because
   they apply to all current and future objects in that schema.

## Phase 2: Custom Roles (SEC-002)

Queries `sys.database_principals` and `sys.database_role_members` to
assess role hygiene.

Three sub-checks:

1. **Excessive `db_owner` membership** — `db_owner` bypasses all
   permission checks. If the member count exceeds
   `max_db_owner_members` (default: 2), a HIGH finding is raised.
2. **Empty custom roles** — roles with zero members add clutter and may
   indicate incomplete provisioning (LOW).
3. **Users without any custom role** — database users who are not
   assigned to any custom role may be relying on direct grants or the
   `public` role, making access harder to audit (MEDIUM).

## Phase 3: Row-Level Security (SEC-003)

Analyses `sys.security_policies` joined with `sys.security_predicates`
and `sys.objects` to assess RLS coverage.

Three sub-checks:

1. **Disabled policies** — an RLS policy that exists but has
   `is_enabled = 0` provides no protection (HIGH).
2. **BLOCK predicates** — Microsoft Fabric Warehouse supports only
   FILTER predicates. BLOCK predicates may be silently ignored (MEDIUM).
3. **Tables without RLS** — user tables with no active FILTER predicate
   are flagged as INFO so you can evaluate whether they need protection.

!!! note "Scope filtering"
    When `table_names` is configured, only the specified tables are
    evaluated for RLS coverage.

## Phase 4: Column-Level Security (SEC-004)

Checks `sys.database_permissions` (filtered to column-scoped grants)
against a configurable list of sensitive column name patterns.

Two sub-checks:

1. **Sensitive columns without DENY SELECT** — columns whose names match
   `sensitive_column_patterns` (e.g. `%ssn%`, `%salary%`) but have no
   column-level DENY SELECT grant are flagged as HIGH.
2. **Protected columns** — columns that already have DENY SELECT are
   reported as INFO for visibility.

The default sensitive patterns cover common PII and financial columns:

```python
["%ssn%", "%social_security%", "%salary%", "%compensation%",
 "%credit_card%", "%card_number%", "%password%", "%secret%",
 "%date_of_birth%", "%dob%"]
```

## Phase 5: Dynamic Data Masking (SEC-005)

Analyses `sys.masked_columns` and `sys.database_permissions` (for
UNMASK grants) to assess DDM coverage and hygiene.

Two sub-checks:

1. **Excessive UNMASK grants** — if more principals have UNMASK
   permission than `max_unmask_principals` (default: 3), a HIGH finding
   is raised.
2. **Weak default masking** — `default()` masking on short string
   columns (`max_length ≤ 4`) may be trivially reversible and is
   flagged as MEDIUM.

## Data Flow

```text
Phase 1 (permissions) ──────────┐
Phase 2 (roles) ────────────────┤
Phase 3 (RLS) ─────────────────┼──► Report Generation
Phase 4 (CLS) ─────────────────┤        │
Phase 5 (DDM) ─────────────────┘        ▼
                               SecurityCheckResult
                               ├── findings[]
                               ├── summary (CheckSummary)
                               ├── text_report
                               ├── markdown_report
                               └── html_report
```

## Throttle Protection

Each phase is separated by a configurable `phase_delay` (default: 1.0
second) to reduce the risk of HTTP 429 throttling from the Fabric
control-plane API when running multiple queries in quick succession.
Set `phase_delay=0` to disable the delay.

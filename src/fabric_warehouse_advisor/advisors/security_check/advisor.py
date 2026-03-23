"""
Fabric Warehouse Advisor — Security Check Advisor (Main Orchestrator)
======================================================================
Provides the :class:`SecurityCheckAdvisor` class that runs all
configured security checks and collects findings.

Usage
-----
::

    from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

    config = SecurityCheckConfig(warehouse_name="MyWarehouse")
    advisor = SecurityCheckAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
    # result.findings -> list of Finding
    # result.has_critical -> bool
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pyspark.sql import SparkSession

from .config import SecurityCheckConfig
from .findings import (
    Finding, CheckSummary,
    LEVEL_INFO,
    CATEGORY_ROLES,
    CATEGORY_RLS,
    CATEGORY_CLS,
    CATEGORY_DDM,
)
from .checks.schema_permissions import check_schema_permissions
from .checks.custom_roles import check_custom_roles
from .checks.row_level_security import check_row_level_security
from .checks.column_level_security import check_column_level_security
from .checks.dynamic_data_masking import check_dynamic_data_masking
from .checks.workspace_roles import check_workspace_roles
from .checks.network_isolation import check_network_isolation
from .checks.sql_audit import check_sql_audit
from .checks.item_permissions import check_item_permissions
from .checks.sensitivity_labels import check_sensitivity_labels
from .checks.role_alignment import check_role_alignment
from .checks.auth_mode import detect_auth_mode
from .checks.onelake_data_access_roles import check_onelake_data_access_roles
from .checks.onelake_settings import check_onelake_settings
from .checks.onelake_security_sync import check_onelake_security_sync
from ..performance_check.checks.warehouse_type import detect_warehouse_edition
from .report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
)
from ...core.report import save_report
from ...core.warehouse_reader import read_warehouse_query
from ...core.fabric_rest_client import FabricRestClient, FabricRestError
from ...core.scope_resolver import resolve_table_scope
from ...core.phase_tracker import PhaseTracker, PhaseResult, PHASE_COMPLETED, PHASE_SKIPPED, PHASE_FAILED


# ------------------------------------------------------------------
# Result container
# ------------------------------------------------------------------

@dataclass
class SecurityCheckResult:
    """Container for all outputs produced by a security check run."""

    #: All findings from every check.
    findings: list[Finding] = field(default_factory=list)

    #: Aggregated summary.
    summary: CheckSummary | None = None

    #: Pre-formatted text report.
    text_report: str = ""

    #: Markdown report.
    markdown_report: str = ""

    #: HTML report.
    html_report: str = ""

    #: ISO-8601 timestamp (UTC).
    captured_at: str = ""

    @property
    def has_critical(self) -> bool:
        """True if any CRITICAL-level finding was detected."""
        return any(f.is_critical for f in self.findings)

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if f.is_critical)

    @property
    def high_count(self) -> int:
        return sum(1 for f in self.findings if f.is_high)

    @property
    def medium_count(self) -> int:
        return sum(1 for f in self.findings if f.is_medium)

    @property
    def low_count(self) -> int:
        return sum(1 for f in self.findings if f.is_low)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if f.is_info)

    def save(self, path: str, format: str = "html") -> str:
        """Save the report to a file.

        Parameters
        ----------
        path : str
            Destination file path.
        format : str
            One of ``"html"``, ``"md"``, ``"txt"``.

        Returns
        -------
        str
            The absolute path that was written.
        """
        content_map = {
            "html": self.html_report,
            "md": self.markdown_report,
            "txt": self.text_report,
        }
        content = content_map.get(format)
        if content is None:
            raise ValueError(
                f"Unknown format '{format}'. Use 'html', 'md', or 'txt'."
            )
        return save_report(content, path, format=format)


# ------------------------------------------------------------------
# Advisor class
# ------------------------------------------------------------------

class SecurityCheckAdvisor:
    """Orchestrates the full security-check advisory pipeline.

    Parameters
    ----------
    spark : SparkSession
        An active PySpark session.
    config : SecurityCheckConfig
        Configuration object.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: SecurityCheckConfig | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or SecurityCheckConfig()

    # ---- helpers ----

    def _log(self, msg: str) -> None:
        if self.config.verbose:
            print(msg)

    def _log_header(self, title: str) -> None:
        if self.config.verbose:
            print(f"  ┌── {title} ──")

    def _log_footer(self) -> None:
        if self.config.verbose:
            print(f"  └{'─' * 60}")

    def _log_kv(self, key: str, value: object, indent: int = 4) -> None:
        if self.config.verbose:
            pad = ' ' * indent
            print(f"{pad}{key:<30}: {value}")

    def _log_findings_detail(self, findings: list[Finding]) -> None:
        """Print per-finding detail when verbose is enabled."""
        if not self.config.verbose or not findings:
            return
        self._log_header("Findings Detail")
        for f in findings:
            icon = {"CRITICAL": "❌", "HIGH": "🔴", "MEDIUM": "🟠", "LOW": "🟡", "INFO": "✅"}.get(f.level, "•")
            self._log(f"    {icon} [{f.level}] {f.object_name}")
            self._log(f"       Check : {f.check_name}")
            self._log(f"       Msg   : {f.message}")
            if f.detail:
                self._log(f"       Detail: {f.detail}")
            if f.recommendation:
                self._log(f"       Rec   : {f.recommendation}")
            if f.sql_fix:
                self._log(f"       SQL   : {f.sql_fix}")
        self._log_footer()

    # ---- public API ----

    def run(self) -> SecurityCheckResult:
        """Execute all enabled security checks.

        Returns
        -------
        SecurityCheckResult
            Container with all findings, summary, and reports.

        Raises
        ------
        ValueError
            If the configuration is invalid.
        """
        cfg = self.config
        cfg.validate()
        spark = self.spark

        print("╔══════════════════════════════════════════════════╗")
        print("║  Fabric Warehouse Security Check Advisor         ║")
        print("╚══════════════════════════════════════════════════╝")
        print(f"  Warehouse : {cfg.warehouse_name}")
        if cfg.workspace_id:
            print(f"  Workspace : {cfg.workspace_id} (cross-workspace)")
        print(f"  Timestamp : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()

        # Verbose: show active configuration
        self._log_header("Configuration")
        self._log_kv("Schemas filter", cfg.schema_names or "(all)")
        self._log_kv("Tables filter", cfg.table_names or "(all)")
        self._log_kv("check_schema_permissions", cfg.check_schema_permissions)
        self._log_kv("check_custom_roles", cfg.check_custom_roles)
        self._log_kv("check_rls", cfg.check_rls)
        self._log_kv("check_cls", cfg.check_cls)
        self._log_kv("check_ddm", cfg.check_ddm)
        self._log_kv("check_workspace_roles", cfg.check_workspace_roles)
        self._log_kv("check_network_isolation", cfg.check_network_isolation)
        self._log_kv("check_sql_audit", cfg.check_sql_audit)
        self._log_kv("check_item_permissions", cfg.check_item_permissions)
        self._log_kv("check_sensitivity_labels", cfg.check_sensitivity_labels)
        self._log_kv("check_role_alignment", cfg.check_role_alignment)
        self._log_kv("check_auth_mode", cfg.check_auth_mode)
        self._log_kv("check_onelake_data_access_roles", cfg.check_onelake_data_access_roles)
        self._log_kv("check_onelake_settings", cfg.check_onelake_settings)
        self._log_kv("check_onelake_security_sync", cfg.check_onelake_security_sync)
        self._log_footer()

        _run_start = time.perf_counter()
        tracker = PhaseTracker(
            log_fn=self._log,
            log_findings_fn=self._log_findings_detail,
        )
        all_findings: list[Finding] = []

        # ================================================================
        # Phase 0a: Detect warehouse edition
        # ================================================================
        edition = ""

        def _detect_edition_wrapper() -> list[Finding]:
            nonlocal edition
            edition, findings = detect_warehouse_edition(
                spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id,
                cfg.sql_endpoint_id,
            )
            self._log(f"  Edition: {edition}")
            return findings

        pr = tracker.run_phase(
            "Phase 0a: Edition detection", _detect_edition_wrapper,
        )
        all_findings.extend(pr.findings)

        # Set user-facing item label based on detected edition
        is_sql_endpoint = edition == "LakeWarehouse"
        if is_sql_endpoint or cfg.sql_endpoint_id:
            cfg.item_label = "SQL endpoint"
        else:
            cfg.item_label = "warehouse"

        # ================================================================
        # REST API client — resolve token once for all REST phases
        # ================================================================
        rest_client: FabricRestClient | None = None
        any_rest_check = (
            cfg.check_workspace_roles
            or cfg.check_network_isolation
            or cfg.check_sql_audit
            or cfg.check_item_permissions
            or cfg.check_sensitivity_labels
            or cfg.check_role_alignment
            or cfg.check_onelake_data_access_roles
            or cfg.check_onelake_settings
            or cfg.check_auth_mode
        )
        if any_rest_check:
            rest_client = FabricRestClient(
                token=cfg.fabric_token,
                use_notebook_token=cfg.use_notebook_token,
                max_retries=5,
                verbose=cfg.verbose,
            )
            if not rest_client.is_available():
                print(
                    "  ℹ REST API checks skipped — no Fabric token "
                    "available (set fabric_token or run in a notebook)."
                )
                rest_client = None

        # Auto-resolve workspace_id from notebook context if not provided
        if rest_client and not cfg.workspace_id:
            auto_ws = FabricRestClient.get_current_workspace_id(spark)
            if auto_ws:
                cfg.workspace_id = auto_ws
                self._log(f"  Auto-detected workspace_id: {auto_ws}")
            else:
                print(
                    "  ℹ workspace_id not set and could not be auto-detected.\n"
                    "    Set workspace_id in config to enable REST API checks."
                )

        # ── Resolve workspace display name + capacity SKU ───────────
        ws_display_name = ""
        cap_sku = ""
        if rest_client and cfg.workspace_id:
            ws_display_name, cap_sku = rest_client.get_workspace_metadata(
                cfg.workspace_id
            )
            self._log(f"  Workspace name: {ws_display_name}")
            if cap_sku:
                self._log(f"  Capacity SKU : {cap_sku}")

        # ================================================================
        # Resolve warehouse_id from warehouse_name if needed
        # ================================================================
        resolved_warehouse_id = cfg.warehouse_id or cfg.sql_endpoint_id
        resolved_warehouse_info: dict | None = None
        needs_warehouse = (
            cfg.check_sql_audit
            or cfg.check_item_permissions
            or cfg.check_sensitivity_labels
        )
        if rest_client and cfg.workspace_id and not resolved_warehouse_id and needs_warehouse:
            if is_sql_endpoint:
                self._log("Resolving SQL endpoint from endpoint name ...")
                try:
                    resolved_warehouse_info = (
                        rest_client.resolve_sql_endpoint(
                            cfg.workspace_id, cfg.warehouse_name,
                        )
                    )
                    if resolved_warehouse_info:
                        resolved_warehouse_id = resolved_warehouse_info.get("id", "")
                        self._log(
                            f"  Resolved (SQL Endpoint): {cfg.warehouse_name} → "
                            f"{resolved_warehouse_id}"
                        )
                    else:
                        self._log(
                            f"  ⚠ SQL endpoint '{cfg.warehouse_name}' not "
                            f"found in workspace {cfg.workspace_id}."
                        )
                except FabricRestError as exc:
                    self._log(f"  ⚠ Could not resolve SQL endpoint: {exc}")
            else:
                self._log("Resolving warehouse_id from warehouse_name ...")
                try:
                    resolved_warehouse_info = (
                        rest_client.resolve_warehouse(
                            cfg.workspace_id, cfg.warehouse_name,
                        )
                    )
                    if resolved_warehouse_info:
                        resolved_warehouse_id = resolved_warehouse_info.get("id", "")
                        self._log(
                            f"  Resolved: {cfg.warehouse_name} → "
                            f"{resolved_warehouse_id}"
                        )
                    else:
                        self._log(
                            f"  ⚠ Warehouse '{cfg.warehouse_name}' not "
                            f"found in workspace {cfg.workspace_id}."
                        )
                except FabricRestError as exc:
                    self._log(f"  ⚠ Could not resolve warehouse_id: {exc}")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 0b: Auth mode detection (LakeWarehouse only)
        # ================================================================
        auth_mode = ""
        resolved_sql_endpoint_id = cfg.sql_endpoint_id or (
            resolved_warehouse_id if is_sql_endpoint else ""
        )

        if (
            cfg.check_auth_mode
            and is_sql_endpoint
            and rest_client
            and resolved_sql_endpoint_id
        ):
            def _detect_auth_mode_wrapper() -> list[Finding]:
                nonlocal auth_mode
                auth_mode, findings = detect_auth_mode(
                    rest_client, resolved_sql_endpoint_id, cfg,
                )
                self._log(f"  Auth mode: {auth_mode or '(unknown)'}")
                return findings

            pr = tracker.run_phase(
                "Phase 0b: Auth mode detection", _detect_auth_mode_wrapper,
            )
            all_findings.extend(pr.findings)
        elif cfg.check_auth_mode and is_sql_endpoint and not resolved_sql_endpoint_id:
            print("  ℹ Phase 0b: Auth mode detection — SKIPPED (sql_endpoint_id not resolved)")
            tracker.record(PhaseResult(name="Phase 0b: Auth mode detection", status=PHASE_SKIPPED, skip_reason="sql_endpoint_id not resolved"))
        elif cfg.check_auth_mode and not is_sql_endpoint:
            self._log("  Phase 0b: Auth mode detection — N/A (Warehouse edition)")

        user_identity_mode = auth_mode == "user_identity"

        if auth_mode:
            mode_desc = "User Identity" if user_identity_mode else "Delegated Identity"
            print(f"  Auth Mode : {mode_desc}")
            print()

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Workspace REST data — fetch once, share across phases
        # ================================================================
        workspace_role_assignments: list[dict] | None = None
        _needs_ws_roles = (
            cfg.check_workspace_roles
            or cfg.check_item_permissions
            or cfg.check_role_alignment
        )
        if _needs_ws_roles and rest_client and cfg.workspace_id:
            try:
                workspace_role_assignments = (
                    rest_client.get_workspace_role_assignments(cfg.workspace_id)
                )
            except FabricRestError:
                workspace_role_assignments = None

        # ================================================================
        # Phase 1: Workspace Roles (SEC-006) — REST API
        # ================================================================
        if cfg.check_workspace_roles and rest_client and cfg.workspace_id:
            pr = tracker.run_phase(
                "Phase 1: Workspace Roles",
                check_workspace_roles, rest_client, cfg.workspace_id, cfg,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_workspace_roles
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 1: Workspace Roles — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 1: Workspace Roles", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 2: Network Isolation (SEC-007) — REST API
        # ================================================================
        if cfg.check_network_isolation and rest_client and cfg.workspace_id:
            pr = tracker.run_phase(
                "Phase 2: Network Isolation",
                check_network_isolation, rest_client, cfg.workspace_id, cfg,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_network_isolation
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 2: Network Isolation — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 2: Network Isolation", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 3: OneLake Settings (SEC-013) — REST API (workspace-level)
        # ================================================================
        if cfg.check_onelake_settings and rest_client and cfg.workspace_id:
            pr = tracker.run_phase(
                "Phase 3: OneLake Settings",
                check_onelake_settings, rest_client, cfg.workspace_id, cfg,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_onelake_settings
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 3: OneLake Settings — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 3: OneLake Settings", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 4: SQL Audit Settings (SEC-008) — REST API
        # ================================================================
        if cfg.check_sql_audit and rest_client and cfg.workspace_id and resolved_warehouse_id:
            pr = tracker.run_phase(
                "Phase 4: SQL Audit Settings",
                check_sql_audit, rest_client, cfg.workspace_id,
                resolved_warehouse_id, cfg, is_sql_endpoint,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_sql_audit
                else "no REST token" if not rest_client
                else "workspace_id not set" if not cfg.workspace_id
                else "warehouse_id could not be resolved"
            )
            print(f"  ℹ Phase 4: SQL Audit Settings — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 4: SQL Audit Settings", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 5: Item Permissions (SEC-009) — Admin API
        # ================================================================
        if cfg.check_item_permissions and rest_client and cfg.workspace_id and resolved_warehouse_id:
            ws_principal_roles: dict[str, str] = {}
            if workspace_role_assignments:
                for a in workspace_role_assignments:
                    pid = a.get("principal", {}).get("id", "")
                    if pid:
                        ws_principal_roles[pid] = a.get("role", "")

            pr = tracker.run_phase(
                "Phase 5: Item Permissions",
                check_item_permissions, rest_client, cfg.workspace_id,
                resolved_warehouse_id, cfg, ws_principal_roles,
                is_sql_endpoint,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_item_permissions
                else "no REST token" if not rest_client
                else "workspace_id not set" if not cfg.workspace_id
                else "warehouse_id could not be resolved"
            )
            print(f"  ℹ Phase 5: Item Permissions — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 5: Item Permissions", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 6: Sensitivity Labels (SEC-010)
        # ================================================================
        if cfg.check_sensitivity_labels and rest_client and cfg.workspace_id:
            wh_info = resolved_warehouse_info
            if not wh_info and resolved_warehouse_id:
                try:
                    if is_sql_endpoint:
                        wh_info = rest_client.resolve_sql_endpoint(
                            cfg.workspace_id, cfg.warehouse_name,
                        )
                    else:
                        wh_info = rest_client.resolve_warehouse(
                            cfg.workspace_id, cfg.warehouse_name,
                        )
                except FabricRestError:
                    pass
            if not wh_info and resolved_warehouse_id:
                wh_info = {"id": resolved_warehouse_id, "displayName": cfg.warehouse_name}
            if wh_info:
                pr = tracker.run_phase(
                    "Phase 6: Sensitivity Labels",
                    check_sensitivity_labels, wh_info, cfg,
                )
                all_findings.extend(pr.findings)
            else:
                print(
                    "  ℹ Phase 6: Sensitivity Labels — SKIPPED "
                    "(warehouse could not be resolved)"
                )
                tracker.record(PhaseResult(name="Phase 6: Sensitivity Labels", status=PHASE_SKIPPED, skip_reason="warehouse could not be resolved"))
        else:
            reason = (
                "disabled in config" if not cfg.check_sensitivity_labels
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 6: Sensitivity Labels — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 6: Sensitivity Labels", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 7: OneLake Data Access Roles (SEC-012) — REST API
        #   Gated behind LakeWarehouse edition.
        #   In Delegated mode, findings are reported as INFO.
        # ================================================================
        is_onelake_security_supported = is_sql_endpoint
        onelake_roles: list[dict] | None = None
        onelake_role_names: list[str] | None = None

        resolved_lakehouse_id: str = ""
        if (
            is_onelake_security_supported
            and rest_client
            and cfg.workspace_id
            and (cfg.check_onelake_data_access_roles or cfg.check_onelake_security_sync)
        ):
            self._log("Resolving lakehouse_id for OneLake security APIs ...")
            try:
                lh_info = rest_client.resolve_lakehouse(
                    cfg.workspace_id, cfg.warehouse_name,
                )
                if lh_info:
                    resolved_lakehouse_id = lh_info.get("id", "")
                    self._log(
                        f"  Resolved lakehouse: {cfg.warehouse_name} → "
                        f"{resolved_lakehouse_id}"
                    )
                else:
                    self._log(
                        f"  ⚠ Lakehouse '{cfg.warehouse_name}' not "
                        f"found in workspace {cfg.workspace_id}."
                    )
            except FabricRestError as exc:
                self._log(f"  ⚠ Could not resolve lakehouse: {exc}")

        # Fetch roles from API once — shared by SEC-012 and SEC-014
        if (
            is_onelake_security_supported
            and rest_client
            and cfg.workspace_id
            and resolved_lakehouse_id
            and (cfg.check_onelake_data_access_roles or cfg.check_onelake_security_sync)
        ):
            try:
                onelake_roles = rest_client.list_data_access_roles(
                    cfg.workspace_id, resolved_lakehouse_id,
                )
                onelake_role_names = [
                    r.get("name", "<unnamed>") for r in (onelake_roles or [])
                ]
            except FabricRestError as exc:
                self._log(f"  ⚠ Could not fetch OneLake data access roles: {exc}")
                onelake_roles = None

        if (
            cfg.check_onelake_data_access_roles
            and is_onelake_security_supported
            and onelake_roles is not None
        ):
            pr = tracker.run_phase(
                "Phase 7: OneLake Data Access Roles",
                check_onelake_data_access_roles, onelake_roles,
                resolved_lakehouse_id, cfg,
                user_identity_mode=user_identity_mode,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_onelake_data_access_roles
                else "not supported (Warehouse edition)" if not is_onelake_security_supported
                else "no REST token" if not rest_client
                else "workspace_id not set" if not cfg.workspace_id
                else "lakehouse could not be resolved" if not resolved_lakehouse_id
                else "roles could not be fetched"
            )
            print(f"  ℹ Phase 7: OneLake Data Access Roles — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 7: OneLake Data Access Roles", status=PHASE_SKIPPED, skip_reason=reason))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # SQL T-SQL checks — Phases 8-14
        # ================================================================

        # ================================================================
        # Phase 8: Schema Permissions (SEC-001)
        #   Runs always, but downgrades table-level findings in
        #   User Identity mode.
        # ================================================================
        if cfg.check_schema_permissions:
            pr = tracker.run_phase(
                "Phase 8: Schema Permissions",
                check_schema_permissions, spark, cfg.warehouse_name, cfg,
                user_identity_mode=user_identity_mode,
            )
            all_findings.extend(pr.findings)
        else:
            print("Phase 8: Schema Permissions — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 8: Schema Permissions", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 9: Custom Roles (SEC-002)
        #   In User Identity mode, SQL custom roles are inactive —
        #   produce a single INFO finding instead of running the check.
        # ================================================================
        if cfg.check_custom_roles:
            if user_identity_mode:
                all_findings.append(Finding(
                    level=LEVEL_INFO,
                    category=CATEGORY_ROLES,
                    check_name="custom_roles_inactive",
                    object_name=cfg.warehouse_name,
                    message=(
                        "Custom database roles are inactive in User "
                        "Identity mode."
                    ),
                    detail=(
                        "Table-level access is controlled by OneLake "
                        "security roles. SQL custom roles and their "
                        "members are not enforced."
                    ),
                ))
                print("Phase 9: Custom Roles — INFO (inactive in User Identity mode)")
                tracker.record(PhaseResult(name="Phase 9: Custom Roles", status=PHASE_COMPLETED))
            else:
                pr = tracker.run_phase(
                    "Phase 9: Custom Roles",
                    check_custom_roles, spark, cfg.warehouse_name, cfg,
                )
                all_findings.extend(pr.findings)
        else:
            print("Phase 9: Custom Roles — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 9: Custom Roles", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Scope resolution for table-level checks
        # ================================================================
        _any_table_checks = cfg.check_rls or cfg.check_cls or cfg.check_ddm
        _skip_table_checks = False

        if _any_table_checks and not user_identity_mode:
            scope = resolve_table_scope(
                spark, cfg.warehouse_name,
                cfg.schema_names, cfg.table_names,
                check_labels="RLS, CLS, DDM",
                workspace_id=cfg.workspace_id,
                warehouse_id=cfg.warehouse_id,
                log_fn=self._log,
            )
            _skip_table_checks = scope.skip

        # ================================================================
        # Phase 10: Row-Level Security (SEC-003)
        #   Inactive in User Identity mode.
        # ================================================================
        if cfg.check_rls:
            if user_identity_mode:
                all_findings.append(Finding(
                    level=LEVEL_INFO,
                    category=CATEGORY_RLS,
                    check_name="rls_inactive",
                    object_name=cfg.warehouse_name,
                    message=(
                        "SQL Row-Level Security is inactive in User "
                        "Identity mode."
                    ),
                    detail=(
                        "Table-level access is controlled by OneLake "
                        "security roles. SQL RLS security policies are "
                        "not enforced."
                    ),
                ))
                print("Phase 10: Row-Level Security — INFO (inactive in User Identity mode)")
                tracker.record(PhaseResult(name="Phase 10: Row-Level Security", status=PHASE_COMPLETED))
            elif not _skip_table_checks:
                pr = tracker.run_phase(
                    "Phase 10: Row-Level Security",
                    check_row_level_security, spark, cfg.warehouse_name, cfg,
                )
                all_findings.extend(pr.findings)
            else:
                print("Phase 10: Row-Level Security — SKIPPED (no tables in scope)")
                tracker.record(PhaseResult(name="Phase 10: Row-Level Security", status=PHASE_SKIPPED, skip_reason="no tables in scope"))
        else:
            print("Phase 10: Row-Level Security — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 10: Row-Level Security", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 11: Column-Level Security (SEC-004)
        #   Inactive in User Identity mode.
        # ================================================================
        if cfg.check_cls:
            if user_identity_mode:
                all_findings.append(Finding(
                    level=LEVEL_INFO,
                    category=CATEGORY_CLS,
                    check_name="cls_inactive",
                    object_name=cfg.warehouse_name,
                    message=(
                        "SQL Column-Level Security is inactive in User "
                        "Identity mode."
                    ),
                    detail=(
                        "Table-level access is controlled by OneLake "
                        "security roles. SQL CLS restrictions are "
                        "not enforced."
                    ),
                ))
                print("Phase 11: Column-Level Security — INFO (inactive in User Identity mode)")
                tracker.record(PhaseResult(name="Phase 11: Column-Level Security", status=PHASE_COMPLETED))
            elif not _skip_table_checks:
                pr = tracker.run_phase(
                    "Phase 11: Column-Level Security",
                    check_column_level_security, spark, cfg.warehouse_name, cfg,
                )
                all_findings.extend(pr.findings)
            else:
                print("Phase 11: Column-Level Security — SKIPPED (no tables in scope)")
                tracker.record(PhaseResult(name="Phase 11: Column-Level Security", status=PHASE_SKIPPED, skip_reason="no tables in scope"))
        else:
            print("Phase 11: Column-Level Security — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 11: Column-Level Security", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 12: Dynamic Data Masking (SEC-005)
        #   DDM is a SQL-engine feature enforced at query time,
        #   regardless of auth mode.
        # ================================================================
        if cfg.check_ddm:
            if not _skip_table_checks:
                pr = tracker.run_phase(
                    "Phase 12: Dynamic Data Masking",
                    check_dynamic_data_masking, spark, cfg.warehouse_name, cfg,
                )
                all_findings.extend(pr.findings)
            else:
                print("Phase 12: Dynamic Data Masking — SKIPPED (no tables in scope)")
                tracker.record(PhaseResult(name="Phase 12: Dynamic Data Masking", status=PHASE_SKIPPED, skip_reason="no tables in scope"))
        else:
            print("Phase 12: Dynamic Data Masking — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 12: Dynamic Data Masking", status=PHASE_SKIPPED, skip_reason="disabled in config"))

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 13: Security Sync Health (SEC-014) — T-SQL
        #   Only meaningful in User Identity + LakeWarehouse edition.
        # ================================================================
        if (
            cfg.check_onelake_security_sync
            and is_onelake_security_supported
        ):
            pr = tracker.run_phase(
                "Phase 13: Security Sync Health",
                check_onelake_security_sync, spark, cfg.warehouse_name,
                cfg, onelake_role_names,
            )
            all_findings.extend(pr.findings)
        else:
            reason = (
                "disabled in config" if not cfg.check_onelake_security_sync
                else "not supported (Warehouse edition)"
            )
            print(f"  ℹ Phase 13: Security Sync Health — SKIPPED ({reason})")
            tracker.record(PhaseResult(name="Phase 13: Security Sync Health", status=PHASE_SKIPPED, skip_reason=reason))

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 14: Role Alignment (SEC-011) — T-SQL + REST cross-ref
        # ================================================================
        if cfg.check_role_alignment:
            pr = tracker.run_phase(
                "Phase 14: Role Alignment",
                check_role_alignment, spark, cfg.warehouse_name, cfg,
                workspace_role_assignments,
            )
            all_findings.extend(pr.findings)
        else:
            print("  ℹ Phase 14: Role Alignment — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 14: Role Alignment", status=PHASE_SKIPPED, skip_reason="disabled in config"))

        # ================================================================
        # Build summary and reports
        # ================================================================
        _t0 = time.perf_counter()
        self._log("Generating reports ...")

        summary = CheckSummary(
            warehouse_name=cfg.warehouse_name,
            warehouse_edition=edition,
            auth_mode=auth_mode,
            workspace_display_name=ws_display_name,
            capacity_sku=cap_sku,
            findings=all_findings,
        )

        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        text_report = generate_text_report(summary)
        markdown_report = generate_markdown_report(summary)
        html_report = generate_html_report(summary, captured_at=captured_at)

        _total_elapsed = time.perf_counter() - _run_start

        # Phase timings (verbose only)
        tracker.print_summary(verbose=cfg.verbose, total_elapsed=_total_elapsed)

        print("\n\u2713 Security Check Advisor completed successfully.")
        print("  Use  displayHTML(result.html_report)  for a rich HTML view.")
        print("  Use  result.save('report.html')  to save as HTML (default).")
        print("  Other formats: result.save('report.md', format='md') or result.save('report.txt', format='txt')")

        return SecurityCheckResult(
            findings=all_findings,
            summary=summary,
            text_report=text_report,
            markdown_report=markdown_report,
            html_report=html_report,
            captured_at=captured_at,
        )

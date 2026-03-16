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
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

from .config import SecurityCheckConfig
from .findings import Finding, CheckSummary
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
from ..performance_check.checks.warehouse_type import detect_warehouse_edition
from .report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
)
from ...core.report import save_report
from ...core.fabric_rest_client import FabricRestClient, FabricRestError


# ------------------------------------------------------------------
# Result container
# ------------------------------------------------------------------

@dataclass
class SecurityCheckResult:
    """Container for all outputs produced by a security check run."""

    #: All findings from every check.
    findings: List[Finding] = field(default_factory=list)

    #: Aggregated summary.
    summary: Optional[CheckSummary] = None

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

    def _log_findings_detail(self, findings: List[Finding]) -> None:
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

    def _run_phase(
        self,
        phase_label: str,
        check_fn,
        *args: object,
        **kwargs: object,
    ) -> Tuple[List[Finding], float]:
        """Run a check phase with timing and verbose severity logging.

        Parameters
        ----------
        phase_label : str
            Printed to stdout, e.g. ``"Phase 1: Checking permissions …"``.
        check_fn : callable
            The check function.  Must return ``List[Finding]``.
        *args, **kwargs
            Forwarded to *check_fn*.

        Returns
        -------
        tuple[list[Finding], float]
            The findings produced and elapsed wall-clock seconds.
        """
        _t0 = time.perf_counter()
        print(f"{phase_label} ...")
        findings = check_fn(*args, **kwargs)
        _ct = sum(1 for f in findings if f.is_critical)
        _ht = sum(1 for f in findings if f.is_high)
        _mt = sum(1 for f in findings if f.is_medium)
        _lt = sum(1 for f in findings if f.is_low)
        _it = sum(1 for f in findings if f.is_info)
        self._log(f"  Findings: {_ct} critical, {_ht} high, {_mt} medium, {_lt} low, {_it} info")
        self._log_findings_detail(findings)
        elapsed = time.perf_counter() - _t0
        self._log(f"  ⏱ {phase_label.split(':')[0]} completed in {elapsed:.2f}s")
        return findings, elapsed

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

        # Resolve sql_endpoint_id → warehouse_id so all downstream
        # check functions that use config.warehouse_id work for SQL
        # Endpoints without changes.
        if not cfg.warehouse_id and cfg.sql_endpoint_id:
            cfg.warehouse_id = cfg.sql_endpoint_id

        # Verbose: show active configuration
        self._log_header("Configuration")
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
        self._log_footer()

        _run_start = time.perf_counter()
        _phase_timings: Dict[str, float] = {}
        all_findings: List[Finding] = []

        # ================================================================
        # Phase 0: Detect warehouse edition
        # ================================================================
        _phase_start = time.perf_counter()
        print("Phase 0: Detecting warehouse edition ...")
        edition, _ = detect_warehouse_edition(
            spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id,
            cfg.sql_endpoint_id,
        )
        print(f"  Edition: {edition}")
        _phase_timings["Phase 0: Edition detection"] = time.perf_counter() - _phase_start

        # Set user-facing item label based on detected edition
        if edition == "LakeWarehouse" or cfg.sql_endpoint_id:
            cfg.item_label = "SQL endpoint"
        else:
            cfg.item_label = "warehouse"

        # ================================================================
        # Phase 1: Schema Permissions (SEC-001)
        # ================================================================
        if cfg.check_schema_permissions:
            findings, elapsed = self._run_phase(
                "Phase 1: Analysing schema permissions",
                check_schema_permissions, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 1: Schema Permissions"] = elapsed
        else:
            self._log("Phase 1: Schema permissions — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 2: Custom Roles (SEC-002)
        # ================================================================
        if cfg.check_custom_roles:
            findings, elapsed = self._run_phase(
                "Phase 2: Analysing custom roles",
                check_custom_roles, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 2: Custom Roles"] = elapsed
        else:
            self._log("Phase 2: Custom roles — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 3: Row-Level Security (SEC-003)
        # ================================================================
        if cfg.check_rls:
            findings, elapsed = self._run_phase(
                "Phase 3: Analysing Row-Level Security",
                check_row_level_security, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 3: Row-Level Security"] = elapsed
        else:
            self._log("Phase 3: Row-Level Security — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 4: Column-Level Security (SEC-004)
        # ================================================================
        if cfg.check_cls:
            findings, elapsed = self._run_phase(
                "Phase 4: Analysing Column-Level Security",
                check_column_level_security, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 4: Column-Level Security"] = elapsed
        else:
            self._log("Phase 4: Column-Level Security — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 5: Dynamic Data Masking (SEC-005)
        # ================================================================
        if cfg.check_ddm:
            findings, elapsed = self._run_phase(
                "Phase 5: Analysing Dynamic Data Masking",
                check_dynamic_data_masking, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 5: Dynamic Data Masking"] = elapsed
        else:
            self._log("Phase 5: Dynamic Data Masking — SKIPPED (disabled in config)")

        # ================================================================
        # REST API checks — resolve token once for all REST phases
        # ================================================================
        rest_client: FabricRestClient | None = None
        any_rest_check = (
            cfg.check_workspace_roles
            or cfg.check_network_isolation
            or cfg.check_sql_audit
            or cfg.check_item_permissions
            or cfg.check_sensitivity_labels
            or cfg.check_role_alignment
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
                print(f"  ℹ Auto-detected workspace_id: {auto_ws}")
            else:
                print(
                    "  ℹ workspace_id not set and could not be auto-detected.\n"
                    "    Set workspace_id in config to enable REST API checks."
                )

        # Resolve warehouse_id from warehouse_name if needed
        is_sql_endpoint = edition == "LakeWarehouse"
        resolved_warehouse_id = cfg.warehouse_id
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
        # Phase 6: Workspace Roles (SEC-006) — REST API
        # ================================================================
        # Fetch workspace role assignments once — shared by SEC-006,
        # SEC-009, and SEC-011.
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

        if cfg.check_workspace_roles and rest_client and cfg.workspace_id:
            findings, elapsed = self._run_phase(
                "Phase 6: Analysing workspace role assignments",
                check_workspace_roles, rest_client, cfg.workspace_id, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 6: Workspace Roles"] = elapsed
        else:
            reason = (
                "disabled in config" if not cfg.check_workspace_roles
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 6: Workspace roles — SKIPPED ({reason})")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 7: Network Isolation (SEC-007) — REST API
        # ================================================================
        if cfg.check_network_isolation and rest_client and cfg.workspace_id:
            findings, elapsed = self._run_phase(
                "Phase 7: Analysing network isolation",
                check_network_isolation, rest_client, cfg.workspace_id, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 7: Network Isolation"] = elapsed
        else:
            reason = (
                "disabled in config" if not cfg.check_network_isolation
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 7: Network isolation — SKIPPED ({reason})")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 8: SQL Audit Settings (SEC-008) — REST API
        # ================================================================
        if cfg.check_sql_audit and rest_client and cfg.workspace_id and resolved_warehouse_id:
            findings, elapsed = self._run_phase(
                "Phase 8: Analysing SQL audit settings",
                check_sql_audit, rest_client, cfg.workspace_id,
                resolved_warehouse_id, cfg, is_sql_endpoint,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 8: SQL Audit Settings"] = elapsed
        else:
            reason = (
                "disabled in config" if not cfg.check_sql_audit
                else "no REST token" if not rest_client
                else "workspace_id not set" if not cfg.workspace_id
                else "warehouse_id could not be resolved"
            )
            print(f"  ℹ Phase 8: SQL audit settings — SKIPPED ({reason})")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 9: Item Permissions (SEC-009) — Admin API
        # ================================================================
        if cfg.check_item_permissions and rest_client and cfg.workspace_id and resolved_warehouse_id:
            # Build principal→role lookup for SEC-009 cross-reference
            ws_principal_roles: dict[str, str] = {}
            if workspace_role_assignments:
                for a in workspace_role_assignments:
                    pid = a.get("principal", {}).get("id", "")
                    if pid:
                        ws_principal_roles[pid] = a.get("role", "")

            findings, elapsed = self._run_phase(
                "Phase 9: Analysing item permissions (Admin API)",
                check_item_permissions, rest_client, cfg.workspace_id,
                resolved_warehouse_id, cfg, ws_principal_roles,
                is_sql_endpoint,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 9: Item Permissions"] = elapsed
        else:
            reason = (
                "disabled in config" if not cfg.check_item_permissions
                else "no REST token" if not rest_client
                else "workspace_id not set" if not cfg.workspace_id
                else "warehouse_id could not be resolved"
            )
            print(f"  ℹ Phase 9: Item permissions — SKIPPED ({reason})")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 10: Sensitivity Labels (SEC-010) — from list_warehouses
        #           or list_sql_endpoints (LakeWarehouse edition)
        # ================================================================
        if cfg.check_sensitivity_labels and rest_client and cfg.workspace_id:
            wh_info = resolved_warehouse_info
            # If warehouse_id was provided manually (not resolved via
            # list_warehouses / list_sql_endpoints), we lack the
            # sensitivityLabel field.  Attempt a fresh resolution;
            # fall back to a minimal dict which will cause the check
            # to report "no label found."
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
                findings, elapsed = self._run_phase(
                    "Phase 10: Checking sensitivity labels",
                    check_sensitivity_labels, wh_info, cfg,
                )
                all_findings.extend(findings)
                _phase_timings["Phase 10: Sensitivity Labels"] = elapsed
            else:
                print(
                    "  ℹ Phase 10: Sensitivity labels — SKIPPED "
                    "(warehouse could not be resolved)"
                )
        else:
            reason = (
                "disabled in config" if not cfg.check_sensitivity_labels
                else "no REST token" if not rest_client
                else "workspace_id not set"
            )
            print(f"  ℹ Phase 10: Sensitivity labels — SKIPPED ({reason})")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 11: Role Alignment (SEC-011) — T-SQL + REST cross-ref
        # ================================================================
        if cfg.check_role_alignment:
            findings, elapsed = self._run_phase(
                "Phase 11: Analysing role alignment",
                check_role_alignment, spark, cfg.warehouse_name, cfg,
                workspace_role_assignments,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 11: Role Alignment"] = elapsed
        else:
            print("  ℹ Phase 11: Role alignment — SKIPPED (disabled in config)")

        # ================================================================
        # Build summary and reports
        # ================================================================
        _t0 = time.perf_counter()
        self._log("Generating reports ...")

        summary = CheckSummary(
            warehouse_name=cfg.warehouse_name,
            warehouse_edition=edition,
            findings=all_findings,
        )

        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        text_report = generate_text_report(summary)
        markdown_report = generate_markdown_report(summary)
        html_report = generate_html_report(summary, captured_at=captured_at)

        _total_elapsed = time.perf_counter() - _run_start

        # Print summary
        print()
        print("═" * 52)
        print(f"  SUMMARY")
        print(f"  CRITICAL  : {summary.critical_count}")
        print(f"  HIGH      : {summary.high_count}")
        print(f"  MEDIUM    : {summary.medium_count}")
        print(f"  LOW       : {summary.low_count}")
        print(f"  INFO      : {summary.info_count}")
        print(f"  Total     : {len(all_findings)} findings")
        print("═" * 52)
        print()

        # Phase timings (verbose only)
        if cfg.verbose:
            self._log("Phase Timings:")
            for phase, elapsed in _phase_timings.items():
                self._log(f"  {phase:<40} {elapsed:.2f}s")
            self._log(f"  {'Total':<40} {_total_elapsed:.2f}s")

        return SecurityCheckResult(
            findings=all_findings,
            summary=summary,
            text_report=text_report,
            markdown_report=markdown_report,
            html_report=html_report,
            captured_at=captured_at,
        )

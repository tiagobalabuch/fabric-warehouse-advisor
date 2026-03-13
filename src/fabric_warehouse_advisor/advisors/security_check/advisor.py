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
from .report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
)
from ...core.report import save_report


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

        # Verbose: show active configuration
        self._log_header("Configuration")
        self._log_kv("Tables filter", cfg.table_names or "(all)")
        self._log_kv("check_schema_permissions", cfg.check_schema_permissions)
        self._log_kv("check_custom_roles", cfg.check_custom_roles)
        self._log_kv("check_rls", cfg.check_rls)
        self._log_kv("check_cls", cfg.check_cls)
        self._log_kv("check_ddm", cfg.check_ddm)
        self._log_footer()

        _run_start = time.perf_counter()
        _phase_timings: Dict[str, float] = {}
        all_findings: List[Finding] = []

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
        # Build summary and reports
        # ================================================================
        _t0 = time.perf_counter()
        self._log("Generating reports ...")

        summary = CheckSummary(
            warehouse_name=cfg.warehouse_name,
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

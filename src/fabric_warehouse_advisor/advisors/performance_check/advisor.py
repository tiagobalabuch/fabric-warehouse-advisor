"""
Fabric Warehouse Advisor — Performance Check Advisor (Main Orchestrator)
=========================================================================
Provides the :class:`PerformanceCheckAdvisor` class that runs all
configured performance checks and collects findings.

Usage
-----
::

    from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

    config = PerformanceCheckConfig(warehouse_name="MyWarehouse")
    advisor = PerformanceCheckAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
    # result.findings -> list of Finding
    # result.has_critical -> bool
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import SparkSession

from .config import PerformanceCheckConfig
from .findings import Finding, CheckSummary
from .checks.warehouse_type import detect_warehouse_edition
from .checks.data_types import check_data_types
from .checks.caching import check_caching
from .checks.vorder import check_vorder
from .checks.statistics import check_statistics
from .checks.collation import check_collation
from .checks.query_regression import check_query_regression
from .report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
)
from ...core.report import save_report
from ...core.warehouse_reader import read_warehouse_query
from ...core.scope_resolver import resolve_table_scope
from ...core.phase_tracker import PhaseTracker, PhaseResult, PHASE_COMPLETED, PHASE_SKIPPED, PHASE_FAILED


# ------------------------------------------------------------------
# Safe display() wrapper
# ------------------------------------------------------------------

def _display(df) -> None:
    """Call the Fabric/Databricks ``display()`` built-in if available."""
    try:
        _builtin_display = display  # type: ignore[name-defined]
        _builtin_display(df)
    except NameError:
        if hasattr(df, 'show'):
            df.show(100, truncate=False)


# ------------------------------------------------------------------
# Result container
# ------------------------------------------------------------------

@dataclass
class PerformanceCheckResult:
    """Container for all outputs produced by a performance check run."""

    #: All findings from every check.
    findings: List[Finding] = field(default_factory=list)

    #: Aggregated summary.
    summary: Optional[CheckSummary] = None

    #: Detected warehouse edition.
    warehouse_edition: str = ""

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

class PerformanceCheckAdvisor:
    """Orchestrates the full performance-check advisory pipeline.

    Parameters
    ----------
    spark : SparkSession
        An active PySpark session.
    config : PerformanceCheckConfig
        Configuration object.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: PerformanceCheckConfig | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or PerformanceCheckConfig()

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

    # ---- public API ----

    def run(self) -> PerformanceCheckResult:
        """Execute all enabled performance checks.

        Returns
        -------
        PerformanceCheckResult
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
        print("║  Fabric Warehouse Performance Check Advisor      ║")
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
        self._log_kv("check_data_types", cfg.check_data_types)
        self._log_kv("check_caching", cfg.check_caching)
        self._log_kv("check_statistics", cfg.check_statistics)
        self._log_kv("check_vorder", cfg.check_vorder)
        self._log_kv("check_collation", cfg.check_collation)
        self._log_kv("check_query_regression", cfg.check_query_regression)
        self._log_footer()

        _run_start = time.perf_counter()
        tracker = PhaseTracker(
            log_fn=self._log,
            log_findings_fn=self._log_findings_detail,
        )
        all_findings: List[Finding] = []
        total_tables = 0
        total_columns = 0

        # ================================================================
        # Phase 0: Warehouse Type Detection
        # ================================================================
        _t0 = time.perf_counter()
        print("Phase 0: Detecting warehouse edition ...")
        edition, edition_findings = detect_warehouse_edition(
            spark, cfg.warehouse_name,
            cfg.workspace_id, cfg.warehouse_id, cfg.sql_endpoint_id,
        )
        all_findings.extend(edition_findings)
        self._log(f"  Edition: {edition}")
        _p0_elapsed = time.perf_counter() - _t0
        tracker.record(PhaseResult(name="Phase 0: Edition detection", elapsed=_p0_elapsed, findings=edition_findings))
        self._log(f"  ⏱ Phase 0 completed in {_p0_elapsed:.2f}s")
        self._log_findings_detail(edition_findings)
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 1: Caching Analysis  (warehouse-level)
        # ================================================================
        if cfg.check_caching:
            pr = tracker.run_phase(
                "Phase 1: Caching",
                check_caching, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(pr.findings)
        else:
            print("Phase 1: Caching — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 1: Caching", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 2: V-Order Check  (warehouse-level)
        # ================================================================
        if cfg.check_vorder:
            pr = tracker.run_phase(
                "Phase 2: V-Order",
                check_vorder, spark, cfg.warehouse_name, cfg,
                edition=edition,
            )
            all_findings.extend(pr.findings)
        else:
            print("Phase 2: V-Order — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 2: V-Order", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 3: Query Regression Detection  (warehouse-level)
        # ================================================================
        if cfg.check_query_regression:
            pr = tracker.run_phase(
                "Phase 3: Query Regression",
                check_query_regression, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(pr.findings)
        else:
            print("Phase 3: Query Regression — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 3: Query Regression", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Scope resolution
        # ================================================================
        _any_table_checks = (
            cfg.check_data_types or cfg.check_statistics or cfg.check_collation
        )
        _skip_table_checks = False

        if _any_table_checks:
            scope = resolve_table_scope(
                spark, cfg.warehouse_name,
                cfg.schema_names, cfg.table_names,
                check_labels="Data Types, Statistics, Collation",
                workspace_id=cfg.workspace_id,
                warehouse_id=cfg.warehouse_id,
                log_fn=self._log,
            )
            _skip_table_checks = scope.skip
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 4: Data Type Analysis  (table-scoped)
        # ================================================================
        if cfg.check_data_types and not _skip_table_checks:
            def _check_data_types_wrapper(
                _spark, _warehouse, _cfg,
            ) -> List[Finding]:
                nonlocal total_tables, total_columns
                findings, tables, columns = check_data_types(
                    _spark, _warehouse, _cfg,
                )
                total_tables = max(total_tables, tables)
                total_columns = max(total_columns, columns)
                self._log(f"  Tables: {tables} | Columns: {columns}")
                return findings

            pr = tracker.run_phase(
                "Phase 4: Data Types",
                _check_data_types_wrapper, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(pr.findings)
        elif not cfg.check_data_types:
            print("Phase 4: Data Types — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 4: Data Types", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        else:
            print("Phase 4: Data Types — SKIPPED (no tables in scope)")
            tracker.record(PhaseResult(name="Phase 4: Data Types", status=PHASE_SKIPPED, skip_reason="no tables in scope"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 5: Statistics Health  (table-scoped per-table analysis;
        #           warehouse-level config checks always run)
        # ================================================================
        if cfg.check_statistics:
            _stats_note = "config only — no tables in scope" if _skip_table_checks else ""
            pr = tracker.run_phase(
                "Phase 5: Statistics",
                check_statistics, spark, cfg.warehouse_name, cfg,
                note=_stats_note,
                skip_table_checks=_skip_table_checks,
            )
            all_findings.extend(pr.findings)
        else:
            print("Phase 5: Statistics — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 5: Statistics", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ================================================================
        # Phase 6: Collation Mismatch  (table-scoped)
        # ================================================================
        if cfg.check_collation and not _skip_table_checks:
            pr = tracker.run_phase(
                "Phase 6: Collation",
                check_collation, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(pr.findings)
        elif not cfg.check_collation:
            print("Phase 6: Collation — SKIPPED (disabled in config)")
            tracker.record(PhaseResult(name="Phase 6: Collation", status=PHASE_SKIPPED, skip_reason="disabled in config"))
        else:
            print("Phase 6: Collation — SKIPPED (no tables in scope)")
            tracker.record(PhaseResult(name="Phase 6: Collation", status=PHASE_SKIPPED, skip_reason="no tables in scope"))

        # ================================================================
        # Build summary and reports
        # ================================================================
        _t0 = time.perf_counter()
        self._log("Generating reports ...")

        summary = CheckSummary(
            warehouse_name=cfg.warehouse_name,
            warehouse_edition=edition,
            total_tables_analyzed=total_tables,
            total_columns_analyzed=total_columns,
            findings=all_findings,
        )

        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        text_report = generate_text_report(summary)
        markdown_report = generate_markdown_report(summary)
        html_report = generate_html_report(summary, captured_at=captured_at)

        _total_elapsed = time.perf_counter() - _run_start

        # Phase timings (verbose only)
        tracker.print_summary(verbose=cfg.verbose, total_elapsed=_total_elapsed)

        print("\n\u2713 Performance Check Advisor completed successfully.")
        print("  Use  displayHTML(result.html_report)  for a rich HTML view.")
        print("  Use  result.save('report.html')  to save as HTML (default).")
        print("  Other formats: result.save('report.md', format='md') or result.save('report.txt', format='txt')")

        return PerformanceCheckResult(
            findings=all_findings,
            summary=summary,
            warehouse_edition=edition,
            text_report=text_report,
            markdown_report=markdown_report,
            html_report=html_report,
            captured_at=captured_at,
        )

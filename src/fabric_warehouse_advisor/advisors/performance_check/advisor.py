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
from typing import Dict, List, Optional, Tuple

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
            Printed to stdout, e.g. ``"Phase 2: Analysing caching …"``.
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
        _phase_timings: Dict[str, float] = {}
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
        _phase_timings["Phase 0: Edition detection"] = time.perf_counter() - _t0
        self._log(f"  ⏱ Phase 0 completed in {_phase_timings['Phase 0: Edition detection']:.2f}s")
        self._log_findings_detail(edition_findings)
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)
        # ================================================================
        # Phase 1: Data Type Analysis
        # ================================================================
        if cfg.check_data_types:
            _t0 = time.perf_counter()
            print("Phase 1: Analysing data types ...")
            dt_findings, dt_tables, dt_columns = check_data_types(
                spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(dt_findings)
            total_tables = max(total_tables, dt_tables)
            total_columns = max(total_columns, dt_columns)
            _ct = sum(1 for f in dt_findings if f.is_critical)
            _ht = sum(1 for f in dt_findings if f.is_high)
            _mt = sum(1 for f in dt_findings if f.is_medium)
            _lt = sum(1 for f in dt_findings if f.is_low)
            _it = sum(1 for f in dt_findings if f.is_info)
            self._log(f"  Tables: {dt_tables} | Columns: {dt_columns}")
            self._log(f"  Findings: {_ct} critical, {_ht} high, {_mt} medium, {_lt} low, {_it} info")
            self._log_findings_detail(dt_findings)
            _phase_timings["Phase 1: Data types"] = time.perf_counter() - _t0
            self._log(f"  ⏱ Phase 1 completed in {_phase_timings['Phase 1: Data types']:.2f}s")
        else:
            self._log("Phase 1: Data type analysis — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)
        # ================================================================
        # Phase 2: Caching Analysis
        # ================================================================
        if cfg.check_caching:
            findings, elapsed = self._run_phase(
                "Phase 2: Analysing caching configuration",
                check_caching, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 2: Caching"] = elapsed
        else:
            self._log("Phase 2: Caching analysis — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)
        # ================================================================
        # Phase 3: V-Order Check
        # ================================================================
        if cfg.check_vorder:
            findings, elapsed = self._run_phase(
                "Phase 3: Checking V-Order optimization",
                check_vorder, spark, cfg.warehouse_name, cfg,
                edition=edition,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 3: V-Order"] = elapsed
        else:
            self._log("Phase 3: V-Order check — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)
        # ================================================================
        # Phase 4: Statistics Health
        # ================================================================
        if cfg.check_statistics:
            findings, elapsed = self._run_phase(
                "Phase 4: Analysing statistics health",
                check_statistics, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 4: Statistics"] = elapsed
        else:
            self._log("Phase 4: Statistics health — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)
        # ================================================================
        # Phase 5: Collation Mismatch
        # ================================================================
        if cfg.check_collation:
            findings, elapsed = self._run_phase(
                "Phase 5: Checking collation consistency",
                check_collation, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 5: Collation"] = elapsed
        else:
            self._log("Phase 5: Collation check — SKIPPED (disabled in config)")
        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)
        # ================================================================
        # Phase 6: Query Regression Detection
        # ================================================================
        if cfg.check_query_regression:
            findings, elapsed = self._run_phase(
                "Phase 6: Detecting query regressions",
                check_query_regression, spark, cfg.warehouse_name, cfg,
            )
            all_findings.extend(findings)
            _phase_timings["Phase 6: Query regression"] = elapsed
        else:
            self._log("Phase 6: Query regression — SKIPPED (disabled in config)")

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
        if cfg.verbose:
            self._log("Phase Timings:")
            for phase, elapsed in _phase_timings.items():
                self._log(f"  {phase:<40} {elapsed:.2f}s")
            self._log(f"  {'Total':<40} {_total_elapsed:.2f}s")

        print("\n\u2713 Performance Check Advisor completed successfully.")
        print("  Use  displayHTML(result.html_report)  for a rich HTML view.")
        print("  Use  result.save('path.html')  to save the report to a file.")

        return PerformanceCheckResult(
            findings=all_findings,
            summary=summary,
            warehouse_edition=edition,
            text_report=text_report,
            markdown_report=markdown_report,
            html_report=html_report,
            captured_at=captured_at,
        )

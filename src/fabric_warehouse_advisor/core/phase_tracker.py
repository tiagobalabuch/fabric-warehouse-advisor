"""
Fabric Warehouse Advisor — Phase Tracker
==========================================
Structured tracking for multi-phase advisor pipelines.

Each advisor run consists of numbered phases.  :class:`PhaseTracker`
collects a :class:`PhaseResult` for every phase and provides a
unified summary renderer that replaces the ad-hoc ``_phase_timings``
dictionaries previously maintained in each advisor.

The :meth:`PhaseTracker.run_phase` method is the single entry-point
for executing a check function with timing, verbose logging, and
graceful failure handling.  It replaces the duplicate ``_run_phase()``
helpers that previously lived in each advisor class.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .findings import Finding


# ------------------------------------------------------------------
# Phase status constants
# ------------------------------------------------------------------

PHASE_COMPLETED = "completed"
PHASE_SKIPPED = "skipped"
PHASE_FAILED = "failed"


# ------------------------------------------------------------------
# PhaseResult — one per phase
# ------------------------------------------------------------------

@dataclass
class PhaseResult:
    """Captures the outcome of a single advisor phase.

    Parameters
    ----------
    name : str
        Display label, e.g. ``"Phase 0: Edition detection"``.
    status : str
        One of :data:`PHASE_COMPLETED`, :data:`PHASE_SKIPPED`,
        or :data:`PHASE_FAILED`.
    elapsed : float
        Wall-clock seconds.  ``0.0`` when the phase was skipped.
    findings : list[Finding]
        Findings produced (empty for skipped phases or phases that
        don't produce findings, like Data Clustering).
    skip_reason : str
        Human-readable reason when *status* is ``PHASE_SKIPPED``,
        e.g. ``"disabled in config"`` or ``"no tables in scope"``.
        Empty string when the phase ran normally.
    note : str
        Optional annotation appended in summaries, e.g.
        ``"config only — no tables in scope"``.
    """

    name: str
    status: str = PHASE_COMPLETED
    elapsed: float = 0.0
    findings: List[Finding] = field(default_factory=list)
    skip_reason: str = ""
    note: str = ""

    # -- convenience properties --

    @property
    def is_completed(self) -> bool:
        return self.status == PHASE_COMPLETED

    @property
    def is_skipped(self) -> bool:
        return self.status == PHASE_SKIPPED

    @property
    def is_failed(self) -> bool:
        return self.status == PHASE_FAILED

    @property
    def finding_counts(self) -> Dict[str, int]:
        """Return ``{critical, high, medium, low, info}`` counts."""
        return {
            "critical": sum(1 for f in self.findings if f.is_critical),
            "high": sum(1 for f in self.findings if f.is_high),
            "medium": sum(1 for f in self.findings if f.is_medium),
            "low": sum(1 for f in self.findings if f.is_low),
            "info": sum(1 for f in self.findings if f.is_info),
        }


# ------------------------------------------------------------------
# PhaseTracker — collects PhaseResults and renders summary
# ------------------------------------------------------------------

class PhaseTracker:
    """Collects :class:`PhaseResult` objects and renders summaries.

    Usage inside an advisor's ``run()`` method::

        tracker = PhaseTracker()

        # After each phase completes:
        tracker.record(PhaseResult(
            name="Phase 0: Edition detection",
            elapsed=0.42,
            findings=edition_findings,
        ))

        # After a skipped phase:
        tracker.record(PhaseResult(
            name="Phase 1: Caching",
            status=PHASE_SKIPPED,
            skip_reason="disabled in config",
        ))

        # At the end:
        tracker.print_summary(verbose=cfg.verbose)
    """

    def __init__(
        self,
        log_fn: Optional[Callable[[str], None]] = None,
        log_findings_fn: Optional[Callable[[list], None]] = None,
    ) -> None:
        self._phases: List[PhaseResult] = []
        self._log_fn = log_fn
        self._log_findings_fn = log_findings_fn

    # -- internal logging --

    def _log(self, msg: str) -> None:
        if self._log_fn is not None:
            self._log_fn(msg)

    def _log_findings(self, findings: list) -> None:
        if self._log_findings_fn is not None:
            self._log_findings_fn(findings)

    # -- recording --

    def record(self, result: PhaseResult) -> None:
        """Append a phase result."""
        self._phases.append(result)

    def run_phase(
        self,
        name: str,
        check_fn: Callable[..., List[Finding]],
        *args: object,
        note: str = "",
        **kwargs: object,
    ) -> PhaseResult:
        """Execute *check_fn*, record the result, and return it.

        This is the standard entry-point for running a check phase.
        It handles timing, verbose severity logging, findings-detail
        logging, and graceful failure (``PHASE_FAILED``) so that the
        pipeline can continue even if a single phase raises.

        Parameters
        ----------
        name : str
            Display label recorded in the :class:`PhaseResult`, e.g.
            ``"Phase 1: Caching"``.
        check_fn : callable
            The check function.  Must return ``List[Finding]``.
        note : str
            Optional note stored on the :class:`PhaseResult`.
        *args, **kwargs
            Forwarded to *check_fn*.

        Returns
        -------
        PhaseResult
            The result is also appended to the tracker automatically.
        """
        phase_label = name
        _t0 = time.perf_counter()
        print(f"{phase_label} ...")

        try:
            findings = check_fn(*args, **kwargs)
        except Exception as exc:
            elapsed = time.perf_counter() - _t0
            short_err = f"{type(exc).__name__}: {exc}"
            print(f"  ⚠ {phase_label} FAILED — {short_err}")
            self._log(f"  {traceback.format_exc()}")
            result = PhaseResult(
                name=name,
                status=PHASE_FAILED,
                elapsed=elapsed,
                skip_reason=short_err,
            )
            self._phases.append(result)
            return result

        # Severity counts (verbose)
        _ct = sum(1 for f in findings if f.is_critical)
        _ht = sum(1 for f in findings if f.is_high)
        _mt = sum(1 for f in findings if f.is_medium)
        _lt = sum(1 for f in findings if f.is_low)
        _it = sum(1 for f in findings if f.is_info)
        self._log(
            f"  Findings: {_ct} critical, {_ht} high, "
            f"{_mt} medium, {_lt} low, {_it} info"
        )
        self._log_findings(findings)

        elapsed = time.perf_counter() - _t0
        self._log(f"  ⏱ {name} completed in {elapsed:.2f}s")

        result = PhaseResult(
            name=name,
            elapsed=elapsed,
            findings=findings,
            note=note,
        )
        self._phases.append(result)
        return result

    @property
    def phases(self) -> List[PhaseResult]:
        """All recorded phases in order."""
        return list(self._phases)

    # -- aggregation --

    @property
    def total_elapsed(self) -> float:
        """Sum of elapsed seconds across all phases."""
        return sum(p.elapsed for p in self._phases)

    @property
    def all_findings(self) -> List[Finding]:
        """Flattened list of findings from every phase."""
        result: List[Finding] = []
        for p in self._phases:
            result.extend(p.findings)
        return result

    @property
    def completed_count(self) -> int:
        return sum(1 for p in self._phases if p.is_completed)

    @property
    def skipped_count(self) -> int:
        return sum(1 for p in self._phases if p.is_skipped)

    @property
    def failed_count(self) -> int:
        return sum(1 for p in self._phases if p.is_failed)

    # -- summary rendering --

    def print_summary(
        self,
        verbose: bool = False,
        total_elapsed: Optional[float] = None,
        show_pct: bool = False,
    ) -> None:
        """Print the phase summary.

        When *verbose* is ``True``, a compact summary table is printed
        showing each phase's timing, finding counts, and overall totals.
        When *verbose* is ``False``, nothing is printed.

        Parameters
        ----------
        verbose : bool
            If ``True``, print the phase summary table.
        total_elapsed : float, optional
            Override for total run time.  If not given, the sum of
            individual phase elapsed times is used.
        show_pct : bool
            If ``True``, show each phase's percentage of total time
            (used by Data Clustering Advisor).
        """
        if not verbose:
            return

        _total = total_elapsed if total_elapsed is not None else self.total_elapsed
        name_width = 40

        self._print_compact_summary(_total, name_width, show_pct=show_pct)

    # -- compact summary (always visible) --

    def _print_compact_summary(
        self,
        total_elapsed: float,
        name_width: int = 40,
        show_pct: bool = False,
    ) -> None:
        """Print a concise phase summary table — verbose only."""
        sep = "─"
        print()
        print(f"  {sep * 58}")
        print("  Phase Summary")
        print(f"  {sep * 58}")

        for p in self._phases:
            label = p.name
            if p.note:
                label = f"{p.name} ({p.note})"

            if p.is_skipped:
                reason = f"SKIPPED ({p.skip_reason})" if p.skip_reason else "SKIPPED"
                print(f"  {label:<{name_width}} {reason}")
            elif p.is_failed:
                reason = f"FAILED ({p.skip_reason})" if p.skip_reason else "FAILED"
                print(f"  {label:<{name_width}} {reason}")
            else:
                # Elapsed + pct (optional) + finding counts on the same line
                pct_str = ""
                if show_pct and total_elapsed > 0:
                    pct = p.elapsed / total_elapsed * 100
                    pct_str = f"  ({pct:>5.1f}%)"
                parts = [f"{p.elapsed:.2f}s{pct_str}"]
                counts = p.finding_counts
                finding_parts = []
                for level in ("critical", "high", "medium", "low", "info"):
                    n = counts[level]
                    if n > 0:
                        finding_parts.append(f"{n} {level}")
                if finding_parts:
                    parts.append(", ".join(finding_parts))
                print(f"  {label:<{name_width}} {'   '.join(parts)}")

        # Footer totals
        print(f"  {sep * 58}")
        status_parts = [f"{len(self._phases)} phases"]
        if self.failed_count:
            status_parts.append(f"{self.failed_count} failed")
        if self.skipped_count:
            status_parts.append(f"{self.skipped_count} skipped")

        # Aggregate finding counts across all phases
        all_counts: Dict[str, int] = {
            "critical": 0, "high": 0, "medium": 0, "low": 0, "info": 0,
        }
        for p in self._phases:
            for level, n in p.finding_counts.items():
                all_counts[level] += n
        finding_parts = []
        for level in ("critical", "high", "medium", "low", "info"):
            n = all_counts[level]
            if n > 0:
                finding_parts.append(f"{n} {level}")

        summary_line = f"Total: {total_elapsed:.2f}s | {' | '.join(status_parts)}"
        if finding_parts:
            summary_line += f" | {', '.join(finding_parts)}"
        print(f"  {summary_line}")
        print(f"  {sep * 58}")

"""
Fabric Warehouse Advisor — Core Findings
==========================================
Shared finding and summary types used by all advisor modules.

Each check produces zero or more :class:`Finding` instances.  The
advisor collects them into a flat list that the report layer can
group, sort, and render in any format.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


# Severity levels (ordered by impact)
LEVEL_INFO = "INFO"
LEVEL_LOW = "LOW"
LEVEL_MEDIUM = "MEDIUM"
LEVEL_HIGH = "HIGH"
LEVEL_CRITICAL = "CRITICAL"

# Ordered list for iteration (most severe first)
SEVERITY_ORDER = [LEVEL_CRITICAL, LEVEL_HIGH, LEVEL_MEDIUM, LEVEL_LOW, LEVEL_INFO]


@dataclass
class Finding:
    """Single finding produced by an advisor check.

    Parameters
    ----------
    level : str
        Severity: ``"CRITICAL"``, ``"HIGH"``, ``"MEDIUM"``,
        ``"LOW"``, or ``"INFO"``.
    category : str
        Check category (e.g. ``"data_types"``, ``"caching"``).
    check_name : str
        Machine-readable identifier such as ``"varchar_max_detected"``.
    object_name : str
        Fully-qualified object the finding applies to, e.g.
        ``"dbo.FactSales.Description"`` or the database name.
    message : str
        Human-readable one-line summary.
    detail : str
        Additional context (current value, comparison, etc.).
    recommendation : str
        Actionable guidance to resolve the finding.
    sql_fix : str
        Optional T-SQL statement that addresses the issue.
    """

    level: str
    category: str
    check_name: str
    object_name: str
    message: str
    detail: str = ""
    recommendation: str = ""
    sql_fix: str = ""

    @property
    def is_critical(self) -> bool:
        return self.level == LEVEL_CRITICAL

    @property
    def is_high(self) -> bool:
        return self.level == LEVEL_HIGH

    @property
    def is_medium(self) -> bool:
        return self.level == LEVEL_MEDIUM

    @property
    def is_low(self) -> bool:
        return self.level == LEVEL_LOW

    @property
    def is_info(self) -> bool:
        return self.level == LEVEL_INFO

    @property
    def is_actionable(self) -> bool:
        """True if the finding requires user action (CRITICAL/HIGH/MEDIUM/LOW)."""
        return self.level in (LEVEL_CRITICAL, LEVEL_HIGH, LEVEL_MEDIUM, LEVEL_LOW)


@dataclass
class CheckSummary:
    """Aggregated summary across all findings from a single advisor run."""

    warehouse_name: str = ""
    warehouse_edition: str = ""
    total_tables_analyzed: int = 0
    total_columns_analyzed: int = 0
    findings: List[Finding] = field(default_factory=list)

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

    @property
    def has_critical(self) -> bool:
        return self.critical_count > 0

    @property
    def has_high(self) -> bool:
        return self.high_count > 0

    def findings_by_category(self, category: str) -> List[Finding]:
        return [f for f in self.findings if f.category == category]

    def findings_by_level(self, level: str) -> List[Finding]:
        return [f for f in self.findings if f.level == level]

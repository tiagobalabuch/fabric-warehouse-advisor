"""
Fabric Warehouse Advisor — Scope Resolver
==========================================
Shared scope-resolution logic used by advisors that have table-scoped
checks.  When ``schema_names`` or ``table_names`` filters are configured,
this module queries ``sys.tables`` once and determines whether any user
tables match the filters, avoiding unnecessary SQL round-trips for
downstream phases.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from .warehouse_reader import read_warehouse_query


@dataclass
class ScopeResult:
    """Outcome of scope resolution."""

    skip: bool = False
    """``True`` when no tables matched — downstream checks should be skipped."""

    matched: set[tuple[str, str]] = field(default_factory=set)
    """Set of ``(schema_name, table_name)`` pairs that matched the filters."""

    elapsed: float = 0.0
    """Wall-clock time spent on scope resolution (seconds)."""


def resolve_table_scope(
    spark,
    warehouse_name: str,
    schema_names: list[str] | None,
    table_names: list[str] | None,
    check_labels: str,
    workspace_id: str | None = None,
    warehouse_id: str | None = None,
    log_fn: Callable[..., None] | None = None,
) -> ScopeResult:
    """Resolve which tables are in scope.

    If *schema_names* and *table_names* are both ``None`` (or empty),
    no resolution is needed and the function returns immediately with
    ``skip=False``.

    Parameters
    ----------
    spark
        Active Spark session.
    warehouse_name
        Fabric warehouse name.
    schema_names, table_names
        Filter lists from advisor config.  May be ``None`` or empty.
    check_labels
        Human-readable list of check names shown in the skip message
        (e.g. ``"Data Types, Statistics, Collation"``).
    workspace_id, warehouse_id
        Optional overrides for cross-workspace access.
    log_fn
        Verbose-logging callback (only called when verbose is on).

    Returns
    -------
    ScopeResult
        Contains ``skip``, ``matched``, and ``elapsed``.
    """
    if not schema_names and not table_names:
        return ScopeResult()

    _log = log_fn or (lambda *_a, **_kw: None)
    t0 = time.perf_counter()

    try:
        tbl_df = read_warehouse_query(
            spark,
            warehouse_name,
            "SELECT SCHEMA_NAME(schema_id) AS schema_name, "
            "name AS table_name FROM sys.tables",
            workspace_id,
            warehouse_id,
        )
        rows = tbl_df.collect()

        matched: set[tuple[str, str]] = set()
        schema_filter = (
            {x.lower() for x in schema_names} if schema_names else None
        )
        table_filter = set(table_names) if table_names else None

        for r in rows:
            s, t = r["schema_name"], r["table_name"]
            if schema_filter and s.lower() not in schema_filter:
                continue
            if table_filter and t not in table_filter and f"{s}.{t}" not in table_filter:
                continue
            matched.add((s, t))

        elapsed = time.perf_counter() - t0

        if not matched:
            scope_parts = []
            if schema_names:
                scope_parts.append(f"schema_names={schema_names}")
            if table_names:
                scope_parts.append(f"table_names={table_names}")
            scope_msg = ", ".join(scope_parts)
            print(
                f"  ℹ No tables match the configured scope ({scope_msg}).\n"
                f"    Skipping table-scoped checks ({check_labels})."
            )
            _log(f"  ⏱ Scope resolution in {elapsed:.2f}s")
            return ScopeResult(skip=True, matched=set(), elapsed=elapsed)

        _log(f"  Scope resolved: {len(matched)} table(s) match filters.")
        _log(f"  ⏱ Scope resolution in {elapsed:.2f}s")
        return ScopeResult(skip=False, matched=matched, elapsed=elapsed)

    except Exception:
        # If scope query fails, run the checks normally.
        elapsed = time.perf_counter() - t0
        _log(f"  ⏱ Scope resolution in {elapsed:.2f}s")
        return ScopeResult(skip=False, elapsed=elapsed)

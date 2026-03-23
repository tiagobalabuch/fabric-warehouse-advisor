"""
Fabric Warehouse Advisor — Performance Check Reports
======================================================
Generates text, Markdown, and HTML reports from :class:`CheckSummary`.
"""

from __future__ import annotations

from typing import Dict, List

from .findings import (
    CheckSummary,
    Finding,
    LEVEL_CRITICAL,
    LEVEL_HIGH,
    LEVEL_MEDIUM,
    LEVEL_LOW,
    LEVEL_INFO,
    SEVERITY_ORDER,
    CATEGORY_WAREHOUSE_TYPE,
    CATEGORY_DATA_TYPES,
    CATEGORY_CACHING,
    CATEGORY_STATISTICS,
    CATEGORY_VORDER,
    CATEGORY_COLLATION,
    CATEGORY_QUERY_REGRESSION,
)


# -- Category display metadata ------------------------------------------

_CATEGORY_LABELS = {
    CATEGORY_WAREHOUSE_TYPE: "Warehouse Type",
    CATEGORY_DATA_TYPES: "Data Types",
    CATEGORY_CACHING: "Caching",
    CATEGORY_VORDER: "V-Order Optimization",
    CATEGORY_STATISTICS: "Statistics Health",
    CATEGORY_COLLATION: "Collation Consistency",
    CATEGORY_QUERY_REGRESSION: "Query Regression",
}

_CATEGORY_ORDER = [
    # Query Performance
    CATEGORY_CACHING,
    CATEGORY_STATISTICS,
    CATEGORY_QUERY_REGRESSION,
    # Storage & Layout
    CATEGORY_VORDER,
    CATEGORY_DATA_TYPES,
    CATEGORY_COLLATION,
]

_CATEGORY_SECTIONS = {
    CATEGORY_CACHING: "Query Performance",
    CATEGORY_STATISTICS: "Query Performance",
    CATEGORY_QUERY_REGRESSION: "Query Performance",
    CATEGORY_VORDER: "Storage & Layout",
    CATEGORY_DATA_TYPES: "Storage & Layout",
    CATEGORY_COLLATION: "Storage & Layout",
}

_LEVEL_ICONS = {
    LEVEL_CRITICAL: "❌",
    LEVEL_HIGH: "🔴",
    LEVEL_MEDIUM: "🟠",
    LEVEL_LOW: "🟡",
    LEVEL_INFO: "✅",
}


# ======================================================================
# TEXT REPORT
# ======================================================================

def generate_text_report(summary: CheckSummary) -> str:
    """Generate a plain-text report with Unicode box drawing."""
    lines: List[str] = []
    w = 72

    lines.append("╔" + "═" * w + "╗")
    lines.append("║" + " Fabric Warehouse Performance Check Report".center(w) + "║")
    lines.append("╚" + "═" * w + "╝")
    lines.append("")
    lines.append(f"  Warehouse : {summary.warehouse_name}")
    lines.append(f"  Edition   : {summary.warehouse_edition}")
    lines.append(f"  Tables    : {summary.total_tables_analyzed}")
    lines.append(f"  Columns   : {summary.total_columns_analyzed}")
    lines.append("")

    # Summary bar
    lines.append("━" * w)
    lines.append("  SUMMARY")
    lines.append(f"    ❌ CRITICAL : {summary.critical_count}")
    lines.append(f"    🔴 HIGH     : {summary.high_count}")
    lines.append(f"    🟠 MEDIUM   : {summary.medium_count}")
    lines.append(f"    🟡 LOW      : {summary.low_count}")
    lines.append(f"    ✅ INFO     : {summary.info_count}")
    lines.append(f"    Total      : {len(summary.findings)} findings")
    lines.append("━" * w)
    lines.append("")

    # Findings grouped by category
    for cat in _CATEGORY_ORDER:
        cat_findings = summary.findings_by_category(cat)
        if not cat_findings:
            continue

        label = _CATEGORY_LABELS.get(cat, cat)
        crit = sum(1 for f in cat_findings if f.is_critical)
        high = sum(1 for f in cat_findings if f.is_high)
        med = sum(1 for f in cat_findings if f.is_medium)
        low = sum(1 for f in cat_findings if f.is_low)
        info = sum(1 for f in cat_findings if f.is_info)

        lines.append(f"━━━ {label.upper()} ({crit} critical, {high} high, {med} medium, {low} low, {info} info) ━━━")
        lines.append("")

        if cat == CATEGORY_QUERY_REGRESSION:
            lines.append("  ⚠️  This analysis runs warehouse-wide and is not filtered")
            lines.append("     by schema/table selections.")
            lines.append("")

        # Group same-check findings for deduplication
        grouped = _group_findings(cat_findings)
        for check_name, findings in grouped.items():
            if len(findings) > 5:
                # Summarize large groups
                lines.append(f"  {_LEVEL_ICONS.get(findings[0].level, '•')} "
                             f"{findings[0].message} (×{len(findings)} occurrences)")
                lines.append(f"    Affected objects:")
                for f in findings[:5]:
                    lines.append(f"      • {f.object_name}")
                if len(findings) > 5:
                    lines.append(f"      ... and {len(findings) - 5} more")
                if findings[0].recommendation:
                    lines.append(f"    → {findings[0].recommendation}")
                lines.append("")
            else:
                for f in findings:
                    icon = _LEVEL_ICONS.get(f.level, "•")
                    lines.append(f"  {icon} [{f.object_name}] {f.message}")
                    if f.detail:
                        lines.append(f"    {f.detail}")
                    if f.recommendation:
                        lines.append(f"    → {f.recommendation}")
                    if f.sql_fix:
                        lines.append(f"    SQL: {f.sql_fix}")
                    lines.append("")

    # Footer
    lines.append("═" * w)
    lines.append("  Generated by Fabric Warehouse Advisor")
    lines.append("═" * w)

    return "\n".join(lines)


# ======================================================================
# MARKDOWN REPORT
# ======================================================================

def generate_markdown_report(summary: CheckSummary) -> str:
    """Generate a Markdown report."""
    lines: List[str] = []

    lines.append("# 🔍 Fabric Warehouse Performance Check Report")
    lines.append("")
    lines.append(f"| Property | Value |")
    lines.append(f"|----------|-------|")
    lines.append(f"| Warehouse | `{summary.warehouse_name}` |")
    lines.append(f"| Edition | `{summary.warehouse_edition}` |")
    lines.append(f"| Tables Analysed | {summary.total_tables_analyzed} |")
    lines.append(f"| Columns Analysed | {summary.total_columns_analyzed} |")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Level | Count |")
    lines.append(f"|-------|-------|")
    lines.append(f"| ❌ Critical | **{summary.critical_count}** |")
    lines.append(f"| 🔴 High | **{summary.high_count}** |")
    lines.append(f"| 🟠 Medium | **{summary.medium_count}** |")
    lines.append(f"| 🟡 Low | **{summary.low_count}** |")
    lines.append(f"| ✅ Info | {summary.info_count} |")
    lines.append(f"| **Total** | **{len(summary.findings)}** |")
    lines.append("")

    # Findings by category
    for cat in _CATEGORY_ORDER:
        cat_findings = summary.findings_by_category(cat)
        if not cat_findings:
            continue

        label = _CATEGORY_LABELS.get(cat, cat)
        crit = sum(1 for f in cat_findings if f.is_critical)
        high = sum(1 for f in cat_findings if f.is_high)

        badge = ""
        if crit > 0:
            badge = f" — ❌ {crit} critical"
        elif high > 0:
            badge = f" — 🔴 {high} high"

        lines.append(f"## {label}{badge}")
        lines.append("")

        if cat == CATEGORY_QUERY_REGRESSION:
            lines.append("> **⚠️ Note:** This analysis runs warehouse-wide and is not filtered by schema/table selections.")
            lines.append("")

        grouped = _group_findings(cat_findings)
        for check_name, findings in grouped.items():
            if len(findings) > 5:
                f0 = findings[0]
                lines.append(f"### {_LEVEL_ICONS.get(f0.level, '•')} {f0.message} (×{len(findings)})")
                lines.append("")
                if f0.detail:
                    lines.append(f"> {f0.detail}")
                    lines.append("")
                lines.append("<details>")
                lines.append(f"<summary>Affected objects ({len(findings)})</summary>")
                lines.append("")
                for f in findings:
                    lines.append(f"- `{f.object_name}`")
                lines.append("")
                lines.append("</details>")
                lines.append("")
                if f0.recommendation:
                    lines.append(f"**Recommendation:** {f0.recommendation}")
                    lines.append("")
                if f0.sql_fix:
                    lines.append("```sql")
                    lines.append(f0.sql_fix)
                    lines.append("```")
                    lines.append("")
            else:
                for f in findings:
                    icon = _LEVEL_ICONS.get(f.level, "•")
                    lines.append(f"- {icon} **`{f.object_name}`** — {f.message}")
                    if f.detail:
                        lines.append(f"  > {f.detail}")
                    if f.recommendation:
                        lines.append(f"  - **Recommendation:** {f.recommendation}")
                    if f.sql_fix:
                        lines.append(f"  ```sql")
                        lines.append(f"  {f.sql_fix}")
                        lines.append(f"  ```")
                    lines.append("")

    # Footer
    lines.append("---")
    lines.append("*Generated by Fabric Warehouse Advisor*")

    return "\n".join(lines)


# ======================================================================
# HTML REPORT
# ======================================================================

def generate_html_report(summary: CheckSummary, captured_at: str | None = None) -> str:
    """Generate a self-contained HTML report with sidebar dashboard layout."""
    from datetime import datetime, timezone
    from ...core.html_template import (
        esc, html_open, html_close, render_sidebar, render_main_open,
        render_severity_stats, render_info_stats, severity_pill,
        render_sql_block, render_footer,
    )
    from ...core.icon_data import ICON_WARNING

    _HTML_WARN = f'<img src="{ICON_WARNING}" width="20" height="20" alt="warning" style="vertical-align:middle">'

    if captured_at is None:
        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    total = len(summary.findings)
    h: List[str] = []

    # ── Document open ───────────────────────────────────────────────
    h.append(html_open("Performance Advisor Dashboard | Fabric Warehouse"))

    # ── Build tab list ──────────────────────────────────────────────
    active_cats = [
        (cat, _CATEGORY_LABELS.get(cat, cat))
        for cat in _CATEGORY_ORDER
        if summary.findings_by_category(cat)
    ]

    # Build tabs with section dividers for the sidebar
    tabs: list = []
    last_section = ""
    pane_idx = 0
    for cat, label in active_cats:
        section = _CATEGORY_SECTIONS.get(cat, "")
        if section and section != last_section:
            tabs.append(section)  # plain string = section divider
            last_section = section
        tabs.append((f"pane-{pane_idx}", label))
        pane_idx += 1

    # Determine badge label based on edition
    badge_label = "SQL Endpoint" if summary.warehouse_edition == "LakeWarehouse" else "Warehouse"

    # ── Sidebar ─────────────────────────────────────────────────
    h.append(render_sidebar(
        brand_text="Performance",
        report_type="performance",
        warehouse_name=summary.warehouse_name,
        tabs=tabs,
        generated_at=captured_at,
        badge_label=badge_label,
        workspace_display_name=summary.workspace_display_name,
        capacity_sku=summary.capacity_sku,
    ))

    # ── Main content ────────────────────────────────────────────
    h.append(render_main_open(
        "Advisor Dashboard",
        "Analysis of warehouse performance patterns and optimizations.",
    ))

    # Metadata info row
    h.append(render_info_stats([
        ("Edition", summary.warehouse_edition, "stat-primary"),
        ("Tables", summary.total_tables_analyzed, "stat-primary"),
        ("Columns", summary.total_columns_analyzed, "stat-primary"),
    ]))

    # Severity stat cards (clickable filters)
    h.append(render_severity_stats(
        summary.critical_count, summary.high_count,
        summary.medium_count, summary.low_count,
        summary.info_count, total,
    ))

    if not active_cats:
        h.append(
            '<div style="text-align:center;padding:48px;color:var(--text-muted);">'
            'No findings to display.</div>'
        )
        h.append(render_footer("Generated by Fabric Warehouse Advisor"))
        h.append(html_close())
        return "\n".join(h)

    # ── Tab panes ───────────────────────────────────────────────────
    for idx, (cat, label) in enumerate(active_cats):
        cat_findings = summary.findings_by_category(cat)
        active_cls = " active" if idx == 0 else ""
        hidden = "" if idx == 0 else ' hidden'

        h.append(
            f'<div class="tab-pane{active_cls}" id="pane-{idx}" '
            f'role="tabpanel"{hidden}>'
        )
        h.append(f'<h2 class="print-title">{esc(label)}</h2>')

        if cat in (CATEGORY_QUERY_REGRESSION, CATEGORY_VORDER):
            h.append(
                '<div class="warn-box">'
                f'{_HTML_WARN} This analysis runs warehouse-wide and is '
                'not filtered by schema/table selections.'
                '</div>'
            )

        h.append('<div class="table-container"><div class="table-scroll">')
        h.append('<table>')
        h.append(
            '<thead><tr>'
            '<th class="col-level" data-sort="level">Level '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th data-sort="text">Object '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th data-sort="text">Rule '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th>Finding</th>'
            '<th>Recommendation</th>'
            '</tr></thead>'
        )
        h.append('<tbody>')

        grouped = _group_findings(cat_findings)
        for check_name, findings in grouped.items():
            if len(findings) > 10:
                f0 = findings[0]
                lc = f0.level.lower()
                h.append(
                    f'<tr data-level="{lc}">'
                    f'<td class="col-level">{severity_pill(f0.level)}</td>'
                    f'<td><em>{len(findings)} objects</em></td>'
                    f'<td><code>{esc(f0.check_name)}</code></td>'
                    f'<td>{esc(f0.message)}'
                )
                if f0.detail:
                    h.append(f'<small>{esc(f0.detail)}</small>')
                h.append(
                    f'<details class="sql-details" style="margin-top:8px">'
                    f'<summary>Show all {len(findings)} objects</summary>'
                    f'<div class="object-list-container"><div class="object-grid">'
                )
                for f in findings:
                    h.append(f'<span class="obj-pill">{esc(f.object_name)}</span>')
                h.append('</div></div></details></td>')
                rec = esc(f0.recommendation) if f0.recommendation else ""
                all_sql = []
                for f in findings:
                    if f.sql_fix and f.sql_fix not in all_sql:
                        all_sql.append(f.sql_fix)
                if all_sql:
                    rec += render_sql_block("\n\n".join(all_sql))
                h.append(f'<td>{rec}</td></tr>')
            else:
                for f in findings:
                    lc = f.level.lower()
                    h.append(
                        f'<tr data-level="{lc}">'
                        f'<td class="col-level">{severity_pill(f.level)}</td>'
                        f'<td><code>{esc(f.object_name)}</code></td>'
                        f'<td><code>{esc(f.check_name)}</code></td>'
                        f'<td>{esc(f.message)}'
                    )
                    if f.detail:
                        h.append(f'<small>{esc(f.detail)}</small>')
                    h.append('</td>')
                    rec = esc(f.recommendation) if f.recommendation else ""
                    if f.sql_fix:
                        rec += render_sql_block(f.sql_fix)
                    h.append(f'<td>{rec}</td></tr>')

        h.append('</tbody></table>')
        h.append('</div></div>')  # table-scroll + table-container
        h.append('</div>')  # tab-pane

    # ── Footer ──────────────────────────────────────────────────────
    h.append(render_footer("Generated by Fabric Warehouse Advisor"))
    h.append(html_close())
    return "\n".join(h)


# -- Helpers -------------------------------------------------------------

def _group_findings(findings: List[Finding]) -> Dict[str, List[Finding]]:
    """Group findings by check_name, preserving order."""
    grouped: Dict[str, List[Finding]] = {}
    for f in findings:
        grouped.setdefault(f.check_name, []).append(f)
    return grouped


def _esc(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

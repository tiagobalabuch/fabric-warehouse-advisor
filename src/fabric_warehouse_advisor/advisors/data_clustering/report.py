"""
Fabric Warehouse Advisor — Data Clustering Report Generator
===================================================
Produces visually rich, human-readable summaries and actionable output
from the data clustering scoring results.

Three output formats are available:

* **Text**     – for ``print()`` in a Fabric notebook cell.
* **Markdown** – for saving as ``.md`` or piping through a Markdown renderer.
* **HTML**     – for ``displayHTML()`` in Fabric notebooks (renders natively).
"""

from __future__ import annotations

import html as _html
from datetime import datetime, timezone
from typing import List

from .scoring import ColumnScore, TableRecommendation


# ══════════════════════════════════════════════════════════════════════
# Unicode icons used throughout reports
# ══════════════════════════════════════════════════════════════════════

_ICON_CHECK   = "\u2705"          # ✅
_ICON_WARN    = "\u26a0\ufe0f"    # ⚠️
_ICON_CROSS   = "\u274c"          # ❌
_ICON_STAR    = "\u2b50"          # ⭐
_ICON_ROCKET  = "\U0001f680"      # 🚀
_ICON_MAG     = "\U0001f50d"      # 🔍
_ICON_BAR     = "\U0001f4ca"      # 📊
_ICON_GEAR    = "\u2699\ufe0f"    # ⚙️
_ICON_DB      = "\U0001f4be"      # 💾
_ICON_BOLT    = "\u26a1"          # ⚡
_ICON_TABLE   = "\U0001f4cb"      # 📋
_ICON_KEY     = "\U0001f511"      # 🔑
_ICON_BULB    = "\U0001f4a1"      # 💡
_ICON_LOCK    = "\U0001f512"      # 🔒
_ICON_DIAMOND = "\U0001f538"      # 🔸  (Medium cardinality)
_ICON_MINUS   = "\u2796"          # ➖  (Not recommended)


# ── helper functions ────────────────────────────────────────────────

def _score_bar(score: int, width: int = 20) -> str:
    """Return a text progress-bar for a 0-100 score."""
    filled = int(score / 100 * width)
    empty = width - filled
    return f"[{'█' * filled}{'░' * empty}] {score}"


def _score_emoji(score: int, min_score: int, recommendation: str = "") -> str:
    """Return an icon matching the recommendation label."""
    rec_upper = recommendation.upper()
    if "NOT RECOMMENDED" in rec_upper:
        return _ICON_MINUS
    if score >= min_score + 20:
        return _ICON_STAR
    if score >= min_score:
        return _ICON_CHECK
    return ""


def _cardinality_icon(level: str) -> str:
    lev = level.upper()
    if lev == "HIGH":
        return _ICON_BOLT
    if lev == "MEDIUM":
        return _ICON_DIAMOND
    if lev == "LOW":
        return _ICON_CROSS
    return ""


def _cardinality_display(level: str) -> str:
    """Return the full display name for a cardinality level."""
    mapping = {"HIGH": "High", "MID": "Medium", "MEDIUM": "Medium", "LOW": "Low"}
    return mapping.get(level.upper(), level)


def _ratio_pct(ratio: float) -> str:
    """Format a cardinality ratio as a percentage string."""
    if ratio < 0:
        return "N/A"
    return f"{ratio * 100:.2f}%"


def _type_display(cs: ColumnScore) -> str:
    if cs.data_type in ("decimal", "numeric"):
        return f"{cs.data_type}({cs.precision})"
    if cs.data_type in ("char", "varchar"):
        ml = "max" if cs.max_length == -1 else str(cs.max_length)
        return f"{cs.data_type}({ml})"
    return cs.data_type


# ══════════════════════════════════════════════════════════════════════
# TEXT REPORT
# ══════════════════════════════════════════════════════════════════════

def generate_text_report(
    recommendations: List[TableRecommendation],
    min_score: int = 40,
    captured_at: str | None = None,
    warehouse_name: str = "",
) -> str:
    """Generate a visually rich text report for a Fabric notebook cell.

    Box-drawing uses only left borders to avoid emoji display-width
    misalignment across different terminals and browsers.
    """
    if captured_at is None:
        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    L: list[str] = []

    # ── Banner ──────────────────────────────────────────────────────
    L.append("")
    L.append(f"  {'═' * 58}")
    L.append(f"  {_ICON_ROCKET}  FABRIC WAREHOUSE DATA CLUSTERING ADVISOR")
    if warehouse_name:
        L.append(f"     Warehouse: {warehouse_name}")
    L.append(f"     Generated: {captured_at}")
    L.append(f"  {'═' * 58}")
    L.append("")

    total_tables = len(recommendations)
    tables_with_recs = sum(1 for r in recommendations if r.recommended_columns)
    tables_clustered = sum(
        1 for r in recommendations if r.currently_clustered_columns
    )

    # ── Executive Summary ───────────────────────────────────────────
    L.append(f"  {_ICON_BAR}  EXECUTIVE SUMMARY")
    L.append(f"  {'─' * 50}")
    L.append(f"    {_ICON_TABLE}  Tables analysed              : {total_tables}")
    L.append(f"    {_ICON_STAR}  Tables with recommendations  : {tables_with_recs}")
    L.append(f"    {_ICON_LOCK}  Tables already clustered      : {tables_clustered}")
    L.append(f"    {_ICON_GEAR}  Minimum score threshold       : {min_score}")
    L.append("")

    if not recommendations:
        L.append(f"  {_ICON_WARN}  No candidate columns met the scoring threshold.")
        L.append("     Ensure Query Insights is enabled and queries with WHERE")
        L.append("     filters have been running against the warehouse.")
        return "\n".join(L)

    # ── Per-table sections ──────────────────────────────────────────
    L.append(f"  {_ICON_MAG}  TABLE-LEVEL RECOMMENDATIONS")
    L.append(f"  {'═' * 72}")

    for idx, rec in enumerate(recommendations, 1):
        L.append("")
        L.append(f"  ┌─ {_ICON_DB} [{rec.schema_name}].[{rec.table_name}]")
        L.append(f"  │  Rows: {rec.row_count:,}")

        if rec.currently_clustered_columns:
            L.append(
                f"  │  {_ICON_KEY} Current CLUSTER BY: "
                f"{', '.join(rec.currently_clustered_columns)}"
            )
        else:
            L.append(f"  │  {_ICON_KEY} Current CLUSTER BY: (none)")

        if hasattr(rec, "warnings") and rec.warnings:
            for w in rec.warnings:
                L.append(f"  │  {_ICON_WARN}  {w}")

        L.append("  │")

        # Column detail header
        hdr = (
            f"  │  {'Column':<28} {'Type':<14} {'Hits':>5} "
            f"{'Distinct':>10} {'Ratio':>8} {'Pct':>7} "
            f"{'Cardinality':<13} {'Score':>28}  Recommendation"
        )
        L.append(hdr)
        L.append(f"  │  {'─' * (len(hdr) - 5)}")

        for cs in rec.recommended_columns:
            ci = _cardinality_icon(cs.cardinality_level)
            card_display = _cardinality_display(cs.cardinality_level)
            si = _score_emoji(cs.composite_score, min_score, cs.recommendation)
            distinct_str = (
                f"{cs.approx_distinct:,}" if cs.approx_distinct >= 0 else "N/A"
            )
            ratio_str = (
                f"{cs.cardinality_ratio:.4f}"
                if cs.cardinality_ratio >= 0 else "N/A"
            )
            pct_str = _ratio_pct(cs.cardinality_ratio)
            bar = _score_bar(cs.composite_score)
            L.append(
                f"  │  {cs.column_name:<28} {_type_display(cs):<14} "
                f"{cs.predicate_hits:>5} {distinct_str:>10} "
                f"{ratio_str:>8} {pct_str:>7} "
                f"{ci} {card_display:<10} "
                f"{bar}  {si} {cs.recommendation}"
            )
            if cs.optimization_flag and cs.optimization_flag != "OK":
                L.append(f"  │     {_ICON_WARN}  {cs.optimization_flag}")

        if rec.cluster_by_ddl:
            L.append("  │")
            L.append(f"  │  {_ICON_BOLT} SUGGESTED DDL (one CTAS per candidate):")
            for ddl_line in rec.cluster_by_ddl.split("\n"):
                L.append(f"  │     {ddl_line}")

        L.append(f"  └{'─' * 74}")

    # ── All DDL ─────────────────────────────────────────────────────
    ddl_lines = [r.cluster_by_ddl for r in recommendations if r.cluster_by_ddl]
    if ddl_lines:
        L.append("")
        L.append(f"  {_ICON_BOLT}  ALL SUGGESTED DDL STATEMENTS (CTAS)")
        L.append(f"  {'═' * 72}")
        L.append("")
        L.append(f"  {_ICON_BULB}  Fabric uses CREATE TABLE ... AS SELECT (CTAS) to apply")
        L.append("     data clustering.  Each statement creates a NEW clustered table.")
        L.append("     Pick the column(s) that best suit your workload, then drop the")
        L.append("     original table and rename the new one.")
        L.append("")
        for ddl in ddl_lines:
            for dl in ddl.split("\n"):
                L.append(f"     {dl}")
            L.append("")

    # ── Best Practices ──────────────────────────────────────────────
    L.append("")
    L.append(f"  {'═' * 58}")
    L.append(f"  {_ICON_BULB}  BEST PRACTICES")
    L.append(f"  {'─' * 58}")
    tips = [
        f"{_ICON_CHECK}  Data clustering is most effective on LARGE tables.",
        f"{_ICON_CHECK}  Choose columns with mid-to-high cardinality in WHERE filters.",
        f"{_ICON_BOLT}  Batch ingestion (>= 1M rows per DML) for optimal clustering.",
        f"{_ICON_CROSS}  Equality JOIN conditions do NOT benefit from data clustering.",
        f"{_ICON_WARN}  Don't cluster on more columns than strictly necessary.",
        f"{_ICON_GEAR}  Column order in CLUSTER BY does not affect row storage.",
        f"{_ICON_WARN}  For char/varchar, only the first 32 characters produce stats.",
        f"{_ICON_WARN}  For decimal with precision > 18, predicates won't push down.",
    ]
    for tip in tips:
        L.append(f"    {tip}")
    L.append(f"  {'═' * 58}")
    L.append("")
    L.append("  Generated by Fabric Warehouse Advisor")
    L.append("")

    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# MARKDOWN REPORT
# ══════════════════════════════════════════════════════════════════════

def generate_markdown_report(
    recommendations: List[TableRecommendation],
    min_score: int = 40,
    captured_at: str | None = None,
    warehouse_name: str = "",
) -> str:
    """Generate a Markdown report (for saving as ``.md``)."""
    if captured_at is None:
        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    md: list[str] = []

    md.append(f"# {_ICON_ROCKET} Fabric Warehouse Data Clustering Advisor")
    if warehouse_name:
        md.append(f"**Warehouse:** {warehouse_name}\n")
    md.append(f"*{_ICON_GEAR} Generated: {captured_at}*\n")

    total_tables = len(recommendations)
    tables_with_recs = sum(1 for r in recommendations if r.recommended_columns)
    tables_clustered = sum(
        1 for r in recommendations if r.currently_clustered_columns
    )

    md.append(f"## {_ICON_BAR} Executive Summary\n")
    md.append("| | Metric | Value |")
    md.append("|---|--------|------:|")
    md.append(f"| {_ICON_TABLE} | Tables analysed | **{total_tables}** |")
    md.append(f"| {_ICON_STAR} | Tables with recommendations | **{tables_with_recs}** |")
    md.append(f"| {_ICON_LOCK} | Tables already clustered | **{tables_clustered}** |")
    md.append(f"| {_ICON_GEAR} | Score threshold | **{min_score}** |")
    md.append("")

    if not recommendations:
        md.append(
            f"> {_ICON_WARN} No candidates met the threshold.  Ensure Query "
            "Insights is enabled and queries have been running.\n"
        )
        return "\n".join(md)

    md.append(f"## {_ICON_MAG} Recommendations by Table\n")

    for idx, rec in enumerate(recommendations, 1):
        md.append(
            f"### {_ICON_DB} "
            f"`[{rec.schema_name}].[{rec.table_name}]`\n"
        )
        md.append("| | Detail |")
        md.append("|---|--------|")
        md.append(f"| **Rows** | {rec.row_count:,} |")
        if rec.currently_clustered_columns:
            md.append(
                f"| **{_ICON_KEY} Current CLUSTER BY** | "
                f"`{', '.join(rec.currently_clustered_columns)}` |"
            )
        else:
            md.append(f"| **{_ICON_KEY} Current CLUSTER BY** | _(none)_ |")
        md.append("")

        if hasattr(rec, "warnings") and rec.warnings:
            for w in rec.warnings:
                md.append(f"> {_ICON_WARN} {w}\n")

        md.append(
            "| | Column | Type | Pred. Hits | Distinct "
            "| Ratio | Pct | Cardinality | Score | Recommendation |"
        )
        md.append(
            "|---|--------|------|----------:|----------:"
            "|------:|------:|------------|------:|----------------|"
        )
        for cs in rec.recommended_columns:
            ci = _cardinality_icon(cs.cardinality_level)
            card_display = _cardinality_display(cs.cardinality_level)
            si = _score_emoji(cs.composite_score, min_score, cs.recommendation)
            distinct_str = (
                f"{cs.approx_distinct:,}" if cs.approx_distinct >= 0 else "N/A"
            )
            ratio_str = (
                f"{cs.cardinality_ratio:.4f}"
                if cs.cardinality_ratio >= 0 else "N/A"
            )
            pct_str = _ratio_pct(cs.cardinality_ratio)
            flag = ""
            if cs.optimization_flag and cs.optimization_flag != "OK":
                flag = f" {_ICON_WARN} *{cs.optimization_flag}*"
            md.append(
                f"| {si} | `{cs.column_name}` | `{_type_display(cs)}` "
                f"| {cs.predicate_hits} | {distinct_str} "
                f"| {ratio_str} | {pct_str} "
                f"| {ci} {card_display} "
                f"| **{cs.composite_score}** "
                f"| {cs.recommendation}{flag} |"
            )
        md.append("")

        if rec.cluster_by_ddl:
            md.append(
                f"<details><summary>{_ICON_BOLT} <b>Suggested DDL "
                f"(one CTAS per candidate)</b></summary>\n"
            )
            md.append(f"```sql\n{rec.cluster_by_ddl}\n```\n")
            md.append("</details>\n")

        md.append("---\n")

    ddl_lines = [r.cluster_by_ddl for r in recommendations if r.cluster_by_ddl]
    if ddl_lines:
        md.append(f"## {_ICON_BOLT} All Suggested DDL (CTAS)\n")
        md.append(
            f"> {_ICON_BULB} Fabric uses `CREATE TABLE ... AS SELECT` (CTAS) "
            "to apply data clustering.  Each statement creates a **new** "
            "clustered table.  Pick the column(s) that best suit your "
            "workload, then drop the original table and rename the new one.\n"
        )
        md.append("```sql")
        for ddl in ddl_lines:
            md.append(ddl)
            md.append("")
        md.append("```\n")

    md.append(f"## {_ICON_BULB} Best Practices\n")
    md.append("| | Recommendation |")
    md.append("|---|---|")
    md.append(f"| {_ICON_CHECK} | Data clustering is most effective on **large tables**. |")
    md.append(f"| {_ICON_CHECK} | Choose columns with **mid-to-high cardinality** used in `WHERE` filters. |")
    md.append(f"| {_ICON_BOLT} | Batch ingestion (>= 1 M rows per DML) for optimal clustering quality. |")
    md.append(f"| {_ICON_CROSS} | Equality `JOIN` conditions do **NOT** benefit from data clustering. |")
    md.append(f"| {_ICON_WARN} | Don't cluster on more columns than strictly necessary. |")
    md.append(f"| {_ICON_GEAR} | Column order in `CLUSTER BY` does not affect how rows are stored. |")
    md.append(f"| {_ICON_WARN} | For `char`/`varchar`, only the first 32 characters produce stats. |")
    md.append(f"| {_ICON_WARN} | For `decimal` with precision > 18, predicates won't push down. |")
    md.append("")
    md.append("---")
    md.append("*Generated by Fabric Warehouse Advisor*")

    return "\n".join(md)


# ══════════════════════════════════════════════════════════════════════
# HTML REPORT  (for displayHTML() in Fabric notebooks)
# ══════════════════════════════════════════════════════════════════════


def _score_html_bar(score: int, width_px: int = 120) -> str:
    fill = int(score / 100 * width_px)
    return (
        f'<span class="score-bar" style="width:{width_px}px">'
        f'<span class="score-fill" style="width:{fill}px"></span>'
        f'<span class="score-empty" style="width:{width_px - fill}px"></span>'
        f'</span> <b>{score}</b>'
    )


def _rec_pill(recommendation: str) -> str:
    r = recommendation.upper()
    if "RECOMMENDED" in r and "NOT" not in r:
        cls = "pill-rec"
    elif "CONSIDER" in r:
        cls = "pill-consider"
    elif "ALREADY" in r:
        cls = "pill-clust"
    else:
        cls = "pill-no"
    return f'<span class="pill {cls}">{_html.escape(recommendation)}</span>'


def generate_html_report(
    recommendations: List[TableRecommendation],
    min_score: int = 40,
    captured_at: str | None = None,
    warehouse_name: str = "",
) -> str:
    """Generate a self-contained HTML report for ``displayHTML()`` in Fabric."""
    from ...core.html_template import (
        esc, html_open, html_close, render_sidebar, render_main_open,
        render_info_stats, render_footer,
    )

    if captured_at is None:
        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    total_tables = len(recommendations)
    tables_with_recs = sum(1 for r in recommendations if r.recommended_columns)
    tables_clustered = sum(
        1 for r in recommendations if r.currently_clustered_columns
    )

    h: list[str] = []

    # ── Document open ───────────────────────────────────────────────
    h.append(html_open("Data Clustering Dashboard | Fabric Warehouse"))

    # ── Build tab list (tables + optional All DDL + Best Practices) ─
    tabs: list[tuple[str, str]] = []
    for idx, rec in enumerate(recommendations):
        tabs.append((f"pane-{idx}", f"[{rec.schema_name}].[{rec.table_name}]"))

    ddl_lines = [r.cluster_by_ddl for r in recommendations if r.cluster_by_ddl]
    if ddl_lines:
        tabs.append(("pane-ddl", "All DDL"))
    tabs.append(("pane-bp", "Best Practices"))

    # ── Sidebar ─────────────────────────────────────────────────────
    h.append(render_sidebar(
        brand_text="Clustering Advisor",
        report_type="clustering",
        warehouse_name=warehouse_name,
        tabs=tabs,
        generated_at=captured_at,
    ))

    # ── Main content ────────────────────────────────────────────────
    h.append(render_main_open(
        "Dashboard",
        "Recommendations for optimizing table clustering in your warehouse.",
    ))

    h.append(render_info_stats([
        ("Tables Analysed", total_tables, "stat-primary"),
        ("Recommendations", tables_with_recs, "stat-primary"),
        ("Already Clustered", tables_clustered, "stat-primary"),
        ("Score Threshold", min_score, "stat-primary"),
    ]))

    # ── Per-table tab panes ─────────────────────────────────────────
    for idx, rec in enumerate(recommendations):
        esc_tbl = esc(f"[{rec.schema_name}].[{rec.table_name}]")
        active_cls = " active" if idx == 0 else ""
        hidden = "" if idx == 0 else ' hidden'

        h.append(
            f'<div class="tab-pane{active_cls}" id="pane-{idx}" '
            f'role="tabpanel"{hidden}>'
        )
        h.append(f'<h2 class="print-title">{esc_tbl}</h2>')

        # Meta row
        h.append('<div class="section-meta">')
        h.append(
            f'<span class="pill pill-clust">{rec.row_count:,} rows</span>'
        )
        if rec.currently_clustered_columns:
            cols_str = ", ".join(rec.currently_clustered_columns)
            h.append(
                f'<span><strong>{_ICON_KEY} Current CLUSTER BY:</strong> '
                f'<code>{esc(cols_str)}</code></span>'
            )
        else:
            h.append(
                f'<span><strong>{_ICON_KEY} Current CLUSTER BY:</strong> '
                '<em>(none)</em></span>'
            )
        h.append('</div>')

        if hasattr(rec, "warnings") and rec.warnings:
            for w in rec.warnings:
                h.append(
                    f'<div class="warn-box">'
                    f'{_ICON_WARN} {_html.escape(w)}</div>'
                )

        # Column score table
        h.append('<div class="table-container"><div class="table-scroll">')
        h.append('<table>')
        h.append(
            '<thead><tr>'
            '<th></th>'
            '<th data-sort="text">Column '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th data-sort="text">Type '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th data-sort="num" style="text-align:right">Pred.&nbsp;Hits '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th data-sort="num" style="text-align:right">Distinct '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th style="text-align:right">Ratio</th>'
            '<th style="text-align:right">Pct</th>'
            '<th>Cardinality</th>'
            '<th data-sort="num">Score '
            '<span class="sort-icon">\u21C5</span></th>'
            '<th>Recommendation</th>'
            '</tr></thead>'
        )
        h.append('<tbody>')

        for cs in rec.recommended_columns:
            ci = _cardinality_icon(cs.cardinality_level)
            card_display = _cardinality_display(cs.cardinality_level)
            si = _score_emoji(cs.composite_score, min_score, cs.recommendation)
            distinct_str = (
                f"{cs.approx_distinct:,}" if cs.approx_distinct >= 0 else "N/A"
            )
            ratio_str = (
                f"{cs.cardinality_ratio:.4f}"
                if cs.cardinality_ratio >= 0 else "N/A"
            )
            pct_str = _ratio_pct(cs.cardinality_ratio)
            flag_html = ""
            if cs.optimization_flag and cs.optimization_flag != "OK":
                flag_html = (
                    f'<br><small>{_ICON_WARN} '
                    f'{_html.escape(cs.optimization_flag)}</small>'
                )
            h.append(
                f'<tr>'
                f'<td>{si}</td>'
                f'<td><code>{_html.escape(cs.column_name)}</code></td>'
                f'<td><code>{_html.escape(_type_display(cs))}</code></td>'
                f'<td style="text-align:right">{cs.predicate_hits}</td>'
                f'<td style="text-align:right">{distinct_str}</td>'
                f'<td style="text-align:right">{ratio_str}</td>'
                f'<td style="text-align:right">{pct_str}</td>'
                f'<td>{ci} {card_display}</td>'
                f'<td>{_score_html_bar(cs.composite_score)}</td>'
                f'<td>{_rec_pill(cs.recommendation)}{flag_html}</td>'
                f'</tr>'
            )
        h.append('</tbody></table>')
        h.append('</div></div>')  # table-scroll + table-container

        # DDL block
        if rec.cluster_by_ddl:
            h.append(
                f'<details class="sql-details ddl-details">'
                f'<summary>{_ICON_BOLT} Suggested DDL (one CTAS per candidate)</summary>'
                f'<div class="ddl-block">{_html.escape(rec.cluster_by_ddl)}</div>'
                f'</details>'
            )

        h.append('</div>')  # tab-pane

    # ── All DDL tab pane ────────────────────────────────────────────
    if ddl_lines:
        h.append('<div class="tab-pane" id="pane-ddl" role="tabpanel" hidden>')
        h.append(f'<h2 class="print-title">All Suggested DDL (CTAS)</h2>')
        h.append(
            f'<div class="warn-box" style="border-left-color:var(--primary);">'
            f'{_ICON_BULB} Fabric uses '
            '<code>Create Table \u2026 As Select</code> (CTAS) to apply data '
            'clustering. Each statement creates a <b>new</b> clustered '
            'table. Pick the column(s) that best suit your workload, then '
            'drop the original table and rename the new one.</div>'
        )
        all_ddl = "\n\n".join(ddl_lines)
        h.append(f'<div class="ddl-block">{_html.escape(all_ddl)}</div>')
        h.append('</div>')  # tab-pane

    # ── Best Practices tab pane ─────────────────────────────────────
    first_is_bp = not recommendations
    bp_active = " active" if first_is_bp else ""
    bp_hidden = "" if first_is_bp else ' hidden'
    h.append(
        f'<div class="tab-pane{bp_active}" id="pane-bp" '
        f'role="tabpanel"{bp_hidden}>'
    )
    h.append(f'<h2 class="print-title">Best Practices</h2>')

    if not recommendations:
        h.append(
            '<div class="warn-box">'
            '\u26a0\ufe0f No candidate columns met the scoring threshold. '
            'Ensure Query Insights is enabled and queries with WHERE '
            'filters have been running.'
            '</div>'
        )

    tips_html = [
        (_ICON_CHECK, "Data clustering is most effective on <b>large tables</b>."),
        (_ICON_CHECK, "Choose columns with <b>mid-to-high cardinality</b> used in <code>WHERE</code> filters."),
        (_ICON_BOLT, "Batch ingestion (&ge; 1 M rows per DML) for optimal clustering quality."),
        (_ICON_CROSS, "Equality <code>JOIN</code> conditions do <b>NOT</b> benefit from data clustering."),
        (_ICON_WARN, "Don't cluster on more columns than strictly necessary."),
        (_ICON_GEAR, "Column order in <code>CLUSTER BY</code> does not affect how rows are stored."),
        (_ICON_WARN, "For <code>char</code>/<code>varchar</code>, only the first 32 characters produce stats."),
        (_ICON_WARN, "For <code>decimal</code> with precision &gt; 18, predicates won't push down."),
    ]
    h.append('<div class="table-container"><div class="table-scroll"><table>')
    for icon, tip in tips_html:
        h.append(f'<tr><td style="text-align:center;width:36px">{icon}</td><td>{tip}</td></tr>')
    h.append('</table></div></div>')
    h.append('</div>')  # tab-pane

    # ── Footer ──────────────────────────────────────────────────────
    h.append(render_footer("Generated by Fabric Warehouse Advisor"))
    h.append(html_close())
    return "\n".join(h)

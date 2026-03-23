"""
Fabric Warehouse Advisor — Security Check Reports
====================================================
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
    CATEGORY_AUTH_MODE,
    CATEGORY_PERMISSIONS,
    CATEGORY_ROLES,
    CATEGORY_RLS,
    CATEGORY_CLS,
    CATEGORY_DDM,
    CATEGORY_WORKSPACE_ROLES,
    CATEGORY_NETWORK,
    CATEGORY_SQL_AUDIT,
    CATEGORY_ITEM_PERMISSIONS,
    CATEGORY_SENSITIVITY_LABELS,
    CATEGORY_ROLE_ALIGNMENT,
    CATEGORY_ONELAKE_DATA_ACCESS,
    CATEGORY_ONELAKE_SETTINGS,
    CATEGORY_ONELAKE_SECURITY_SYNC,
)


# -- Category display metadata ------------------------------------------

_CATEGORY_LABELS = {
    CATEGORY_AUTH_MODE: "Access Mode",
    CATEGORY_PERMISSIONS: "Schema Permissions",
    CATEGORY_ROLES: "Custom Roles",
    CATEGORY_RLS: "Row-Level Security",
    CATEGORY_CLS: "Column-Level Security",
    CATEGORY_DDM: "Dynamic Data Masking",
    CATEGORY_WORKSPACE_ROLES: "Workspace Roles",
    CATEGORY_NETWORK: "Network Isolation",
    CATEGORY_SQL_AUDIT: "SQL Audit Settings",
    CATEGORY_ITEM_PERMISSIONS: "Item Permissions",
    CATEGORY_SENSITIVITY_LABELS: "Sensitivity Labels",
    CATEGORY_ROLE_ALIGNMENT: "Role Alignment",
    CATEGORY_ONELAKE_DATA_ACCESS: "OneLake Data Access Roles",
    CATEGORY_ONELAKE_SETTINGS: "OneLake Settings",
    CATEGORY_ONELAKE_SECURITY_SYNC: "OneLake Security Sync",
}

_CATEGORY_ORDER = [
    # Workspace & Platform
    CATEGORY_WORKSPACE_ROLES,
    CATEGORY_NETWORK,
    CATEGORY_ONELAKE_SETTINGS,
    CATEGORY_SENSITIVITY_LABELS,
    # Item Security
    CATEGORY_SQL_AUDIT,
    CATEGORY_ITEM_PERMISSIONS,
    # OneLake Security
    CATEGORY_ONELAKE_DATA_ACCESS,
    CATEGORY_ONELAKE_SECURITY_SYNC,
    # SQL Security
    CATEGORY_PERMISSIONS,
    CATEGORY_ROLES,
    CATEGORY_RLS,
    CATEGORY_CLS,
    CATEGORY_DDM,
    # Cross-Reference
    CATEGORY_ROLE_ALIGNMENT,
]

# Sidebar section groups — maps each category to a section label.
# Categories sharing the same label are grouped under one heading.
_CATEGORY_SECTIONS = {
    CATEGORY_WORKSPACE_ROLES: "Workspace & Platform",
    CATEGORY_NETWORK: "Workspace & Platform",
    CATEGORY_ONELAKE_SETTINGS: "Workspace & Platform",
    CATEGORY_SENSITIVITY_LABELS: "Workspace & Platform",
    CATEGORY_SQL_AUDIT: "Item Security",
    CATEGORY_ITEM_PERMISSIONS: "Item Security",
    CATEGORY_ONELAKE_DATA_ACCESS: "OneLake Security",
    CATEGORY_ONELAKE_SECURITY_SYNC: "OneLake Security",
    CATEGORY_PERMISSIONS: "SQL Security",
    CATEGORY_ROLES: "SQL Security",
    CATEGORY_RLS: "SQL Security",
    CATEGORY_CLS: "SQL Security",
    CATEGORY_DDM: "SQL Security",
    CATEGORY_ROLE_ALIGNMENT: "Cross-Reference",
}

_LEVEL_ICONS = {
    LEVEL_CRITICAL: "❌",
    LEVEL_HIGH: "🔴",
    LEVEL_MEDIUM: "🟠",
    LEVEL_LOW: "🟡",
    LEVEL_INFO: "✅",
}


def _edition_label(edition: str | None, *, capitalize: bool = True) -> str:
    """Return 'SQL Endpoint' or 'Warehouse' based on warehouse edition."""
    is_sql_endpoint = edition == "LakeWarehouse"
    if capitalize:
        return "SQL Endpoint" if is_sql_endpoint else "Warehouse"
    return "SQL endpoint" if is_sql_endpoint else "warehouse"


def _auth_mode_label(auth_mode: str) -> str:
    """Return a human-readable auth mode label."""
    if auth_mode == "user_identity":
        return "User Identity (OneLake security for tables)"
    if auth_mode == "delegated":
        return "Delegated Identity (SQL security for all objects)"
    return ""


_AUTH_MODE_EXPLAINER = {
    "user_identity": (
        "This SQL endpoint is running in User Identity mode. "
        "Table-level access is controlled exclusively by OneLake "
        "security roles. SQL RLS, CLS, and custom database roles "
        "are inactive. Dynamic Data Masking and SQL permissions on "
        "views, stored procedures, and functions remain enforced."
    ),
    "delegated": (
        "This SQL endpoint is running in Delegated Identity mode. "
        "SQL security (permissions, RLS, CLS, DDM) controls access "
        "to all objects. OneLake data access roles exist but are not "
        "enforced at SQL query time — the item owner's identity is "
        "used for OneLake access."
    ),
}

_AUTH_MODE_RECOMMENDATIONS = {
    "user_identity": (
        "• Review OneLake data access roles to ensure least-privilege "
        "table access.\n"
        "• DefaultReader role should be removed or scoped down if "
        "custom roles are defined.\n"
        "• SQL permissions on views/sprocs/functions are still "
        "enforced — audit them via the Schema Permissions section.\n"
        "• Monitor the Security Sync Health section for propagation "
        "issues."
    ),
    "delegated": (
        "• Ensure SQL RLS, CLS, and DDM policies are correctly "
        "configured for all sensitive tables.\n"
        "• Custom database roles should follow least-privilege "
        "principles.\n"
        "• OneLake data access roles shown above are informational "
        "only — they are not enforced in this mode.\n"
        "• Consider switching to User Identity mode if you want "
        "OneLake security to control table-level access."
    ),
}


# ======================================================================
# TEXT REPORT
# ======================================================================

def generate_text_report(summary: CheckSummary) -> str:
    """Generate a plain-text report with Unicode box drawing."""
    lines: List[str] = []
    w = 72
    item = _edition_label(summary.warehouse_edition)

    lines.append("╔" + "═" * w + "╗")
    lines.append("║" + " Fabric Warehouse Security Check Report".center(w) + "║")
    lines.append("╚" + "═" * w + "╝")
    lines.append("")
    lines.append(f"  {item} : {summary.warehouse_name}")
    if summary.auth_mode:
        lines.append(f"  Auth Mode : {_auth_mode_label(summary.auth_mode)}")
    lines.append("")

    # Auth mode explainer
    if summary.auth_mode in _AUTH_MODE_EXPLAINER:
        lines.append("━" * w)
        lines.append(f"  {_AUTH_MODE_EXPLAINER[summary.auth_mode]}")
        lines.append("━" * w)
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

    # Auth mode recommendations
    if summary.auth_mode in _AUTH_MODE_RECOMMENDATIONS:
        mode_label = _auth_mode_label(summary.auth_mode)
        lines.append(f"━━━ RECOMMENDATIONS ({mode_label}) ━━━")
        lines.append("")
        for line in _AUTH_MODE_RECOMMENDATIONS[summary.auth_mode].split("\n"):
            lines.append(f"  {line}")
        lines.append("")

    # Footer
    lines.append("═" * w)
    lines.append("  Generated by Fabric Warehouse Advisor — Security Check")
    lines.append("═" * w)

    return "\n".join(lines)


# ======================================================================
# MARKDOWN REPORT
# ======================================================================

def generate_markdown_report(summary: CheckSummary) -> str:
    """Generate a Markdown report."""
    lines: List[str] = []

    lines.append("# 🔒 Fabric Warehouse Security Check Report")
    lines.append("")
    item = _edition_label(summary.warehouse_edition)
    lines.append(f"| Property | Value |")
    lines.append(f"|----------|-------| ")
    lines.append(f"| {item} | `{summary.warehouse_name}` |")
    if summary.auth_mode:
        lines.append(f"| Auth Mode | {_auth_mode_label(summary.auth_mode)} |")
    lines.append("")

    # Auth mode explainer
    if summary.auth_mode in _AUTH_MODE_EXPLAINER:
        lines.append(f"> **ℹ️ {_AUTH_MODE_EXPLAINER[summary.auth_mode]}**")
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

    # Auth mode recommendations
    if summary.auth_mode in _AUTH_MODE_RECOMMENDATIONS:
        mode_label = _auth_mode_label(summary.auth_mode)
        lines.append(f"## Recommendations ({mode_label})")
        lines.append("")
        for line in _AUTH_MODE_RECOMMENDATIONS[summary.auth_mode].split("\n"):
            lines.append(line)
        lines.append("")

    # Footer
    lines.append("---")
    lines.append("*Generated by Fabric Warehouse Advisor — Security Check*")

    return "\n".join(lines)


# ======================================================================
# HTML REPORT
# ======================================================================

def generate_html_report(summary: CheckSummary, captured_at: str | None = None) -> str:
    """Generate a self-contained HTML report with sidebar dashboard layout."""
    from datetime import datetime, timezone
    from ...core.html_template import (
        esc, html_open, html_close, render_sidebar, render_main_open,
        render_severity_stats, severity_pill, render_sql_block,
        render_footer,
    )

    if captured_at is None:
        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    total = len(summary.findings)
    h: List[str] = []

    # ── Document open ───────────────────────────────────────────────
    h.append(html_open("Security Advisor Dashboard | Fabric Warehouse"))

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
    badge_label = _edition_label(summary.warehouse_edition)

    # ── Sidebar ─────────────────────────────────────────────────
    h.append(render_sidebar(
        brand_text="Security",
        report_type="security",
        warehouse_name=summary.warehouse_name,
        tabs=tabs,
        generated_at=captured_at,
        badge_label=badge_label,
        workspace_display_name=summary.workspace_display_name,
        capacity_sku=summary.capacity_sku,
        auth_mode=summary.auth_mode,
    ))

    item = _edition_label(summary.warehouse_edition, capitalize=False)

    # ── Main content ────────────────────────────────────────────────
    h.append(render_main_open(
        "Advisor Dashboard",
        f"Comprehensive overview of your {item} security posture.",
    ))

    # Severity stat cards
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
        h.append(render_footer(
            "Generated by Fabric Warehouse Advisor \u2014 Security Check"
        ))
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

        # Contextual auth-mode alerts for affected panes
        _ui_alert = _auth_mode_pane_alert(cat, summary.auth_mode)
        if _ui_alert:
            h.append(_ui_alert)

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
                    h.append(f'<small>{esc(f0.detail).replace(chr(10), "<br>")}</small>')
                h.append(
                    f'<details class="sql-details" style="margin-top:8px">'
                    f'<summary>Show all {len(findings)} objects</summary>'
                    f'<div class="object-list-container"><div class="object-grid">'
                )
                for f in findings:
                    h.append(f'<span class="obj-pill">{esc(f.object_name)}</span>')
                h.append('</div></div></details></td>')
                rec = esc(f0.recommendation).replace("\n", "<br>") if f0.recommendation else ""
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
                        h.append(f'<small>{esc(f.detail).replace(chr(10), "<br>")}</small>')
                    h.append('</td>')
                    rec = esc(f.recommendation).replace("\n", "<br>") if f.recommendation else ""
                    if f.sql_fix:
                        rec += render_sql_block(f.sql_fix)
                    h.append(f'<td>{rec}</td></tr>')

        h.append('</tbody></table>')
        h.append('</div></div>')  # table-scroll + table-container
        h.append('</div>')  # tab-pane

    # ── Footer ──────────────────────────────────────────────────────
    h.append(render_footer(
        "Generated by Fabric Warehouse Advisor \u2014 Security Check"
    ))
    h.append(html_close())
    return "\n".join(h)


# -- Helpers -------------------------------------------------------------

# Contextual alerts shown inside specific tab panes based on auth mode.
_AUTH_MODE_PANE_ALERTS: dict[str, dict[str, tuple[str, str, str]]] = {
    # auth_mode -> { category -> (alert_type, icon, message) }
    "user_identity": {
        CATEGORY_RLS: (
            "alert-warning", "\u26A0\uFE0F",
            "SQL Row-Level Security is <strong>inactive</strong> because this "
            "endpoint is running in <strong>User Identity</strong> mode. "
            "Table-level access is controlled by OneLake security roles.",
        ),
        CATEGORY_CLS: (
            "alert-warning", "\u26A0\uFE0F",
            "SQL Column-Level Security is <strong>inactive</strong> in "
            "<strong>User Identity</strong> mode. Column access is governed "
            "by OneLake security roles.",
        ),
        CATEGORY_ROLES: (
            "alert-warning", "\u26A0\uFE0F",
            "Custom database roles are <strong>inactive</strong> in "
            "<strong>User Identity</strong> mode. Role-based SQL access "
            "does not apply \u2014 table access is controlled by OneLake "
            "security roles.",
        ),
        CATEGORY_PERMISSIONS: (
            "alert-info", "\u2139\uFE0F",
            "In <strong>User Identity</strong> mode, SQL permissions on "
            "<strong>views, stored procedures, and functions</strong> remain "
            "enforced. Permissions on tables are inactive \u2014 table access "
            "is controlled by OneLake security roles.",
        ),
    },
    "delegated": {
        CATEGORY_ONELAKE_DATA_ACCESS: (
            "alert-info", "\u2139\uFE0F",
            "This endpoint is running in <strong>Delegated</strong> mode. "
            "OneLake data access roles shown here are informational only "
            "\u2014 they are not enforced at SQL query time. The item "
            "owner\u2019s identity is used for OneLake access.",
        ),
    },
}


def _auth_mode_pane_alert(category: str, auth_mode: str) -> str:
    """Return a contextual alert HTML div for *category*, or empty string."""
    alerts = _AUTH_MODE_PANE_ALERTS.get(auth_mode, {})
    entry = alerts.get(category)
    if not entry:
        return ""
    alert_cls, icon, message = entry
    return (
        f'<div class="context-alert {alert_cls}">'
        f'<span class="context-alert-icon">{icon}</span>'
        f'<span>{message}</span>'
        f'</div>'
    )


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

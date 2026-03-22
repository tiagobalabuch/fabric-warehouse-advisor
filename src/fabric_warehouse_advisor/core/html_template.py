"""
Fabric Warehouse Advisor — Shared HTML Report Template
=======================================================
Common CSS, JavaScript, and Python helpers used by all advisor
HTML reports to produce a consistent, modern SaaS dashboard look.

All HTML reports are **self-contained** single files — the CSS and JS
are inlined so they render correctly when opened from a local file
system or displayed inside a Fabric notebook via ``displayHTML()``.
"""

from __future__ import annotations

from .icon_data import (
    ICON_CLUSTERING,
    ICON_MOON,
    ICON_PERFORMANCE,
    ICON_SEARCH,
    ICON_SECURITY,
    ICON_SUN,
)


# ── HTML escape ─────────────────────────────────────────────────────

def esc(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# ══════════════════════════════════════════════════════════════════════
# SHARED CSS
# ══════════════════════════════════════════════════════════════════════

REPORT_CSS = r"""<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

  /* ── Light Theme Variables (Fabric Inspired) ──────────────────── */
  :root {
    --bg-main: #f8f9fa;
    --bg-surface: #ffffff;
    --bg-sidebar: #13635B;

    --text-main: #201f1e;
    --text-secondary: #605e5c;
    --text-muted: #8a8886;
    --border-color: #edebe9;

    --primary: #008080;
    --primary-hover: #13635B;
    --primary-light: #E1F5F3;

    --fabric-gradient: linear-gradient(135deg, #13635B 0%, #32C5A5 100%);

    --c-crit: #d13438; --c-crit-bg: #fde7e9; --c-crit-text: #a4262c;
    --c-high: #ca5010; --c-high-bg: #fdf3e6; --c-high-text: #8e562e;
    --c-med:  #fce100; --c-med-bg:  #fff9ce; --c-med-text: #5c5300;
    --c-low:  #107c41; --c-low-bg:  #e1dfdd; --c-low-text: #0b5a2f;
    --c-info: #0078d4; --c-info-bg: #deecf9; --c-info-text: #005a9e;

    --sidebar-text: #a5d1cc;
    --sidebar-text-active: #ffffff;
    --sidebar-bg-active: rgba(255,255,255,0.15);
    --shadow-card: 0px 4px 12px rgba(0, 0, 0, 0.05);
  }

  /* ── Dark Theme Variables (Fabric Inspired) ───────────────────── */
  :root.dark {
    --bg-main: #111414;
    --bg-sidebar: #0b0d0d;
    --bg-surface: #1b1f1f;

    --text-main: #ffffff;
    --text-secondary: #a19f9d;
    --text-muted: #8a8886;
    --border-color: #293030;

    --primary: #32C5A5;
    --primary-hover: #97F0D0;
    --primary-light: rgba(50, 197, 165, 0.15);

    --fabric-gradient: linear-gradient(135deg, #32C5A5 0%, #97F0D0 100%);

    --c-crit: #ff99a4; --c-crit-bg: rgba(209, 52, 56, 0.2);  --c-crit-text: #ffb3b8;
    --c-high: #f3a071; --c-high-bg: rgba(202, 80, 16, 0.2);  --c-high-text: #f9cbb3;
    --c-med:  #fce100; --c-med-bg:  rgba(252, 225, 0, 0.15); --c-med-text: #fdf08a;
    --c-low:  #6BB700; --c-low-bg:  rgba(16, 124, 65, 0.2);  --c-low-text: #92c353;
    --c-info: #6cb8f6; --c-info-bg: rgba(0, 120, 212, 0.2);  --c-info-text: #a6d8ff;

    --sidebar-text: #8a8886;
    --sidebar-text-active: #ffffff;
    --sidebar-bg-active: rgba(50, 197, 165, 0.15);

    --shadow-card: 0px 8px 16px rgba(0, 0, 0, 0.4);
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Inter', "Segoe UI", -apple-system, BlinkMacSystemFont,
                 Roboto, Helvetica, Arial, sans-serif;
    background: var(--bg-main);
    color: var(--text-main);
    font-size: 14px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
    transition: background-color 0.3s ease, color 0.3s ease;
  }

  /* ── Layout (sidebar + main) ──────────────────────────────────── */
  .app-container {
    display: flex;
    min-height: 100vh;
  }

  /* ── Sidebar ──────────────────────────────────────────────────── */
  .sidebar {
    width: 240px;
    background: var(--bg-sidebar);
    color: var(--sidebar-text);
    display: flex;
    flex-direction: column;
    flex-shrink: 0;
    position: sticky;
    top: 0;
    height: 100vh;
    overflow-y: auto;
    z-index: 100;
  }

  .brand {
    min-height: 80px;
    display: flex;
    align-items: center;
    padding: 0 24px;
    font-size: 16px;
    font-weight: 700;
    color: #ffffff;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    letter-spacing: -0.3px;
  }
  .brand-icon {
    margin-right: 10px;
    flex-shrink: 0;
    width: 28px;
    height: 28px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
  }
  .brand-icon img {
    width: 100%;
    height: 100%;
    display: block;
  }

  /* Warehouse badge — prominent in sidebar */
  .warehouse-badge {
    margin: 16px 16px 0;
    padding: 14px 16px;
    background: rgba(255,255,255,0.08);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 12px;
  }
  .warehouse-badge-label {
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: rgba(255,255,255,0.4);
    margin-bottom: 4px;
  }
  .warehouse-badge-name {
    font-size: 16px;
    font-weight: 700;
    color: #ffffff;
    word-break: break-all;
  }
  .auth-mode-kv {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 12px;
    padding-top: 10px;
    border-top: 1px dashed rgba(255,255,255,0.15);
    font-size: 11px;
  }
  .mode-label {
    color: rgba(255,255,255,0.4);
  }
  .mode-value {
    color: #32C5A5;
    font-weight: 600;
  }

  .tabs-nav {
    flex: 1;
    overflow-y: auto;
    padding: 16px 12px;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .sidebar-section-label {
    font-size: 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: rgba(255,255,255,0.35);
    padding: 12px 16px 4px;
    margin-top: 4px;
    user-select: none;
  }
  .sidebar-section-label:first-child { margin-top: 0; padding-top: 0; }

  .tab-btn {
    background: transparent;
    border: none;
    padding: 11px 16px;
    font-size: 13px;
    font-weight: 600;
    font-family: inherit;
    color: var(--sidebar-text);
    cursor: pointer;
    border-radius: 10px;
    text-align: left;
    transition: all 0.2s;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .tab-btn:hover { background: rgba(255,255,255,0.05); color: #fff; }
  .tab-btn.active {
    background: var(--fabric-gradient);
    color: #fff;
    box-shadow: 0 4px 10px rgba(19, 99, 91, 0.3);
  }
  .tab-count {
    background: rgba(0,0,0,0.2); padding: 2px 8px;
    border-radius: 20px; font-size: 11px; font-weight: 700;
    min-width: 24px; text-align: center;
  }

  .sidebar-footer {
    padding: 16px 20px;
    font-size: 11px;
    color: rgba(255,255,255,0.3);
    border-top: 1px solid rgba(255,255,255,0.05);
  }

  /* ── Main Content Area ────────────────────────────────────────── */
  .main-content {
    flex: 1;
    min-width: 0;
    padding: 0 32px 40px 32px;
  }

  /* Topbar */
  .topbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    min-height: 80px;
    padding: 20px 0;
    position: sticky;
    top: 0;
    background: var(--bg-main);
    z-index: 50;
  }
  .page-header h1 {
    font-size: 28px; font-weight: 700;
    letter-spacing: -0.5px; margin-bottom: 2px;
  }
  .page-header p { color: var(--text-secondary); font-weight: 500; font-size: 14px; }

  .controls-area {
    display: flex; gap: 12px; align-items: center;
    background: var(--bg-surface); padding: 6px 12px;
    border-radius: 30px; box-shadow: var(--shadow-card);
  }

  .search-wrapper { position: relative; }
  .search-input {
    background: var(--bg-main); border: none; padding: 10px 16px 10px 38px;
    border-radius: 20px; font-family: inherit; font-size: 13px;
    color: var(--text-main); width: 260px; outline: none; transition: 0.2s;
  }
  .search-input:focus { box-shadow: 0 0 0 2px var(--primary); }
  .search-icon {
    position: absolute; left: 14px; top: 50%; transform: translateY(-50%);
    pointer-events: none; width: 14px; height: 14px;
  }
  .search-icon img {
    width: 100%; height: 100%; display: block;
    opacity: 0.5;
  }

  .theme-toggle {
    background: transparent; border: none; color: var(--text-main);
    cursor: pointer; font-size: 18px; width: 36px; height: 36px;
    border-radius: 50%; display: flex; align-items: center;
    justify-content: center; transition: background 0.2s;
  }
  .theme-toggle:hover { background: var(--bg-main); }
  .theme-toggle .icon {
    display: inline-block;
    transition: transform 0.4s cubic-bezier(0.34, 1.56, 0.64, 1),
                opacity 0.2s ease;
  }
  .theme-toggle.rotating .icon {
    transform: rotate(180deg) scale(0.85);
  }

  /* ── Stats Grid (Cards) ───────────────────────────────────────── */
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }

  .stat-item {
    background: var(--bg-surface);
    padding: 20px;
    border-radius: 16px;
    box-shadow: var(--shadow-card);
    transition: transform 0.25s ease, box-shadow 0.25s ease, opacity 0.2s;
    position: relative;
    overflow: hidden;
  }
  .stat-item[data-filter] { cursor: pointer; }
  .stat-item:hover {
    transform: translateY(-3px);
    box-shadow: 0px 8px 24px rgba(0, 0, 0, 0.1);
  }
  :root.dark .stat-item:hover {
    box-shadow: 0px 8px 24px rgba(0, 0, 0, 0.5);
  }

  .stat-item::before {
    content: ''; position: absolute; left: 0;
    top: 0; bottom: 0; width: 4px;
  }
  .stat-crit::before { background: var(--c-crit); }
  .stat-high::before { background: var(--c-high); }
  .stat-med::before  { background: var(--c-med); }
  .stat-low::before  { background: var(--c-low); }
  .stat-info::before { background: var(--c-info); }
  .stat-total::before{ background: var(--primary); }
  .stat-primary::before { background: var(--primary); }

  .stat-label {
    font-size: 12px; font-weight: 700; color: var(--text-secondary);
    text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;
  }
  .stat-val { font-size: 32px; font-weight: 700; line-height: 1; }
  .stat-total .stat-val { color: var(--text-main); }
  .stat-crit .stat-val  { color: var(--c-crit); }
  .stat-high .stat-val  { color: var(--c-high); }
  .stat-med  .stat-val  { color: var(--c-med); }
  .stat-low  .stat-val  { color: var(--c-low); }
  .stat-info .stat-val  { color: var(--c-info); }
  .stat-primary .stat-val { color: var(--primary); }

  /* ── Pill Badges ──────────────────────────────────────────────── */
  .pill {
    padding: 5px 12px; border-radius: 8px; font-size: 12px;
    font-weight: 700; display: inline-flex; justify-content: center;
    text-transform: uppercase; letter-spacing: 0.3px;
    transition: box-shadow 0.2s ease;
  }
  .p-crit:hover { box-shadow: 0 0 10px rgba(209, 52, 56, 0.4); }
  .p-high:hover { box-shadow: 0 0 10px rgba(202, 80, 16, 0.4); }
  .p-med:hover  { box-shadow: 0 0 10px rgba(252, 225, 0, 0.35); }
  .p-low:hover  { box-shadow: 0 0 10px rgba(16, 124, 65, 0.4); }
  .p-info:hover { box-shadow: 0 0 10px rgba(0, 120, 212, 0.4); }
  .pill-rec:hover     { box-shadow: 0 0 10px rgba(16, 124, 65, 0.4); }
  .pill-consider:hover{ box-shadow: 0 0 10px rgba(252, 225, 0, 0.35); }
  .pill-no:hover      { box-shadow: 0 0 10px rgba(209, 52, 56, 0.4); }
  .pill-clust:hover   { box-shadow: 0 0 10px rgba(0, 120, 212, 0.4); }
  .p-crit { background: var(--c-crit-bg); color: var(--c-crit-text); }
  .p-high { background: var(--c-high-bg); color: var(--c-high-text); }
  .p-med  { background: var(--c-med-bg);  color: var(--c-med-text); }
  .p-low  { background: var(--c-low-bg);  color: var(--c-low-text); }
  .p-info { background: var(--c-info-bg); color: var(--c-info-text); }

  /* ── Recommendation Badges (data clustering) ──────────────────── */
  .pill-rec     { background: var(--c-low-bg);  color: var(--c-low-text); }
  .pill-consider{ background: var(--c-med-bg);  color: var(--c-med-text); }
  .pill-no      { background: var(--c-crit-bg); color: var(--c-crit-text); }
  .pill-clust   { background: var(--c-info-bg); color: var(--c-info-text); }

  /* ── Tab Panes ────────────────────────────────────────────────── */
  .tab-pane {
    display: none;
    animation: fadeSlideIn 0.2s ease forwards;
  }
  .tab-pane.active { display: block; }

  /* Contextual alert boxes inside tab panes */
  .context-alert {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 12px 16px;
    margin-bottom: 16px;
    border-radius: 10px;
    font-size: 13px;
    font-weight: 500;
    line-height: 1.5;
  }
  .context-alert.alert-warning {
    background: var(--c-high-bg);
    border-left: 4px solid var(--c-high);
    color: var(--c-high-text);
  }
  .context-alert.alert-info {
    background: var(--c-info-bg);
    border-left: 4px solid var(--c-info);
    color: var(--c-info-text);
  }
  .context-alert-icon { flex-shrink: 0; font-size: 16px; margin-top: 1px; }
  @keyframes fadeSlideIn {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
  }

  /* ── Tables ───────────────────────────────────────────────────── */
  .table-container {
    background: var(--bg-surface); border-radius: 16px;
    box-shadow: var(--shadow-card); overflow: hidden;
    margin-bottom: 16px;
  }
  .table-scroll {
    max-height: 600px; overflow-y: auto; overflow-x: auto;
  }
  table {
    width: 100%; border-collapse: separate;
    border-spacing: 0; text-align: left;
  }
  th {
    background: var(--bg-surface); font-size: 12px; font-weight: 700;
    text-transform: uppercase; color: var(--text-secondary);
    padding: 16px 18px; border-bottom: 2px solid var(--border-color);
    letter-spacing: 0.5px; user-select: none;
    position: sticky; top: 0; z-index: 10;
  }
  th[data-sort] { cursor: pointer; transition: color 0.2s; }
  th[data-sort]:hover { color: var(--primary); }
  th .sort-icon { margin-left: 6px; font-size: 11px; opacity: 0.4; }
  th.sort-asc .sort-icon, th.sort-desc .sort-icon {
    opacity: 1; color: var(--primary);
  }
  td {
    padding: 16px 18px; border-bottom: 1px solid var(--border-color);
    vertical-align: top; font-size: 13px; font-weight: 500;
    color: var(--text-main);
  }
  tr:last-child td { border-bottom: none; }
  tbody tr {
    transition: background 0.15s ease;
  }
  tbody tr:hover td {
    background: var(--bg-main);
    box-shadow: inset 3px 0 0 var(--primary);
  }
  tbody tr:hover td:first-child {
    box-shadow: inset 3px 0 0 var(--primary);
  }
  .col-level { width: 90px; text-align: center; }

  /* ── Score bar (data clustering) ──────────────────────────────── */
  .score-bar {
    display: inline-flex; height: 14px; border-radius: 4px;
    overflow: hidden; vertical-align: middle;
  }
  .score-fill { background: var(--primary); height: 100%; }
  .score-empty { background: var(--border-color); height: 100%; }

  /* ── Code & SQL blocks ────────────────────────────────────────── */
  code {
    background: var(--bg-main); color: var(--primary);
    padding: 3px 7px; border-radius: 6px;
    font-family: Consolas, 'Courier New', monospace;
    font-size: 12px; font-weight: 600;
    border: 1px solid var(--border-color);
  }
  :root.dark code {
    color: #32C5A5;
    background: rgba(50,197,165,0.1);
    border-color: rgba(50,197,165,0.15);
  }
  details.sql-details {
    margin-top: 12px; background: var(--bg-main);
    border: 1px solid var(--border-color); border-radius: 10px;
    overflow: hidden;
  }
  details.sql-details summary {
    cursor: pointer; color: var(--primary); font-weight: 700;
    font-size: 13px; padding: 12px 16px; outline: none;
    list-style: none; display: flex; justify-content: space-between;
    align-items: center; transition: background 0.2s;
  }
  details.sql-details summary::-webkit-details-marker { display: none; }
  details.sql-details summary:hover { background: var(--border-color); }
  pre.sql {
    padding: 16px;
    font-family: Consolas, 'Courier New', monospace;
    font-size: 12px; color: var(--text-main); overflow-x: auto;
    margin: 0; background: var(--bg-surface);
    border-top: 1px solid var(--border-color); white-space: pre-wrap;
  }
  .copy-btn {
    background: var(--bg-surface); border: 1px solid var(--border-color);
    color: var(--text-main); padding: 6px 14px; border-radius: 8px;
    cursor: pointer; font-size: 11px; font-weight: 700;
    font-family: inherit; transition: all 0.2s; text-transform: uppercase;
    letter-spacing: 0.05em;
    opacity: 0; pointer-events: none;
  }
  .sql-details summary:hover .copy-btn,
  .sql-details[open] .copy-btn,
  .ddl-details summary:hover .copy-btn,
  .ddl-details[open] .copy-btn {
    opacity: 1; pointer-events: auto;
  }
  .copy-btn:hover {
    background: var(--primary); color: white;
    border-color: var(--primary);
  }
  .copy-btn.success {
    background: var(--c-low-bg); color: var(--c-low-text);
    border-color: var(--c-low-bg);
    opacity: 1; pointer-events: auto;
  }

  /* ── DDL block (data clustering) ──────────────────────────────── */
  .ddl-block {
    background: #1a1d1d; color: #d4d4d4;
    padding: 16px 18px; border-radius: 10px;
    font-family: Consolas, 'Courier New', monospace;
    font-size: 12px; overflow-x: auto; white-space: pre-wrap;
    margin: 8px 0;
  }

  /* ── Misc ─────────────────────────────────────────────────────── */
  small {
    display: block; color: var(--text-muted);
    margin-top: 6px; line-height: 1.6; font-size: 12px; font-weight: 400;
  }
  .footer {
    text-align: center; margin-top: 40px; color: var(--text-muted);
    font-size: 12px; font-weight: 500; padding-bottom: 24px;
  }
  .warn-box {
    background: var(--c-high-bg); border-left: 4px solid var(--c-high);
    padding: 12px 16px; border-radius: 0 10px 10px 0;
    font-size: 13px; margin: 12px 0; color: var(--text-main);
  }

  /* ── Collapsible sections (data clustering) ───────────────────── */
  .section-card {
    background: var(--bg-surface); border-radius: 16px;
    box-shadow: var(--shadow-card);
    margin-bottom: 16px; overflow: hidden;
  }
  .section-card > summary {
    cursor: pointer; padding: 18px 20px; font-weight: 600;
    font-size: 15px; list-style: none; display: flex;
    justify-content: space-between; align-items: center;
    background: var(--bg-surface); transition: background 0.15s;
    color: var(--text-main);
  }
  .section-card > summary::-webkit-details-marker { display: none; }
  .section-card > summary:hover { background: var(--bg-main); }
  .section-card > .section-body { padding: 0 20px 20px 20px; }
  .section-meta {
    display: flex; flex-wrap: wrap; gap: 16px; padding: 8px 0 12px 0;
    font-size: 13px; color: var(--text-muted);
  }
  .section-meta strong { color: var(--text-main); }

  /* ── Custom Scrollbars ─────────────────────────────────────────── */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb {
    background: var(--border-color); border-radius: 3px;
  }
  ::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
  * { scrollbar-width: thin; scrollbar-color: var(--border-color) transparent; }

  /* ── Best Practices Cards ─────────────────────────────────────── */
  .bp-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
    padding: 8px 0;
  }
  .bp-card {
    background: var(--bg-surface);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 20px;
    display: flex;
    gap: 16px;
    box-shadow: var(--shadow-card);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    position: relative;
    overflow: hidden;
  }
  .bp-card:hover {
    transform: translateY(-2px);
    box-shadow: 0px 8px 24px rgba(0, 0, 0, 0.08);
  }
  :root.dark .bp-card:hover {
    box-shadow: 0px 8px 24px rgba(0, 0, 0, 0.4);
  }
  .bp-card::before {
    content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
  }
  .bp-card.do-card::before   { background: var(--c-low); }
  .bp-card.dont-card::before { background: var(--c-crit); }
  .bp-card.warn-card::before { background: var(--c-med); }
  .bp-card.info-card::before { background: var(--primary); }
  .bp-icon {
    font-size: 24px; flex-shrink: 0;
    width: 40px; height: 40px;
    display: flex; align-items: center; justify-content: center;
    border-radius: 8px;
  }
  .do-card .bp-icon   { background: var(--c-low-bg); color: var(--c-low); }
  .dont-card .bp-icon { background: var(--c-crit-bg); color: var(--c-crit); }
  .warn-card .bp-icon { background: var(--c-med-bg); color: var(--c-med); }
  .info-card .bp-icon { background: var(--primary-light); color: var(--primary); }
  .bp-content h4 {
    margin: 0 0 8px 0; font-size: 15px; color: var(--text-main);
  }
  .bp-content ul {
    margin: 0; padding-left: 20px;
    color: var(--text-secondary); font-size: 13px; line-height: 1.6;
  }
  .bp-content li { margin-bottom: 6px; }
  .bp-content li:last-child { margin-bottom: 0; }
  .bp-content code { font-size: 11px; }

  /* ── Responsive ───────────────────────────────────────────────── */
  @media (max-width: 1024px) {
    .app-container { flex-direction: column; }
    .sidebar {
      width: 100%; height: auto; position: relative;
      flex-direction: row; flex-wrap: wrap; align-items: center;
    }
    .brand { border-bottom: none; min-height: 56px; width: 100%; }
    .warehouse-badge {
      width: 100%; margin: 0 16px 8px;
      padding: 10px 16px; display: flex; gap: 8px; align-items: center;
    }
    .warehouse-badge-label { margin-bottom: 0; }
    .tabs-nav {
      flex-direction: row; overflow-x: auto; padding: 8px 12px;
      width: 100%; gap: 8px;
    }
    .tab-btn { white-space: nowrap; padding: 8px 14px; }
    .sidebar-footer { display: none; }
    .main-content { padding: 0 16px 40px 16px; }
    .topbar { flex-direction: column; align-items: flex-start; gap: 16px; }
    .search-input { width: 200px; }
  }

  /* ── Print ────────────────────────────────────────────────────── */
  @media print {
    .sidebar, .controls-area, .copy-btn,
    .theme-toggle { display: none !important; }
    .app-container { display: block; }
    .main-content { padding: 0; }
    .tab-pane { display: block !important; margin-bottom: 32px; }
    .table-container { box-shadow: none; border: 1px solid #ccc; }
    .table-scroll { max-height: none; overflow: visible; }
    .section-card { box-shadow: none; break-inside: avoid; border: 1px solid #ccc; }
    .stat-item { box-shadow: none; border: 1px solid #ccc; cursor: default; }
    .print-title { display: block !important; }
    .topbar { position: static; }
  }
  .print-title { display: none; }
</style>
"""


# ══════════════════════════════════════════════════════════════════════
# SHARED JAVASCRIPT
# ══════════════════════════════════════════════════════════════════════

REPORT_JS = r"""<script>
document.addEventListener('DOMContentLoaded', function() {
  /* ── Dark Mode Logic ──────────────────────────────────────────── */
  var themeBtn = document.getElementById('theme-toggle');
  var root = document.documentElement;
  if (themeBtn) {
    var themeImg = document.getElementById('theme-icon');
    var saved = null;
    try { saved = localStorage.getItem('fwa-theme'); } catch(e) {}
    var prefersDark = window.matchMedia &&
      window.matchMedia('(prefers-color-scheme: dark)').matches;
    if (saved === 'dark' || (!saved && prefersDark)) {
      root.classList.add('dark');
      if (themeImg) themeImg.src = themeImg.dataset.dark;
    } else {
      if (themeImg) themeImg.src = themeImg.dataset.light;
    }
    themeBtn.addEventListener('click', function() {
      themeBtn.classList.add('rotating');
      root.classList.toggle('dark');
      var isDark = root.classList.contains('dark');
      try { localStorage.setItem('fwa-theme', isDark ? 'dark' : 'light'); } catch(e) {}
      if (themeImg) themeImg.src = isDark ? themeImg.dataset.dark : themeImg.dataset.light;
      setTimeout(function() { themeBtn.classList.remove('rotating'); }, 400);
    });
  }

  /* ── Tab Switching ────────────────────────────────────────────── */
  var activeFilter = null;
  var searchTerm = '';

  var tabsNav = document.querySelector('.tabs-nav');
  if (tabsNav) {
    tabsNav.setAttribute('role', 'tablist');
    tabsNav.addEventListener('click', function(e) {
      var btn = e.target.closest('.tab-btn');
      if (!btn) return;
      document.querySelectorAll('.tab-btn').forEach(function(b) {
        b.classList.remove('active');
        b.setAttribute('aria-selected', 'false');
      });
      document.querySelectorAll('.tab-pane').forEach(function(p) {
        p.classList.remove('active');
        p.setAttribute('hidden', '');
      });
      btn.classList.add('active');
      btn.setAttribute('aria-selected', 'true');
      var pane = document.getElementById(btn.getAttribute('data-target'));
      if (pane) { pane.classList.add('active'); pane.removeAttribute('hidden'); }
    });
  }

  /* ── Combined Filter (search + severity) ──────────────────────── */
  function applyFilters() {
    document.querySelectorAll('.tab-pane tbody tr').forEach(function(row) {
      var matchSearch = !searchTerm ||
        row.textContent.toLowerCase().indexOf(searchTerm) >= 0;
      var matchLevel = !activeFilter ||
        row.getAttribute('data-level') === activeFilter;
      row.style.display = (matchSearch && matchLevel) ? '' : 'none';
    });
    document.querySelectorAll('.section-card').forEach(function(sec) {
      var matchSearch = !searchTerm ||
        sec.textContent.toLowerCase().indexOf(searchTerm) >= 0;
      sec.style.display = matchSearch ? '' : 'none';
    });
    updateTabCounts();
  }

  /* ── Search ───────────────────────────────────────────────────── */
  var searchInput = document.getElementById('global-search');
  if (searchInput) {
    searchInput.addEventListener('input', function() {
      searchTerm = this.value.toLowerCase();
      applyFilters();
    });
  }

  /* ── Severity Filter (stat card clicks) ───────────────────────── */
  document.querySelectorAll('.stat-item[data-filter]').forEach(function(card) {
    card.addEventListener('click', function() {
      var f = card.getAttribute('data-filter');
      if (f === 'all' || activeFilter === f) {
        activeFilter = null;
        document.querySelectorAll('.stat-item').forEach(function(c) {
          c.style.opacity = '';
        });
      } else {
        activeFilter = f;
        document.querySelectorAll('.stat-item').forEach(function(c) {
          c.style.opacity =
            (c === card || c.getAttribute('data-filter') === 'all')
            ? '' : '0.4';
        });
      }
      applyFilters();
      if (tabsNav) {
        var first = Array.from(document.querySelectorAll('.tab-btn')).find(
          function(btn) {
            var c = btn.querySelector('.tab-count');
            return c && parseInt(c.textContent.replace(/\D/g,'')) > 0;
          }
        );
        if (first && !first.classList.contains('active')) first.click();
      }
    });
  });

  /* ── Tab Counts ───────────────────────────────────────────────── */
  function updateTabCounts() {
    document.querySelectorAll('.tab-btn').forEach(function(btn) {
      var pane = document.getElementById(btn.getAttribute('data-target'));
      if (!pane) return;
      var tbody = pane.querySelector('tbody');
      var el = btn.querySelector('.tab-count');
      if (!tbody) {
        // No table in this pane (e.g. Best Practices cards) — hide badge
        if (el) el.style.display = 'none';
        return;
      }
      var rows = tbody.querySelectorAll('tr');
      var visible = Array.from(rows).filter(function(r) {
        return r.style.display !== 'none';
      }).length;
      if (el) el.textContent = visible;
      btn.style.opacity =
        (visible === 0 && !btn.classList.contains('active')) ? '0.5' : '';
    });
  }

  /* ── Column Sorting ───────────────────────────────────────────── */
  document.querySelectorAll('th[data-sort]').forEach(function(th) {
    th.addEventListener('click', function() {
      var table = th.closest('table');
      var tbody = table.querySelector('tbody');
      if (!tbody) return;
      var colIdx = Array.from(th.parentNode.children).indexOf(th);
      var sortType = th.getAttribute('data-sort');
      var isAsc = th.classList.contains('sort-asc');

      table.querySelectorAll('th').forEach(function(h) {
        h.classList.remove('sort-asc', 'sort-desc');
      });
      th.classList.add(isAsc ? 'sort-desc' : 'sort-asc');

      var rows = Array.from(tbody.querySelectorAll('tr'));
      var levelOrder = {critical:0, high:1, medium:2, low:3, info:4};

      rows.sort(function(a, b) {
        var aEl = a.children[colIdx], bEl = b.children[colIdx];
        var aVal = aEl ? aEl.textContent.trim() : '';
        var bVal = bEl ? bEl.textContent.trim() : '';
        if (sortType === 'num') {
          aVal = parseFloat(aVal) || 0;
          bVal = parseFloat(bVal) || 0;
        } else if (sortType === 'level') {
          aVal = levelOrder[aVal.toLowerCase()] !== undefined
            ? levelOrder[aVal.toLowerCase()] : 5;
          bVal = levelOrder[bVal.toLowerCase()] !== undefined
            ? levelOrder[bVal.toLowerCase()] : 5;
        } else {
          aVal = aVal.toLowerCase();
          bVal = bVal.toLowerCase();
        }
        if (aVal < bVal) return isAsc ? 1 : -1;
        if (aVal > bVal) return isAsc ? -1 : 1;
        return 0;
      });
      rows.forEach(function(row) { tbody.appendChild(row); });
    });
  });

  /* ── Copy Buttons on SQL / DDL blocks ─────────────────────────── */
  document.querySelectorAll('.sql-details summary, .ddl-details summary')
    .forEach(function(summary) {
      var pre = summary.parentNode.querySelector('pre.sql, .ddl-block');
      if (!pre) return;
      var btn = document.createElement('button');
      btn.className = 'copy-btn';
      btn.textContent = 'Copy';
      btn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        navigator.clipboard.writeText(pre.textContent).then(function() {
          btn.textContent = 'Copied!';
          btn.classList.add('success');
          setTimeout(function() {
            btn.textContent = 'Copy';
            btn.classList.remove('success');
          }, 2000);
        });
      });
      summary.appendChild(btn);
    });

  /* ── Initialise ───────────────────────────────────────────────── */
  updateTabCounts();
});
</script>
"""


# ══════════════════════════════════════════════════════════════════════
# REPORT TYPE ICONS
# ══════════════════════════════════════════════════════════════════════

REPORT_ICONS = {
    "security": f'<img src="{ICON_SECURITY}" width="28" height="28" alt="">',
    "performance": f'<img src="{ICON_PERFORMANCE}" width="28" height="28" alt="">',
    "clustering": f'<img src="{ICON_CLUSTERING}" width="28" height="28" alt="">',
}


# ══════════════════════════════════════════════════════════════════════
# PYTHON HELPERS
# ══════════════════════════════════════════════════════════════════════

def html_open(title: str, *, extra_css: str = "") -> str:
    """Opening boilerplate: doctype, head, body open, script, and app container start."""
    return (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{esc(title)}</title>\n'
        f'{REPORT_CSS}\n'
        f'{extra_css}\n'
        '</head>\n<body>\n'
        f'{REPORT_JS}\n'
        '<div class="app-container">\n'
    )


def html_close() -> str:
    """Closing tags for main-content, app-container, body, html."""
    return '</main>\n</div>\n</body>\n</html>'


def render_sidebar(
    brand_text: str,
    report_type: str,
    warehouse_name: str,
    tabs: list[tuple[str, str]],
    *,
    generated_at: str = "",
    badge_label: str = "Warehouse",
    auth_mode: str = "",
) -> str:
    """Render the sidebar with brand, warehouse badge, tab nav, and footer.

    Parameters
    ----------
    brand_text : str
        Brand label shown in the sidebar header (e.g. "Security").
    report_type : str
        One of ``"security"``, ``"performance"``, ``"clustering"`` — used
        to pick the brand icon.
    warehouse_name : str
        Displayed prominently inside a dedicated warehouse badge.
    tabs : list of (pane_id, label)
        Tab buttons. First tab is marked active.
    generated_at : str
        Timestamp shown in the sidebar footer.
    badge_label : str
        Label above the warehouse name (e.g. "Warehouse" or "SQL Endpoint").
    auth_mode : str
        Auth mode string ("user_identity" or "delegated"). If set, a small
        pill badge is rendered below the warehouse name in the sidebar.
    """
    icon = REPORT_ICONS.get(report_type, "\U0001f4cb")
    parts: list[str] = [
        '<aside class="sidebar">',
        '<div class="brand">',
        f'<span class="brand-icon">{icon}</span> {esc(brand_text)}',
        '</div>',
        # Warehouse badge — prominent
        '<div class="warehouse-badge">',
        f'<div class="warehouse-badge-label">{esc(badge_label)}</div>',
        f'<div class="warehouse-badge-name">{esc(warehouse_name)}</div>',
    ]
    if auth_mode:
        mode_text = 'User Identity' if auth_mode == 'user_identity' else 'Delegated'
        parts.append(
            '<div class="auth-mode-kv">'
            '<span class="mode-label">Access Mode</span>'
            f'<span class="mode-value">{mode_text}</span>'
            '</div>'
        )
    parts += [
        '</div>',
    ]

    # Tab navigation
    # Items can be (pane_id, label) tuples for tab buttons,
    # or plain strings for non-clickable section dividers.
    if tabs:
        parts.append('<nav class="tabs-nav" role="tablist">')
        first_tab = True
        for item in tabs:
            if isinstance(item, str):
                # Section divider label
                parts.append(
                    f'<div class="sidebar-section-label">{esc(item)}</div>'
                )
            else:
                pane_id, label = item
                active_cls = " active" if first_tab else ""
                selected = "true" if first_tab else "false"
                parts.append(
                    f'<button class="tab-btn{active_cls}" '
                    f'data-target="{pane_id}" '
                    f'role="tab" aria-selected="{selected}">'
                    f'{esc(label)} '
                    f'<span class="tab-count"></span></button>'
                )
                first_tab = False
        parts.append('</nav>')

    # Footer
    parts.append('<div class="sidebar-footer">')
    if generated_at:
        parts.append(f'Generated: {esc(generated_at)}')
    parts.append('Fabric Warehouse Advisor</div>')
    parts.append('</aside>')
    return '\n'.join(parts)


def render_main_open(
    title: str,
    subtitle: str,
    *,
    show_search: bool = True,
) -> str:
    """Open the ``<main>`` content area with topbar (title + controls)."""
    parts: list[str] = [
        '<main class="main-content">',
        '<header class="topbar">',
        '<div class="page-header">',
        f'<h1>{esc(title)}</h1>',
        f'<p>{subtitle}</p>',
        '</div>',
        '<div class="controls-area">',
    ]
    if show_search:
        parts += [
            '<div class="search-wrapper">',
            f'<span class="search-icon"><img src="{ICON_SEARCH}" alt=""></span>',
            '<input type="text" class="search-input" id="global-search"'
            ' placeholder="Search rules, objects, findings\u2026">',
            '</div>',
        ]
    parts += [
        f'<button id="theme-toggle" class="theme-toggle" '
        f'aria-label="Toggle Theme"><span class="icon">'
        f'<img id="theme-icon" src="{ICON_MOON}" '
        f'data-light="{ICON_MOON}" data-dark="{ICON_SUN}" '
        f'width="20" height="20" alt="">'
        f'</span></button>',
        '</div>',  # controls-area
        '</header>',
    ]
    return '\n'.join(parts)


def render_severity_stats(
    critical: int,
    high: int,
    medium: int,
    low: int,
    info: int,
    total: int,
) -> str:
    """Clickable severity stat cards with ``data-filter`` attributes."""
    return (
        '<div class="stats-grid" id="stats-filters">\n'
        f'<div class="stat-item stat-crit" data-filter="critical">'
        f'<div class="stat-label">Critical</div>'
        f'<div class="stat-val">{critical}</div></div>\n'
        f'<div class="stat-item stat-high" data-filter="high">'
        f'<div class="stat-label">High</div>'
        f'<div class="stat-val">{high}</div></div>\n'
        f'<div class="stat-item stat-med" data-filter="medium">'
        f'<div class="stat-label">Medium</div>'
        f'<div class="stat-val">{medium}</div></div>\n'
        f'<div class="stat-item stat-low" data-filter="low">'
        f'<div class="stat-label">Low</div>'
        f'<div class="stat-val">{low}</div></div>\n'
        f'<div class="stat-item stat-info" data-filter="info">'
        f'<div class="stat-label">Info</div>'
        f'<div class="stat-val">{info}</div></div>\n'
        f'<div class="stat-item stat-total" data-filter="all">'
        f'<div class="stat-label">Total</div>'
        f'<div class="stat-val">{total}</div></div>\n'
        '</div>\n'
    )


def render_info_stats(items: list[tuple[str, str | int, str]]) -> str:
    """Non-clickable summary stat cards.

    Parameters
    ----------
    items : list of (label, value, css_class)
        *css_class* is e.g. ``"stat-primary"`` or ``""`` for default.
    """
    parts = ['<div class="stats-grid">']
    for label, value, css_class in items:
        parts.append(
            f'<div class="stat-item {css_class}">'
            f'<div class="stat-label">{esc(label)}</div>'
            f'<div class="stat-val">{esc(str(value))}</div>'
            f'</div>'
        )
    parts.append('</div>')
    return '\n'.join(parts)


def severity_pill(level: str) -> str:
    """Return a ``<span class="pill ...">`` for a severity level."""
    cls_map = {
        "critical": "p-crit",
        "high": "p-high",
        "medium": "p-med",
        "low": "p-low",
        "info": "p-info",
    }
    css = cls_map.get(level.lower(), "p-info")
    return f'<span class="pill {css}">{esc(level)}</span>'


def render_sql_block(sql: str) -> str:
    """Collapsible SQL block; the JS auto-injects a *Copy* button."""
    return (
        '<details class="sql-details">'
        '<summary>Show SQL</summary>'
        f'<pre class="sql">{esc(sql)}</pre>'
        '</details>'
    )


def render_footer(text: str) -> str:
    return f'<p class="footer">{esc(text)}</p>\n'

"""
dash_app/ui/components.py
=========================
Dash component renderers for each result section.

Takes section dicts deserialized from ``dcc.Store`` (i.e. the ``to_payload``
output of a ``RunResult``) and returns ``html.Div`` component trees.

Color palette and structural conventions mirror the notebook HTML output in
``data_consistency_checks.py`` for visual parity.
"""
from __future__ import annotations

from dash import html

# ─────────────────────────────────────────────────────────────────────────────
# STYLE CONSTANTS  (mirror data_consistency_checks.py _STATUS_COLOR/_STATUS_BG)
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_COLOR = {
    "PASS":    "#2e7d32",
    "WARNING": "#e65100",
    "FAIL":    "#b71c1c",
    "SKIP":    "#616161",
}
_STATUS_BG = {
    "PASS":    "#e8f5e9",
    "WARNING": "#fff3e0",
    "FAIL":    "#ffebee",
    "SKIP":    "#f5f5f5",
}

_SECTION_STYLE = {
    "border": "1px solid #d0d7de",
    "borderRadius": "6px",
    "padding": "16px",
    "marginBottom": "14px",
    "backgroundColor": "#ffffff",
    "fontFamily": "Arial, sans-serif",
    "fontSize": "0.9em",
}
_H2_STYLE = {
    "fontSize": "1.05em",
    "fontWeight": "bold",
    "margin": "0 0 10px",
    "color": "#1a237e",
    "borderBottom": "2px solid #c5cae9",
    "paddingBottom": "4px",
}
_TABLE_STYLE = {
    "borderCollapse": "collapse",
    "margin": "4px 0 10px",
    "fontSize": "0.9em",
    "width": "100%",
}
_TH_STYLE = {
    "background": "#37474f",
    "color": "white",
    "padding": "3px 12px",
    "textAlign": "left",
    "fontWeight": "normal",
}
_TD_STYLE = {
    "padding": "2px 12px",
    "borderBottom": "1px solid #eeeeee",
}
_NOTE_STYLE = {"margin": "3px 0 3px 14px"}


# ─────────────────────────────────────────────────────────────────────────────
# LOW-LEVEL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _badge(status: str) -> html.Span:
    color = _STATUS_COLOR.get(status, "#000")
    bg    = _STATUS_BG.get(status, "#fff")
    return html.Span(
        status,
        style={
            "display": "inline-block",
            "padding": "1px 7px",
            "borderRadius": "3px",
            "fontWeight": "bold",
            "fontSize": "0.88em",
            "color": color,
            "backgroundColor": bg,
            "marginLeft": "6px",
        },
    )


def _records_table(records: list[dict], max_rows: int = 10) -> html.Div:
    """Render a list-of-dicts as a compact html.Table, capped at max_rows."""
    if not records:
        return html.Em("No rows", style={"color": "#9e9e9e"})

    shown = records[:max_rows]
    cols  = list(shown[0].keys())

    header = html.Thead(html.Tr([html.Th(c, style=_TH_STYLE) for c in cols]))

    def _td(val, i):
        bg = "#fafafa" if i % 2 == 1 else "white"
        return html.Td(str(val) if val is not None else "", style={**_TD_STYLE, "background": bg})

    body_rows = [
        html.Tr([_td(row.get(c), idx) for c in cols])
        for idx, row in enumerate(shown)
    ]

    extra = []
    if len(records) > max_rows:
        extra = [html.Tr(html.Td(
            f"… {len(records) - max_rows} more rows not shown",
            colSpan=len(cols),
            style={"color": "#9e9e9e", "fontStyle": "italic", "padding": "2px 12px"},
        ))]

    return html.Table(
        [header, html.Tbody(body_rows + extra)],
        style=_TABLE_STYLE,
    )


def _pass_summary_line(table_names: list[str]) -> html.Div:
    return html.Div(
        [_badge("PASS"), f" {len(table_names)} table(s): {', '.join(table_names)}"],
        style={"color": "#2e7d32", "margin": "4px 0"},
    )


def _section_wrapper(section_id: str, header_text: str, status: str, body: list) -> html.Div:
    return html.Div(
        id=f"section-{section_id}",
        children=[
            html.H3(
                [header_text, _badge(status)],
                style=_H2_STYLE,
            ),
            *body,
        ],
        style=_SECTION_STYLE,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SECTION RENDERERS
# ─────────────────────────────────────────────────────────────────────────────

def render_section1(section: dict) -> html.Div:
    """Check 1 — Key Column Registration."""
    checks      = section.get("checks", [])
    fail_checks = [c for c in checks if c["status"] == "FAIL"]
    pass_tables = [c["table_name"] for c in checks if c["status"] == "PASS"]

    body = []
    for c in fail_checks:
        missing = c.get("details", {}).get("missing", [])
        body.append(html.Div(
            [html.Span(c["table_name"], style={"fontFamily": "monospace", "fontWeight": "bold",
                                               "color": "#283593"}),
             _badge("FAIL")],
            style={"margin": "10px 0 2px"},
        ))
        if missing:
            body.append(_records_table(missing))

    if pass_tables:
        body.append(_pass_summary_line(pass_tables))

    if not checks:
        body.append(html.P("No checks executed.", style={"color": "#9e9e9e"}))

    return _section_wrapper("check1", "Check 1 — Key Column Registration",
                            section.get("status", "SKIP"), body)


def render_section2(section: dict) -> html.Div:
    """Checks 2–5 — Referential Integrity, Row Uniqueness & Parent Columns."""
    checks      = section.get("checks", [])
    pass_tables = [c["table_name"] for c in checks
                   if c["status"] == "PASS" and c["table_name"]]
    fail_checks = [c for c in checks if c["status"] != "PASS"]

    body = []
    for c in fail_checks:
        d   = c.get("details", {})
        tbl = c["table_name"] or c["check_label"]
        body.append(html.Div(
            [html.Span(tbl, style={"fontFamily": "monospace", "fontWeight": "bold",
                                   "color": "#283593"}),
             _badge(c["status"])],
            style={"margin": "10px 0 2px"},
        ))

        # Forward RI
        if d.get("forward_unmatched", 0) > 0:
            total = d.get("forward_unmatched", 0)
            prem  = d.get("forward_premium")
            pct   = d.get("forward_premium_pct")
            summary = f"{total} unmatched combination(s)"
            if prem is not None:
                summary += f" — {prem:,.0f} premium ({pct:.1f}% of total)" if pct is not None else f" — {prem:,.0f} premium"
            top10 = d.get("forward_top10", [])
            body.append(html.Div(
                [html.B("DetailedData → Reference table "), _badge("FAIL"),
                 f" {summary} (top {min(10, len(top10))} by premium shown)"],
                style=_NOTE_STYLE,
            ))
            body.append(_records_table(top10))

        # Reverse RI
        if d.get("reverse_orphaned", 0) > 0:
            total = d.get("reverse_orphaned", 0)
            fp    = d.get("forecast_orphaned_premium")
            summary = f"{total} orphaned combination(s) in reference table"
            if fp is not None:
                summary += f" — {fp:,.0f} forecast premium affected"
            rows = d.get("forecast_orphaned_df") or d.get("reverse_orphaned_rows", [])
            body.append(html.Div(
                [html.B("Reference table → DetailedData "), _badge("WARNING"), f" {summary}"],
                style=_NOTE_STYLE,
            ))
            body.append(_records_table(rows))

        # Row uniqueness
        if d.get("duplicate_count", 0) > 0:
            body.append(html.Div(
                [html.B("Row uniqueness "), _badge("FAIL"),
                 f" {d['duplicate_count']} duplicate combination(s) — "
                 "join to DetailedData is ambiguous"],
                style=_NOTE_STYLE,
            ))
            body.append(_records_table(d.get("duplicates", [])))

        # Parent columns
        if d.get("failing_cols"):
            notes = "; ".join(
                f"{f['column']} (vs {f['key_col']})" for f in d["failing_cols"]
            )
            body.append(html.Div(
                [html.B("Parent column "), _badge("FAIL"), f" non-parent columns: {notes}"],
                style=_NOTE_STYLE,
            ))

        # Domain membership (tbl_Min_Large_Load)
        for df_fail in d.get("domain_fails", []):
            vals_str = ", ".join(str(v) for v in df_fail["invalid_values"][:10])
            extra    = "…" if len(df_fail["invalid_values"]) > 10 else ""
            body.append(html.Div(
                [html.B("Domain membership "), _badge("FAIL"),
                 f" {df_fail['column']}: values not in DetailedData: {vals_str}{extra}"],
                style=_NOTE_STYLE,
            ))

    if pass_tables:
        body.append(_pass_summary_line(pass_tables))

    if not checks:
        body.append(html.P("No checks executed.", style={"color": "#9e9e9e"}))

    return _section_wrapper(
        "checks2_5",
        "Checks 2–5 — Referential Integrity, Row Uniqueness & Parent Columns",
        section.get("status", "SKIP"),
        body,
    )


def render_section3(section: dict) -> html.Div:
    """Check 6 — Mapping Table Uniqueness."""
    checks = section.get("checks", [])
    body   = []

    for c in checks:
        d     = c.get("details", {})
        st    = c["status"]
        total = d.get("total_duplicates", 0)
        body.append(html.Div(
            [html.B("Non-Key_* uniqueness "), _badge(st),
             (f" — {total} duplicate combination(s)" if st == "FAIL"
              else (" — No duplicate combinations." if st == "PASS"
                    else f" — {c.get('summary', '')}"))],
            style=_NOTE_STYLE,
        ))
        if st == "FAIL":
            body.append(_records_table(d.get("duplicates", [])))

    if not checks:
        body.append(html.P("tbl_Key_Mapping not loaded.", style={"color": "#9e9e9e"}))

    return _section_wrapper("check6", "Check 6 — tbl_Key_Mapping",
                            section.get("status", "SKIP"), body)


def render_section4(section: dict) -> html.Div:
    """Check 7 — Value Range Checks."""
    checks = section.get("checks", [])
    body   = []

    if not checks:
        body.append(html.P("No value range checks performed.", style={"color": "#9e9e9e"}))
        return _section_wrapper("check7", "Check 7 — Value Range Checks",
                                section.get("status", "SKIP"), body)

    # Summary table for all non-DevM checks
    non_devm = [c for c in checks if c.get("details", {}).get("column") != "DevM"]
    if non_devm:
        rows = [html.Tr([
            html.Th("Table",  style=_TH_STYLE),
            html.Th("Column", style=_TH_STYLE),
            html.Th("Status", style=_TH_STYLE),
            html.Th("Detail", style=_TH_STYLE),
        ])]
        for c in non_devm:
            d = c.get("details", {})
            rows.append(html.Tr([
                html.Td(d.get("table", ""), style=_TD_STYLE),
                html.Td(d.get("column", ""), style=_TD_STYLE),
                html.Td(_badge(c["status"]), style=_TD_STYLE),
                html.Td(d.get("detail", c.get("summary", "")), style=_TD_STYLE),
            ]))
        body.append(html.Table(rows, style=_TABLE_STYLE))

    # DevM detail — failing patterns only
    for c in checks:
        d = c.get("details", {})
        if d.get("column") != "DevM":
            continue
        fails = d.get("failing_patterns", [])
        if not fails:
            continue
        body.append(html.Div(
            html.B(f"{d.get('table', '')} — DevM sequence failures ({len(fails)} shown)"),
            style=_NOTE_STYLE,
        ))
        body.append(_records_table(fails))

    return _section_wrapper("check7", "Check 7 — Value Range Checks",
                            section.get("status", "SKIP"), body)


def render_section5(section: dict) -> html.Div:
    """Check 8 — Key_Modelling Coverage."""
    checks = section.get("checks", [])
    body   = []

    if checks:
        header_row = html.Tr([
            html.Th("Column",           style=_TH_STYLE),
            html.Th("Status",           style=_TH_STYLE),
            html.Th("Unmapped rows",    style={**_TH_STYLE, "textAlign": "right"}),
            html.Th("Unmapped premium", style={**_TH_STYLE, "textAlign": "right"}),
            html.Th("% of total",       style={**_TH_STYLE, "textAlign": "right"}),
        ])
        data_rows = []
        for idx, c in enumerate(checks):
            d  = c.get("details", {})
            bg = "#fafafa" if idx % 2 == 1 else "white"
            data_rows.append(html.Tr([
                html.Td(d.get("column", ""),
                        style={**_TD_STYLE, "fontFamily": "monospace", "background": bg}),
                html.Td(_badge(c["status"]), style={**_TD_STYLE, "background": bg}),
                html.Td(f"{d.get('unmapped_count', 0):,}",
                        style={**_TD_STYLE, "textAlign": "right", "background": bg}),
                html.Td(f"{d.get('unmapped_premium', 0):,.0f}",
                        style={**_TD_STYLE, "textAlign": "right", "background": bg}),
                html.Td(f"{d.get('premium_pct', 0.0):.1f}%",
                        style={**_TD_STYLE, "textAlign": "right", "background": bg}),
            ]))
        body.append(html.Table([header_row, *data_rows], style=_TABLE_STYLE))
        body.append(html.Div(
            "Unmapped = null or blank value — these rows will not join to any reference table.",
            style={"color": "#616161", "fontSize": "0.85em", "margin": "3px 0 3px 14px"},
        ))
    else:
        body.append(html.P("No Key_Modelling* columns found in tbl_DetailedData.",
                           style={"color": "#9e9e9e"}))

    return _section_wrapper("check8", "Check 8 — Key_Modelling Coverage",
                            section.get("status", "SKIP"), body)


# ─────────────────────────────────────────────────────────────────────────────
# BANNER
# ─────────────────────────────────────────────────────────────────────────────

def render_banner(status: str, summary: str, metadata: dict) -> html.Div:
    """Top-level status banner matching the notebook display_banner() output."""
    color = _STATUS_COLOR.get(status, "#000")
    bg    = _STATUS_BG.get(status, "#fff")
    n     = metadata.get("dataset_count", 0)
    ms    = metadata.get("runtime_ms", 0)
    return html.Div(
        style={
            "background": bg,
            "border": f"2px solid {color}",
            "padding": "14px 22px",
            "borderRadius": "4px",
            "marginBottom": "16px",
        },
        children=[
            html.Span(
                f"PAT Data Consistency — {status}",
                style={"fontSize": "1.5em", "fontWeight": "bold", "color": color},
            ),
            html.Span(
                f"  {n} table(s) loaded  ·  {ms} ms",
                style={"color": "#616161", "marginLeft": "18px", "fontSize": "0.9em"},
            ),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# TOP-LEVEL DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

def render_all_sections(payload: dict | None) -> tuple:
    """
    Given the deserialized ``RunResult`` payload from ``dcc.Store``, return a
    7-tuple: (banner, section1, section2, section3, section4, section5, no_run_msg_hidden)

    Returns placeholder content when ``payload`` is None or status is SKIP.
    """
    if payload is None or payload.get("status") == "SKIP":
        placeholder = html.P("Press Run to execute checks.",
                             style={"color": "#9e9e9e", "margin": "12px 0"})
        empty_section = html.Div(style=_SECTION_STYLE)
        return (
            placeholder,   # banner area
            empty_section, empty_section, empty_section,
            empty_section, empty_section,
        )

    status   = payload.get("status", "SKIP")
    summary  = payload.get("summary", "")
    metadata = payload.get("metadata", {})
    sections = {s["section_id"]: s for s in payload.get("sections", [])}

    banner = render_banner(status, summary, metadata)

    s1 = render_section1(sections.get("check1",    {"checks": [], "status": "SKIP"}))
    s2 = render_section2(sections.get("checks2_5", {"checks": [], "status": "SKIP"}))
    s3 = render_section3(sections.get("check6",    {"checks": [], "status": "SKIP"}))
    s4 = render_section4(sections.get("check7",    {"checks": [], "status": "SKIP"}))
    s5 = render_section5(sections.get("check8",    {"checks": [], "status": "SKIP"}))

    return banner, s1, s2, s3, s4, s5

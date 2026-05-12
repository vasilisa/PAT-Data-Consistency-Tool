"""
tests/test_ui_components.py
===========================
Unit tests for Dash UI renderers in dash_app/ui/components.py.
"""
from __future__ import annotations

from dash import html

from dash_app.ui.components import _records_table, render_section5


def _collect_text(node) -> list[str]:
    """Recursively collect text from Dash component trees."""
    texts: list[str] = []

    if node is None:
        return texts

    if isinstance(node, str):
        return [node]

    if isinstance(node, (list, tuple)):
        for item in node:
            texts.extend(_collect_text(item))
        return texts

    children = getattr(node, "children", None)
    if children is not None:
        texts.extend(_collect_text(children))
    return texts


def _count_components(node, component_type) -> int:
    """Count components of a given Dash type in a nested component tree."""
    if node is None:
        return 0

    count = 1 if isinstance(node, component_type) else 0

    if isinstance(node, (list, tuple)):
        return count + sum(_count_components(item, component_type) for item in node)

    children = getattr(node, "children", None)
    if children is not None:
        count += _count_components(children, component_type)
    return count


def test_render_section5_includes_breakdown_table_for_warning_rows():
    section = {
        "section_id": "check8",
        "section_label": "Check 8 — Key_Modelling Coverage",
        "status": "WARNING",
        "checks": [
            {
                "check_id": "check8.Key_Modelling",
                "check_label": "Key_Modelling coverage — Key_Modelling",
                "table_name": None,
                "status": "WARNING",
                "summary": "2 unmapped rows — 100 premium (25.0% of total)",
                "details": {
                    "column": "Key_Modelling",
                    "unmapped_count": 2,
                    "unmapped_premium": 100.0,
                    "premium_pct": 25.0,
                    "total_premium": 400.0,
                    "id_cols": ["State", "LOB"],
                    "breakdown": [
                        {"State": "NSW", "LOB": "A", "Premium": 60.0},
                        {"State": "VIC", "LOB": "B", "Premium": 40.0},
                    ],
                },
            }
        ],
    }

    rendered = render_section5(section)

    assert isinstance(rendered, html.Div)
    assert _count_components(rendered, html.Table) == 2

    text = " ".join(_collect_text(rendered))
    assert "Key_Modelling — unmapped premium by segment" in text
    assert "identifier columns from tbl_Key_Mapping" in text
    assert "% of total" in text
    assert "Premium_%" in text or "Premium %" in text


def test_render_section5_omits_breakdown_block_when_no_warning_rows():
    section = {
        "section_id": "check8",
        "section_label": "Check 8 — Key_Modelling Coverage",
        "status": "PASS",
        "checks": [
            {
                "check_id": "check8.Key_Modelling",
                "check_label": "Key_Modelling coverage — Key_Modelling",
                "table_name": None,
                "status": "PASS",
                "summary": "No unmapped rows.",
                "details": {
                    "column": "Key_Modelling",
                    "unmapped_count": 0,
                    "unmapped_premium": 0.0,
                    "premium_pct": 0.0,
                    "total_premium": 400.0,
                    "id_cols": [],
                    "breakdown": None,
                },
            }
        ],
    }

    rendered = render_section5(section)

    assert isinstance(rendered, html.Div)
    assert _count_components(rendered, html.Table) == 1

    text = " ".join(_collect_text(rendered))
    assert "unmapped premium by segment" not in text
    assert "identifier columns from tbl_Key_Mapping" not in text


def test_records_table_scrollable_and_not_capped_by_default():
    records = [{"col": i} for i in range(25)]

    rendered = _records_table(records)

    assert isinstance(rendered, html.Div)
    assert rendered.style["maxHeight"] == "300px"
    assert rendered.style["overflowY"] == "auto"
    assert rendered.style["overflowX"] == "auto"

    text = " ".join(_collect_text(rendered))
    assert "more rows not shown" not in text

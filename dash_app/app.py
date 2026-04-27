from __future__ import annotations

from datetime import datetime, timezone
import logging

from dash import Dash, Input, Output, html

from dash_app.engine.contracts import RunMetadata, RunResult, Status, empty_run_result, to_payload
from dash_app.runner.orchestrator import run_all_checks
from dash_app.ui.components import render_all_sections
from dash_app.ui.layout import build_layout

logger = logging.getLogger(__name__)

app = Dash(__name__)
app.layout = build_layout()


@app.callback(
    Output("run-result-store", "data"),
    Output("run-status-banner", "children"),
    Input("run-button", "n_clicks"),
)
def on_run_clicked(n_clicks: int):
    """Stage 2 callback: loads datasets and executes all consistency checks."""

    if not n_clicks:
        payload = to_payload(empty_run_result())
        return payload, "No run executed yet."

    try:
        result = run_all_checks()
    except Exception as exc:  # pragma: no cover - callback-level guard
        logger.exception("Unhandled callback error in on_run_clicked")
        now = datetime.now(timezone.utc).isoformat()
        result = RunResult(
            status=Status.FAIL,
            summary="Fatal callback error while running checks.",
            metadata=RunMetadata(
                started_at_utc=now,
                completed_at_utc=now,
                runtime_ms=0,
                dataset_count=0,
                dataset_names=[],
            ),
            sections=[],
            errors=[str(exc)],
        )

    payload = to_payload(result)
    warning_note = f" | warnings: {len(result.warnings)}" if result.warnings else ""
    error_note = f" | errors: {len(result.errors)}" if result.errors else ""
    banner = f"Overall Status: {result.status.value} | {result.summary}{warning_note}{error_note}"
    return payload, banner


@app.callback(
    Output("result-banner",    "children"),
    Output("section-check1",   "children"),
    Output("section-checks2_5", "children"),
    Output("section-check6",   "children"),
    Output("section-check7",   "children"),
    Output("section-check8",   "children"),
    Input("run-result-store",  "data"),
)
def on_result_stored(payload: dict | None):
    """Stage 3 callback: renders all result sections from the dcc.Store payload."""
    banner, s1, s2, s3, s4, s5 = render_all_sections(payload)

    # Force React remount per execution to avoid stale/stacked section content
    # when the app is run repeatedly in a long-lived backend session.
    run_key = "no-run"
    if payload:
        run_key = payload.get("metadata", {}).get("started_at_utc", "no-run")

    return (
        banner,
        html.Div(s1, key=f"{run_key}-check1"),
        html.Div(s2, key=f"{run_key}-checks2_5"),
        html.Div(s3, key=f"{run_key}-check6"),
        html.Div(s4, key=f"{run_key}-check7"),
        html.Div(s5, key=f"{run_key}-check8"),
    )


if __name__ == "__main__":
    app.run(debug=True)


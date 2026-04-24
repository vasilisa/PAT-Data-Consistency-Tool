from __future__ import annotations

from dash import dcc, html


def build_layout() -> html.Div:
    """Stage 3 layout: live result sections rendered by callback from dcc.Store."""

    return html.Div(
        children=[
            html.H1(
                "PAT Data Consistency Tool",
                style={"color": "#1a237e", "marginBottom": "4px"},
            ),
            html.P(
                "Run all PAT data consistency checks against the Dataiku project datasets.",
                style={"color": "#616161", "marginTop": "0", "marginBottom": "16px"},
            ),
            html.Button(
                "Run",
                id="run-button",
                n_clicks=0,
                style={
                    "backgroundColor": "#1a237e",
                    "color": "white",
                    "border": "none",
                    "padding": "8px 24px",
                    "borderRadius": "4px",
                    "fontSize": "1em",
                    "cursor": "pointer",
                    "marginBottom": "16px",
                },
            ),
            dcc.Store(id="run-result-store"),
            dcc.Loading(
                id="run-loading",
                type="dot",
                children=html.Div(id="run-status-banner"),
            ),
            # Result banner populated by render callback
            html.Div(id="result-banner"),
            # One output div per section — populated by render callback
            html.Div(id="section-check1"),
            html.Div(id="section-checks2_5"),
            html.Div(id="section-check6"),
            html.Div(id="section-check7"),
            html.Div(id="section-check8"),
        ],
        style={
            "maxWidth": "1100px",
            "margin": "0 auto",
            "padding": "24px",
            "backgroundColor": "#f6f8fa",
            "fontFamily": "Arial, sans-serif",
        },
    )


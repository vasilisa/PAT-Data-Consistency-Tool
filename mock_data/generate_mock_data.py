#!/usr/bin/env python3
"""Generate mock PAT data consistency CSVs.

Run from anywhere:
    python mock_data/generate_mock_data.py

Outputs are written to the same folder as this script.
"""

from __future__ import annotations

import csv
from pathlib import Path


OUTPUT_DIR = Path(__file__).resolve().parent


def write_csv(filename: str, rows: list[dict]) -> None:
    if not rows:
        raise ValueError(f"No rows provided for {filename}")
    out_path = OUTPUT_DIR / filename
    fieldnames = list(rows[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {out_path}")


def build_tbl_detaileddata() -> None:
    rows = [
        {
            "AsAt_Month": 202604,
            "Premium": 100000,
            "Segment": "S1",
            "Region": "North",
            "LOB": "Motor",
            "Key_RateChange": "RC_A",
            "Key_Trend": "TR_A",
            "Key_Pattern_Attr": "PA_A",
            "Key_Pattern_Large": "PL_A",
            "Key_Pattern_Prem": "PP_A",
            "Key_Forecast": "F1",
        },
        {
            "AsAt_Month": 202604,
            "Premium": 80000,
            "Segment": "S2",
            "Region": "South",
            "LOB": "Motor",
            "Key_RateChange": "RC_B",
            "Key_Trend": "TR_B",
            "Key_Pattern_Attr": "PA_B",
            "Key_Pattern_Large": "PL_B",
            "Key_Pattern_Prem": "PP_B",
            "Key_Forecast": "F1",
        },
        {
            "AsAt_Month": 202604,
            "Premium": 60000,
            "Segment": "S3",
            "Region": "East",
            "LOB": "Property",
            "Key_RateChange": "RC_MISS",
            "Key_Trend": "TR_A",
            "Key_Pattern_Attr": "PA_A",
            "Key_Pattern_Large": "PL_A",
            "Key_Pattern_Prem": "PP_A",
            "Key_Forecast": "F1",
        },
        {
            "AsAt_Month": 202613,
            "Premium": 50000,
            "Segment": "S4",
            "Region": "West",
            "LOB": "Liability",
            "Key_RateChange": "RC_A",
            "Key_Trend": "TR_MISS",
            "Key_Pattern_Attr": "PA_C",
            "Key_Pattern_Large": "PL_C",
            "Key_Pattern_Prem": "PP_C",
            "Key_Forecast": "F2",
        },
    ]
    write_csv("tbl_DetailedData.csv", rows)


def build_tbl_key_mapping() -> None:
    rows = [
        {
            "Segment": "S1",
            "Region": "North",
            "LOB": "Motor",
            "Key_RateChange": "RC_A",
            "Key_Trend": "TR_A",
            "Key_Pattern_Attr": "PA_A",
            "Key_Pattern_Large": "PL_A",
            "Key_Pattern_Prem": "PP_A",
        },
        {
            "Segment": "S2",
            "Region": "South",
            "LOB": "Motor",
            "Key_RateChange": "RC_B",
            "Key_Trend": "TR_B",
            "Key_Pattern_Attr": "PA_B",
            "Key_Pattern_Large": "PL_B",
            "Key_Pattern_Prem": "PP_B",
        },
        {
            "Segment": "S3",
            "Region": "East",
            "LOB": "Property",
            "Key_RateChange": "RC_C",
            "Key_Trend": "TR_C",
            "Key_Pattern_Attr": "PA_A",
            "Key_Pattern_Large": "PL_A",
            "Key_Pattern_Prem": "PP_A",
        },
        {
            "Segment": "S4",
            "Region": "West",
            "LOB": "Liability",
            "Key_RateChange": "RC_A",
            "Key_Trend": "TR_D",
            "Key_Pattern_Attr": "PA_C",
            "Key_Pattern_Large": "PL_C",
            "Key_Pattern_Prem": "PP_C",
        },
        {
            "Segment": "S2",
            "Region": "South",
            "LOB": "Motor",
            "Key_RateChange": "RC_B",
            "Key_Trend": "TR_BX",
            "Key_Pattern_Attr": "PA_B",
            "Key_Pattern_Large": "PL_B",
            "Key_Pattern_Prem": "PP_B",
        },
    ]
    write_csv("tbl_Key_Mapping.csv", rows)


def build_tbl_ratechange() -> None:
    rows = [
        {"Key_RateChange": "RC_A", "Region": "North", "RateChange": 0.05},
        {"Key_RateChange": "RC_B", "Region": "South", "RateChange": 0.90},
        {"Key_RateChange": "RC_C", "Region": "East", "RateChange": -0.80},
        {"Key_RateChange": "RC_ORPHAN", "Region": "West", "RateChange": 0.01},
    ]
    write_csv("tbl_RateChange.csv", rows)


def build_tbl_ratechange_pol() -> None:
    rows = [
        {"Key_RateChange": "RC_A", "RateChange": 0.02},
        {"Key_RateChange": "RC_B", "RateChange": 0.60},
        {"Key_RateChange": "RC_C", "RateChange": -0.55},
    ]
    write_csv("tbl_RateChange_Pol.csv", rows)


def build_tbl_trend() -> None:
    rows = [
        {"Key_Trend": "TR_A", "Region": "North", "Trend_Value": 0.03},
        {"Key_Trend": "TR_B", "Region": "South", "Trend_Value": 0.40},
        {"Key_Trend": "TR_C", "Region": "East", "Trend_Value": -0.10},
        {"Key_Trend": "TR_ORPHAN", "Region": "West", "Trend_Value": 0.02},
    ]
    write_csv("tbl_Trend.csv", rows)


def build_tbl_patterns_attr() -> None:
    rows = [
        {"Key_Pattern_Attr": "PA_A", "DevM": 1, "DevPct_Incd_Attr": 0.20, "DevPct_Paid_Attr": 0.15},
        {"Key_Pattern_Attr": "PA_A", "DevM": 2, "DevPct_Incd_Attr": 0.45, "DevPct_Paid_Attr": 0.35},
        {"Key_Pattern_Attr": "PA_A", "DevM": 4, "DevPct_Incd_Attr": 1.25, "DevPct_Paid_Attr": 1.10},
        {"Key_Pattern_Attr": "PA_B", "DevM": 2, "DevPct_Incd_Attr": 0.80, "DevPct_Paid_Attr": 0.75},
        {"Key_Pattern_Attr": "PA_C", "DevM": 1, "DevPct_Incd_Attr": 1.30, "DevPct_Paid_Attr": 1.25},
    ]
    write_csv("tbl_Patterns_Attr.csv", rows)


def build_tbl_patterns_large() -> None:
    rows = [
        {"Key_Pattern_Large": "PL_A", "DevM": 1, "DevPct_Incd_Large": 0.30},
        {"Key_Pattern_Large": "PL_A", "DevM": 2, "DevPct_Incd_Large": 0.50},
        {"Key_Pattern_Large": "PL_B", "DevM": 1, "DevPct_Incd_Large": 0.95},
        {"Key_Pattern_Large": "PL_B", "DevM": 3, "DevPct_Incd_Large": 1.40},
        {"Key_Pattern_Large": "PL_C", "DevM": 5, "DevPct_Incd_Large": 1.20},
    ]
    write_csv("tbl_Patterns_Large.csv", rows)


def build_tbl_patterns_prem() -> None:
    rows = [
        {"Key_Pattern_Prem": "PP_A", "DevM": 1, "DevPct_Premium": 0.25},
        {"Key_Pattern_Prem": "PP_A", "DevM": 2, "DevPct_Premium": 0.55},
        {"Key_Pattern_Prem": "PP_B", "DevM": 1, "DevPct_Premium": 0.90},
        {"Key_Pattern_Prem": "PP_B", "DevM": 2, "DevPct_Premium": 1.10},
        {"Key_Pattern_Prem": "PP_C", "DevM": 10, "DevPct_Premium": 1.30},
    ]
    write_csv("tbl_Patterns_Prem.csv", rows)


def build_tbl_forecast_adjustment() -> None:
    rows = [
        {"Key_Forecast": "F1", "ForecastAdj": 1.02},
        {"Key_Forecast": "F2", "ForecastAdj": 0.98},
    ]
    write_csv("tbl_Forecast_Adjustment.csv", rows)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    build_tbl_detaileddata()
    build_tbl_key_mapping()
    build_tbl_ratechange()
    build_tbl_ratechange_pol()
    build_tbl_trend()
    build_tbl_patterns_attr()
    build_tbl_patterns_large()
    build_tbl_patterns_prem()
    build_tbl_forecast_adjustment()

    print("Mock data generation complete.")


if __name__ == "__main__":
    main()

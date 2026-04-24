from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from dash_app.engine.checks import (
    check_key_modelling_unmapped,
    check_key_registration,
    check_mapping_uniqueness,
    check_parent_columns,
    check_referential_integrity,
    check_row_uniqueness,
    check_value_ranges,
)
from dash_app.engine.contracts import CheckResult, RunMetadata, RunResult, SectionResult, Status
from dash_app.engine.loader import classify_tables, load_tbl_datasets


def _derive_overall_status(statuses: list[Status]) -> Status:
    """Apply precedence FAIL > WARNING > PASS > SKIP for rollup."""

    if Status.FAIL in statuses:
        return Status.FAIL
    if Status.WARNING in statuses:
        return Status.WARNING
    if Status.PASS in statuses:
        return Status.PASS
    return Status.SKIP


def build_static_stage1_result() -> RunResult:
    """Static Stage 1 payload used by the Dash skeleton before logic migration."""

    started = datetime.now(timezone.utc)

    sections = [
        SectionResult(
            section_id="check1",
            section_label="Check 1 - Key Column Registration",
            status=Status.SKIP,
            summary="Placeholder: Stage 2 will bind real Check 1 results.",
            checks=[
                CheckResult(
                    check_id="check1.key_registration",
                    check_label="Key column registration",
                    table_name=None,
                    status=Status.SKIP,
                    summary="No execution in Stage 1.",
                )
            ],
        ),
        SectionResult(
            section_id="checks2_5",
            section_label="Checks 2-5 - Referential Integrity, Row Uniqueness, Parent Columns",
            status=Status.SKIP,
            summary="Placeholder: Stage 2 will bind checks 2-5 with parity ordering.",
            checks=[],
        ),
        SectionResult(
            section_id="check6",
            section_label="Check 6 - Mapping Table Uniqueness",
            status=Status.SKIP,
            summary="Placeholder: Stage 2 will bind mapping uniqueness details.",
            checks=[],
        ),
        SectionResult(
            section_id="check7",
            section_label="Check 7 - Value Range Checks",
            status=Status.SKIP,
            summary="Placeholder: Stage 2 will bind range-check validators.",
            checks=[],
        ),
        SectionResult(
            section_id="check8",
            section_label="Check 8 - Key_Modelling Coverage",
            status=Status.SKIP,
            summary="Placeholder: Stage 2 will bind Key_Modelling coverage outputs.",
            checks=[],
        ),
    ]

    overall = _derive_overall_status([section.status for section in sections])
    completed = datetime.now(timezone.utc)

    return RunResult(
        status=overall,
        summary="Stage 1 scaffold ready. No data checks executed yet.",
        metadata=RunMetadata(
            started_at_utc=started.isoformat(),
            completed_at_utc=completed.isoformat(),
            runtime_ms=int((completed - started).total_seconds() * 1000),
            dataset_count=0,
            dataset_names=[],
        ),
        sections=sections,
    )


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — REAL ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

def _df_to_records(df: pd.DataFrame | None, max_rows: int = 20) -> list[dict]:
    """Serialize a DataFrame (or None) to a JSON-safe list of row dicts."""
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    return df.head(max_rows).astype(object).where(df.head(max_rows).notna(), None).to_dict("records")


def _status(raw: str) -> Status:
    """Map a raw status string from a check function to the Status enum."""
    try:
        return Status(raw)
    except ValueError:
        return Status.SKIP


def _section_status(checks: list[CheckResult]) -> Status:
    """Roll up a list of CheckResult statuses using FAIL > WARNING > PASS > SKIP."""
    statuses = [c.status for c in checks]
    return _derive_overall_status(statuses)


# ── Section builders ──────────────────────────────────────────────────────────

def _build_section1(reg_results: dict) -> SectionResult:
    checks = []
    for tbl, res in reg_results.items():
        st = _status(res["status"])
        missing = res.get("missing", [])
        summary = (
            f"{len(missing)} missing key column registration(s)"
            if missing else "All Key_* columns registered."
        )
        checks.append(CheckResult(
            check_id=f"check1.{tbl}",
            check_label=f"Key column registration — {tbl}",
            table_name=tbl,
            status=st,
            summary=summary,
            details={"missing": missing},
        ))
    sec_status = _section_status(checks) if checks else Status.SKIP
    return SectionResult(
        section_id="check1",
        section_label="Check 1 — Key Column Registration",
        status=sec_status,
        summary=_section_summary(checks, "table(s) checked"),
        checks=checks,
    )


def _build_section2(
    ri_results: dict,
    uniq_results: dict,
    parent_results: dict,
) -> SectionResult:
    all_tables = sorted({
        t for t in list(ri_results) + list(uniq_results) + list(parent_results)
    })
    checks = []
    for tbl in all_tables:
        ri     = ri_results.get(tbl, {})
        uniq   = uniq_results.get(tbl, {})
        parent = parent_results.get(tbl, {})

        sub_statuses = [
            _status(ri.get("status", "PASS")),
            _status(uniq.get("status", "PASS")),
            _status(parent.get("status", "PASS")),
        ]
        tbl_status = _derive_overall_status(sub_statuses)

        fwd = ri.get("forward", {})
        rev = ri.get("reverse", {})

        details: dict[str, Any] = {
            "ri_status":          ri.get("status", "PASS"),
            "join_key":           ri.get("join_key", []),
            "forward_unmatched":  fwd.get("total_unmatched", 0),
            "forward_premium":    fwd.get("total_premium"),
            "forward_premium_pct": fwd.get("premium_pct"),
            "forward_top10":      _df_to_records(fwd.get("top10")),
            "reverse_orphaned":   rev.get("total_orphaned", 0),
            "reverse_orphaned_rows": _df_to_records(rev.get("orphaned")),
            "forecast_orphaned_premium": rev.get("forecast_orphaned_premium"),
            "forecast_orphaned_df": _df_to_records(rev.get("forecast_orphaned_df")),
            "uniq_status":        uniq.get("status", "PASS"),
            "duplicate_count":    uniq.get("duplicate_count", 0),
            "duplicates":         _df_to_records(uniq.get("duplicates")),
            "uniqueness_cols":    uniq.get("uniqueness_cols", []),
            "parent_status":      parent.get("status", "PASS"),
            "failing_cols":       parent.get("failing_cols", []),
            "domain_fails":       parent.get("domain_fails", []),
        }

        parts = []
        if fwd.get("total_unmatched", 0) > 0:
            parts.append(f"Forward RI: {fwd['total_unmatched']} unmatched combination(s)")
        if rev.get("total_orphaned", 0) > 0:
            parts.append(f"Reverse RI: {rev['total_orphaned']} orphaned combination(s)")
        if uniq.get("duplicate_count", 0) > 0:
            parts.append(f"Uniqueness: {uniq['duplicate_count']} duplicate combination(s)")
        if parent.get("failing_cols"):
            parts.append(f"Parent cols: {len(parent['failing_cols'])} failing")
        summary = "; ".join(parts) if parts else "All checks passed."

        checks.append(CheckResult(
            check_id=f"checks2_5.{tbl}",
            check_label=f"RI / Uniqueness / Parent columns — {tbl}",
            table_name=tbl,
            status=tbl_status,
            summary=summary,
            details=details,
        ))

    sec_status = _section_status(checks) if checks else Status.SKIP
    return SectionResult(
        section_id="checks2_5",
        section_label="Checks 2–5 — Referential Integrity, Row Uniqueness & Parent Columns",
        status=sec_status,
        summary=_section_summary(checks, "table(s) checked"),
        checks=checks,
    )


def _build_section3(map_uniq: dict) -> SectionResult:
    st = _status(map_uniq.get("status", "SKIP"))
    total_dups = map_uniq.get("total_duplicates", 0)
    summary = (
        map_uniq.get("reason", "SKIP")
        if st == Status.SKIP
        else (f"{total_dups} duplicate non-Key_* combination(s)." if total_dups > 0
              else "No duplicate combinations.")
    )
    checks = [CheckResult(
        check_id="check6.tbl_Key_Mapping",
        check_label="Non-Key_* uniqueness — tbl_Key_Mapping",
        table_name="tbl_Key_Mapping",
        status=st,
        summary=summary,
        details={
            "total_duplicates": total_dups,
            "duplicates": _df_to_records(map_uniq.get("duplicates")),
        },
    )]
    return SectionResult(
        section_id="check6",
        section_label="Check 6 — Mapping Table Uniqueness",
        status=st,
        summary=summary,
        checks=checks,
    )


def _build_section4(value_results: dict) -> SectionResult:
    checks = []
    for check_key, res in value_results.items():
        st = _status(res.get("status", "PASS"))
        details = {k: v for k, v in res.items() if k != "status"}
        checks.append(CheckResult(
            check_id=f"check7.{check_key}",
            check_label=f"Value range — {res.get('table', '')} / {res.get('column', '')}",
            table_name=res.get("table"),
            status=st,
            summary=res.get("detail", ""),
            details=details,
        ))
    sec_status = _section_status(checks) if checks else Status.SKIP
    return SectionResult(
        section_id="check7",
        section_label="Check 7 — Value Range Checks",
        status=sec_status,
        summary=_section_summary(checks, "column check(s) performed"),
        checks=checks,
    )


def _build_section5(km_results: dict) -> SectionResult:
    checks = []
    for col, res in km_results.items():
        st = _status(res.get("status", "PASS"))
        pct = res.get("premium_pct", 0.0)
        summary = (
            f"{res.get('unmapped_count', 0):,} unmapped rows — "
            f"{res.get('unmapped_premium', 0):,.0f} premium ({pct:.1f}% of total)"
            if res.get("unmapped_premium", 0) > 0
            else "No unmapped rows."
        )
        checks.append(CheckResult(
            check_id=f"check8.{col}",
            check_label=f"Key_Modelling coverage — {col}",
            table_name=None,
            status=st,
            summary=summary,
            details={
                "column":           col,
                "unmapped_count":   res.get("unmapped_count", 0),
                "unmapped_premium": res.get("unmapped_premium", 0),
                "premium_pct":      pct,
            },
        ))
    sec_status = _section_status(checks) if checks else Status.SKIP
    return SectionResult(
        section_id="check8",
        section_label="Check 8 — Key_Modelling Coverage",
        status=sec_status,
        summary=_section_summary(checks, "Key_Modelling column(s) checked"),
        checks=checks,
    )


def _section_summary(checks: list[CheckResult], unit: str) -> str:
    n_fail    = sum(1 for c in checks if c.status == Status.FAIL)
    n_warning = sum(1 for c in checks if c.status == Status.WARNING)
    n_pass    = sum(1 for c in checks if c.status == Status.PASS)
    parts = []
    if n_fail:
        parts.append(f"{n_fail} FAIL")
    if n_warning:
        parts.append(f"{n_warning} WARNING")
    if n_pass:
        parts.append(f"{n_pass} PASS")
    prefix = f"{len(checks)} {unit}"
    return f"{prefix}: {', '.join(parts)}." if parts else f"{prefix}."


# ── Public entry point ────────────────────────────────────────────────────────

def run_all_checks() -> RunResult:
    """
    Load all PAT project datasets and run all consistency checks.
    Returns a fully populated RunResult ready for serialization into dcc.Store.
    Execution order matches data_consistency_checks.py run_and_display().
    """
    started = datetime.now(timezone.utc)

    datasets = load_tbl_datasets()
    dd_df, mapping_df, ref_tables = classify_tables(datasets)

    if dd_df is None:
        completed = datetime.now(timezone.utc)
        return RunResult(
            status=Status.FAIL,
            summary="tbl_DetailedData not found — cannot run checks.",
            metadata=RunMetadata(
                started_at_utc=started.isoformat(),
                completed_at_utc=completed.isoformat(),
                runtime_ms=int((completed - started).total_seconds() * 1000),
                dataset_count=len(datasets),
                dataset_names=list(datasets.keys()),
            ),
            sections=[],
            errors=["tbl_DetailedData not found — cannot run checks."],
        )

    reg_results    = check_key_registration(dd_df, mapping_df, ref_tables)
    ri_results     = check_referential_integrity(dd_df, ref_tables)
    uniq_results   = check_row_uniqueness(dd_df, ref_tables)
    parent_results = check_parent_columns(dd_df, mapping_df, ref_tables)
    map_uniq       = check_mapping_uniqueness(mapping_df)
    value_results  = check_value_ranges(dd_df, ref_tables)
    km_results     = check_key_modelling_unmapped(dd_df)

    sections = [
        _build_section1(reg_results),
        _build_section2(ri_results, uniq_results, parent_results),
        _build_section3(map_uniq),
        _build_section4(value_results),
        _build_section5(km_results),
    ]

    overall = _derive_overall_status([s.status for s in sections])
    completed = datetime.now(timezone.utc)

    n = len(datasets)
    return RunResult(
        status=overall,
        summary=f"Overall: {overall.value} — {n} table(s) loaded.",
        metadata=RunMetadata(
            started_at_utc=started.isoformat(),
            completed_at_utc=completed.isoformat(),
            runtime_ms=int((completed - started).total_seconds() * 1000),
            dataset_count=n,
            dataset_names=list(datasets.keys()),
        ),
        sections=sections,
    )

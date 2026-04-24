from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Status(str, Enum):
    """Status values used by engine, runner, and UI."""

    PASS = "PASS"
    WARNING = "WARNING"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass(frozen=True)
class CheckResult:
    """Normalized result payload for a single table or rule check."""

    check_id: str
    check_label: str
    table_name: str | None
    status: Status
    summary: str
    details: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SectionResult:
    """Normalized result payload for one display section in the Dash UI."""

    section_id: str
    section_label: str
    status: Status
    summary: str
    checks: list[CheckResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RunMetadata:
    """Execution metadata captured by the runner."""

    started_at_utc: str
    completed_at_utc: str
    runtime_ms: int
    dataset_count: int
    dataset_names: list[str]


@dataclass(frozen=True)
class RunResult:
    """Top-level payload contract consumed by the Dash callback and UI."""

    status: Status
    summary: str
    metadata: RunMetadata
    sections: list[SectionResult]
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def to_payload(run_result: RunResult) -> dict[str, Any]:
    """Serialize dataclass payload into a plain dict for dcc.Store."""

    return asdict(run_result)


def empty_run_result() -> RunResult:
    """Initial placeholder payload before any validation run."""

    now = datetime.now(timezone.utc).isoformat()
    metadata = RunMetadata(
        started_at_utc=now,
        completed_at_utc=now,
        runtime_ms=0,
        dataset_count=0,
        dataset_names=[],
    )
    return RunResult(
        status=Status.SKIP,
        summary="No run executed yet.",
        metadata=metadata,
        sections=[],
        warnings=[],
        errors=[],
    )

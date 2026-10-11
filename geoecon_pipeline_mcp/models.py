"""Output models. These ARE the tools' output schemas, so every field is documented.

Two families:

* read side  — RunCard, RunDetail, StageRow, StageError, HeldRecord, GateRow, Page,
               SourceHealth, HealthVerdict, CostReport, ResumePreview
* job side   — JobHandle, Job, Progress, InputRequest, Cancelled, Answered

The job family maps directly onto the MCP tasks extension's `Task` object (`job_id` ~ taskId,
the same five statuses, `poll_after_s` ~ pollInterval, `ttl_s` ~ ttl, `result`/`error` on
terminal states). `input_request` is this server's own elicitation shape for the budget
question (key, message, JSON Schema), not the extension's `inputRequests` wire format; the
adapter would translate it. The table needs no change to adopt the extension.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

RunStatus = Literal["running", "completed", "failed", "partial"]
StageStatus = Literal["pending", "running", "completed", "failed", "skipped"]
JobStatus = Literal["working", "input_required", "completed", "failed", "cancelled"]
JobKind = Literal["resume_run", "run_stage", "reconcile_abandoned"]
SourceState = Literal["healthy", "silent", "held", "retired", "dead", "unknown"]

JOB_STATUSES: tuple[str, ...] = ("working", "input_required", "completed", "failed", "cancelled")


# ── read side ───────────────────────────────────────────────────────────────────────


class RunCard(BaseModel):
    run_id: int
    window_start: datetime = Field(description="Start of the fixed [T-24h, T] window the run covers.")
    window_end: datetime = Field(description="End of the window; a resume passes this, not `now`.")
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None
    stages_done: int = Field(description="Stage rows with status=completed.")
    stages_total: int = Field(description="Stages the run is expected to have; completed runs report their own count.")
    last_failed_stage: str | None = Field(description="Name of the most recent failed stage, if any.")


class StageRow(BaseModel):
    stage_name: str
    status: StageStatus
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None = Field(description="The stored exception text; read it before guessing.")
    checkpoint: dict[str, Any] = Field(default_factory=dict, description="The stage's result metrics.")


class RunDetail(BaseModel):
    run_id: int
    window_start: datetime
    window_end: datetime
    scheduled_for: datetime
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None
    synthesis_model: str | None
    stages: list[StageRow] = Field(description="Every stage_runs row, in pipeline order.")
    metrics: dict[str, Any]
    notes: str | None = Field(description="Run notes, including why a resumed run stopped before `send`.")
    next_stage: str | None = Field(description="First stage that is not completed; None when the run is complete.")
    active_job_id: str | None = Field(description="A job currently working on this run, if any.")


class StageError(BaseModel):
    run_id: int
    stage_name: str
    status: StageStatus
    error: str | None = Field(description="Stored error text (up to 4000 chars).")
    checkpoint: dict[str, Any]
    started_at: datetime | None
    finished_at: datetime | None
    duration_s: float | None


class HeldRecord(BaseModel):
    record_id: int
    cluster_id: int
    flag: str = Field(description="synthesis_quality_flag other than `ok`.")
    reasons: list[str] = Field(description="quality_detail.reasons from the validator.")
    action: str | None = Field(description="quality_detail.action from the validator.")
    headline: str | None
    article_count: int
    is_headline_only: bool


class GateRow(BaseModel):
    article_id: int
    title: str | None
    domain: str | None
    registry_id: str | None
    gate_score: float | None
    gate_reason: str | None
    unparsed: bool = Field(description="True when the model never produced a verdict (gate_unparsed_dropped).")


class Page(BaseModel):
    items: list[GateRow]
    next_cursor: str | None = Field(description="Pass back as `cursor` to get the next page; None at the end.")
    total_dropped: int = Field(description="All articles the gate dropped for the run.")
    unparsed_dropped: int = Field(description="Of those, how many were dropped unjudged (counted separately).")


class SourceHealth(BaseModel):
    registry_id: str
    institution: str
    tier: int
    registry_status: str | None = Field(description="live | needs_review | retired | dead, from sources.yaml.")
    fetch_type: str
    articles: int | None = Field(description="Rows this source produced in the run; None when unknown.")
    state: SourceState = Field(
        description="healthy (live, rows>0) | silent (live, 0 rows) | held (needs_review) | retired | dead | "
        "unknown (the run has no discovery_registry stage)."
    )


class HealthVerdict(BaseModel):
    ok: bool
    subject: str | None = Field(description="The watchdog's own subject line when not ok.")
    body: str | None = Field(description="The watchdog's own body when not ok.")
    checked_at: datetime
    day_start: datetime
    day_end: datetime
    latest_run_id: int | None
    stale_running: int = Field(description="Runs still `running` that started more than 6 h before `checked_at`.")
    stale_run_ids: list[int]
    note: str | None = None


class ModelUsage(BaseModel):
    model: str
    prompt_tokens: int
    completion_tokens: int
    calls: int
    usd: float


class StageUsage(BaseModel):
    stage: str
    models: list[ModelUsage]
    usd: float


class CostReport(BaseModel):
    run_id: int
    stages: list[StageUsage]
    estimated_usd: float
    partial: bool = Field(description="True when some completed stage has no usage (it ran outside this server).")
    stages_without_usage: list[str]
    pricing_as_of: str
    jobs_counted: list[str]


class Estimate(BaseModel):
    stage: str
    model: str
    tokens_in: int
    tokens_out: int
    usd: float
    basis: str = Field(description="How the number was derived, in one sentence.")


class ResumePreview(BaseModel):
    run_id: int
    window_start: datetime
    window_end: datetime
    remaining: list[str] = Field(description="Stages start_job(resume_run) would run, in order.")
    skipped_completed: list[str]
    send_withheld: bool = Field(description="True when `send` was removed by the guard (ALLOW_SEND / pipeline:send).")
    estimates: list[Estimate]
    estimated_usd: float
    default_budget_usd: float
    would_require_approval: bool
    message: str


# ── job side ────────────────────────────────────────────────────────────────────────


class Progress(BaseModel):
    done: int
    total: int | None
    message: str | None


class InputRequest(BaseModel):
    key: str = Field(description="`budget` for the cost gate.")
    message: str
    requested_schema: dict[str, Any] = Field(description="JSON Schema of the answer `answer_job` expects.")
    requires_new_budget_usd: bool = Field(default=False, description="True for a runtime overspend: approving needs an explicit new ceiling.")


class JobHandle(BaseModel):
    job_id: str
    status: JobStatus
    poll_after_s: float = Field(description="Suggested interval before the next job_status call.")
    stages_planned: list[str]
    run_id: int | None
    note: str | None = None


class Job(BaseModel):
    job_id: str
    kind: JobKind
    run_id: int | None
    status: JobStatus
    stage: str | None = Field(description="Stage currently running (or last touched).")
    stages_planned: list[str]
    stages_completed: list[str]
    progress: Progress | None
    poll_after_s: float
    ttl_s: int | None = Field(description="Seconds until the row expires: the remaining TTL of a terminal job, or the TTL that will apply once it ends.")
    started_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    budget_usd: float
    usage: dict[str, Any] = Field(description="tokens by stage and model, plus `usd_total`.")
    input_request: InputRequest | None
    result: dict[str, Any] | None
    error: str | None
    principal: str | None
    trace: dict[str, Any]


class Answered(BaseModel):
    job_id: str
    status: JobStatus
    accepted: bool
    budget_usd: float
    message: str


class Cancelled(BaseModel):
    job_id: str
    status: JobStatus
    message: str

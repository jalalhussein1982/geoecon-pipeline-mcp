"""Incident-report scaffolding in the house format.

The format is distilled from the pipeline's two incident documents (2026-07-28 gate hang,
2026-07-29 extraction hard kill): a status line, a facts table, root cause, fixes in
priority order with P0..P3 and ✓/⟳ markers, suggested tests, operational follow-up.
`TEMPLATE` is served as `geoecon://incidents/template`; `incident_prompt` and
`morning_check_prompt` are the two prompts' texts.
"""

from __future__ import annotations

TEMPLATE = """\
# Incident {date} — run {run_id}: {one_line_summary}

**Status:** {status_line}

---

## 1. What happened

| | |
|---|---|
| Run | {run_id}, started {started_at} (window {window_start} → {window_end}) |
| Failed stage | `{stage}`, started {stage_started_at} |
| Failed at | {stage_failed_at} after {duration} |
| Detected by | {detected_by} |
| Error | `{error}` |
| Node / pod | {node} |
| Outcome | {outcome} |

{narrative}

## 2. Root cause

{root_cause}

## 3. Fixes in priority order

### P0 — {p0_title} {p0_marker}
{p0_body}

### P1 — {p1_title} {p1_marker}
{p1_body}

### P2 — {p2_title} {p2_marker}
{p2_body}

### P3 — {p3_title} {p3_marker}
{p3_body}

Markers: ✓ implemented and live · ⟳ written, awaiting deploy · (none) open.

## 4. Suggested test coverage

{tests}

## 5. Operational follow-up

{follow_up}
"""


def incident_prompt(run_id: int) -> str:
    return (
        f"Write the incident report for pipeline run {run_id} in the house format.\n\n"
        f"First gather the facts by calling, in this order: run_status({run_id}), failed_stage_error({run_id}), "
        f"list_held_records({run_id}), pipeline_health(), cost_report({run_id}). If failed_stage_error says the run "
        f"has no failed stage, say so in the status line and report on what the run did instead. Read "
        f"geoecon://incidents/template for the skeleton and fill every placeholder.\n\n"
        "Rules:\n"
        "- Quote the tools' numbers and the stored error text verbatim; never paraphrase a count.\n"
        "- Section 1 is a facts table (run, stage, started, failed, duration, error, node). Timestamps in UTC with Z.\n"
        "- Section 2 names the root cause in one paragraph and distinguishes upstream causes from our own faults.\n"
        "- Section 3 lists fixes as P0..P3 with ✓ (live) / ⟳ (written, not deployed) / nothing (open) markers.\n"
        "- Section 4 proposes tests that would have caught this. Section 5 lists the follow-up with owners.\n"
        "- If you resumed the run through this server, cite the job id and the cost_report figures.\n"
        "- Do not soften the verdict and do not invent facts the tools did not return."
    )


def morning_check_prompt() -> str:
    return (
        "Run the operator's morning check and report in five short lines.\n\n"
        "1. pipeline_health(): quote ok, the subject and any stale_run_ids.\n"
        "2. list_runs(limit=3) then run_status on the newest run: status, next_stage, and the failed stage's error "
        "(failed_stage_error) if there is one.\n"
        "3. source_health() on that run: name every `silent` source (live sources with zero rows).\n"
        "4. list_held_records on that run: held count versus records_total from run_status.metrics.\n"
        "5. Recommend exactly one of: nothing to do / preview_resume(run_id) then start_job(resume_run) / "
        "start_job(reconcile_abandoned) / write_incident_report(run_id). State the budget you would use.\n\n"
        "Numbers come from the tools; do not estimate anything the tools can tell you."
    )

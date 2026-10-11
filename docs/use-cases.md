
# Use cases

Eight operator scenarios against the synthetic history (`GEOECON_MODE=synthetic`). Every tool output below was captured by `scripts/capture_use_cases.py` from the in-memory server; nothing is typed by hand. The fixture's clock is frozen at 2026-10-11 08:00Z (10:00 Prague), so 'today' means that day.

The synthetic history: run 101 (older, completed), 102 (completed, three held records, two silent sources), 103 (failed in `gate`), 104 (still `running` nine hours after it started), 105 (a LIMIT=50 smoke rerun that completed with nothing to send).


## 1. Morning check finds a stale run

**Host prompt:** the `morning_check` prompt, or simply *"Is the pipeline healthy this morning?"*

**Tool call** `pipeline_health()`

```json
{
  "ok": false,
  "subject": "pipeline ran but sent no newsletter",
  "body": "Run 105 completed but no region newsletter was sent (likely zero records survived gating/synthesis).\nstarted:  2026-10-11 05:00:02+00:00\nfinished: 2026-10-11 05:06:48+00:00\n",
  "checked_at": "2026-10-11T08:00:00Z",
  "day_start": "2026-10-10T22:00:00Z",
  "day_end": "2026-10-11T22:00:00Z",
  "latest_run_id": 105,
  "stale_running": 1,
  "stale_run_ids": [
    104
  ],
  "note": "1 run(s) still 'running' more than 6 h after start: 104. A process died without unwinding; start_job(reconcile_abandoned) closes them out."
}
```

Two facts, both from the pipeline's own watchdog logic plus the stale-run count the watchdog cannot see: today's latest run (105) completed but sent nothing, and run 104 is still `running` nine hours after it started. The operator looks at 104:

**Tool call** `run_status(run_id=104)`

```json
{
  "run_id": 104,
  "status": "running",
  "started_at": "2026-10-10T23:00:02Z",
  "next_stage": "gate",
  "active_job_id": null,
  "stages": [
    {
      "stage_name": "discovery",
      "status": "completed",
      "started_at": "2026-10-10T23:00:02Z",
      "finished_at": "2026-10-10T23:00:57Z"
    },
    {
      "stage_name": "discovery_registry",
      "status": "completed",
      "started_at": "2026-10-10T23:00:58Z",
      "finished_at": "2026-10-10T23:02:50Z"
    },
    {
      "stage_name": "extraction",
      "status": "completed",
      "started_at": "2026-10-10T23:02:51Z",
      "finished_at": "2026-10-10T23:06:41Z"
    },
    {
      "stage_name": "gate",
      "status": "running",
      "started_at": "2026-10-10T23:06:42Z",
      "finished_at": null
    }
  ]
}
```

`gate` has been `running` since 2026-10-10T23:06:42Z with no error row: a process that died without unwinding. `start_job(reconcile_abandoned)` closes it out the way the orchestrator's own reconciliation would:

**Tool call** `start_job(kind="reconcile_abandoned")`

```json
{
  "job_id": "ef31eb30-847e-4718-a229-f94b13f282d4",
  "status": "working",
  "stages_planned": [],
  "poll_after_s": 2.0
}
```

**Tool call** `job_status(job_id="<job_id>")`

```json
{
  "status": "completed",
  "result": {
    "reconciled": [
      104
    ],
    "note": "abandoned: still 'running' with no progress since before 2026-10-11 02:00Z; process ended without unwinding (cause not recorded). Closed out via MCP at 2026-10-11 08:00Z."
  }
}
```


## 2. Read why run 103 failed

**Host prompt:** *"Why did last Thursday's run fail?"* (fresh copy of the history)

**Tool call** `list_runs(status="failed")`

```json
{
  "result": [
    {
      "run_id": 103,
      "window_start": "2026-10-08T23:00:00Z",
      "window_end": "2026-10-09T23:00:00Z",
      "status": "failed",
      "started_at": "2026-10-09T23:00:02Z",
      "finished_at": "2026-10-09T23:55:02Z",
      "stages_done": 3,
      "stages_total": 9,
      "last_failed_stage": "gate"
    }
  ]
}
```

**Tool call** `failed_stage_error(run_id=103)`

```json
{
  "run_id": 103,
  "stage_name": "gate",
  "status": "failed",
  "error": "NullGateError: gate kept 0 of 228 articles; every verdict came back unparsable (gate_unparsed_dropped=228/228) — refusing to hand an empty article set downstream. {'gate_total': 228, 'gate_kept': 0, 'gate_dropped': 228, 'gate_unparsed': 228, 'gate_unparsed_dropped': 228}",
  "checkpoint": {},
  "started_at": "2026-10-09T23:06:42Z",
  "finished_at": "2026-10-09T23:55:02Z",
  "duration_s": 2900.0
}
```

**Tool call** `list_gate_rejections(run_id=103, limit=3)`

```json
{
  "items": [
    {
      "article_id": 431,
      "title": "Lithium concession auction draws 475 qualified bidders",
      "domain": "sa-pif.example",
      "registry_id": "sa_pif",
      "gate_score": 0.0,
      "gate_reason": "gate_unparsed_dropped",
      "unparsed": true
    },
    {
      "article_id": 432,
      "title": "Currency swap line of 161 million renewed with partner bank",
      "domain": "ae-mof.example",
      "registry_id": "ae_mof",
      "gate_score": 0.0,
      "gate_reason": "gate_unparsed_dropped",
      "unparsed": true
    },
    {
      "article_id": 433,
      "title": "Currency swap line of 166 million renewed with partner bank",
      "domain": "ae-mof.example",
      "registry_id": "ae_mof",
      "gate_score": 0.0,
      "gate_reason": "gate_unparsed_dropped",
      "unparsed": true
    }
  ],
  "next_cursor": "433",
  "total_dropped": 228,
  "unparsed_dropped": 228
}
```

`unparsed_dropped == total_dropped` (228 of 228): the model never produced a verdict for any article. The failure is the LLM backend, not the news; the gate's health guard refused to hand an empty set downstream. Resume at `gate` once the backend is back.


## 3. Preview and resume run 103 within a budget, watching progress

**Host prompt:** *"Resume run 103 but do not spend more than 50 cents."* The dry run first:

**Tool call** `preview_resume(run_id=103)`

```json
{
  "run_id": 103,
  "window_start": "2026-10-08T23:00:00Z",
  "window_end": "2026-10-09T23:00:00Z",
  "remaining": [
    "gate",
    "cluster",
    "synthesis",
    "summary",
    "render"
  ],
  "skipped_completed": [
    "discovery",
    "discovery_registry",
    "extraction"
  ],
  "send_withheld": true,
  "estimates": [
    {
      "stage": "gate",
      "model": "deepseek-v3.2",
      "tokens_in": 48000,
      "tokens_out": 9600,
      "usd": 0.02352,
      "basis": "228 non-failed articles in 16 batches of 15 x 3000/600 tokens; the gate re-judges every article"
    },
    {
      "stage": "cluster",
      "model": "deepseek-v3.2",
      "tokens_in": 8640,
      "tokens_out": 1200,
      "usd": 0.003653,
      "basis": "48 kept articles across 12 countries, one grouping call per country (projected from historical ratios because the upstream stage has not completed)"
    },
    {
      "stage": "synthesis",
      "model": "deepseek-v3.2",
      "tokens_in": 165000,
      "tokens_out": 26400,
      "usd": 0.07359,
      "basis": "22 clusters x SYNTH_TOTAL_CHARS/4 (7500) in, 1200 out (projected from historical ratios because the upstream stage has not completed)"
    },
    {
      "stage": "summary",
      "model": "deepseek-v3.2",
      "tokens_in": 5400,
      "tokens_out": 1500,
      "usd": 0.003108,
      "basis": "3 region buckets x 1800/500 tokens"
    }
  ],
  "estimated_usd": 0.103871,
  "default_budget_usd": 2.0,
  "would_require_approval": false,
  "message": "start_job(resume_run, 103) would run gate, cluster, synthesis, summary, render with the original window 2026-10-08 23:00Z -> 2026-10-09 23:00Z; estimated 0.1039 USD against a default budget of 2.00 USD; send is withheld (ALLOW_SEND unset or pipeline:send missing), the run will stop after render."
}
```

The plan starts at `gate` (the failed stage is re-run, not skipped), passes the run's original window, and stops after `render` because `send` is withheld. Start it with the budget:

**Tool call** `start_job(kind="resume_run", run_id=103, budget_usd=0.5)`

```json
{
  "job_id": "70353814-5d31-42a4-82ea-673cda9f5c60",
  "status": "working",
  "poll_after_s": 2.0,
  "stages_planned": [
    "gate",
    "cluster",
    "synthesis",
    "summary",
    "render"
  ],
  "run_id": 103,
  "note": "send withheld: the run will stop after render (ALLOW_SEND unset or pipeline:send missing); run_status will say so"
}
```

`job_status` polled while the job runs (four of the snapshots; a host that opened `subscriptions/listen` on `job://<id>` is told when to refetch instead of polling):

**Tool call** `job_status(job_id="<job_id>")`

```json
[
  {
    "status": "working",
    "stage": "gate",
    "progress": {
      "done": 3,
      "total": 16,
      "message": "gate batch 3/16"
    },
    "stages_completed": []
  },
  {
    "status": "working",
    "stage": "cluster",
    "progress": {
      "done": 10,
      "total": 18,
      "message": "country IS"
    },
    "stages_completed": [
      "gate"
    ]
  },
  {
    "status": "working",
    "stage": "synthesis",
    "progress": {
      "done": 16,
      "total": 34,
      "message": "cluster 16/34"
    },
    "stages_completed": [
      "gate",
      "cluster"
    ]
  },
  {
    "status": "completed",
    "stage": "render",
    "progress": {
      "done": 1,
      "total": 1,
      "message": "render completed"
    },
    "stages_completed": [
      "gate",
      "cluster",
      "synthesis",
      "summary",
      "render"
    ]
  }
]
```

**Tool call** `job_status(job_id="<job_id>")` — terminal

```json
{
  "status": "completed",
  "stages_completed": [
    "gate",
    "cluster",
    "synthesis",
    "summary",
    "render"
  ],
  "result": {
    "gate": {
      "gate_total": 228,
      "gate_kept": 44,
      "gate_dropped": 184,
      "gate_unparsed": 0,
      "gate_unparsed_dropped": 0
    },
    "cluster": {
      "clusters": 34,
      "gated_articles": 44
    },
    "synthesis": {
      "records_total": 34,
      "records_ok": 33,
      "records_held": 1
    },
    "summary": {
      "summaries_total": 3,
      "summaries_ok": 3,
      "summaries_empty": 0
    },
    "render": {
      "record_count": 31,
      "newsletters": 3
    }
  },
  "usage": {
    "stages": {
      "gate": {
        "deepseek-v3.2": {
          "prompt_tokens": 48000,
          "completion_tokens": 9600,
          "calls": 16,
          "usd": 0.02352
        }
      },
      "cluster": {
        "deepseek-v3.2": {
          "prompt_tokens": 54000,
          "completion_tokens": 10800,
          "calls": 18,
          "usd": 0.02646
        }
      },
      "synthesis": {
        "deepseek-v3.2": {
          "prompt_tokens": 102000,
          "completion_tokens": 20400,
          "calls": 34,
          "usd": 0.04998
        }
      },
      "summary": {
        "deepseek-v3.2": {
          "prompt_tokens": 9000,
          "completion_tokens": 1800,
          "calls": 3,
          "usd": 0.00441
        }
      }
    },
    "usd_total": 0.10437
  },
  "budget_usd": 0.5,
  "ttl_s": 1209599
}
```

**Tool call** `run_status(run_id=103)`

```json
{
  "status": "partial",
  "next_stage": "send",
  "notes": "send withheld: the run will stop after render (ALLOW_SEND unset or pipeline:send missing); run_status will say so",
  "stages": [
    "discovery: completed",
    "discovery_registry: completed",
    "extraction: completed",
    "gate: completed",
    "cluster: completed",
    "synthesis: completed",
    "summary: completed",
    "render: completed"
  ]
}
```

**Tool call** `cost_report(run_id=103)`

```json
{
  "run_id": 103,
  "stages": [
    {
      "stage": "gate",
      "models": [
        {
          "model": "deepseek-v3.2",
          "prompt_tokens": 48000,
          "completion_tokens": 9600,
          "calls": 16,
          "usd": 0.02352
        }
      ],
      "usd": 0.02352
    },
    {
      "stage": "cluster",
      "models": [
        {
          "model": "deepseek-v3.2",
          "prompt_tokens": 54000,
          "completion_tokens": 10800,
          "calls": 18,
          "usd": 0.02646
        }
      ],
      "usd": 0.02646
    },
    {
      "stage": "synthesis",
      "models": [
        {
          "model": "deepseek-v3.2",
          "prompt_tokens": 102000,
          "completion_tokens": 20400,
          "calls": 34,
          "usd": 0.04998
        }
      ],
      "usd": 0.04998
    },
    {
      "stage": "summary",
      "models": [
        {
          "model": "deepseek-v3.2",
          "prompt_tokens": 9000,
          "completion_tokens": 1800,
          "calls": 3,
          "usd": 0.00441
        }
      ],
      "usd": 0.00441
    }
  ],
  "estimated_usd": 0.10437,
  "partial": true,
  "stages_without_usage": [
    "discovery",
    "discovery_registry",
    "extraction"
  ],
  "pricing_as_of": "2026-10-11",
  "jobs_counted": [
    "70353814-5d31-42a4-82ea-673cda9f5c60"
  ]
}
```

`partial=true` is honest: discovery, registry discovery and extraction ran in the CronJob, which keeps no token ledger; the four LLM stages ran here and are priced from `pricing.yaml`.


## 4. The budget gate fires and the operator approves

**Host prompt:** *"Re-run the gate on 103 for a penny."* (fresh copy of the history)

**Tool call** `start_job(kind="run_stage", run_id=103, stage="gate", budget_usd=0.01)`

```json
{
  "job_id": "75d37e23-c4b2-4e26-bcf6-75c4dce94273",
  "status": "working",
  "poll_after_s": 2.0,
  "stages_planned": [
    "gate"
  ],
  "run_id": 103,
  "note": null
}
```

**Tool call** `job_status(job_id="<job_id>")`

```json
{
  "status": "input_required",
  "stage": "gate",
  "budget_usd": 0.01,
  "input_request": {
    "key": "budget",
    "message": "Stage gate is estimated at 0.0235 USD (48000 in / 9600 out tokens on deepseek-v3.2; 228 non-failed articles in 16 batches of 15 x 3000/600 tokens; the gate re-judges every article); 0.0100 USD of the 0.0100 USD budget is remaining after 0.0000 USD spent. Approve with new_budget_usd >= 0.0235 or abort.",
    "requested_schema": {
      "type": "object",
      "properties": {
        "approve": {
          "type": "boolean",
          "description": "true: continue with new_budget_usd; false: abort the stage"
        },
        "new_budget_usd": {
          "type": "number",
          "minimum": 0,
          "description": "new ceiling in USD"
        }
      },
      "required": [
        "approve"
      ]
    },
    "requires_new_budget_usd": false
  }
}
```

The job is parked in `input_required` with a form-shaped request (the tasks extension's shape). Nothing has been written to the run yet. The operator approves a higher ceiling:

**Tool call** `answer_job(job_id="<job_id>", approve=true, new_budget_usd=1.0)`

```json
{
  "job_id": "75d37e23-c4b2-4e26-bcf6-75c4dce94273",
  "status": "working",
  "accepted": true,
  "budget_usd": 1.0,
  "message": "approved; budget is now 1.0000 USD and the job is working"
}
```

**Tool call** `job_status(job_id="<job_id>")` — terminal

```json
{
  "status": "completed",
  "budget_usd": 1.0,
  "usage": {
    "stages": {
      "gate": {
        "deepseek-v3.2": {
          "prompt_tokens": 48000,
          "completion_tokens": 9600,
          "calls": 16,
          "usd": 0.02352
        }
      }
    },
    "usd_total": 0.02352
  },
  "result": {
    "gate": {
      "gate_total": 228,
      "gate_kept": 44,
      "gate_dropped": 184,
      "gate_unparsed": 0,
      "gate_unparsed_dropped": 0
    }
  }
}
```

Had the operator answered `approve=false`, the stage would have been marked `failed` with `aborted at budget gate` and the job `failed`; `cancel_job` while waiting ends the job `cancelled` and a late `answer_job` is refused with `job is cancelled`.


## 5. Review held records and decide whether to re-run synthesis

**Host prompt:** *"What did the validator hold back in run 102, and should we redo synthesis?"*

**Tool call** `list_held_records(run_id=102)`

```json
{
  "result": [
    {
      "record_id": 25,
      "cluster_id": 25,
      "flag": "empty_figures",
      "reasons": [
        "key_figures is empty but the source lead quotes two amounts"
      ],
      "action": "retry_or_hold",
      "headline": "Regulator fines telecom operator 222 million over spectrum terms",
      "article_count": 2,
      "is_headline_only": false
    },
    {
      "record_id": 26,
      "cluster_id": 26,
      "flag": "zero_entities",
      "reasons": [
        "entities list is empty; headline names a ministry and a port operator"
      ],
      "action": "hold",
      "headline": "Regulator fines telecom operator 175 million over spectrum terms",
      "article_count": 3,
      "is_headline_only": false
    },
    {
      "record_id": 35,
      "cluster_id": 35,
      "flag": "invalid_json",
      "reasons": [
        "response was not a JSON object after fence stripping (synthetic)"
      ],
      "action": "hold",
      "headline": "Film festival opens with 87 entries from the region",
      "article_count": 2,
      "is_headline_only": false
    }
  ]
}
```

**Tool call** `run_status(run_id=102)`

```json
{
  "status": "completed",
  "metrics": {
    "records_total": 24,
    "records_ok": 21,
    "records_held": 3,
    "clusters": 24,
    "newsletters": 3
  }
}
```

3 held of 24 records (12%). Two are model-output defects the retry already failed on (`empty_figures`, `zero_entities`) and one is a parse failure. At this ratio the brief shipped with the OK records; re-running synthesis would cost about `preview_resume`'s synthesis estimate and would re-hold the same clusters unless the model changed. Decision: no re-run; note the three cluster ids for the prompt review.


## 6. Silent sources

**Host prompt:** *"Which registry sources produced nothing in run 102?"*

**Tool call** `source_health(run_id=102)` — filtered to state=silent; the full result has 66 rows

```json
{
  "result": [
    {
      "registry_id": "ae_mof",
      "institution": "Ministry of Finance (UAE)",
      "tier": 0,
      "registry_status": "live",
      "fetch_type": "rss",
      "articles": 0,
      "state": "silent"
    },
    {
      "registry_id": "co_cb_banrep",
      "institution": "Banco de la República (Colombia)",
      "tier": 0,
      "registry_status": "live",
      "fetch_type": "rss",
      "articles": 0,
      "state": "silent"
    }
  ],
  "summary": {
    "held": 25,
    "healthy": 11,
    "silent": 2,
    "retired": 28
  }
}
```

Two `live` sources with zero rows: a feed that moved or a scraper selector that rotted. `held` sources (needs_review) and `retired` ones are not failures: the registry says they do not fetch. For an older run with no `discovery_registry` stage at all the tool refuses to guess:

**Tool call** `source_health(run_id=101)` — first two of 66 rows

```json
{
  "result": [
    {
      "registry_id": "sa_cb_sama",
      "institution": "Saudi Central Bank (SAMA)",
      "tier": 0,
      "registry_status": "needs_review",
      "fetch_type": "scrape",
      "articles": null,
      "state": "unknown"
    },
    {
      "registry_id": "sa_mof",
      "institution": "Ministry of Finance (Saudi Arabia)",
      "tier": 0,
      "registry_status": "needs_review",
      "fetch_type": "scrape",
      "articles": null,
      "state": "unknown"
    }
  ],
  "summary": {
    "unknown": 66
  }
}
```


## 7. Write the incident report

**Host prompt:** the `write_incident_report` prompt with `run_id=103`.

**Prompt text the host receives:**

```text
Write the incident report for pipeline run 103 in the house format.

First gather the facts by calling, in this order: run_status(103), failed_stage_error(103), list_held_records(103), pipeline_health(), cost_report(103). If failed_stage_error says the run has no failed stage, say so in the status line and report on what the run did instead. Read geoecon://incidents/template for the skeleton and fill every placeholder.

Rules:
- Quote the tools' numbers and the stored error text verbatim; never paraphrase a count.
- Section 1 is a facts table (run, stage, started, failed, duration, error, node). Timestamps in UTC with Z.
- Section 2 names the root cause in one paragraph and distinguishes upstream causes from our own faults.
- Section 3 lists fixes as P0..P3 with ✓ (live) / ⟳ (written, not deployed) / nothing (open) markers.
- Section 4 proposes tests that would have caught this. Section 5 lists the follow-up with owners.
- If you resumed the run through this server, cite the job id and the cost_report figures.
- Do not soften the verdict and do not invent facts the tools did not return.
```

**Resource** `geoecon://incidents/template` (ttlMs=3600000, cacheScope=public; first 22 lines):

```markdown
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

...
```

The model then fills the skeleton from `run_status(103)`, `failed_stage_error(103)` (section 1's error cell is the stored text verbatim), `list_held_records(103)`, `pipeline_health()` and `cost_report(103)`.


## 8. The refused send

**Host prompt:** *"Send the newsletter for run 103 now."*

**Tool call** `start_job(kind="run_stage", run_id=103, stage="send", force=true)`

```json
{
  "error": "Error executing tool start_job: The send stage is disabled on this server (ALLOW_SEND is not 1). Nobody mails the newsletter from a chat by accident; the CronJob sends."
}
```

With `ALLOW_SEND=1` on the server but a token without `pipeline:send`:

**Tool call** `start_job(kind="run_stage", run_id=103, stage="send", force=true)` — ALLOW_SEND=1, scopes pipeline:read,pipeline:operate

```json
{
  "error": "Error executing tool start_job: The send stage requires the pipeline:send scope; the current credentials carry ['pipeline:operate', 'pipeline:read']."
}
```

**Tool call** `preview_resume(run_id=103)`

```json
{
  "remaining": [
    "gate",
    "cluster",
    "synthesis",
    "summary",
    "render"
  ],
  "send_withheld": true,
  "message": "start_job(resume_run, 103) would run gate, cluster, synthesis, summary, render with the original window 2026-10-08 23:00Z -> 2026-10-09 23:00Z; estimated 0.1039 USD against a default budget of 2.00 USD; send is withheld (ALLOW_SEND unset or pipeline:send missing), the run will stop after render."
}
```

Both conditions must hold: `ALLOW_SEND=1` on the server and `pipeline:send` on the caller. The CronJob keeps sending the daily brief; this server stops after `render` and `run_status.notes` records why.

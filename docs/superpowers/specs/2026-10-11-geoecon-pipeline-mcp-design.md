# geoecon-pipeline-mcp — design

**Status:** draft for review, 2026-10-11.
**Source app:** [`jalalhussein1982/geoeconomics-news-fetcher`](https://github.com/jalalhussein1982/geoeconomics-news-fetcher) (ten-stage resumable news pipeline, PostgreSQL run-state, daily Kubernetes CronJob on CERIT-SC).
**Protocol:** MCP revision 2026-07-28. **SDK:** MCP Python SDK 2.x.

## 1. What this server proves

The agent as **pipeline operator**: it inspects runs, resumes a failed run from the right stage, watches progress, reads what the validator rejected, and writes the incident report in the house format. The other servers in the portfolio read data; this one runs long jobs safely.

| Capability | Where |
|---|---|
| **Long-running work with durable handles**: a stage run returns a job id, the host polls, the job survives a dropped connection | §4.2, §7 |
| **Progress notifications** on the request stream for bounded tools | §4.2 |
| **`subscriptions/listen`** change notifications on `run://` resources (job finished, record held) | §5 |
| **Cost gate**: LLM stages estimate tokens before running and stop at a budget | §4.3 |
| **Tasks extension readiness**: the job model is the extension's `Task` object one-to-one, so the server adopts `io.modelcontextprotocol/tasks` the day the SDK ships it | §7.2 |
| OpenTelemetry trace context propagated from `_meta` into the pipeline's logs | §8 |
| Fixes two documented production defects from the outside without touching the pipeline: the dead resume path and the missing token accounting | §2 |

Non-goals: no new stage logic; no edits to prompts or the registry; no replacement for the CronJob. The CronJob keeps running the daily job; this server is how a person or an agent intervenes.

## 2. Relationship to the source app

The pipeline is installed as a pinned dependency (`geoecon @ git+https://...@<commit>`) and driven through its own public objects:

| Pipeline object | Used for |
|---|---|
| `Config.from_env` | one configuration, same env as the CronJob |
| `geoecon.state.db.connect`, `apply_migrations` | PostgreSQL connection |
| `geoecon.state.repo.Repo` | `completed_stages`, `failed_stages`, `start_stage`, `complete_stage`, `fail_stage`, `set_run_status`, `merge_run_metrics`, `stale_running_runs`, `mark_run_failed`, `articles_for_run`, `clusters_for_run`, `ok_records`, `newsletters_for_run`, `get_region_summary` |
| `geoecon.orchestrator._default_stages(cfg, client)` | the ordered `(name, fn)` list; the server calls stage functions itself |
| `geoecon.llm.client.LLMClient` | wrapped, not replaced, to count tokens |
| `geoecon.watchdog.check`, `local_day_window` | health verdict |
| `geoecon.registry.load_sources` | source registry |

Two facts from the incident docs shape the design:

1. `find_resumable_run` never matches in production because `window_end = now` is fresh on every start. The server resumes by `completed_stages(run_id)` and calling the remaining stage functions in order, passing the **original** `window_start`/`window_end` from the `runs` row. That is the resume the orchestrator meant to have.
2. No stage reads `LLMResponse.prompt_tokens`/`completion_tokens`. The server wraps `LLMClient.complete` with a counting proxy per job and persists totals, giving the cost ledger the pipeline lacks.

Raw SQL the `Repo` does not offer, kept in one module (`sql.py`): list runs with status and stage summary; full `stage_runs` for a run; held records (`synthesis_quality_flag <> 'ok'`); gate rejections (`gate_geoeconomic = false`); per-source article counts for a run; the `funnel_report.sql` numbers.

## 3. Architecture

```
MCP host ── Streamable HTTP (stateless) or stdio ──▶ geoecon_pipeline_mcp.server
                                                        ├─ jobs.py        job registry + runner (thread per job)
                                                        ├─ stages.py      resume logic over _default_stages
                                                        ├─ cost.py        token-counting LLMClient proxy, pricing table
                                                        ├─ progress.py    logging handler → counters
                                                        ├─ health.py      watchdog + stale-run reconciliation
                                                        ├─ sql.py         read queries the Repo lacks
                                                        ├─ incidents.py   incident-report prompt scaffolding
                                                        └─ synth.py       synthetic run-state for demo/tests
                                                                 │
                                           geoecon (pinned)  ──▶ PostgreSQL (runs, stage_runs, articles, …)
                                                                 + table mcp_jobs (ours)
```

The server runs **one replica**: a job executes in a worker thread of the server process (the stage functions are synchronous and use their own `ThreadPoolExecutor`s). The job row in PostgreSQL is the durable handle, so a host that disconnects and reconnects, or a different host, can poll the same id. Stateless transport, stateful work, handle passed as an ordinary tool argument: the pattern the 2026-07-28 revision prescribes in place of sessions.

## 4. Tools

Registration order as listed. All outputs are Pydantic models.

### 4.1 Inspection (`read_only_hint=True`)

| Tool | Input | Output | Notes |
|---|---|---|---|
| `list_runs` | `status?`, `since?`, `limit≤50` | `list[RunCard]` | `run_id, window_start, window_end, status, started_at, finished_at, stages_done/total, last_failed_stage` |
| `run_status` | `run_id` | `RunDetail` | every `stage_runs` row (`status, started_at, finished_at, error, checkpoint`) plus `runs.metrics`; names the next stage to run |
| `failed_stage_error` | `run_id` | `StageError` | the stored `error` text and the stage's checkpoint, so the agent reads the failure instead of guessing |
| `list_held_records` | `run_id`, `flag?` | `list[HeldRecord]` | `synthesis_records` with `synthesis_quality_flag <> 'ok'`: flag, `quality_detail.reasons`, `action`, headline, cluster id, article count. The validator's rejections, made visible |
| `list_gate_rejections` | `run_id`, `limit`, `cursor?` | `Page[GateRow]` | articles the gate dropped with `gate_reason`; `gate_unparsed_dropped` counted separately |
| `source_health` | `run_id?` (default latest) | `list[SourceHealth]` | registry status from `sources.yaml` joined with per-run article counts; `silent` when a `live` source produced zero rows |
| `pipeline_health` | none | `HealthVerdict` | `watchdog.check` for the local day plus `stale_running_runs` count; `ok=false` carries the watchdog's own subject and body |
| `cost_report` | `run_id` | `CostReport` | tokens by stage and model from `mcp_jobs.usage` (jobs run through this server) with `estimated_usd` from `pricing.yaml`; states `partial=true` when some stages ran outside the server |

### 4.2 Operation (write; long-running)

| Tool | Annotations | Behaviour |
|---|---|---|
| `start_job(kind: resume_run \| run_stage \| reconcile_abandoned, run_id?, stage?, budget_usd?)` | `destructive_hint=False`, `idempotent_hint=False`, `open_world_hint=True` | Validates: run exists, stage is the next incomplete stage (or any for `run_stage` when `force=True`), no other job active on that run. Inserts an `mcp_jobs` row `status=working`, starts the worker thread, returns `JobHandle{job_id, status, poll_after_s, stages_planned}` immediately. |
| `job_status(job_id)` | `read_only_hint=True` | `Job{status, stage, progress{done,total,message}, started_at, finished_at, result?, error?, usage, input_request?}`. Status vocabulary is the tasks extension's: `working, input_required, completed, failed, cancelled`. |
| `answer_job(job_id, answer: approve_budget \| abort)` | `idempotent_hint=True` | When a job is `input_required` (budget exceeded, §4.3) this supplies the decision. Approving raises the job's budget to the new estimate and resumes; aborting fails the stage cleanly with `fail_stage`. |
| `cancel_job(job_id)` | `idempotent_hint=True` | Cooperative: sets a flag the runner checks between stages and between LLM batches; the running batch finishes; the stage is marked `failed` with `error="cancelled via MCP"`. |
| `preview_resume(run_id)` | `read_only_hint=True` | What `start_job(resume_run)` would do: stages remaining in order, the original window it will pass, and the cost estimate (§4.3). The dry run before the real one. |

Progress: the runner installs a `logging.Handler` on the `geoecon.*` loggers for the job's duration and parses the stages' existing log lines into counters (`extracted/failed` from `geoecon.extract`, batches from `geoecon.gate`, records from `geoecon.synth`), falling back to DB counts (`articles.fetch_status`, `gate_geoeconomic IS NOT NULL`, `synthesis_records` rows) every 5 s. These feed `job_status.progress` and, for a host that holds the `start_job` response stream open, `ctx.report_progress` for the first stage. `preview_resume` and `source_health` report progress per source over their own request stream.

### 4.3 Cost gate

Before any LLM stage runs, `cost.estimate(stage, run_id)` computes expected tokens from the rows the stage will touch (gate: articles pending × average prompt size per batch; synthesis: clusters × `SYNTH_TOTAL_CHARS`; summary: buckets) and multiplies by `pricing.yaml` (per-model USD per 1M tokens, maintained in this repo since the pipeline has none). If the estimate exceeds `budget_usd` (default `DEFAULT_BUDGET_USD=2.00`), the job moves to `input_required` with a form elicitation `{"approve": boolean, "new_budget_usd": number}` surfaced in `job_status.input_request`; the host answers through `answer_job`. During the run the counting proxy enforces the ceiling: when actual spend passes the approved budget, the job pauses at the next batch boundary and asks again. Actual usage is persisted per stage in `mcp_jobs.usage`.

### 4.4 Error policy

`ToolError` for: unknown run or job (names `list_runs`/`job_status`), stage not next and `force` absent (names the next stage), another job active on the run (names its id), `answer_job` on a job not awaiting input, budget below the estimate without approval. `gate.NullGateError` and any stage exception become `failed` jobs with the exception text in `error`, never crashes of the server.

## 5. Resources, notifications, prompts

| URI | Content | `ttlMs`/`cacheScope` |
|---|---|---|
| `run://{run_id}` | the `RunDetail` as JSON | 0 / private |
| `run://{run_id}/newsletter/{bucket}` | `newsletters.email_html` for the bucket, `text/html` | 86400000 / private |
| `run://{run_id}/archive/{bucket}` | the archive file at `newsletters.archive_html_path` if readable from this process, else a `ToolError`-style text explaining where it lives | same |
| `run://{run_id}/held` | the held records as JSON | 0 / private |
| `job://{job_id}` | the `Job` as JSON | 0 / private |
| `geoecon://sources` | the registry with per-source status | 3600000 / private |
| `geoecon://pricing` | `pricing.yaml` | 3600000 / public |
| `geoecon://incidents/template` | the incident report skeleton (§5 prompt) | 3600000 / public |

Notifications: the runner calls `ctx.notify_resource_updated("job://{id}")` on every status change and `notify_resource_updated("run://{run_id}")` when a stage completes or fails, and `("run://{run_id}/held")` when synthesis holds a record. A host that opened `subscriptions/listen` for those URIs refetches; one that did not polls `job_status`. The SDK's in-memory bus is fine for one replica; the README states that a second replica would need a shared `SubscriptionBus`.

Prompts:

- `write_incident_report(run_id)` — produces the house format from both existing incident docs: status line, `## 1. What happened` with a facts table (run, stage, started, failed, duration, error, node), `## 2. Root cause`, `## 3. Fixes in priority order` (`P0`…`P3`, ✓/⟳ markers), `## 4. Suggested test coverage`, `## 5. Operational follow-up`. Instructs the model to call `run_status`, `failed_stage_error`, `list_held_records`, `pipeline_health`, `cost_report` first and to quote their numbers.
- `morning_check` — the operator's checklist: `pipeline_health`, latest `run_status`, `source_health` silent sources, held-record count versus the run's `records_total`.

## 6. Authorization

Writes are real (they run LLM stages that cost money and send email on the `send` stage). The server therefore:

- Runs over Streamable HTTP with the same OAuth 2.1 resource-server wiring as `hostel-ops-mcp` (`TokenVerifier` + `AuthSettings`), scopes `pipeline:read` and `pipeline:operate`. The two servers share the verifier code by copy, not by a shared package, so each repo stands alone.
- Refuses to run the `send` stage unless `ALLOW_SEND=1` **and** the token has `pipeline:send`; by default a resumed run stops after `render` and `run_status` says so. Nobody should mail the newsletter from a chat by accident.
- Over stdio (operator's laptop with cluster credentials) the configured `STDIO_SCOPES` apply.

## 7. State

### 7.1 `mcp_jobs` table (owned by this server, created by its own migration)

| Column | Meaning |
|---|---|
| `job_id` uuid PK | the handle |
| `kind`, `run_id`, `stages_planned text[]` | what was asked |
| `status` | `working / input_required / completed / failed / cancelled` |
| `stage`, `progress jsonb` | current stage and counters |
| `input_request jsonb`, `input_response jsonb` | the budget elicitation and its answer |
| `budget_usd`, `usage jsonb` | approved ceiling; tokens by stage/model and USD |
| `result jsonb`, `error text` | terminal payload |
| `principal`, `trace jsonb` | token subject; `traceparent`/`tracestate` from `_meta` |
| `created_at`, `updated_at`, `finished_at`, `ttl_expires_at` | lifecycle; completed rows kept 14 days |

A server restart marks any `working` job `failed` with `error="server restarted"`; the stage row is already `failed` or `running` in the pipeline's own tables, and `pipeline_health` reports the stale run, as the watchdog would.

### 7.2 Tasks extension readiness

The MCP Python SDK 2.x does not yet implement the official tasks extension (`io.modelcontextprotocol/tasks`, SEP-2663); it removed the experimental API. This design therefore exposes jobs as ordinary tools with a server-minted handle, which every host supports today. The `Job` model is deliberately the extension's `Task` object: `taskId`, the five statuses, `pollIntervalMs` (our `poll_after_s`), `ttlMs`, `inputRequests` on `input_required`, `result`/`error` on terminal states. When the SDK ships the extension, `start_job` returns a `CreateTaskResult` to clients that declare the capability, and `tasks/get`, `tasks/update`, `tasks/cancel` map to `job_status`, `answer_job`, `cancel_job` with no change to the job table. That adapter is milestone 7, conditional on SDK support, and the README says so plainly rather than claiming the extension.

## 8. Observability

Each request's `_meta` may carry `traceparent`/`tracestate`/`baggage`. The server stores them on the job and sets them as the logging context for the worker thread, so the pipeline's existing log lines from `geoecon.*` carry the trace id. The server emits its own structured logs to stderr (stdio) or stdout (HTTP). No OpenTelemetry SDK dependency is required for this; adding an exporter is a one-module change documented in the README.

## 9. Testing

Two layers.

**In-memory protocol tests** (`Client(mcp)`, SQLite-backed fake of the state tables through `synth.py`, stage functions replaced by fakes that sleep and log in the pipeline's log format):

1. Tool order fixed; output schemas present; annotations as in §4.
2. `list_runs`, `run_status`, `list_held_records`, `list_gate_rejections`, `source_health` return the synthetic fixture's known numbers.
3. `preview_resume` names exactly the stages after the last completed one and passes the original window.
4. `start_job` → `working`; the fake stage logs lines → `job_status.progress` advances; completion → `completed` with the stage result as `result`; the pipeline's `complete_stage` was called once per stage.
5. Second `start_job` on the same run while active → `ToolError` naming the job.
6. Budget: estimate above budget → `input_required` with the form request; `answer_job(abort)` → `failed` and `fail_stage` called; `answer_job(approve)` → resumes; mid-run overspend pauses again.
7. `cancel_job` between stages → `cancelled`, no further stage started.
8. Notifications: a client `listen(resource_subscriptions=["job://…"])` receives `ResourceUpdated` on each status change.
9. `send` refused without `ALLOW_SEND` and `pipeline:send`.
10. Restart recovery: a `working` row at startup becomes `failed`.

**Database tests** (opt-in marker, ephemeral PostgreSQL via the pipeline's own `apply_migrations` + ours): `sql.py` queries against rows inserted with the pipeline's `Repo`, and the `mcp_jobs` migration.

Target: 50–70 in-memory tests under 15 s; 10–15 DB tests.

## 10. Deployment and demo

- **Demo (no cluster)**: `GEOECON_MODE=synthetic` serves a SQLite copy of a fabricated run history (three completed runs, one failed in `gate` with the real incident's error text paraphrased, one with held records). `start_job` runs fake stages that take ~20 s with visible progress, so the Inspector recording shows a live job, a budget prompt, and a notification. No real LLM calls, no email.
- **Operator mode**: runs where the CronJob's `Secret` is available, as a `Deployment` in the same namespace (`deploy/mcp-deployment.yaml`, one replica, Streamable HTTP behind the cluster ingress with the token verifier) or on the operator's laptop over stdio with a port-forward to PostgreSQL. The image installs the same pinned pipeline commit as the CronJob image so stage code matches.

## 11. Repository layout

```
geoecon-pipeline-mcp/
  README.md
  pyproject.toml              mcp[cli], pydantic, psycopg[binary], PyJWT[crypto], pyyaml
  .mcp.json
  pricing.yaml                per-model USD per 1M tokens, dated
  geoecon_pipeline_mcp/
    server.py jobs.py stages.py cost.py progress.py health.py sql.py incidents.py synth.py models.py auth.py
    migrations/001_mcp_jobs.sql
  fixtures/synthetic.db
  scripts/make_fixtures.py
  deploy/mcp-deployment.yaml deploy/secret.example.yaml
  tests/  test_protocol.py test_inspection.py test_jobs.py test_budget.py test_cancel.py
          test_notifications.py test_send_guard.py test_restart.py  db/test_sql.py db/test_migration.py
  evals/geoecon-pipeline.xml  10 read-only QA pairs over the synthetic history
  docs/superpowers/specs/     this document
```

## 12. Build order

1. Skeleton, synthetic state, inspection tools, protocol tests.
2. `sql.py` + DB tests against ephemeral PostgreSQL.
3. Job registry, runner thread, `start_job`/`job_status`/`cancel_job` with fake stages; progress handler.
4. Real `stages.py` resume over `_default_stages` with the original window; `preview_resume`.
5. Cost: counting proxy, estimator, `pricing.yaml`, budget `input_required` + `answer_job`.
6. Notifications on `job://` and `run://`; `send` guard; auth; HTTP smoke.
7. Prompts, resources, trace propagation, README, evals, deployment manifest. Tasks-extension adapter only if the SDK has shipped it by then.

## 13. Open decisions for the owner

- Default budget (`2.00` USD here) and whether approval can be pre-granted by passing `budget_usd` up front (this spec says yes).
- Whether `pipeline:send` should exist at all, or sending stays CronJob-only forever (this spec keeps the scope but ships it disabled).
- Whether the demo fixture paraphrases the real incident text or uses invented errors.

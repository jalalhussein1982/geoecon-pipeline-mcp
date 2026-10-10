# geoecon-pipeline-mcp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An MCP server (protocol 2026-07-28, Python SDK 2.x) that operates the geoeconomic news pipeline: inspection tools, durable job handles for stage runs with progress and a cost gate, `subscriptions/listen` notifications, incident-report prompt, synthetic mode for demo and tests.

**Architecture:** A `Store` protocol with two implementations: `SyntheticStore` (SQLite, fabricated run history, fake stages) and `PostgresStore` (the pipeline's own tables through `geoecon.state.repo.Repo` plus `sql.py`). `jobs.py` owns the `mcp_jobs` registry and a worker thread per job; `stages.py` resumes a run by calling the pipeline's stage functions with the run's original window; `cost.py` wraps `LLMClient.complete` to count tokens and gates on a budget via a job-level `input_required`. `server.py` is the thin MCP adapter.

**Tech Stack:** Python 3.12, `uv`, `mcp[cli]>=2.2`, `pydantic`, `psycopg[binary]`, `PyJWT[crypto]`, `pyyaml`, `pytest`, `anyio`. Pipeline pinned as a pip git dependency (`geoecon @ git+https://github.com/jalalhussein1982/geoeconomics-news-fetcher@<commit>`; it has a `pyproject.toml`). DB tests opt-in via marker `db` with an ephemeral PostgreSQL.

**Spec:** `docs/superpowers/specs/2026-10-11-geoecon-pipeline-mcp-design.md`

## Global Constraints

- Protocol 2026-07-28; `mcp>=2.2.0`; Python `>=3.12`.
- The pipeline package is never modified; the server imports `Config.from_env`, `geoecon.state.db`, `geoecon.state.repo.Repo`, `geoecon.orchestrator._default_stages`, `geoecon.llm.client.LLMClient`, `geoecon.watchdog`, `geoecon.registry`.
- Job status vocabulary is exactly `working | input_required | completed | failed | cancelled`; the `Job` model mirrors the tasks extension's Task (`job_id`, `status`, `poll_after_s`, `ttl_s`, `input_request`, `result`, `error`).
- The `send` stage never runs unless `ALLOW_SEND=1` and the caller holds `pipeline:send`.
- One replica; a server restart marks `working` jobs `failed` with `error="server restarted"`.
- Tools register in spec §4 order; every tool returns a Pydantic model.
- Synthetic fixtures use invented error texts, never copied production data.
- Commit after every task with the Co-Authored-By trailer from the session.

## Review Focus

1. `start_job(resume_run)` on a run whose last stage failed with `gate.NullGateError`: the resume must start at `gate` again, not skip it. Test in Task 6.
2. Two `job_status` polls for an unknown or expired (TTL) job id: `ToolError` naming `list_runs`/`start_job`, never a crash. Test in Task 5.
3. Budget approval that arrives after the job was cancelled: `answer_job` must return `ToolError("job is cancelled")` and not resume. Test in Task 7.
4. A stage function that raises mid-way after writing partial rows: the job is `failed`, `fail_stage` was called once, and `run_status` shows the stage `failed` with the error text. Test in Task 5.
5. `source_health` when the latest run has no `discovery_registry` stage row at all (an older run): every registry source reports `unknown`, not `silent`. Test in Task 4.

---

## File structure

```
geoecon-pipeline-mcp/
  pyproject.toml, .python-version, Makefile, .mcp.json, pricing.yaml
  geoecon_pipeline_mcp/
    __init__.py
    models.py      RunCard, RunDetail, StageRow, StageError, HeldRecord, GateRow, Page, SourceHealth, HealthVerdict,
                   CostReport, JobHandle, Job, Progress, InputRequest, ResumePreview, Cancelled
    store.py       Store protocol (read methods + job table methods) ; SyntheticStore(sqlite) ; PostgresStore
    sql.py         SQL strings the pipeline's Repo lacks (runs list, stage_runs, held, gate rejections, per-source counts)
    synth.py       build_synthetic(path) -> run history + fake stage functions
    jobs.py        JobRegistry (create/get/update/list, restart recovery) + JobRunner (thread, cancel flag, budget pause)
    stages.py      plan_resume(run) -> list[str]; run_stage(name, run, ctx) using pipeline stage fns with original window
    progress.py    LogCounter(logging.Handler) parsing geoecon.* lines; DbCounter polling counts
    cost.py        CountingLLMClient proxy; estimate(stage, run) ; Pricing.load(pricing.yaml)
    health.py      verdict() -> HealthVerdict via watchdog.check + stale runs
    incidents.py   TEMPLATE string + prompt text
    auth.py        JwtVerifier (same shape as hostel-ops-mcp, copied)
    server.py      MCPServer wiring, tools in order, resources, prompts, notifications, main()
    migrations/001_mcp_jobs.sql
  fixtures/synthetic.db
  scripts/make_fixtures.py
  deploy/mcp-deployment.yaml, deploy/secret.example.yaml
  tests/conftest.py, test_protocol.py, test_inspection.py, test_jobs.py, test_resume.py, test_budget.py,
        test_cancel.py, test_notifications.py, test_send_guard.py, test_restart.py, test_auth.py, test_http.py
  tests/db/test_sql.py, tests/db/test_migration.py   (marker: db)
  evals/geoecon-pipeline.xml
  docs/architecture.md, docs/use-cases.md, docs/operations.md
```

---

### Task 1: Skeleton, dependency pin, `Store` protocol, synthetic store

**Files:** Create `pyproject.toml`, `.python-version`, `Makefile`, `geoecon_pipeline_mcp/__init__.py`, `models.py` (read models only), `store.py` (protocol + `SyntheticStore`), `synth.py`, `scripts/make_fixtures.py`, `fixtures/synthetic.db`, `tests/conftest.py`, `tests/test_synth.py`.

**Interfaces produced (store.py):**
```python
class Store(Protocol):
    def list_runs(self, status: str | None, since: datetime | None, limit: int) -> list[dict]
    def run_detail(self, run_id: int) -> dict | None        # runs row + stage_runs rows
    def held_records(self, run_id: int, flag: str | None) -> list[dict]
    def gate_rejections(self, run_id: int, limit: int, cursor: str | None) -> tuple[list[dict], str | None]
    def source_counts(self, run_id: int) -> dict[str, int]  # registry_id -> articles
    def newsletters(self, run_id: int) -> list[dict]
    def latest_run_id(self) -> int | None
    # jobs
    def job_insert(self, job: dict) -> None; def job_get(self, job_id: str) -> dict | None
    def job_update(self, job_id: str, **fields) -> None; def jobs_working(self) -> list[dict]
```
Synthetic history: runs 101 (completed), 102 (completed, 3 held records: `empty_figures`, `zero_entities`, `invalid_json`), 103 (failed at `gate`, error `"NullGateError: 0 of 240 kept"`), 104 (running, stale, started 9 h ago), 105 (completed, `send_status=skipped_empty`). 66 registry sources mirrored from `config/sources.yaml` ids with per-run counts where two `live` sources have zero rows in 102.

- [ ] Failing tests: `list_runs()` returns 5 cards newest first; `run_detail(103)` shows `gate` failed with the error text and next stage `gate`; `held_records(102)` has 3 with flags; determinism digest.
- [ ] Implement; `make fixtures`; commit `chore: skeleton, store protocol, synthetic history`.

### Task 2: Inspection tools (synthetic)

**Files:** Create `server.py` (tools 1–8 of spec §4.1), `tests/test_protocol.py`, `tests/test_inspection.py`.

Tool names in order: `list_runs, run_status, failed_stage_error, list_held_records, list_gate_rejections, source_health, pipeline_health, cost_report, start_job, job_status, answer_job, cancel_job, preview_resume`. Register all names now with the later ones raising `ToolError("not implemented")` so the order test passes from this task; later tasks replace bodies.

- [ ] Failing tests: exact order; `outputSchema` on every tool; read tools `readOnlyHint=True`; `run_status(103).next_stage == "gate"`; `failed_stage_error(103).error` contains `NullGateError`; `list_held_records(102)` 3 rows; `list_gate_rejections(102, limit=5)` paginates; `source_health(102)` marks the two zero-row live sources `silent`; `source_health` for a run without a `discovery_registry` stage → all `unknown` (Review Focus 5).
- [ ] Implement; commit `feat: inspection tools`.

### Task 3: Health and cost report (read side)

**Files:** Create `health.py`, `cost.py` (Pricing + `CostReport` from job usage), `pricing.yaml`; modify `server.py`; create `tests/test_health_cost.py`.

`health.verdict(store, now)`: uses `geoecon.watchdog.check(repo, day_start, day_end)` in Postgres mode; in synthetic mode a local equivalent over `SyntheticStore` with the same three verdict strings; adds `stale_running = count(status=running and started_at < now-6h)`.
`pricing.yaml`: `{ "deepseek-v3.2": {"input_per_m": 0.27, "output_per_m": 1.10}, "kimi-k2.6": {...}, "default": {...}, "as_of": "2026-10-11" }` (values are placeholders to be set by the owner; the file states so).
`cost_report(run_id)`: sums `mcp_jobs.usage` for jobs on that run; `partial=True` when any completed stage has no usage.

- [ ] Failing tests: run 104 → `ok=false` with the stale verdict; run 103 → `partial=True`, `estimated_usd=0`; pricing load validates keys.
- [ ] Implement; commit `feat: pipeline_health and cost_report`.

### Task 4: Job registry and runner with fake stages

**Files:** Create `jobs.py`, `migrations/001_mcp_jobs.sql`, `progress.py`; modify `store.py` (`SyntheticStore` job methods on a `mcp_jobs` SQLite table with the same columns), `models.py` (Job models), `server.py` (`start_job`, `job_status`, `cancel_job`); create `tests/test_jobs.py`, `tests/test_cancel.py`, `tests/test_restart.py`.

**Interfaces produced (jobs.py):**
```python
class JobRegistry:
    def __init__(self, store: Store, stage_fns: Mapping[str, Callable], notify: Callable[[str], Awaitable[None]] | None)
    def start(self, kind: str, run_id: int | None, stages: list[str], budget_usd: float, principal: str, trace: dict) -> dict  # JobHandle
    def get(self, job_id: str) -> dict        # raises UnknownJob
    def cancel(self, job_id: str) -> dict
    def answer(self, job_id: str, answer: dict) -> dict
    def recover_after_restart(self) -> int
class JobRunner(threading.Thread):  # one per job; checks cancel flag between stages and via CountingLLMClient between batches
```
`stage_fns` are `fn(run: dict, report: Callable[[int,int|None,str], None], cancel: threading.Event) -> dict`. Synthetic fakes sleep 0.05 s per unit and call `report`. `progress.LogCounter` attaches to `geoecon.*` loggers for the job's thread (filter by thread id) and maps known message patterns (`extracted %d`, `gate batch %d/%d`, `synth record %d`) to `report`.

- [ ] Failing `test_jobs.py`: `start_job(run_stage, 103, stage="gate")` → `status=working`; polling reaches `completed` with `result` = stage metrics; `store.run_detail(103)` now shows `gate completed`; a second `start_job` on 103 while working → `ToolError` naming the job id; a stage that raises after partial writes → `failed`, `fail_stage` once, `run_status` shows `failed` (Review Focus 4); unknown/expired job id → `ToolError` (Review Focus 2).
- [ ] Failing `test_cancel.py`: cancel between two planned stages → `cancelled`, second stage never started.
- [ ] Failing `test_restart.py`: insert a `working` row, construct registry → row `failed`, `error="server restarted"`.
- [ ] Implement; commit `feat: durable job handles with runner, cancel, restart recovery`.

### Task 5: Resume planning over the real pipeline API

**Files:** Create `stages.py`, `tests/test_resume.py`; modify `server.py` (`preview_resume`, `start_job(resume_run)`).

`plan_resume(run_detail) -> ResumePlan{run_id, window_start, window_end, remaining: list[str], estimate}`: ordered stage names from `geoecon.orchestrator._default_stages` names (hardcode the canonical order `discovery, discovery_registry, discovery_search?, extraction, gate, cluster, synthesis, summary, render, send` and intersect with what `_default_stages` returns at runtime), minus `completed_stages`, starting at the first non-completed; a `failed` stage is included (Review Focus 1). `send` is removed unless the guard passes (Task 8).
`make_pipeline_stage_fns(cfg, repo, run, client) -> Mapping[str, fn]`: wraps each `(name, fn)` from `_default_stages(cfg, client)` as `repo.start_stage(run_id, name); result = fn(cfg, repo, run_id); repo.complete_stage(run_id, name, result)` with `fail_stage` on exception, and passes `now=run["window_end"]` where the orchestrator derives the window. Unit-tested with a fake `Repo` and fake stage tuples; the real pipeline import is exercised only in the `db` tests.

- [ ] Failing tests: run 103 → remaining starts with `gate`; run 101 → remaining `[]` and `preview_resume` says "nothing to resume"; `send` excluded by default; wrapper calls `start_stage/complete_stage` in order and `fail_stage` on error.
- [ ] Implement; commit `feat: resume planning with the run's original window`.

### Task 6: Cost gate (estimate, counting proxy, budget `input_required`)

**Files:** Modify `cost.py` (`CountingLLMClient`, `estimate`), `jobs.py` (pause → `input_required`, `answer`), `server.py` (`answer_job`); create `tests/test_budget.py`.

`CountingLLMClient(inner, pricing, budget: Budget)`: `complete(model, messages, **p)` → calls inner, adds `prompt_tokens`/`completion_tokens` to `usage[stage][model]`, computes USD; when spend ≥ `budget.limit`, sets `budget.exceeded` and blocks on `budget.decision` (threading.Event) until `answer_job` approves (raises new limit) or aborts (raises `BudgetAborted`). `estimate(stage, run, store, pricing)` per spec §4.3 formulas; returns `{tokens_in, tokens_out, usd, basis}`.
Job flow: before each LLM stage (`gate, cluster, synthesis, summary`) compare estimate to `budget_usd`; if above → `status=input_required`, `input_request={"key":"budget","message":..., "requested_schema":{"type":"object","properties":{"approve":{"type":"boolean"},"new_budget_usd":{"type":"number","minimum":0}},"required":["approve"]}}`; `answer_job(job_id, approve, new_budget_usd?)` resumes or fails cleanly (`fail_stage` with `"aborted at budget gate"`).

- [ ] Failing tests: estimate above budget → `input_required` with the schema; approve → `working` → `completed`, `usage` populated; abort → `failed` and stage `failed`; mid-run overspend (fake client reporting big token counts) pauses again; `answer_job` on a non-waiting job → `ToolError`; `answer_job` after cancel → `ToolError("job is cancelled")` (Review Focus 3).
- [ ] Implement; commit `feat: LLM cost gate with budget approval`.

### Task 7: Notifications and resources

**Files:** Modify `server.py`, `jobs.py` (notify hook); create `tests/test_notifications.py`, `tests/test_resources.py`.

Resources per spec §5 with `ttlMs`/`cacheScope`; `job://{job_id}`, `run://{run_id}`, `run://{run_id}/held`, `run://{run_id}/newsletter/{bucket}`, `geoecon://sources`, `geoecon://pricing`, `geoecon://incidents/template`. Notifications: the runner thread schedules `ctx.notify_resource_updated(uri)` on the server loop (`asyncio.run_coroutine_threadsafe`) for `job://` on each status change and `run://` on each stage completion/failure.

- [ ] Failing tests: `client.listen(resource_subscriptions=["job://<id>"])` receives ≥2 `ResourceUpdated` for a two-stage job; `run://103` read after completion shows the stage completed; `geoecon://pricing` is `public` cache scope.
- [ ] Implement; commit `feat: resources and subscriptions/listen notifications`.

### Task 8: Send guard, auth, HTTP

**Files:** Create `auth.py`, `tests/test_send_guard.py`, `tests/test_auth.py`, `tests/test_http.py`; modify `server.py` (`main()` with `--stdio/--http`, `AuthSettings`, scopes `pipeline:read`, `pipeline:operate`, `pipeline:send`; `STDIO_SCOPES` default `pipeline:read,pipeline:operate`).

- [ ] Failing tests: `start_job` whose plan includes `send` without `ALLOW_SEND` → `send` dropped and `run_status.note` says so; with `ALLOW_SEND=1` but no `pipeline:send` → `ToolError`; both present → included. Auth and HTTP tests as in hostel-ops-mcp Task 10 (copy the shape).
- [ ] Implement; commit `feat: send guard, OAuth resource server, HTTP mode`.

### Task 9: Postgres store and DB tests

**Files:** Modify `store.py` (`PostgresStore(repo, conn)` using `sql.py`), create `sql.py`, `tests/db/conftest.py` (starts `pg_ctl`/`initdb` ephemeral cluster if available, else skips), `tests/db/test_sql.py`, `tests/db/test_migration.py`.

- [ ] Failing DB tests: apply pipeline migrations + ours; insert runs/stage_runs/synthesis_records via `Repo`; `PostgresStore.run_detail`, `held_records`, `gate_rejections`, `source_counts` return the inserted rows; `job_insert/job_get` round-trip.
- [ ] Implement; `make db-test`; commit `feat: PostgreSQL store`.

### Task 10: Prompts, trace propagation, documentation, use cases, evals, deploy

**Files:** Create `incidents.py`, `docs/architecture.md`, `docs/use-cases.md`, `docs/operations.md`, `evals/geoecon-pipeline.xml`, `deploy/mcp-deployment.yaml`, `deploy/secret.example.yaml`, `.mcp.json`; rewrite `README.md`; modify `server.py` (prompts `write_incident_report`, `morning_check`; read `traceparent/tracestate/baggage` from request `_meta` into the job's `trace`).

`docs/use-cases.md` — scenarios with host prompt, tool calls, outputs: (1) morning check finds a stale run; (2) read why run 103 failed; (3) preview and resume 103 within a budget, watching progress; (4) budget gate fires and the operator approves; (5) review held records and decide whether to re-run synthesis; (6) silent sources; (7) write the incident report; (8) the refused send.
`README.md` — what it proves, run (synthetic, Inspector recording, operator mode), host configs, tool table, job lifecycle diagram, tasks-extension readiness statement (honest: not the extension yet), test counts, limitations.

- [ ] Write; tests for prompts (mention the tools) and trace stored on job; `uv run pytest -q`; commit `docs: README, architecture, use cases, operations, evals, deployment`.

---

## Self-review notes

Spec coverage: §4.1 → Tasks 2–3; §4.2 → Tasks 4–6; §4.3 → Task 6; §5 → Task 7; §6 → Task 8; §7 → Tasks 4, 9; §7.2 → README statement in Task 10; §8 → Task 10; §9 → Tasks 1–9; §10 → Task 10. Names consistent: `JobRegistry.start/get/cancel/answer`, `Store` methods, `plan_resume`, `CountingLLMClient`. Review Focus pinned to Tasks 5, 4, 6, 4, 2.

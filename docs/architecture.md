# Architecture

The server is a thin MCP adapter over two things it does not own: the pipeline package
(`geoecon`, pinned to one commit) and the pipeline's PostgreSQL state. Everything the server
adds — job handles, the cost ledger, the resume path, notifications — lives in its own
modules and one table of its own (`mcp_jobs`).

```
MCP host ── stdio  |  Streamable HTTP (stateless, OAuth RS) ──▶ geoecon_pipeline_mcp.server
                                                                   │  tools (13, spec order) · resources · prompts
                                                                   │  ResourceCacheMiddleware (ttlMs/cacheScope per URI)
                                                                   │  App.notify ── run_coroutine_threadsafe ──▶ InMemorySubscriptionBus
                                                                   ▼
         jobs.JobRegistry ──▶ JobRunner (thread per job) ──▶ Store.start_stage / complete_stage / fail_stage
                │                   │                            │
                │                   ├─ stages.make_pipeline_stage_fns(cfg, repo, run, client)   (operator mode)
                │                   │     └─ geoecon.stages.*  with now=run.window_end
                │                   ├─ synth.make_fake_stages(store, client)                   (synthetic mode)
                │                   ├─ cost.CountingLLMClient(inner LLMClient)  tokens → usage, budget gate, cancel
                │                   └─ progress.LogCounter (geoecon.* log lines) · progress.DbCounter (row counts)
                ▼
         Store protocol ── SyntheticStore (SQLite, fixtures/synthetic.db)  |  PostgresStore (pg.py: Repo + sql.py)
                                                                              └─ pipeline tables + mcp_jobs
```

## Modules

| Module | Owns |
|---|---|
| `server.py` | `Settings` (env), `App` (store, registry, bus, pricing), tool/resource/prompt registration, scopes, `main()` |
| `models.py` | every output schema; the `Job` family mirrors the tasks extension's `Task` |
| `store.py` | the `Store` protocol, `SyntheticStore`, canonical stage order, `next_stage_of` |
| `pg.py` + `sql.py` | `PostgresStore`: reads via `sql.py`, stage bookkeeping via the pipeline's `Repo`, `mcp_jobs` CRUD, operator-mode factories |
| `synth.py` | the fabricated history (`build_synthetic`), fake stage functions, `FakeLLM` |
| `jobs.py` | `JobRegistry`, `JobRunner`, the five-status state machine, restart recovery |
| `stages.py` | `plan_resume`, `make_pipeline_stage_fns` (the resume the orchestrator meant to have) |
| `cost.py` | `Pricing` (pricing.yaml), `estimate` (spec §4.3), `CountingLLMClient`, `Budget`, `cost_report` |
| `progress.py` | `LogCounter` (logging handler, thread-filtered), `DbCounter` (polling thread) |
| `health.py` | `verdict`: the pipeline's `watchdog.check` over a Store view + stale-run count |
| `auth.py` | `JwtVerifier` (TokenVerifier) and a dev token minter |
| `incidents.py` | the incident template and the two prompt texts |
| `migrations/001_mcp_jobs.sql` | the one table this server owns |

## The Store protocol

Tools never touch SQL. They call `Store` methods that return plain dicts with timezone-aware
datetimes; `SyntheticStore` (SQLite) and `PostgresStore` implement the same methods, so every
test that runs against the fixture exercises the code path the operator uses. The Postgres
store delegates stage bookkeeping (`start_stage`, `complete_stage`, `fail_stage`,
`set_run_status`, `merge_run_metrics`, `mark_run_failed`) to the pipeline's `Repo`, so a stage
row written through this server is the row the orchestrator would have written.

The synthetic store has a frozen clock (`store.now()` returns the fixture's anchor,
2026-10-11 08:00Z). "Today", "stale" and "nine hours ago" therefore mean the same thing on
every machine and in every test; the Postgres store returns the wall clock.

## Jobs

```
start_job ──▶ row(status=working) ──▶ thread ──┬─ for each planned stage:
                                                │    estimate > budget? ──▶ input_required ──(answer_job approve)──▶ working
                                                │                                           ──(answer_job abort)───▶ failed  (fail_stage "aborted at budget gate")
                                                │                                           ──(cancel_job)─────────▶ cancelled
                                                │    start_stage ─▶ fn(run, report, cancel) ─▶ complete_stage
                                                │         │  exception ─▶ fail_stage(text) · run failed ─▶ job failed
                                                │         │  JobCancelled (between LLM calls) ─▶ fail_stage "cancelled via MCP" ─▶ cancelled
                                                │         └  budget exceeded between LLM calls ─▶ input_required (same handshake)
                                                └─ all done ─▶ settle run status (completed | partial + note) ─▶ completed
```

* A job is a row first: `start_job` inserts it and returns before the thread has done anything.
  Any client of this server instance can poll the id; a dropped connection loses nothing. The row
  carries `owner_id` (this process) and a `heartbeat_at` refreshed every 3 s while the job is active.
* The status vocabulary is exactly the tasks extension's: `working | input_required |
  completed | failed | cancelled`. Terminal rows get `ttl_expires_at = now + 14 d`;
  `job_status` on an expired or unknown id is a `ToolError` naming `start_job`/`list_runs`.
* Cooperation points: between stages (always), between LLM calls (the counting client), and
  while waiting for a budget answer. `cancel_job` sets a flag and wakes the waiter; the
  running LLM call finishes, the next one raises.
* Restart: `JobRegistry.__init__` fails every active row whose heartbeat is older than 30 s (or
  absent) with `error="server restarted"`, through a conditional update that never touches a row
  that reached a terminal state in the meantime; a row with a fresh heartbeat belongs to another
  live process on the same database and is left alone. The pipeline's stage row stays
  `running`/`failed`; `pipeline_health` then reports the stale run, as the watchdog would.
* One replica. Two replicas would race on `active_for_run` and would need a shared
  `SubscriptionBus`; the README says so.

## Resume with the original window

`plan_resume(run_detail)` takes the canonical stage order (migration 008), intersects it with
what `geoecon.orchestrator._default_stages(cfg, client)` offers at runtime, drops the
completed stages, and starts at the first stage that is not completed — a `failed` or
`running` stage included. `send` is removed unless `ALLOW_SEND=1` **and** the caller holds
`pipeline:send`.

`make_pipeline_stage_fns` rebuilds the orchestrator's closures with two changes: the three
discovery stages get `now=run.window_end` (so the window is the run's own, not today's) and
render/send derive their date string from that window. The orchestrator's own
`find_resumable_run` keys on `window_end = now` and never matches; this is the fix from the
outside.

## Cost

* `pricing.yaml` is the only price list (the pipeline has none). `Pricing` validates it and
  resolves gateway aliases (`deepseek` → `deepseek-v3.2`).
* `estimate(stage, run)` counts the rows the stage will touch: gate re-judges every non-failed
  article in batches of 15; cluster is one call per country; synthesis is one call per cluster
  at `SYNTH_TOTAL_CHARS/4` input tokens; summary is one call per bucket. When an upstream stage
  has not completed, the counts are projected from the historical 21 % keep rate and the
  estimate's `basis` says so.
* `CountingLLMClient` has the same `complete(model, messages, **params)` as the pipeline's
  `LLMClient`; stage code cannot tell the difference. It records `prompt_tokens` /
  `completion_tokens` per stage and model into `mcp_jobs.usage`, and between calls it checks
  cancel first, then the ceiling. The gate and synthesis stages fan calls out to four worker
  threads; the ledger is lock-guarded and the "ask the operator" path is serialised so one
  overspend produces one question.

## Notifications

`ctx.notify_resource_updated` needs a request context; the job thread has none. `App.notify`
takes the server's `InMemorySubscriptionBus` (passed as `MCPServer(subscriptions=bus)`) and
schedules `bus.publish(ResourceUpdated(uri))` onto the event loop with
`asyncio.run_coroutine_threadsafe`. The loop is captured by the operation tools, which are
`async def` for that reason (the SDK runs synchronous tools in worker threads, where there is
no running loop). A host that opened `subscriptions/listen` for `job://{id}` or `run://{id}`
receives `ResourceUpdated` and refetches; one that did not polls `job_status`.

## Cache hints

The SDK fills `ttlMs`/`cacheScope` per *method* (`cache_hints={"resources/read": ...}`). The
spec wants them per *resource*, so `ResourceCacheMiddleware` rewrites the dumped
`resources/read` result by URI prefix: pricing and the incident template are `public` for an
hour, the registry `private` for an hour, newsletters `private` for a day, run and job
documents immediately stale.

## Authorization

HTTP mode runs as an OAuth 2.1 resource server: `JwtVerifier` checks signature, `iss`, `aud`
(= `AUTH_RESOURCE_URL`) and `exp`, and the SDK's bearer middleware re-checks the resource
indicator (`validate_token_resource=True`) and the required scope (`pipeline:read`). Tools
check `pipeline:operate` for writes and `pipeline:send` for the send stage. Over stdio no
token exists and `STDIO_SCOPES` applies (default `pipeline:read,pipeline:operate`, never
`pipeline:send`).

## Synthetic mode

`GEOECON_MODE=synthetic` copies `fixtures/synthetic.db` to a private file and runs fake
stages that sleep per unit of work, log in the pipeline's format and call a `FakeLLM` whose
token counts flow through the real counting client. The Inspector recording therefore shows
a live job, a budget prompt and a `ResourceUpdated` without a cluster or an LLM. The history
is deterministic (`scripts/make_fixtures.py`; a test compares the committed file with a fresh
build) and every error text in it is invented.

## Tasks-extension readiness

| Tasks extension | This server |
|---|---|
| `tasks/create` (a tool call with task metadata) → `CreateTaskResult{taskId, status, pollInterval, ttl}` | `start_job` → `JobHandle{job_id, status, poll_after_s}`; `ttl_s` on the Job |
| `tasks/get` → `Task{status, statusMessage, pollInterval, ttl}` | `job_status` → `Job` with the same five statuses, `progress`, `poll_after_s`, `ttl_s` |
| `input_required` with `inputRequests` | `Job.input_request{key, message, requested_schema}` |
| `tasks/update` (answer) | `answer_job` |
| `tasks/cancel` | `cancel_job` |
| `tasks/result` | `Job.result` / `Job.error` on terminal rows |
| `notifications/tasks/status` | `ResourceUpdated("job://{id}")` on `subscriptions/listen` |

The MCP Python SDK 2.x does not implement the extension; when it does, the adapter is a
mapping of these methods onto the existing table and no schema change.

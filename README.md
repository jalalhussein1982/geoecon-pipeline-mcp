# geoecon-pipeline-mcp

An MCP server that **operates** the geoeconomic news pipeline
([`jalalhussein1982/geoeconomics-news-fetcher`](https://github.com/jalalhussein1982/geoeconomics-news-fetcher)):
inspects runs, resumes a failed run from the right stage with the run's original window, watches
progress, reads what the validator held back, stays inside an LLM budget, and writes the incident
report in the house format. MCP protocol revision **2026-07-28**, MCP Python SDK **2.x**
(`mcp>=2.2`), Python 3.12.

The other servers in this portfolio read data. This one runs long jobs safely on a stateless
transport.

## What it proves

| Capability | How | Where |
|---|---|---|
| Long-running work with **durable handles** | `start_job` inserts an `mcp_jobs` row and returns a job id at once; a thread in this server process does the work; any client of this server instance polls `job_status(job_id)`; a dropped connection loses nothing (the row is durable, the thread is not) | `jobs.py`, `tests/test_jobs.py` |
| **Progress** | `job_status.progress` from the pipeline's own log lines and row counts; `preview_resume`/`source_health` report progress on their own request stream | `progress.py` |
| **`subscriptions/listen`** change notifications | `ResourceUpdated` on `job://{id}` at every status change and `run://{id}` at every stage boundary, published from the worker thread onto the server's bus | `tests/test_notifications.py` |
| **Cost gate** | every LLM stage is estimated first; above budget the job parks in `input_required` with a form-shaped request; a counting proxy enforces the ceiling between LLM calls; `answer_job` approves or aborts | `cost.py`, `tests/test_budget.py` |
| **Tasks-extension readiness** | the `Job` model is the extension's `Task` object one-to-one (five statuses, `poll_after_s`, `ttl_s`, `input_request`, `result`/`error`) | below |
| OpenTelemetry context | `traceparent`/`tracestate`/`baggage` from the request `_meta` are stored on the job | `tests/test_prompts_trace.py` |
| Fixes two production defects from the outside | the dead resume path (`find_resumable_run` never matches) and the missing token ledger, without touching the pipeline | `stages.py`, `cost.py` |

Non-goals: no new stage logic, no prompt or registry edits, no replacement for the CronJob. The
CronJob keeps running the daily job; this server is how a person or an agent intervenes.

## Run it

```bash
uv sync                                                    # once; the pipeline is a pinned git dependency
GEOECON_MODE=synthetic uv run python -m geoecon_pipeline_mcp.server --stdio     # demo over stdio
GEOECON_MODE=synthetic uv run python -m geoecon_pipeline_mcp.server --http --port 8000   # Streamable HTTP, stateless
GEOECON_MODE=synthetic uv run mcp dev geoecon_pipeline_mcp/server.py             # MCP Inspector
```

**Synthetic mode** (default when `DB_DSN` is unset) serves a private copy of
`fixtures/synthetic.db`: five fabricated runs — 101 completed (older, no registry stage), 102
completed with three held records and two silent sources, 103 failed in `gate`, 104 still
`running` nine hours later, 105 a smoke rerun that sent nothing. Jobs run fake stages that take a
few seconds with visible progress and call a fake LLM whose token counts flow through the real
counting client, so the Inspector shows a live job, a budget prompt and a notification without a
cluster or an LLM. Every error text in the fixture is invented. The fixture's clock is frozen at
2026-10-11 08:00Z so "today" and "stale" are the same on every machine.

**Operator mode** needs the CronJob's environment (`DB_DSN`, `MAETCENTRUM_KEY`, `SMTP_*`,
`SOURCES_PATH`, model switches): see [docs/operations.md](docs/operations.md) and
[deploy/](deploy/). It drives the pipeline's own stage functions through its `Repo`.

### Host configs

Claude Code (`.mcp.json` in this repo, synthetic over stdio):

```json
{ "mcpServers": { "geoecon-pipeline": {
    "command": "uv",
    "args": ["run", "--directory", ".", "python", "-m", "geoecon_pipeline_mcp.server", "--stdio"],
    "env": { "GEOECON_MODE": "synthetic" } } } }
```

Claude Desktop (`claude_desktop_config.json`):

```json
{ "mcpServers": { "geoecon-pipeline": {
    "command": "uv",
    "args": ["run", "--directory", "/path/to/geoecon-pipeline-mcp", "python", "-m", "geoecon_pipeline_mcp.server", "--stdio"],
    "env": { "GEOECON_MODE": "synthetic" } } } }
```

Remote (Streamable HTTP, OAuth resource server; the host obtains a token from the issuer named in
`/.well-known/oauth-protected-resource/mcp`):

```json
{ "mcpServers": { "geoecon-pipeline": { "type": "http", "url": "https://geoecon-mcp.example.org/mcp" } } }
```

## Tools (registration order)

| # | Tool | Kind | What it returns |
|---|---|---|---|
| 1 | `list_runs(status?, since?, limit≤50)` | read | `RunCard[]`: window, status, stages done/total, last failed stage |
| 2 | `run_status(run_id)` | read | `RunDetail`: every stage row, metrics, notes, `next_stage`, `active_job_id` |
| 3 | `failed_stage_error(run_id)` | read | `StageError`: the stored error text, checkpoint, duration |
| 4 | `list_held_records(run_id, flag?)` | read | `HeldRecord[]`: the validator's rejections with reasons and action |
| 5 | `list_gate_rejections(run_id, limit, cursor?)` | read | `Page[GateRow]` with `total_dropped` and `unparsed_dropped` |
| 6 | `source_health(run_id?)` | read, progress | `SourceHealth[]`: 66 registry sources; `silent` = live with zero rows; `unknown` when the run has no registry stage |
| 7 | `pipeline_health()` | read | `HealthVerdict`: the watchdog's verdict for the local day + stale `running` runs |
| 8 | `cost_report(run_id)` | read | `CostReport`: tokens by stage and model from this server's jobs; `partial` when stages ran elsewhere |
| 9 | `start_job(kind, run_id?, stage?, budget_usd?, force?)` | write | `JobHandle` immediately; kinds `resume_run`, `run_stage`, `reconcile_abandoned` |
| 10 | `job_status(job_id)` | read | `Job`: `working | input_required | completed | failed | cancelled`, progress, usage, `input_request`, result/error |
| 11 | `answer_job(job_id, approve, new_budget_usd?)` | write, idempotent | `Answered`: the budget decision |
| 12 | `cancel_job(job_id)` | write, idempotent | `Cancelled`: cooperative, at the next stage or LLM-batch boundary |
| 13 | `preview_resume(run_id)` | read, progress | `ResumePreview`: remaining stages, original window, per-stage estimates, whether `send` is withheld |

Errors the model can fix are `ToolError`s that name the next move (unknown run → `list_runs`;
unknown or expired job → `start_job`; stage not next → the next stage and `force`; another job
active → its id and `cancel_job`; budget below estimate → `answer_job`). A stage that raises becomes
a `failed` job carrying the exception text; the server never crashes on it.

**Resources:** `run://{id}` · `run://{id}/held` · `run://{id}/newsletter/{bucket}` ·
`run://{id}/archive/{bucket}` · `job://{id}` · `geoecon://sources` · `geoecon://pricing` ·
`geoecon://incidents/template` — with `ttlMs`/`cacheScope` per URI (pricing and the template are
`public` for an hour; run and job documents are immediately stale).
**Prompts:** `write_incident_report(run_id)` (the house format: status line, facts table, root
cause, P0–P3 fixes with ✓/⟳, tests, follow-up) and `morning_check`.

## Job lifecycle

```
                       start_job ─────────────▶ ┌─────────┐
                                                │ working │◀──────────────────────────────┐
   estimate > budget, or spend > ceiling ──────▶│         │── answer_job(approve, new_budget) ┤
                                                └────┬────┘                                 │
                                                     │                              ┌────────┴───────┐
                                                     ├──────────────────────────────▶│ input_required │
                                                     │                              └───┬─────┬──────┘
         every planned stage completed ──────────────┤          answer_job(abort) ───────┘     │ cancel_job
         (run → completed, or partial + note)        ▼                                        ▼
                                              ┌───────────┐   stage raised / aborted   ┌────────┐   ┌───────────┐
                                              │ completed │   fail_stage(text) ───────▶│ failed │   │ cancelled │
                                              └───────────┘                            └────────┘   └───────────┘
                                                                 cancel_job between stages / LLM calls ──▶ cancelled
                                            server restart while working ──▶ failed (error="server restarted")
```

Each transition publishes `ResourceUpdated("job://{id}")`; each stage start/completion/failure
publishes `ResourceUpdated("run://{run_id}")`. Terminal rows are kept 14 days (`ttl_s`).

A resume starts at the first stage that is not completed — a failed `gate` is re-run, not
skipped — and passes the run's **original** `window_start`/`window_end` to the stage functions.
`send` is never in the plan unless `ALLOW_SEND=1` on the server **and** the caller holds
`pipeline:send`; by default a resumed run stops after `render` and `run_status.notes` says so.

## Tasks-extension readiness, stated plainly

This server does **not** implement the MCP tasks extension (`io.modelcontextprotocol/tasks`,
SEP-2663). The MCP Python SDK 2.x does not ship it, so jobs are ordinary tools with a
server-minted handle, which every host supports today. The `Job` model is deliberately the
extension's `Task` object: `job_id` ≈ `taskId`, the same five statuses, `poll_after_s` ≈
`pollInterval`, `ttl_s` ≈ `ttl`, `input_request` ≈ `inputRequests`, `result`/`error` on terminal
states. When the SDK ships the extension, `tasks/get`, `tasks/update`, `tasks/cancel` map onto
`job_status`, `answer_job`, `cancel_job` with no change to the table
([docs/architecture.md](docs/architecture.md#tasks-extension-readiness)).

## Authorization

Over Streamable HTTP the server is an OAuth 2.1 resource server (`TokenVerifier` + `AuthSettings`,
`validate_token_resource=True`): JWTs are checked for signature (HS256 secret, PEM or JWKS), `iss`,
`aud` = the resource URL and `exp`. Scopes: `pipeline:read` (required by the HTTP layer; all
reads), `pipeline:operate` (`start_job`, `answer_job`, `cancel_job`), `pipeline:send` (the send
stage, and only with `ALLOW_SEND=1`). Over stdio `STDIO_SCOPES` applies (default
`pipeline:read,pipeline:operate`).

## Tests

```bash
uv run pytest -q          # 128 passed: 119 in-memory + 9 ephemeral-PostgreSQL (db) tests
make db-test              # only the db tests; they skip cleanly when initdb/pg_ctl are missing
```

In-memory tests talk to the server object through the SDK's `Client` over the synthetic store:
tool order and schemas, every inspection tool's numbers, the job state machine (completion,
conflicts, partial-write failure, unknown/expired ids, restart recovery), cancel at both
boundaries, the budget gate (estimate, approve, abort, mid-run overspend, answer-after-cancel),
`subscriptions/listen` events, the send guard, JWT verification, uvicorn HTTP with and without
bearer tokens, prompts, trace propagation, and all ten QA pairs in
[`evals/geoecon-pipeline.xml`](evals/geoecon-pipeline.xml) check by check. The `db` tests apply the
pipeline's migrations plus ours to a cluster started with `initdb`/`pg_ctl`, insert rows through the
pipeline's own `Repo`, and read them back through `PostgresStore`.

[`docs/use-cases.md`](docs/use-cases.md) shows eight operator scenarios with tool calls and outputs
captured from the synthetic server by `scripts/capture_use_cases.py`.

## Limitations

- **One replica.** The job row is durable; the thread is not. A restart fails `working` jobs with
  `error="server restarted"`; a second replica would race on jobs and need a shared
  `SubscriptionBus`.
- **Progress for the real stages is coarse.** The pipeline logs end-of-stage summaries, not
  per-batch lines; the DB counter polls row counts every 5 s for extraction, gate and synthesis.
- **The cost ledger covers only what ran here.** The CronJob keeps no token counts;
  `cost_report.partial=true` says which stages ran outside. `pricing.yaml` values are placeholders
  until the owner sets them; `pricing_as_of` travels with every figure.
- **Operator mode was not run against the production cluster** in this repository's tests; the
  Postgres store was verified against an ephemeral local PostgreSQL with the pipeline's own `Repo`.
- **The archive resource** reads the PVC only when it is mounted into this process; otherwise it
  says where the file lives.

## Layout

```
geoecon_pipeline_mcp/   server.py models.py store.py pg.py sql.py synth.py jobs.py stages.py cost.py
                        progress.py health.py auth.py incidents.py migrations/001_mcp_jobs.sql
fixtures/               synthetic.db (deterministic; `make fixtures`), sources.yaml (the pipeline's registry)
tests/                  in-memory suite; tests/db/ ephemeral PostgreSQL (marker `db`)
docs/                   architecture.md operations.md use-cases.md superpowers/{specs,plans}
evals/                  geoecon-pipeline.xml (10 verified QA pairs)
deploy/                 mcp-deployment.yaml secret.example.yaml · Dockerfile · .mcp.json · pricing.yaml
```

## Related servers in this portfolio

- [`ihkam-eval-mcp`](https://github.com/jalalhussein1982/ihkam-eval-mcp) — read-only evaluation harness over a legal RAG (stdio)
- `phase1/siem-mcp` in [`conflict-trends-dashboard`](https://github.com/jalalhussein1982/conflict-trends-dashboard) — conflict analytics over PostGIS (stdio)
- `energy_platform/mcp` in [`energy-platform`](https://github.com/jalalhussein1982/energy-platform) — the contributor path of an ingestion platform (stdio)
- `hostel-ops-mcp`, `geoecon-pipeline-mcp`, `coin-wargame-mcp` — this series: remote transport and OAuth, long-running jobs, and MCP as an agent harness

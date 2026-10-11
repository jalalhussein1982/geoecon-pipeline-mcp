# Implementation notes and deviations from the plan

Recorded while implementing `2026-10-11-geoecon-pipeline-mcp.md`. Each item says what the plan
said, what was done, and why.

## Dependencies and layout

- **Pipeline dependency.** `geoecon @ git+https://github.com/jalalhussein1982/geoeconomics-news-fetcher@c6ae09b6209b2472a4e209856b413fafddcb1a79`
  (main at the time of implementation) installs cleanly in a few seconds (trafilatura, feedparser,
  Jinja2, psycopg, googlenewsdecoder, cssselect). No `[pipeline]` extra was needed; `geoecon` is a
  plain dependency and `health.py` / `stages.py` / `server.py` import it directly. The installed
  package does **not** ship `config/sources.yaml`, so the registry is copied to
  `fixtures/sources.yaml` (data, not code) and `SOURCES_PATH` points at it.
- **`PostgresStore` lives in `pg.py`, not `store.py`.** The plan put both stores in `store.py`;
  keeping the Postgres store (psycopg, Repo imports) in its own module keeps synthetic mode's
  import graph small. `store.py` holds the protocol and `SyntheticStore`.
- **`mcp_jobs` methods on `SyntheticStore` were written in Task 1** together with the schema, since
  the SQLite schema mirrors the migration; Task 4 used them without further store changes.
- **`hostel-ops-mcp/auth.py` did not exist to copy** (that repo holds only its spec and plan today), so
  `auth.py` was written fresh in the shape the spec describes (`TokenVerifier` over PyJWT with HS256
  / PEM / JWKS, `iss`/`aud`/`exp`, `scope` or `scp`). hostel-ops-mcp can copy it back.

## Synthetic history

- **Run ordering.** The plan listed 104 as "running, stale, started 9 h ago" and 105 as
  "completed, send_status=skipped_empty". Run ids are assigned at insert in the real database, so
  105 must be later in time than 104. The fixture keeps both facts and makes 105 a manual
  `LIMIT=50` smoke rerun started at 05:00Z on the fixture's day (three hours before the anchor),
  which completed with nothing to send. Consequence: the watchdog's verdict for the fixture's
  morning is "pipeline ran but sent no newsletter" (about 105) **and** `stale_running=1` (104).
  `pipeline_health` carries both; the health test asserts both.
- **Frozen clock.** `SyntheticStore.now()` returns the fixture's anchor (2026-10-11 08:00Z, stored
  in a `meta` table) so "today", "stale" and "nine hours ago" are the same on every machine. The
  Postgres store returns the wall clock. Job timestamps (created/updated/ttl) always use the wall
  clock.
- **Run 101 is an older run** without a `discovery_registry` stage row (8 stages, GDELT only). That
  gives Review Focus 5 a natural fixture: `source_health(101)` is all `unknown`.
- **Invented error text.** Run 103's gate error is
  `NullGateError: gate kept 0 of 228 articles; every verdict came back unparsable ...` — invented,
  not the production text. 240 articles were discovered, 12 failed extraction, so the gate judged
  228; the text and the rejection counts agree (the plan's "0 of 240" was adjusted for that).

## Server behaviour

- **`start_job` returns immediately and does not stream progress on its own response** (the
  spec mentioned `ctx.report_progress` for the first stage "for a host that holds the response
  stream open"; a tool that returns at once has no stream to hold). Progress is in
  `job_status.progress` and in `job://` notifications. `preview_resume` and `source_health` do
  report progress over their own request stream.
- **Operation tools are `async def`.** The SDK runs synchronous tools in worker threads where no
  event loop is running; the loop must be captured to schedule notifications from the job thread
  (`asyncio.run_coroutine_threadsafe(bus.publish(...), loop)`). The server passes its own
  `InMemorySubscriptionBus` to `MCPServer(subscriptions=...)` and keeps the reference.
- **Per-resource cache hints via middleware.** The SDK fills `ttlMs`/`cacheScope` per method
  (`cache_hints={"resources/read": ...}`) and the envelope pass runs *inside* the middleware chain,
  so a `ServerMiddleware` sees the dumped dict and can rewrite the two keys by URI prefix. That is
  how `geoecon://pricing` is `public`/1 h while `run://` is 0/private.
- **`cost_report.partial`.** The plan's wording ("any completed stage has no usage") would make
  every run partial forever since non-LLM stages never spend tokens. Implemented as: a completed
  stage that no job of this server covered (`result` keys) ⇒ partial, and `stages_without_usage`
  names them. Run 103 is partial with three such stages, as the plan's test expects.
- **Estimate projection.** When `gate`/`cluster` have not completed, downstream estimates project
  kept articles (21 %) and clusters (kept / 2.2) and say so in `basis`. The trigger is the stage
  table, not row counts: run 103 has verdicts on every row (all unparsable) and the real gate
  re-judges them all on resume.
- **Run status after a job.** Once all planned stages complete: `completed` if nothing remains;
  `partial` (the pipeline's own CHECK vocabulary) if stages remain and none is failed/running,
  with a note when `send` was withheld. A re-run single stage therefore moves a `failed` run to
  `partial`.
- **`reconcile_abandoned` uses `store.now()`** (anchor in synthetic mode) so the fixture's stale run
  reconciles deterministically.
- **Progress patterns.** The pipeline's stages log only end-of-stage summaries (`gate: {...}`,
  `extraction: {...}`, `synthesis: {...}`, `cluster: N clusters from M gated articles`,
  `render[bucket]: N records`); the per-unit lines the plan named (`extracted N`, `gate batch i/n`,
  `synth record N`) exist only in the fake stages. `LogCounter` parses both; `DbCounter` polls row
  counts every 5 s in Postgres mode.
- **Threading in Postgres mode.** A psycopg connection is not thread-safe. `PostgresStore` holds one
  connection behind a lock for tool reads and stage bookkeeping; `pipeline_factories` opens a
  separate connection + `Repo` per job for the stage functions, which write on the job thread.

## Tests

- 128 tests: 119 in-memory (synthetic store, in-process `Client`, uvicorn HTTP smoke) and 9 `db`
  tests. The `db` tests start an ephemeral PostgreSQL with `initdb`/`pg_ctl` on a free TCP port
  (Unix sockets disabled: pytest tmp paths exceed the socket path limit on macOS) and skip cleanly
  when the binaries are absent. They ran green here against PostgreSQL 18.4 (Homebrew).
- Review Focus mapping: 1 → `test_resume.py::test_run_103_resumes_at_gate_with_everything_after`
  and `test_synth.py::test_run_103_failed_in_gate_and_resumes_at_gate`; 2 →
  `test_jobs.py::test_unknown_or_expired_job_is_a_tool_error_twice`; 3 →
  `test_budget.py::test_answer_after_cancel_is_refused_and_nothing_resumes`; 4 →
  `test_jobs.py::test_stage_that_raises_after_partial_writes_fails_cleanly`; 5 →
  `test_inspection.py::test_source_health_without_registry_stage_is_all_unknown`.
- `docs/use-cases.md` is generated by `scripts/capture_use_cases.py` from real tool output;
  `evals/geoecon-pipeline.xml` is verified check-by-check by `tests/test_evals.py`.

## Not done / not verifiable here

- Operator mode against the real cluster (CERIT-SC) was not run: no cluster credentials. The
  Postgres store was exercised against an ephemeral local PostgreSQL with rows written by the
  pipeline's own `Repo`; `pipeline_factories` and `make_pipeline_stage_fns` were exercised with the
  real `geoecon` modules imported but not executed (no LLM, no network).
- The Docker image was not built here (no registry push in scope).
- `pricing.yaml` values are placeholders, as the file states.

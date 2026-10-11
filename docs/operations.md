# Operations

## Modes

| Mode | Set by | State | Stages | LLM |
|---|---|---|---|---|
| synthetic (default) | `GEOECON_MODE=synthetic`, or no `DB_DSN` | private copy of `fixtures/synthetic.db` | fakes (sleep, log, write SQLite) | `FakeLLM` |
| postgres (operator) | `DB_DSN=postgresql://...` (and `GEOECON_MODE` unset or `postgres`) | the pipeline's database + `mcp_jobs` | the pipeline's stage functions | the pipeline's `LLMClient` |

Operator mode needs the CronJob's full environment (`Config.from_env` reads it): `DB_DSN`,
`MAETCENTRUM_KEY`, `SMTP_*`, `FROM_ADDR`, `RECIPIENTS`, `SOURCES_PATH`, the model switches.
Mount the same `Secret` (`geoecon-secrets`) the CronJob uses.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `GEOECON_MODE` | `synthetic` unless `DB_DSN` is set | `synthetic` or `postgres` |
| `GEOECON_SYNTHETIC_DB` | unset (private temp copy) | path of a persistent synthetic DB; mutated by jobs |
| `DB_DSN` | — | pipeline PostgreSQL DSN |
| `SOURCES_PATH` | `fixtures/sources.yaml` | registry YAML (`/app/config/sources.yaml` in the CronJob image) |
| `PRICING_PATH` | `pricing.yaml` | per-model USD per 1M tokens, dated |
| `DEFAULT_BUDGET_USD` | `2.00` | budget when `start_job` gets none |
| `GATE_MODEL`, `SYNTHESIS_MODEL` | `deepseek-v3.2` | model names used for estimates (same env as the CronJob) |
| `ALLOW_SEND` | unset (off) | `1` lets a caller with `pipeline:send` run the send stage |
| `STDIO_SCOPES` | `pipeline:read,pipeline:operate` | scopes assumed over stdio |
| `AUTH_ISSUER`, `AUTH_RESOURCE_URL` | — | both set ⇒ HTTP requires a bearer token for this resource |
| `AUTH_JWKS_URL` / `AUTH_PUBLIC_KEY_PEM` / `AUTH_HS256_SECRET` | — | one key source for the verifier |
| `FAKE_UNIT_SLEEP` | `0.05` | seconds per unit of fake work (synthetic) |
| `HOST`, `PORT` | `127.0.0.1`, `8000` | HTTP bind |

## Running

```bash
make sync                         # uv sync (needs network once: the pipeline is a git dependency)
make stdio                        # synthetic, stdio
make http                         # synthetic, Streamable HTTP on :8000/mcp (stateless)
make inspector                    # MCP Inspector against the synthetic server
uv run python -m geoecon_pipeline_mcp.server --http --host 0.0.0.0 --port 8000   # operator mode with env set
```

Host configs are in the README (`.mcp.json` for Claude Code, a `claude_desktop_config.json`
block, an HTTP block).

## Deploying next to the CronJob

`deploy/mcp-deployment.yaml` is a one-replica `Deployment` (strategy `Recreate`: two replicas
would race on jobs) plus a `Service`, in the pipeline's namespace, with the `app: geoecon`
label the database NetworkPolicy admits. It mounts `geoecon-secrets` (the CronJob's) and
`geoecon-mcp-auth` (`deploy/secret.example.yaml`: issuer, resource URL, JWKS URL,
`ALLOW_SEND`). The archive PVC is mounted read-only so `run://{id}/archive/{bucket}` can
serve the files. Put the cluster ingress in front of the Service with TLS; the resource URL in
the token must be the public URL.

The image (`Dockerfile`) installs this package, which pins the pipeline commit; keep the pin
equal to the CronJob image's commit so stage code matches.

## Tokens and scopes

| Scope | Grants |
|---|---|
| `pipeline:read` | every inspection tool, `job_status`, `preview_resume`, all resources (required by the HTTP layer) |
| `pipeline:operate` | `start_job`, `answer_job`, `cancel_job` |
| `pipeline:send` | the send stage, and only with `ALLOW_SEND=1` on the server |

The verifier accepts `scope` (space-separated) or `scp` (list). `aud` must contain
`AUTH_RESOURCE_URL`. For a quick local check without an IdP:

```bash
export AUTH_ISSUER=https://issuer.local AUTH_RESOURCE_URL=http://127.0.0.1:8000/mcp AUTH_HS256_SECRET=$(openssl rand -hex 32)
uv run python -c "from geoecon_pipeline_mcp.auth import mint_hs256; import os; print(mint_hs256(os.environ['AUTH_HS256_SECRET'], issuer=os.environ['AUTH_ISSUER'], audience=os.environ['AUTH_RESOURCE_URL'], subject='me', scopes=['pipeline:read','pipeline:operate']))"
```

## Runbooks

**Morning: `pipeline_health` says not ok.**
`subject` is the watchdog's own line. `stale_run_ids` non-empty means a process died without
unwinding: `start_job(kind="reconcile_abandoned")` closes those runs the way the orchestrator's
reconciliation does (status `failed`, note starting `abandoned:`), then `write_incident_report`.

**A run failed in `gate` (NullGateError).**
`failed_stage_error` has the text; `list_gate_rejections` tells upstream from ours: if
`unparsed_dropped == total_dropped` the model or its backend was down. Check the gateway, fix
`GATE_MODEL` if needed, then `preview_resume(run_id)` and `start_job(resume_run, run_id,
budget_usd=...)`. The job re-runs `gate` and everything after it, stops before `send`.

**The job is `input_required`.**
Read `job_status.input_request.message`: it quotes the estimate (or the spend), the model and
the basis. `answer_job(approve=true, new_budget_usd=X)` to continue, `approve=false` to abort
cleanly (the stage is marked failed with `aborted at budget gate`). `cancel_job` also ends the
wait.

**Sending.** Leave it to the CronJob. If a brief must go out by hand: set `ALLOW_SEND=1` on
the server, use a token with `pipeline:send`, and `start_job(run_stage, run_id, stage="send")`
after `run_status.next_stage == "send"`.

**The server restarted while a job was working.** The job is `failed` with
`error="server restarted"`; the run's stage row is still `running`. `pipeline_health` will
show it as stale after six hours; before that, `start_job(run_stage, run_id, stage=<that
stage>)` re-runs the stage (the planner treats a running stage as the next one).

## Limits

* One replica. The handle is durable (a row); the thread is not.
* Progress for the pipeline's own stages comes from their end-of-stage log lines and from row
  counts polled every 5 s; per-batch progress exists only where the stages log it.
* `cost_report` counts only what ran through this server; the CronJob keeps no ledger
  (`partial=true`).
* The archive resource reads the PVC only when it is mounted into this process.
* `pricing.yaml` values are placeholders until the owner sets them; `pricing_as_of` travels
  with every figure.

## Tests

```bash
uv run pytest -q              # in-memory suite + db tests (db tests skip without initdb/pg_ctl)
make db-test                  # only the ephemeral-PostgreSQL tests
make fixtures                 # rebuild fixtures/synthetic.db (a test checks it matches a fresh build)
uv run python scripts/capture_use_cases.py   # regenerate docs/use-cases.md from real tool output
```

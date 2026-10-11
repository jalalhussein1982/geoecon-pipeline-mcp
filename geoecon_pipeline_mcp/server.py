"""MCP server that operates the geoeconomic news pipeline.

    GEOECON_MODE=synthetic uv run python -m geoecon_pipeline_mcp.server --stdio   # demo, no cluster
    GEOECON_MODE=synthetic uv run mcp dev geoecon_pipeline_mcp/server.py          # MCP Inspector
    DB_DSN=postgresql://... uv run python -m geoecon_pipeline_mcp.server --http   # operator mode

What a host gets from this server, and why each piece exists:

* inspection tools — read runs, stage errors, held records, gate rejections, source and
                     pipeline health, cost. The evidence an operator reads before acting.
* operation tools  — `start_job` returns a durable handle at once; `job_status` polls it;
                     `answer_job` answers the cost gate; `cancel_job` is cooperative;
                     `preview_resume` is the dry run. Long work on a stateless transport.
* resources        — `run://`, `job://`, `geoecon://` documents with cache hints; a host that
                     opened `subscriptions/listen` is told when a job or run changes.
* prompts          — the incident report in the house format, and the morning checklist.

Design choices, stated once here and once in the README:

* The pipeline is a pinned dependency and is never modified. The server calls its stage
  functions with the run's ORIGINAL window, which is the resume the orchestrator meant to have.
* `ToolError` for anything the model can fix by choosing differently (unknown run or job,
  stage not next, another job active, budget below estimate, scope missing). A stage that
  raises becomes a `failed` job with the exception text; the server itself does not crash.
* Tools register in a fixed order; every tool returns a Pydantic model.
* The `send` stage never runs unless ALLOW_SEND=1 and the caller holds `pipeline:send`.
* One replica. A restart marks `working` jobs `failed` with error="server restarted".
"""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field

from mcp.server import MCPServer
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.settings import AuthSettings
from mcp.server.caching import CacheHint
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ResourceError, ResourceNotFoundError, ToolError
from mcp.server.subscriptions import InMemorySubscriptionBus, ResourceUpdated
from mcp.types import ToolAnnotations

from . import cost as C
from . import health as H
from . import incidents as I
from . import models as M
from .jobs import TERMINAL, JobConflict, JobRegistry, NotWaiting, UnknownJob
from .stages import plan_resume
from .store import CANONICAL_STAGES, LLM_STAGES, Store, SyntheticStore
from .synth import FIXTURES_DIR, SOURCES_PATH, FakeLLM, make_fake_stages

log = logging.getLogger("geoecon_pipeline_mcp.server")

ROOT = Path(__file__).resolve().parent.parent
SCOPE_READ, SCOPE_OPERATE, SCOPE_SEND = "pipeline:read", "pipeline:operate", "pipeline:send"
READ_ONLY = ToolAnnotations(read_only_hint=True)
HELD_FLAGS = ("empty_figures", "zero_entities", "lang_mismatch", "pair_incomplete", "retry_fallback", "invalid_json")

# Resource cache hints (spec §5): (ttl_ms, scope) by URI prefix; the middleware applies them.
RESOURCE_CACHE: tuple[tuple[str, int, str], ...] = (
    ("geoecon://pricing", 3_600_000, "public"),
    ("geoecon://incidents/template", 3_600_000, "public"),
    ("geoecon://sources", 3_600_000, "private"),
    ("run://", 0, "private"),            # overridden below for newsletter/archive
    ("job://", 0, "private"),
)
NEWSLETTER_TTL_MS = 86_400_000


def cache_hint_for(uri: str) -> tuple[int, str]:
    if uri.startswith("run://") and ("/newsletter/" in uri or "/archive/" in uri):
        return NEWSLETTER_TTL_MS, "private"
    for prefix, ttl, scope in RESOURCE_CACHE:
        if uri.startswith(prefix):
            return ttl, scope
    return 0, "private"


class ResourceCacheMiddleware:
    """Set `ttlMs`/`cacheScope` on resources/read results per URI. The runner has already
    applied the server-wide hint and dumped the result by the time middleware sees it."""

    async def __call__(self, ctx, call_next):
        result = await call_next(ctx)
        if ctx.method == "resources/read" and isinstance(result, dict):
            ttl, scope = cache_hint_for(str((ctx.params or {}).get("uri", "")))
            return {**result, "ttlMs": ttl, "cacheScope": scope}
        return result


INSTRUCTIONS = (
    "Operator's view of the geoeconomic news pipeline. Start with pipeline_health, then list_runs and "
    "run_status on the run you care about; failed_stage_error reads the stored error instead of guessing. "
    "Before resuming anything call preview_resume: it names the stages that would run, the original window "
    "they get, and the LLM cost estimate against the budget. start_job returns a job id immediately; poll "
    "job_status (or listen on job://{id}); answer_job answers a budget prompt; cancel_job is cooperative. "
    "The send stage is withheld unless the operator enabled it. Write incidents with the "
    "write_incident_report prompt and quote the tools' numbers."
)


# ── settings ───────────────────────────────────────────────────────────────────────


def _bool(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Settings:
    mode: Literal["synthetic", "postgres"] = "synthetic"
    synthetic_db: Path | None = None            # None: private copy of fixtures/synthetic.db
    db_dsn: str | None = None
    sources_path: Path = SOURCES_PATH
    pricing_path: Path = ROOT / "pricing.yaml"
    default_budget_usd: float = 2.00
    gate_model: str = "deepseek-v3.2"
    synthesis_model: str = "deepseek-v3.2"
    allow_send: bool = False
    stdio_scopes: tuple[str, ...] = (SCOPE_READ, SCOPE_OPERATE)
    fake_unit_sleep: float = 0.05
    fake_prompt_tokens: int = 3000
    fake_completion_tokens: int = 600
    auth_issuer: str | None = None
    auth_resource_url: str | None = None
    auth_jwks_url: str | None = None
    auth_hs256_secret: str | None = None
    auth_public_key_pem: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        e = dict(os.environ if env is None else env)
        mode = e.get("GEOECON_MODE") or ("postgres" if e.get("DB_DSN") else "synthetic")
        if mode not in ("synthetic", "postgres"):
            raise ValueError(f"GEOECON_MODE must be synthetic or postgres, got {mode!r}")
        sources = e.get("SOURCES_PATH")
        return cls(
            mode=mode,  # type: ignore[arg-type]
            synthetic_db=Path(e["GEOECON_SYNTHETIC_DB"]) if e.get("GEOECON_SYNTHETIC_DB") else None,
            db_dsn=e.get("DB_DSN"),
            sources_path=Path(sources) if sources else SOURCES_PATH,
            pricing_path=Path(e.get("PRICING_PATH") or ROOT / "pricing.yaml"),
            default_budget_usd=float(e.get("DEFAULT_BUDGET_USD", "2.00")),
            gate_model=e.get("GATE_MODEL", "deepseek-v3.2"),
            synthesis_model=e.get("SYNTHESIS_MODEL", "deepseek-v3.2"),
            allow_send=_bool(e.get("ALLOW_SEND")),
            stdio_scopes=tuple(s.strip() for s in e.get("STDIO_SCOPES", f"{SCOPE_READ},{SCOPE_OPERATE}").split(",") if s.strip()),
            fake_unit_sleep=float(e.get("FAKE_UNIT_SLEEP", "0.05")),
            auth_issuer=e.get("AUTH_ISSUER"),
            auth_resource_url=e.get("AUTH_RESOURCE_URL"),
            auth_jwks_url=e.get("AUTH_JWKS_URL"),
            auth_hs256_secret=e.get("AUTH_HS256_SECRET"),
            auth_public_key_pem=e.get("AUTH_PUBLIC_KEY_PEM"),
        )


# ── application container ───────────────────────────────────────────────────────────


class App:
    """Everything the tool functions close over. One per server process (or per test)."""

    def __init__(self, settings: Settings, store: Store, stage_factory: Callable | None = None,
                 llm_factory: Callable[[], Any] | None = None):
        self.settings = settings
        self.store = store
        self.bus = InMemorySubscriptionBus()
        verifier, auth = _auth_from(settings)
        self.mcp: MCPServer = MCPServer(
            "geoecon-pipeline", instructions=INSTRUCTIONS, subscriptions=self.bus,
            cache_hints={"resources/read": CacheHint(ttl_ms=0, scope="private")},
            middleware=[ResourceCacheMiddleware()],
            token_verifier=verifier, auth=auth,
        )
        self._sources_cache: list | None = None
        self._tmp_db: Path | None = None
        self.pricing = C.Pricing.load(settings.pricing_path)
        self.stage_models = {"gate": settings.gate_model, "cluster": settings.gate_model,
                             "synthesis": settings.synthesis_model, "summary": settings.synthesis_model}
        self.loop: asyncio.AbstractEventLoop | None = None
        if stage_factory is None or llm_factory is None:
            if settings.mode == "synthetic":
                sf = stage_factory or (lambda client: make_fake_stages(store, client, unit_sleep=settings.fake_unit_sleep))
                lf = llm_factory or (lambda: FakeLLM(settings.fake_prompt_tokens, settings.fake_completion_tokens))
            else:
                from .pg import pipeline_factories
                psf, plf = pipeline_factories(settings, store)
                sf, lf = stage_factory or psf, llm_factory or plf
        else:
            sf, lf = stage_factory, llm_factory
        self.registry = JobRegistry(
            store, sf, pricing=self.pricing, stage_models=self.stage_models, llm_factory=lf,
            notify=self.notify, default_budget_usd=settings.default_budget_usd,
            db_progress=(settings.mode == "postgres"),
        )

    # -- notifications -----------------------------------------------------------------

    def notify(self, uri: str) -> None:
        """Publish a resource-updated event from any thread. Scheduled onto the server's event
        loop, which the operation tools capture when they run."""
        loop = self.loop
        if loop is None or loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(self.bus.publish(ResourceUpdated(uri=uri)), loop)
        except RuntimeError:
            pass

    def capture_loop(self) -> None:
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

    # -- registry --------------------------------------------------------------------

    def sources(self) -> list:
        if self._sources_cache is None:
            from geoecon.registry import load_sources
            self._sources_cache = load_sources(self.settings.sources_path)
        return self._sources_cache

    # -- authorization -----------------------------------------------------------------

    def scopes(self) -> set[str]:
        token = get_access_token()
        if token is not None:
            return set(token.scopes)
        return set(self.settings.stdio_scopes)

    def principal(self) -> str | None:
        token = get_access_token()
        if token is None:
            return "stdio"
        return token.subject or token.client_id

    def require(self, scope: str) -> None:
        if scope not in self.scopes():
            raise ToolError(f"this tool requires the {scope} scope; the current credentials carry "
                            f"{sorted(self.scopes()) or 'no scopes'}")

    # -- lookups -----------------------------------------------------------------------

    def run(self, run_id: int) -> dict:
        d = self.store.run_detail(run_id)
        if d is None:
            raise ToolError(f"No run with id {run_id}. Call list_runs to see the runs that exist.")
        return d

    def close(self) -> None:
        self.registry.shutdown()
        close = getattr(self.store, "close", None)
        if close:
            close()
        if self._tmp_db and self._tmp_db.exists():
            for p in (self._tmp_db, Path(str(self._tmp_db) + "-wal"), Path(str(self._tmp_db) + "-shm")):
                if p.exists():
                    p.unlink()


def _auth_from(settings: Settings):
    """OAuth resource-server wiring when an issuer and a resource URL are configured (HTTP mode).
    Over stdio nothing is configured and STDIO_SCOPES apply."""
    if not (settings.auth_issuer and settings.auth_resource_url):
        return None, None
    from .auth import JwtVerifier

    verifier = JwtVerifier(issuer=settings.auth_issuer, audience=settings.auth_resource_url,
                           hs256_secret=settings.auth_hs256_secret, public_key_pem=settings.auth_public_key_pem,
                           jwks_url=settings.auth_jwks_url)
    auth = AuthSettings(issuer_url=settings.auth_issuer, resource_server_url=settings.auth_resource_url,
                        required_scopes=[SCOPE_READ], validate_token_resource=True)
    return verifier, auth


def _open_store(settings: Settings, app_ref: list) -> Store:
    if settings.mode == "synthetic":
        path = settings.synthetic_db
        tmp = None
        if path is None:
            fixture = FIXTURES_DIR / "synthetic.db"
            if not fixture.exists():
                from .synth import build_synthetic
                build_synthetic(fixture).close()
            fd, name = tempfile.mkstemp(prefix=".synthetic-", suffix=".db", dir=str(ROOT))
            os.close(fd)
            shutil.copyfile(fixture, name)
            path = tmp = Path(name)
        store = SyntheticStore(path)
        app_ref.append(tmp)
        return store
    from .pg import PostgresStore  # lazy: only operator mode needs psycopg connected
    if not settings.db_dsn:
        raise ValueError("DB_DSN is required in postgres mode")
    return PostgresStore.connect(settings.db_dsn)


def create_server(settings: Settings | None = None, store: Store | None = None,
                  stage_factory: Callable | None = None, llm_factory: Callable[[], Any] | None = None) -> App:
    settings = settings or Settings.from_env()
    tmp_ref: list = []
    if store is None:
        store = _open_store(settings, tmp_ref)
    app = App(settings, store, stage_factory, llm_factory)
    app._tmp_db = tmp_ref[0] if tmp_ref else None
    if app._tmp_db is not None:
        atexit.register(app.close)        # remove the private synthetic copy when the process ends
    register_tools(app)
    register_resources(app)
    register_prompts(app)
    return app


# ── tools (spec §4 order) ───────────────────────────────────────────────────────────


def _stage_row(s: dict) -> M.StageRow:
    return M.StageRow(stage_name=s["stage_name"], status=s["status"], started_at=s["started_at"],
                      finished_at=s["finished_at"], error=s["error"], checkpoint=s.get("checkpoint") or {})


def register_tools(app: App) -> None:
    mcp, store = app.mcp, app.store

    @mcp.tool(title="List pipeline runs", annotations=READ_ONLY)
    def list_runs(
        status: Annotated[Literal["running", "completed", "failed", "partial"] | None,
                          Field(description="Only runs in this status.")] = None,
        since: Annotated[datetime | None, Field(description="Only runs started at or after this instant (ISO 8601).")] = None,
        limit: Annotated[int, Field(ge=1, le=50)] = 20,
    ) -> list[M.RunCard]:
        """Runs newest first, one card each: window, status, stages done/total, last failed stage.

        The card's `window_end` is what a resume passes to the stage functions; `last_failed_stage`
        is where run_status will say the run resumes."""
        app.require(SCOPE_READ)
        return [M.RunCard(**r) for r in store.list_runs(status, since, limit)]

    @mcp.tool(title="Run status and stage table", annotations=READ_ONLY)
    def run_status(run_id: Annotated[int, Field(description="Run id from list_runs.")]) -> M.RunDetail:
        """Every stage_runs row of a run (status, timestamps, error, checkpoint metrics), the run's
        merged metrics and notes, and the next stage a resume would run.

        A failed or still-running stage is the next stage: a run that failed in `gate` resumes at
        `gate`. `active_job_id` names a job currently working on this run, if any."""
        app.require(SCOPE_READ)
        d = app.run(run_id)
        return M.RunDetail(
            run_id=d["run_id"], window_start=d["window_start"], window_end=d["window_end"],
            scheduled_for=d["scheduled_for"], status=d["status"], started_at=d["started_at"],
            finished_at=d["finished_at"], synthesis_model=d["synthesis_model"],
            stages=[_stage_row(s) for s in d["stages"]], metrics=d["metrics"], notes=d["notes"],
            next_stage=d["next_stage"], active_job_id=_active_job_id(app, run_id),
        )

    @mcp.tool(title="Read the failed stage's error", annotations=READ_ONLY)
    def failed_stage_error(run_id: Annotated[int, Field(description="Run id from list_runs.")]) -> M.StageError:
        """The stored error text and checkpoint of the run's failed stage, with its duration.

        Read this before deciding what to do; the pipeline writes the real exception here
        (e.g. a NullGateError from the gate's health guard)."""
        app.require(SCOPE_READ)
        d = app.run(run_id)
        failed = [s for s in d["stages"] if s["status"] == "failed"]
        if not failed:
            raise ToolError(f"Run {run_id} has no failed stage (status={d['status']}, next_stage={d['next_stage']}). "
                            "Use run_status for the stage table.")
        s = failed[-1]
        dur = (s["finished_at"] - s["started_at"]).total_seconds() if s["started_at"] and s["finished_at"] else None
        return M.StageError(run_id=run_id, stage_name=s["stage_name"], status=s["status"], error=s["error"],
                            checkpoint=s.get("checkpoint") or {}, started_at=s["started_at"],
                            finished_at=s["finished_at"], duration_s=dur)

    @mcp.tool(title="List held synthesis records", annotations=READ_ONLY)
    def list_held_records(
        run_id: Annotated[int, Field(description="Run id from list_runs.")],
        flag: Annotated[Literal["empty_figures", "zero_entities", "lang_mismatch", "pair_incomplete",
                                "retry_fallback", "invalid_json"] | None,
                        Field(description="Only records held with this synthesis_quality_flag.")] = None,
    ) -> list[M.HeldRecord]:
        """Synthesis records the validator held back (synthesis_quality_flag <> 'ok'): the flag,
        the validator's reasons and action, the headline, cluster id and article count.

        These never ship in the newsletter. A high held count against records_total is the
        signal to re-run synthesis or to look at the model."""
        app.require(SCOPE_READ)
        app.run(run_id)
        return [M.HeldRecord(**h) for h in store.held_records(run_id, flag)]

    @mcp.tool(title="List gate rejections", annotations=READ_ONLY)
    def list_gate_rejections(
        run_id: Annotated[int, Field(description="Run id from list_runs.")],
        limit: Annotated[int, Field(ge=1, le=200)] = 50,
        cursor: Annotated[str | None, Field(description="`next_cursor` from the previous page.")] = None,
    ) -> M.Page:
        """Articles the gate dropped, with the model's reason, paginated by article id.

        `unparsed_dropped` counts articles dropped because the model never produced a verdict
        (reason `gate_unparsed_dropped`); when it equals `total_dropped` the model or its backend
        was down, not the news."""
        app.require(SCOPE_READ)
        app.run(run_id)
        items, nxt = store.gate_rejections(run_id, limit, cursor)
        total, unparsed = store.gate_drop_counts(run_id)
        return M.Page(items=[M.GateRow(**i) for i in items], next_cursor=nxt, total_dropped=total, unparsed_dropped=unparsed)

    @mcp.tool(title="Registry source health", annotations=READ_ONLY)
    async def source_health(
        ctx: Context,
        run_id: Annotated[int | None, Field(description="Run id; defaults to the latest run.")] = None,
    ) -> list[M.SourceHealth]:
        """Every registry source (sources.yaml) joined with the articles it produced in the run.

        `silent` = a `live` source that produced zero rows; `held` = needs_review (never fetched);
        `retired` = covered by search discovery once that flag flips; `unknown` = the run has no
        discovery_registry stage at all, so nothing can be said about any source."""
        app.require(SCOPE_READ)
        rid = run_id if run_id is not None else store.latest_run_id()
        if rid is None:
            raise ToolError("No runs exist yet.")
        d = app.run(rid)
        has_registry_stage = any(s["stage_name"] == "discovery_registry" for s in d["stages"])
        counts = store.source_counts(rid) if has_registry_stage else {}
        sources = app.sources()
        out: list[M.SourceHealth] = []
        for i, s in enumerate(sources, 1):
            if not has_registry_stage:
                n, state = None, "unknown"
            else:
                n = counts.get(s.id, 0)
                if s.status == "needs_review":
                    state = "held"
                elif s.status == "retired":
                    state = "retired"
                elif s.status == "dead" or s.fetch_type == "dead":
                    state = "dead"
                elif n == 0:
                    state = "silent"
                else:
                    state = "healthy"
            out.append(M.SourceHealth(registry_id=s.id, institution=s.institution, tier=s.tier,
                                      registry_status=s.status, fetch_type=s.fetch_type, articles=n, state=state))
            if i % 11 == 0 or i == len(sources):
                await _progress(ctx, i, len(sources), f"{i}/{len(sources)} sources")
        return out

    @mcp.tool(title="Pipeline health (watchdog verdict)", annotations=READ_ONLY)
    def pipeline_health() -> M.HealthVerdict:
        """The watchdog's verdict for the local day plus the count of stale `running` runs.

        `ok=false` carries the watchdog's own subject and body; a stale run (still `running`
        six hours after it started) means a process died without unwinding."""
        app.require(SCOPE_READ)
        return M.HealthVerdict(**H.verdict(store))

    @mcp.tool(title="LLM cost report for a run", annotations=READ_ONLY)
    def cost_report(run_id: Annotated[int, Field(description="Run id from list_runs.")]) -> M.CostReport:
        """Tokens by stage and model from the jobs this server ran on the run, priced with
        pricing.yaml. `partial=true` when some completed stage has no usage because it ran
        outside the server (the CronJob keeps no token ledger)."""
        app.require(SCOPE_READ)
        return M.CostReport(**C.cost_report(store, app.run(run_id), app.pricing))

    @mcp.tool(title="Start a job (resume a run or run one stage)",
              annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=False,
                                          open_world_hint=True))
    async def start_job(
        ctx: Context,
        kind: Annotated[Literal["resume_run", "run_stage", "reconcile_abandoned"], Field(description="What to do.")],
        run_id: Annotated[int | None, Field(description="Run to act on (resume_run, run_stage).")] = None,
        stage: Annotated[str | None, Field(description="Stage name for run_stage.")] = None,
        budget_usd: Annotated[float | None, Field(ge=0, description="LLM budget; default DEFAULT_BUDGET_USD.")] = None,
        force: Annotated[bool, Field(description="run_stage only: allow a stage that is not the next one.")] = False,
    ) -> M.JobHandle:
        """Start long-running work and return a durable job handle immediately.

        Poll job_status(job_id) or listen on job://{job_id}. Refuses when the run does not exist,
        the stage is not next (without force), or another job is active on the run."""
        app.require(SCOPE_OPERATE)
        app.capture_loop()
        trace = _trace_from(ctx)
        note = None
        if kind == "reconcile_abandoned":
            stages: list[str] = []
            run_id = None
        else:
            if run_id is None:
                raise ToolError(f"{kind} needs run_id. Call list_runs to pick one.")
            d = app.run(run_id)
            if kind == "run_stage":
                stages = [_validate_stage(app, d, stage, force)]
            else:
                stages, note = _plan_for_resume(app, d)
                if not stages:
                    raise ToolError(f"Run {run_id} has nothing to resume: every stage is completed "
                                    f"(status={d['status']}). Use run_stage with force=true to re-run one stage.")
        try:
            job = app.registry.start(kind, run_id, stages, budget_usd, app.principal(), trace, note=note)
        except JobConflict as e:
            raise ToolError(str(e))
        return M.JobHandle(job_id=job["job_id"], status=job["status"], poll_after_s=app.registry.poll_after_s,
                           stages_planned=stages, run_id=run_id, note=note)

    @mcp.tool(title="Job status", annotations=READ_ONLY)
    def job_status(job_id: Annotated[str, Field(description="Job id from start_job.")]) -> M.Job:
        """The job: status (working | input_required | completed | failed | cancelled), current
        stage and progress, usage so far, the budget question when input is required, and the
        result or error when terminal."""
        app.require(SCOPE_READ)
        return _job_model(app, _job(app, job_id))

    @mcp.tool(title="Answer a job's budget question",
              annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True,
                                          open_world_hint=False))
    def answer_job(
        job_id: Annotated[str, Field(description="Job id from start_job.")],
        approve: Annotated[bool, Field(description="True to continue with the new budget, False to abort the stage.")],
        new_budget_usd: Annotated[float | None, Field(ge=0, description="New ceiling; required when approving above the estimate.")] = None,
    ) -> M.Answered:
        """Supply the decision an `input_required` job is waiting for. Approving raises the
        budget and resumes; aborting fails the stage cleanly with fail_stage."""
        app.require(SCOPE_OPERATE)
        app.capture_loop()
        _job(app, job_id)
        try:
            row = app.registry.answer(job_id, approve, new_budget_usd)
        except NotWaiting as e:
            raise ToolError(str(e))
        if approve:
            msg = f"approved; budget is now {row['budget_usd']:.4f} USD and the job is {row['status']}"
        else:
            msg = f"aborted at the budget gate; the stage is marked failed and the job is {row['status']}"
        return M.Answered(job_id=job_id, status=row["status"], accepted=True, budget_usd=float(row["budget_usd"]), message=msg)

    @mcp.tool(title="Cancel a job",
              annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True,
                                          open_world_hint=False))
    def cancel_job(job_id: Annotated[str, Field(description="Job id from start_job.")]) -> M.Cancelled:
        """Cooperative cancel: the runner stops between stages and between LLM batches; the
        running batch finishes; the stage is marked failed with error='cancelled via MCP'."""
        app.require(SCOPE_OPERATE)
        app.capture_loop()
        before = _job(app, job_id)
        if before["status"] in TERMINAL:
            return M.Cancelled(job_id=job_id, status=before["status"],
                               message=f"job already {before['status']}; nothing to cancel")
        row = app.registry.cancel(job_id)
        return M.Cancelled(job_id=job_id, status=row["status"],
                           message="cancel requested; the runner stops at the next stage or LLM-batch boundary"
                           if row["status"] not in TERMINAL else f"job {row['status']}")

    @mcp.tool(title="Preview a resume (dry run)", annotations=READ_ONLY)
    async def preview_resume(ctx: Context, run_id: Annotated[int, Field(description="Run id from list_runs.")]) -> M.ResumePreview:
        """What start_job(resume_run) would do: the remaining stages in order, the original
        window they receive, whether `send` is withheld, and the LLM cost estimate per stage
        against the default budget."""
        app.require(SCOPE_READ)
        d = app.run(run_id)
        plan = plan_resume(d, allow_send=_send_allowed(app))
        estimates = []
        llm = [s for s in plan.remaining if s in LLM_STAGES]
        for i, st in enumerate(llm, 1):
            estimates.append(M.Estimate(**app.registry.estimate(st, d)))
            await _progress(ctx, i, len(llm), f"estimated {st}")
        total = round(sum(e.usd for e in estimates), 6)
        budget = app.settings.default_budget_usd
        first_over = next((e for e in estimates if e.usd > budget), None)
        if plan.nothing_to_resume:
            msg = f"Run {run_id} has nothing to resume: every stage is completed (status={d['status']})."
        else:
            msg = (f"start_job(resume_run, {run_id}) would run {', '.join(plan.remaining)} with the original window "
                   f"{plan.window_start:%Y-%m-%d %H:%M}Z -> {plan.window_end:%Y-%m-%d %H:%M}Z; estimated "
                   f"{total:.4f} USD against a default budget of {budget:.2f} USD")
            if first_over:
                msg += f"; stage {first_over.stage} alone ({first_over.usd:.4f} USD) exceeds it, so the job would pause for approval"
            if plan.send_withheld:
                msg += "; send is withheld (ALLOW_SEND unset or pipeline:send missing), the run will stop after render"
            msg += "."
        return M.ResumePreview(
            run_id=run_id, window_start=plan.window_start, window_end=plan.window_end, remaining=plan.remaining,
            skipped_completed=plan.skipped_completed, send_withheld=plan.send_withheld, estimates=estimates,
            estimated_usd=total, default_budget_usd=budget, would_require_approval=first_over is not None, message=msg,
        )


def _active_job_id(app: App, run_id: int) -> str | None:
    return app.registry.active_for_run(run_id)


def _job(app: App, job_id: str) -> dict:
    try:
        return app.registry.get(job_id)
    except UnknownJob as e:
        raise ToolError(str(e))


def _job_model(app: App, row: dict) -> M.Job:
    terminal = row["status"] in TERMINAL
    ttl = None
    if terminal and row.get("ttl_expires_at"):
        ttl = max(0, int((row["ttl_expires_at"] - datetime.now(row["ttl_expires_at"].tzinfo)).total_seconds()))
    result = row.get("result") or None
    return M.Job(
        job_id=row["job_id"], kind=row["kind"], run_id=row["run_id"], status=row["status"], stage=row.get("stage"),
        stages_planned=row.get("stages_planned") or [],
        stages_completed=[k for k in (result or {}) if k in CANONICAL_STAGES],
        progress=M.Progress(**row["progress"]) if row.get("progress") else None,
        poll_after_s=0.0 if terminal else app.registry.poll_after_s, ttl_s=ttl,
        started_at=row["created_at"], updated_at=row["updated_at"], finished_at=row.get("finished_at"),
        budget_usd=float(row.get("budget_usd") or 0.0), usage=row.get("usage") or {},
        input_request=M.InputRequest(**row["input_request"]) if row.get("input_request") else None,
        result=result, error=row.get("error"), principal=row.get("principal"), trace=row.get("trace") or {},
    )


def _trace_from(ctx: Context) -> dict[str, Any]:
    """traceparent / tracestate / baggage from the request's _meta, if the host sent them."""
    try:
        meta = ctx.request_context.meta or {}
    except Exception:  # noqa: BLE001 — no request context (direct call)
        return {}
    return {k: meta[k] for k in ("traceparent", "tracestate", "baggage") if k in meta}


def _validate_stage(app: App, d: dict, stage: str | None, force: bool) -> str:
    if not stage:
        raise ToolError(f"run_stage needs `stage`; the next stage of run {d['run_id']} is {d['next_stage']!r}.")
    known = set(CANONICAL_STAGES)
    if stage not in known:
        raise ToolError(f"Unknown stage {stage!r}. Stages are: {', '.join(CANONICAL_STAGES)}.")
    if stage != d["next_stage"] and not force:
        raise ToolError(f"Stage {stage!r} is not the next stage of run {d['run_id']} (next is {d['next_stage']!r}). "
                        "Pass force=true to run it anyway.")
    if stage == "send":
        _send_guard(app)
    return stage


def _send_guard(app: App) -> None:
    if not app.settings.allow_send:
        raise ToolError("The send stage is disabled on this server (ALLOW_SEND is not 1). Nobody mails the "
                        "newsletter from a chat by accident; the CronJob sends.")
    if SCOPE_SEND not in app.scopes():
        raise ToolError(f"The send stage requires the {SCOPE_SEND} scope; the current credentials carry "
                        f"{sorted(app.scopes())}.")


def _plan_for_resume(app: App, d: dict) -> tuple[list[str], str | None]:
    """Stages after the last completed one, in order; `send` only when the guard passes."""
    plan = plan_resume(d, allow_send=_send_allowed(app))
    note = None
    if plan.send_withheld:
        note = ("send withheld: the run will stop after render (ALLOW_SEND unset or pipeline:send missing); "
                "run_status will say so")
    return plan.remaining, note


def _send_allowed(app: App) -> bool:
    return bool(app.settings.allow_send) and SCOPE_SEND in app.scopes()


async def _progress(ctx: Context, done: int, total: int, message: str) -> None:
    try:
        await ctx.report_progress(done, total, message)
    except Exception:  # noqa: BLE001 — progress is best-effort; a host without a token gets none
        log.debug("progress report skipped", exc_info=True)


# ── resources (spec §5) ────────────────────────────────────────────────────────────


def _json(model_or_dict: Any) -> str:
    if hasattr(model_or_dict, "model_dump"):
        return model_or_dict.model_dump_json(indent=2)
    return json.dumps(model_or_dict, indent=2, default=str)


def register_resources(app: App) -> None:
    mcp, store = app.mcp, app.store

    def _run_or_404(run_id: str) -> dict:
        try:
            rid = int(run_id)
        except ValueError:
            raise ResourceNotFoundError(f"run://{run_id}")
        d = store.run_detail(rid)
        if d is None:
            raise ResourceNotFoundError(f"run://{run_id}")
        return d

    @mcp.resource("run://{run_id}", mime_type="application/json", title="Run detail")
    def run_resource(run_id: str) -> str:
        """The RunDetail document: stage table, metrics, notes, next stage."""
        d = _run_or_404(run_id)
        detail = M.RunDetail(
            run_id=d["run_id"], window_start=d["window_start"], window_end=d["window_end"],
            scheduled_for=d["scheduled_for"], status=d["status"], started_at=d["started_at"],
            finished_at=d["finished_at"], synthesis_model=d["synthesis_model"],
            stages=[_stage_row(s) for s in d["stages"]], metrics=d["metrics"], notes=d["notes"],
            next_stage=d["next_stage"], active_job_id=_active_job_id(app, d["run_id"]),
        )
        return _json(detail)

    @mcp.resource("run://{run_id}/held", mime_type="application/json", title="Held records")
    def held_resource(run_id: str) -> str:
        """Held synthesis records of the run, as JSON."""
        d = _run_or_404(run_id)
        return _json([M.HeldRecord(**h).model_dump() for h in store.held_records(d["run_id"], None)])

    @mcp.resource("run://{run_id}/newsletter/{bucket}", mime_type="text/html", title="Newsletter email HTML")
    def newsletter_resource(run_id: str, bucket: str) -> str:
        """newsletters.email_html for one bucket of the run."""
        d = _run_or_404(run_id)
        for n in store.newsletters(d["run_id"]):
            if n["bucket"] == bucket:
                return n["email_html"] or ""
        raise ResourceNotFoundError(f"run://{run_id}/newsletter/{bucket}")

    @mcp.resource("run://{run_id}/archive/{bucket}", mime_type="text/html", title="Archive HTML")
    def archive_resource(run_id: str, bucket: str) -> str:
        """The archive file at newsletters.archive_html_path when readable from this process;
        otherwise a text saying where it lives."""
        d = _run_or_404(run_id)
        for n in store.newsletters(d["run_id"]):
            if n["bucket"] == bucket:
                path = n.get("archive_html_path")
                if path and Path(path).is_file():
                    return Path(path).read_text(encoding="utf-8")
                raise ResourceError(f"archive for run {run_id}/{bucket} is not readable from this process; it lives at "
                                    f"{path or '(no path recorded)'} on the pipeline's archive volume (PVC geoecon-archive)")
        raise ResourceNotFoundError(f"run://{run_id}/archive/{bucket}")

    @mcp.resource("job://{job_id}", mime_type="application/json", title="Job")
    def job_resource(job_id: str) -> str:
        """The Job document (same as job_status)."""
        try:
            row = app.registry.get(job_id)
        except UnknownJob as e:
            raise ResourceNotFoundError(f"job://{job_id}: {e}")
        return _json(_job_model(app, row))

    @mcp.resource("geoecon://sources", mime_type="application/json", title="Source registry")
    def sources_resource() -> str:
        """The registry with per-source status, tier, fetch type and bucket."""
        return _json([{"id": s.id, "institution": s.institution, "tier": s.tier, "status": s.status,
                       "fetch_type": s.fetch_type, "bucket": s.bucket, "country": s.country, "lang": s.lang,
                       "curated": s.curated} for s in app.sources()])

    @mcp.resource("geoecon://pricing", mime_type="application/x-yaml", title="LLM pricing table")
    def pricing_resource() -> str:
        """pricing.yaml as shipped with this server."""
        return Path(app.settings.pricing_path).read_text(encoding="utf-8")

    @mcp.resource("geoecon://incidents/template", mime_type="text/markdown", title="Incident report template")
    def incident_template() -> str:
        """The house incident-report skeleton. Fill every placeholder; keep the section order."""
        return I.TEMPLATE


# ── prompts (spec §5) ──────────────────────────────────────────────────────────────


def register_prompts(app: App) -> None:
    mcp = app.mcp

    @mcp.prompt(title="Write an incident report")
    def write_incident_report(
        run_id: Annotated[int, Field(description="The run the incident is about, e.g. the failed run from list_runs.")],
    ) -> str:
        """Produce the house-format incident report (status line, facts table, root cause, fixes P0-P3,
        tests, follow-up) from the tools' numbers; never from memory."""
        app.run(run_id)
        return I.incident_prompt(run_id)

    @mcp.prompt(title="Morning check")
    def morning_check() -> str:
        """The operator's five-line checklist: health, latest run, silent sources, held records, one recommendation."""
        return I.morning_check_prompt()


# ── entry points ───────────────────────────────────────────────────────────────────


def __getattr__(name: str):
    """`mcp dev geoecon_pipeline_mcp/server.py` looks for a module attribute named `mcp`.
    Build it lazily so importing this module (tests) never opens a database."""
    if name in ("mcp", "server"):
        return create_server().mcp
    raise AttributeError(name)


def main(argv: list[str] | None = None) -> None:
    import argparse

    p = argparse.ArgumentParser(prog="geoecon-pipeline-mcp")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--stdio", action="store_true", help="serve over stdio (default)")
    g.add_argument("--http", action="store_true", help="serve Streamable HTTP")
    p.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    app = create_server()
    try:
        if args.http:
            app.mcp.run("streamable-http", host=args.host, port=args.port, stateless_http=True)
        else:
            app.mcp.run("stdio")
    except KeyboardInterrupt:
        pass
    finally:
        app.close()


if __name__ == "__main__":
    main()

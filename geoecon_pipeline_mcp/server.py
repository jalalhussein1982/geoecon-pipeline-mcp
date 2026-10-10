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

import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field

from mcp.server import MCPServer
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.subscriptions import InMemorySubscriptionBus
from mcp.types import ToolAnnotations

from . import cost as C
from . import health as H
from . import models as M
from .store import LLM_STAGES, Store, SyntheticStore
from .synth import FIXTURES_DIR, SOURCES_PATH

log = logging.getLogger("geoecon_pipeline_mcp.server")

ROOT = Path(__file__).resolve().parent.parent
SCOPE_READ, SCOPE_OPERATE, SCOPE_SEND = "pipeline:read", "pipeline:operate", "pipeline:send"
READ_ONLY = ToolAnnotations(read_only_hint=True)
HELD_FLAGS = ("empty_figures", "zero_entities", "lang_mismatch", "pair_incomplete", "retry_fallback", "invalid_json")

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

    def __init__(self, settings: Settings, store: Store):
        self.settings = settings
        self.store = store
        self.bus = InMemorySubscriptionBus()
        self.mcp: MCPServer = MCPServer(
            "geoecon-pipeline", instructions=INSTRUCTIONS, subscriptions=self.bus,
        )
        self._sources_cache: list | None = None
        self._tmp_db: Path | None = None
        self.pricing = C.Pricing.load(settings.pricing_path)
        self.stage_models = {"gate": settings.gate_model, "cluster": settings.gate_model,
                             "synthesis": settings.synthesis_model, "summary": settings.synthesis_model}

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
        close = getattr(self.store, "close", None)
        if close:
            close()
        if self._tmp_db and self._tmp_db.exists():
            for p in (self._tmp_db, Path(str(self._tmp_db) + "-wal"), Path(str(self._tmp_db) + "-shm")):
                if p.exists():
                    p.unlink()


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


def create_server(settings: Settings | None = None, store: Store | None = None) -> App:
    settings = settings or Settings.from_env()
    tmp_ref: list = []
    if store is None:
        store = _open_store(settings, tmp_ref)
    app = App(settings, store)
    app._tmp_db = tmp_ref[0] if tmp_ref else None
    register_tools(app)
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
    def start_job(
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
        raise ToolError("not implemented")

    @mcp.tool(title="Job status", annotations=READ_ONLY)
    def job_status(job_id: Annotated[str, Field(description="Job id from start_job.")]) -> M.Job:
        """The job: status (working | input_required | completed | failed | cancelled), current
        stage and progress, usage so far, the budget question when input is required, and the
        result or error when terminal."""
        app.require(SCOPE_READ)
        raise ToolError("not implemented")

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
        raise ToolError("not implemented")

    @mcp.tool(title="Cancel a job",
              annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True,
                                          open_world_hint=False))
    def cancel_job(job_id: Annotated[str, Field(description="Job id from start_job.")]) -> M.Cancelled:
        """Cooperative cancel: the runner stops between stages and between LLM batches; the
        running batch finishes; the stage is marked failed with error='cancelled via MCP'."""
        app.require(SCOPE_OPERATE)
        raise ToolError("not implemented")

    @mcp.tool(title="Preview a resume (dry run)", annotations=READ_ONLY)
    async def preview_resume(ctx: Context, run_id: Annotated[int, Field(description="Run id from list_runs.")]) -> M.ResumePreview:
        """What start_job(resume_run) would do: the remaining stages in order, the original
        window they receive, whether `send` is withheld, and the LLM cost estimate per stage
        against the default budget."""
        app.require(SCOPE_READ)
        raise ToolError("not implemented")


def _active_job_id(app: App, run_id: int) -> str | None:
    for j in app.store.jobs_for_run(run_id):
        if j["status"] in ("working", "input_required"):
            return j["job_id"]
    return None


async def _progress(ctx: Context, done: int, total: int, message: str) -> None:
    try:
        await ctx.report_progress(done, total, message)
    except Exception:  # noqa: BLE001 — progress is best-effort; a host without a token gets none
        log.debug("progress report skipped", exc_info=True)


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
    if args.http:
        app.mcp.run("streamable-http", host=args.host, port=args.port, stateless_http=True)
    else:
        app.mcp.run("stdio")


if __name__ == "__main__":
    main()

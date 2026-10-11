"""Resume planning and the pipeline's stage functions, wrapped for the job runner.

Why this module exists: the orchestrator's own `find_resumable_run` never matches in
production because it keys on `window_end = now`, which is fresh on every start. The resume
the orchestrator meant to have is: read `completed_stages(run_id)`, call the remaining stage
functions in canonical order, and hand them the run's ORIGINAL `window_start`/`window_end`.
That is what `plan_resume` + `make_pipeline_stage_fns` do, from the outside, without touching
the pipeline.

`make_pipeline_stage_fns` mirrors `geoecon.orchestrator._default_stages` closure for closure,
with two differences: the discovery stages get the run's window (`now=run.window_end`; for GDELT
discovery in the default gkg mode, which ignores `now`, the GKG fetch is bound to
`(window_start, window_end)` directly), and render/send derive their date string from the window
instead of today. The set of names is intersected with
what `_default_stages(cfg, client)` returns at runtime, so `discovery_search` appears only
when the pipeline is configured for it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from .store import CANONICAL_STAGES, expected_stages

log = logging.getLogger("geoecon_pipeline_mcp.stages")

Report = Callable[[int, int | None, str], None]


@dataclass
class ResumePlan:
    run_id: int
    window_start: datetime
    window_end: datetime
    remaining: list[str] = field(default_factory=list)
    skipped_completed: list[str] = field(default_factory=list)
    send_withheld: bool = False

    @property
    def nothing_to_resume(self) -> bool:
        return not self.remaining and not self.send_withheld


def plan_resume(run: dict, *, available: Iterable[str] | None = None, allow_send: bool = False) -> ResumePlan:
    """Stages a resume would run, in order, starting at the first stage that is not completed.

    A `failed` or `running` stage IS included (Review Focus 1: a run that failed in `gate`
    resumes at `gate`, not after it). `send` is dropped unless `allow_send`; the plan says so.
    `available` restricts the order to the stage names the pipeline build offers."""
    completed = [s["stage_name"] for s in run["stages"] if s["status"] == "completed"]
    plan = ResumePlan(run["run_id"], run["window_start"], run["window_end"], skipped_completed=completed)
    nxt = run.get("next_stage")
    if nxt is None:
        return plan
    order = expected_stages(run["stages"])
    if available is not None:
        avail = set(available)
        order = [s for s in order if s in avail or s in {x["stage_name"] for x in run["stages"]}]
    if nxt not in order:
        return plan
    remaining = [s for s in order[order.index(nxt):] if s not in completed]
    if "send" in remaining and not allow_send:
        remaining.remove("send")
        plan.send_withheld = True
    plan.remaining = remaining
    return plan


def _default_modules() -> SimpleNamespace:
    from geoecon import regions
    from geoecon.stages import (cluster, discovery, discovery_registry, discovery_search, extract, gate, render,
                                send, summary, synth)
    return SimpleNamespace(discovery=discovery, discovery_registry=discovery_registry, discovery_search=discovery_search,
                           extract=extract, gate=gate, cluster=cluster, synth=synth, summary=summary, render=render,
                           send=send, regions=regions)


def default_stage_names(cfg: Any, client: Any) -> list[str]:
    from geoecon.orchestrator import _default_stages
    return [name for name, _ in _default_stages(cfg, client)]


def make_pipeline_stage_fns(cfg: Any, repo: Any, run: dict, client: Any, *, modules: Any | None = None,
                            names: Iterable[str] | None = None) -> dict[str, Callable[[dict, Report, Any], dict]]:
    """Stage functions with the runner's signature `fn(run, report, cancel) -> dict`, calling the
    pipeline's stage modules with the run's original window. Progress comes from the log
    counter and the DB counter; cancel is observed by the counting LLM client."""
    m = modules or _default_modules()
    allowed = set(names) if names is not None else set(default_stage_names(cfg, client))
    window_start: datetime = run["window_start"]
    window_end: datetime = run["window_end"]
    date_str = window_end.strftime("%Y-%m-%d")
    rid = run["run_id"]

    def discovery(run_, report, cancel):
        # Upstream discovery.run honours now= only on the docapi path; in the default gkg mode it
        # takes datetime.now() and hands that window to fetch(window_start, window_end). Binding
        # fetch= to the run's own window is the only way to give gkg discovery the original window.
        kwargs: dict[str, Any] = {"now": window_end}
        default_fetch = getattr(m.discovery, "_default_fetch", None)
        if default_fetch is not None:
            kwargs["fetch"] = lambda _ws, _we: default_fetch(window_start, window_end)
        return m.discovery.run(cfg, repo, rid, **kwargs)

    def discovery_registry(run_, report, cancel):
        return m.discovery_registry.run(cfg, repo, rid, now=window_end)

    def discovery_search(run_, report, cancel):
        return m.discovery_search.run(cfg, repo, rid, now=window_end)

    def extraction(run_, report, cancel):
        return m.extract.run(cfg, repo, rid)

    def gate(run_, report, cancel):
        return m.gate.run(cfg, repo, rid, client)

    def cluster(run_, report, cancel):
        return m.cluster.run(cfg, repo, rid, client)

    def synthesis(run_, report, cancel):
        return m.synth.run(cfg, repo, rid, client)

    def summary(run_, report, cancel):
        return m.summary.run(cfg, repo, rid, client)

    def render(run_, report, cancel):
        # stats come from discovery's in-memory result in the orchestrator; on a resume that
        # result is gone, and render.run accepts stats=None
        out = m.render.run(cfg, repo, rid, date_str, stats=None)
        return {"record_count": out["record_count"], "newsletters": out["newsletters"]}

    def send(run_, report, cancel):
        if getattr(cfg, "dry_run", False):
            log.info("dry-run: skipping send")
            return {"send_status": "skipped"}
        newsletters = repo.newsletters_for_run(rid)
        if not newsletters:
            return {"send_status": "skipped_empty"}

        def subject_for(bucket):
            display = m.regions.BUCKET_DISPLAY.get(bucket, bucket or "")
            return f"Geoeconomic Brief — {display} — {date_str}"

        out = m.send.send_pending(cfg, repo, newsletters, subject_for)
        if out["all_sent"]:
            repo.mark_emitted(rid)
            return {"send_status": "sent", "sent": out["results"]}
        raise RuntimeError(f"send: some regions failed to send: {out['results']}")

    all_fns = {
        "discovery": discovery, "discovery_registry": discovery_registry, "discovery_search": discovery_search,
        "extraction": extraction, "gate": gate, "cluster": cluster, "synthesis": synthesis, "summary": summary,
        "render": render, "send": send,
    }
    return {name: fn for name, fn in all_fns.items() if name in allowed}


__all__ = ["CANONICAL_STAGES", "ResumePlan", "plan_resume", "make_pipeline_stage_fns", "default_stage_names"]

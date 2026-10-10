"""Durable job handles: the `mcp_jobs` registry and one worker thread per job.

A job is a row first and a thread second. `start_job` inserts the row (`status=working`)
and returns its id before the thread has done anything; `job_status` reads the row; a host
that reconnects, or a different host, polls the same id. The thread updates the row at
every transition and asks the server to publish `job://{id}` and `run://{run_id}` events.

Status vocabulary is the MCP tasks extension's, exactly:
    working | input_required | completed | failed | cancelled

Cooperation points, where a runner notices a cancel or a budget decision:
  * between stages (always),
  * between LLM calls inside a stage (through the counting client, cost.py),
  * while waiting for a budget answer.

A server restart cannot resume a thread, so `recover_after_restart` marks every row still
`working`/`input_required` as `failed` with error="server restarted". The pipeline's own
stage row stays `running` or `failed`, and pipeline_health reports the stale run.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from . import cost as C
from .progress import LogCounter
from .store import JOB_TTL, LLM_STAGES, STALE_AFTER, Store
from .synth import StageCancelled

log = logging.getLogger("geoecon_pipeline_mcp.jobs")

Report = Callable[[int, int | None, str], None]
StageFn = Callable[[dict, Report, threading.Event], dict]
StageFactory = Callable[[Any], Mapping[str, StageFn]]
Notify = Callable[[str], None]

TERMINAL = frozenset({"completed", "failed", "cancelled"})
BUDGET_SCHEMA = {
    "type": "object",
    "properties": {
        "approve": {"type": "boolean", "description": "true: continue with new_budget_usd; false: abort the stage"},
        "new_budget_usd": {"type": "number", "minimum": 0, "description": "new ceiling in USD"},
    },
    "required": ["approve"],
}


class UnknownJob(KeyError):
    pass


class JobConflict(ValueError):
    pass


class NotWaiting(ValueError):
    pass


class JobCancelled(RuntimeError):
    """Raised inside a stage when the cancel flag is observed (by the counting client or a fake stage)."""


class BudgetAborted(RuntimeError):
    """Raised inside a stage when the operator aborts at the budget gate."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


class JobRegistry:
    def __init__(
        self,
        store: Store,
        stage_factory: StageFactory,
        *,
        pricing: C.Pricing,
        stage_models: Mapping[str, str],
        llm_factory: Callable[[], Any],
        notify: Notify | None = None,
        default_budget_usd: float = 2.0,
        poll_after_s: float = 2.0,
        llm_stages: frozenset[str] = LLM_STAGES,
        db_progress: bool = False,
    ):
        self.store = store
        self.stage_factory = stage_factory
        self.pricing = pricing
        self.stage_models = dict(stage_models)
        self.llm_factory = llm_factory
        self._notify = notify
        self.default_budget_usd = default_budget_usd
        self.poll_after_s = poll_after_s
        self.llm_stages = llm_stages
        self.db_progress = db_progress
        self._runners: dict[str, JobRunner] = {}
        self._lock = threading.Lock()
        self.recovered = self.recover_after_restart()

    # -- hooks --------------------------------------------------------------------------

    def notify(self, uri: str) -> None:
        if self._notify is None:
            return
        try:
            self._notify(uri)
        except Exception:  # noqa: BLE001 — a notification must never take a job down
            log.debug("notify(%s) failed", uri, exc_info=True)

    def set_notifier(self, notify: Notify | None) -> None:
        self._notify = notify

    def estimate(self, stage: str, run: dict) -> dict:
        return C.estimate(stage, run, self.store, self.pricing, self.stage_models)

    # -- lifecycle ----------------------------------------------------------------------

    def recover_after_restart(self) -> int:
        n = 0
        for j in self.store.jobs_working():
            self.store.job_update(j["job_id"], status="failed", error="server restarted",
                                  finished_at=_now(), ttl_expires_at=_now() + JOB_TTL, input_request=None)
            n += 1
        if n:
            log.warning("marked %d job(s) failed after restart", n)
        return n

    def active_for_run(self, run_id: int) -> str | None:
        for j in self.store.jobs_for_run(run_id):
            if j["status"] not in TERMINAL:
                return j["job_id"]
        return None

    def start(self, kind: str, run_id: int | None, stages: list[str], budget_usd: float | None,
              principal: str | None, trace: dict | None, *, note: str | None = None) -> dict:
        with self._lock:
            if run_id is not None:
                active = self.active_for_run(run_id)
                if active:
                    raise JobConflict(f"job {active} is already active on run {run_id}; wait for it or cancel_job({active!r})")
            now = _now()
            job = {
                "job_id": str(uuid.uuid4()), "kind": kind, "run_id": run_id, "stages_planned": list(stages),
                "status": "working", "stage": None, "progress": None, "input_request": None, "input_response": None,
                "budget_usd": float(self.default_budget_usd if budget_usd is None else budget_usd),
                "usage": C.empty_usage(), "result": None, "error": None, "principal": principal,
                "trace": dict(trace or {}), "created_at": now, "updated_at": now, "finished_at": None,
                "ttl_expires_at": None,
            }
            if note:
                job["trace"] = {**job["trace"], "note": note}
            self.store.job_insert(job)
            runner = JobRunner(self, job)
            self._runners[job["job_id"]] = runner
        runner.start()
        self.notify(f"job://{job['job_id']}")
        return job

    def get(self, job_id: str) -> dict:
        row = self.store.job_get(job_id)
        if row is None:
            raise UnknownJob(f"No job {job_id!r}. Job ids come from start_job; list_runs shows the runs.")
        exp = row.get("ttl_expires_at")
        if exp is not None and exp < _now():
            raise UnknownJob(f"Job {job_id!r} expired (terminal rows are kept {JOB_TTL.days} days). "
                             "Start a new one with start_job; list_runs shows the runs.")
        return row

    def cancel(self, job_id: str) -> dict:
        row = self.get(job_id)
        if row["status"] in TERMINAL:
            return row
        runner = self._runners.get(job_id)
        if runner is None:
            # row says working but no thread owns it (should only happen after a crash)
            self.store.job_update(job_id, status="failed", error="server restarted", finished_at=_now(),
                                  ttl_expires_at=_now() + JOB_TTL)
            return self.get(job_id)
        runner.request_cancel()
        return self.get(job_id)

    def answer(self, job_id: str, approve: bool, new_budget_usd: float | None) -> dict:
        row = self.get(job_id)
        if row["status"] == "cancelled":
            raise NotWaiting("job is cancelled")
        if row["status"] in TERMINAL:
            raise NotWaiting(f"job is {row['status']}; nothing is waiting for an answer")
        if row["status"] != "input_required":
            raise NotWaiting(f"job is {row['status']} and not awaiting input; answer_job applies only to input_required jobs")
        runner = self._runners.get(job_id)
        if runner is None:
            raise NotWaiting("job is no longer running in this process (server restarted)")
        runner.deliver_answer(approve, new_budget_usd)
        return self.get(job_id)

    def shutdown(self, timeout: float = 5.0) -> None:
        for r in list(self._runners.values()):
            r.request_cancel()
        for r in list(self._runners.values()):
            r.join(timeout)


class JobRunner(threading.Thread):
    """One job's worker. Owns the cancel flag and the budget decision handshake."""

    def __init__(self, registry: JobRegistry, job: dict):
        super().__init__(name=f"job-{job['job_id'][:8]}", daemon=True)
        self.reg = registry
        self.store = registry.store
        self.job_id = job["job_id"]
        self.kind = job["kind"]
        self.run_id = job["run_id"]
        self.stages = list(job["stages_planned"])
        self.budget = C.Budget(limit_usd=job["budget_usd"])
        self.cancel = threading.Event()
        self._decision = threading.Event()
        self._answer: tuple[bool, float | None] | None = None
        self.result: dict[str, Any] = {}
        self.usage = C.empty_usage()
        self._stage: str | None = None

    # -- control from the registry ---------------------------------------------------------

    def request_cancel(self) -> None:
        self.cancel.set()
        self._decision.set()          # wake a budget wait; it will see the cancel flag

    def deliver_answer(self, approve: bool, new_budget_usd: float | None) -> None:
        self._answer = (approve, new_budget_usd)
        self._decision.set()

    # -- helpers -----------------------------------------------------------------------------

    def _update(self, **fields: Any) -> None:
        self.store.job_update(self.job_id, **fields)

    def _set_status(self, status: str, **fields: Any) -> None:
        if status in TERMINAL:
            now = _now()
            fields.setdefault("finished_at", now)
            fields.setdefault("ttl_expires_at", now + JOB_TTL)
            fields.setdefault("input_request", None)
        self._update(status=status, **fields)
        self.reg.notify(f"job://{self.job_id}")

    def _report(self, done: int, total: int | None, message: str) -> None:
        self._update(progress={"done": int(done), "total": total, "message": message})

    def _run_dict(self) -> dict:
        d = self.store.run_detail(self.run_id) if self.run_id is not None else None
        if d is None:
            raise RuntimeError(f"run {self.run_id} disappeared")
        return d

    # -- budget gate -------------------------------------------------------------------------

    def _ask_budget(self, message: str, est_usd: float) -> None:
        """Block the worker until answer_job or cancel_job. Raises BudgetAborted / JobCancelled."""
        req = {"key": "budget", "message": message, "requested_schema": BUDGET_SCHEMA}
        self._decision.clear()
        self._answer = None
        self._set_status("input_required", input_request=req)
        self._decision.wait()
        if self.cancel.is_set() and self._answer is None:
            raise JobCancelled("cancelled via MCP")
        approve, new_budget = self._answer or (False, None)
        self._update(input_response={"approve": approve, "new_budget_usd": new_budget})
        if not approve:
            raise BudgetAborted("aborted at budget gate")
        new_limit = new_budget if new_budget is not None else max(self.budget.limit_usd, est_usd)
        if new_limit < est_usd:
            # approving without enough budget: take the estimate as the new ceiling, say so
            new_limit = est_usd
        self.budget.limit_usd = float(new_limit)
        self._update(budget_usd=self.budget.limit_usd, input_request=None)
        self._set_status("working")

    def _on_budget_exceeded(self, spent: float) -> None:
        """Called by the counting client between LLM calls when spend passed the ceiling."""
        if self.cancel.is_set():
            raise JobCancelled("cancelled via MCP")
        msg = (f"Stage {self._stage}: spent {spent:.4f} USD so far, above the approved ceiling of "
               f"{self.budget.limit_usd:.4f} USD. Approve a higher new_budget_usd to continue or abort.")
        self._ask_budget(msg, spent)

    def _gate_estimate(self, stage: str, run: dict) -> None:
        est = self.reg.estimate(stage, run)
        if est["usd"] > self.budget.limit_usd:
            msg = (f"Stage {stage} is estimated at {est['usd']:.4f} USD ({est['tokens_in']} in / {est['tokens_out']} out "
                   f"tokens on {est['model']}; {est['basis']}), above the budget of {self.budget.limit_usd:.4f} USD. "
                   f"Approve with new_budget_usd >= {est['usd']:.4f} or abort.")
            self._ask_budget(msg, est["usd"])

    # -- main ---------------------------------------------------------------------------------

    def run(self) -> None:  # noqa: C901 — the state machine is clearer in one place
        try:
            if self.kind == "reconcile_abandoned":
                self._reconcile()
                return
            run = self._run_dict()
            inner = self.reg.llm_factory()
            client = C.CountingLLMClient(inner, self.reg.pricing, self.budget, self.usage,
                                         on_exceeded=self._on_budget_exceeded, cancel=self.cancel,
                                         on_usage=lambda: self._update(usage=self.usage))
            fns = self.reg.stage_factory(client)
            counter = LogCounter(self._report, threading.get_ident())
            db_counter = None
            with counter:
                for stage in self.stages:
                    if self.cancel.is_set():
                        self._set_status("cancelled", error="cancelled via MCP")
                        return
                    fn = fns.get(stage)
                    if fn is None:
                        self._set_status("failed", error=f"stage {stage!r} is not available in this pipeline build")
                        return
                    self._stage = stage
                    client.set_stage(stage)
                    try:
                        if stage in self.reg.llm_stages:
                            self._gate_estimate(stage, run)
                    except BudgetAborted as e:
                        self.store.fail_stage(self.run_id, stage, str(e))
                        self.reg.notify(f"run://{self.run_id}")
                        self._set_status("failed", stage=stage, error=str(e))
                        return
                    except JobCancelled:
                        self._set_status("cancelled", stage=stage, error="cancelled via MCP")
                        return
                    self._update(stage=stage, progress={"done": 0, "total": None, "message": f"{stage} starting"})
                    self.store.start_stage(self.run_id, stage)
                    self.reg.notify(f"run://{self.run_id}")
                    if self.reg.db_progress:
                        from .progress import DbCounter
                        db_counter = DbCounter(self.store, self.run_id, stage, self._report)
                        db_counter.start()
                    try:
                        out = fn(run, self._report, self.cancel) or {}
                    except (JobCancelled, StageCancelled):
                        self.store.fail_stage(self.run_id, stage, "cancelled via MCP")
                        self.reg.notify(f"run://{self.run_id}")
                        self._set_status("cancelled", error="cancelled via MCP")
                        return
                    except BudgetAborted as e:
                        self.store.fail_stage(self.run_id, stage, str(e))
                        self.reg.notify(f"run://{self.run_id}")
                        self._set_status("failed", error=str(e), usage=self.usage)
                        return
                    except Exception as e:  # noqa: BLE001 — a stage exception becomes a failed job, never a crash
                        text = f"{type(e).__name__}: {e}"
                        log.exception("job %s: stage %s failed", self.job_id, stage)
                        self.store.fail_stage(self.run_id, stage, text)
                        self.store.set_run_status(self.run_id, "failed")
                        self.reg.notify(f"run://{self.run_id}")
                        self._set_status("failed", error=text, usage=self.usage)
                        return
                    finally:
                        if db_counter is not None:
                            db_counter.stop(); db_counter = None
                    self.store.complete_stage(self.run_id, stage, out)
                    self.store.merge_run_metrics(self.run_id, out if isinstance(out, dict) else {})
                    self.result[stage] = out
                    self._update(result=self.result, usage=self.usage,
                                 progress={"done": 1, "total": 1, "message": f"{stage} completed"})
                    self.reg.notify(f"run://{self.run_id}")
                    self.reg.notify(f"job://{self.job_id}")
                    run = self._run_dict()
            self._finalize_run(run)
            self._set_status("completed", result=self.result, usage=self.usage)
        except Exception as e:  # noqa: BLE001 — last line of defence; the row must end terminal
            log.exception("job %s crashed", self.job_id)
            try:
                self._set_status("failed", error=f"{type(e).__name__}: {e}")
            except Exception:  # noqa: BLE001
                pass
        finally:
            self.reg._runners.pop(self.job_id, None)

    def _finalize_run(self, run: dict) -> None:
        """After every planned stage completed: settle the run's own status."""
        if self.run_id is None:
            return
        remaining = run["next_stage"]
        note = (self.store.job_get(self.job_id) or {}).get("trace", {}).get("note")
        unsettled = {s["status"] for s in run["stages"]} & {"failed", "running"}
        if remaining is None:
            self.store.set_run_status(self.run_id, "completed")
        elif not unsettled:
            # stages done so far are all completed, more remain: the pipeline's own word is `partial`
            self.store.set_run_status(self.run_id, "partial")
            if self.kind == "resume_run" and remaining == "send":
                self.store.append_run_note(self.run_id, note or
                                           "resumed via MCP; stopped after render: send withheld (ALLOW_SEND unset "
                                           "or pipeline:send missing)")
        self.reg.notify(f"run://{self.run_id}")

    def _reconcile(self) -> None:
        now = self.store.now()                 # the fixture's clock in synthetic mode, wall clock in Postgres
        cutoff = now - STALE_AFTER
        stale = self.store.stale_running_runs(cutoff)
        note = (f"abandoned: still 'running' with no progress since before {cutoff:%Y-%m-%d %H:%M}Z; "
                f"process ended without unwinding (cause not recorded). Closed out via MCP at {now:%Y-%m-%d %H:%M}Z.")
        closed = []
        for r in stale:
            if self.cancel.is_set():
                break
            self.store.mark_run_failed(r["run_id"], note)
            closed.append(r["run_id"])
            self.reg.notify(f"run://{r['run_id']}")
            self._report(len(closed), len(stale), f"closed run {r['run_id']}")
        self.result = {"reconciled": closed, "note": note if closed else "nothing to reconcile"}
        if self.cancel.is_set() and len(closed) < len(stale):
            self._set_status("cancelled", result=self.result, error="cancelled via MCP")
        else:
            self._set_status("completed", result=self.result)

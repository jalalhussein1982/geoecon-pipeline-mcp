"""LLM cost: the price table, the pre-run estimator, and the per-run cost report.

The pipeline reads neither `prompt_tokens` nor `completion_tokens` from its LLM responses,
so it has no cost ledger. This module gives the server one: `Pricing` prices tokens,
`estimate` predicts a stage's tokens from the rows it will touch (spec §4.3), and
`cost_report` sums what the server's jobs actually spent (`mcp_jobs.usage`).

`CountingLLMClient` wraps the pipeline's `LLMClient` (same `complete(model, messages, **p)`
signature, so stage code cannot tell the difference), adds every response's token counts
to the job's usage, and enforces the budget between calls: when spend passes the ceiling it
calls back into the runner, which parks the job in `input_required` until the operator
answers. Cancel is checked at the same boundary.
"""

from __future__ import annotations

import copy
import logging
import threading
from dataclasses import dataclass
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from .store import LLM_STAGES, Store

log = logging.getLogger("geoecon_pipeline_mcp.cost")

# Estimator constants (spec §4.3). Tokens, not characters; ~4 chars per token.
GATE_BATCH_SIZE = 15
GATE_TOKENS_IN_PER_BATCH = 3000          # 15 articles x (title + themes + 400-char lead) + system prompt
GATE_TOKENS_OUT_PER_BATCH = 600          # 15 verdict objects
CLUSTER_TOKENS_IN_PER_ARTICLE = 80
CLUSTER_TOKENS_IN_PER_COUNTRY = 400      # grouping prompt per country call
CLUSTER_TOKENS_OUT_PER_ARTICLE = 25
SYNTH_TOTAL_CHARS = 30000                # Config.synth_total_chars default
SYNTH_TOKENS_OUT_PER_CLUSTER = 1200
SUMMARY_TOKENS_IN_PER_BUCKET = 1800      # region_cap (12) records x ~150 tokens
SUMMARY_TOKENS_OUT_PER_BUCKET = 500
HISTORICAL_KEEP_RATE = 0.21              # gate keep rate on full-size runs
ARTICLES_PER_CLUSTER = 2.2


class PricingError(ValueError):
    pass


@dataclass(frozen=True)
class Rate:
    input_per_m: float
    output_per_m: float


class Pricing:
    def __init__(self, models: dict[str, Rate], aliases: dict[str, str], as_of: str, note: str | None = None):
        if "default" not in models:
            raise PricingError("pricing.yaml needs a `default` model entry")
        self.models = models
        self.aliases = aliases
        self.as_of = as_of
        self.note = note

    @classmethod
    def load(cls, path: str | Path) -> "Pricing":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        if "as_of" not in raw:
            raise PricingError("pricing.yaml needs `as_of`")
        models_raw = raw.get("models")
        if not isinstance(models_raw, dict) or not models_raw:
            raise PricingError("pricing.yaml needs a non-empty `models` map")
        models: dict[str, Rate] = {}
        for name, entry in models_raw.items():
            if not isinstance(entry, dict) or {"input_per_m", "output_per_m"} - set(entry):
                raise PricingError(f"model {name!r} needs input_per_m and output_per_m")
            try:
                rate = Rate(float(entry["input_per_m"]), float(entry["output_per_m"]))
            except (TypeError, ValueError) as e:
                raise PricingError(f"model {name!r}: prices must be numbers") from e
            if rate.input_per_m < 0 or rate.output_per_m < 0:
                raise PricingError(f"model {name!r}: prices must be >= 0")
            models[name] = rate
        aliases = {str(k): str(v) for k, v in (raw.get("aliases") or {}).items()}
        for alias, target in aliases.items():
            if target not in models:
                raise PricingError(f"alias {alias!r} points at unknown model {target!r}")
        return cls(models, aliases, str(raw["as_of"]), raw.get("note"))

    def resolve(self, model: str) -> str:
        name = self.aliases.get(model, model)
        return name if name in self.models else "default"

    def rate(self, model: str) -> Rate:
        return self.models[self.resolve(model)]

    def usd(self, model: str, tokens_in: int, tokens_out: int) -> float:
        r = self.rate(model)
        return round(tokens_in / 1e6 * r.input_per_m + tokens_out / 1e6 * r.output_per_m, 6)


# ── counting proxy and budget ───────────────────────────────────────────────────────


@dataclass
class Budget:
    limit_usd: float
    spent_usd: float = 0.0

    @property
    def exceeded(self) -> bool:
        return self.spent_usd > self.limit_usd


class CountingLLMClient:
    """Drop-in for `geoecon.llm.client.LLMClient.complete`. Thread-safe: gate and synthesis fan
    LLM calls out to worker threads, so the ledger is lock-guarded."""

    def __init__(self, inner: Any, pricing: Pricing, budget: Budget, usage: dict[str, Any], *,
                 on_exceeded: Callable[[float], None] | None = None, cancel: threading.Event | None = None,
                 aborted: threading.Event | None = None, on_usage: Callable[[dict[str, Any]], None] | None = None):
        self.inner = inner
        self.pricing = pricing
        self.budget = budget
        self.usage = usage
        self.on_exceeded = on_exceeded
        self.cancel = cancel
        self.aborted = aborted
        self.on_usage = on_usage
        self.stage = "unknown"
        self.calls = 0
        self._lock = threading.Lock()
        self._gate_lock = threading.Lock()

    def set_stage(self, stage: str) -> None:
        self.stage = stage

    def __getattr__(self, name: str) -> Any:          # everything else passes through (base_url, session, ...)
        return getattr(self.inner, name)

    def complete(self, model: str, messages: list[dict], **params: Any) -> Any:
        self._before_call()
        resp = self.inner.complete(model, messages, **params)
        self._record(model, resp)
        return resp

    def _before_call(self) -> None:
        """Cooperation point: cancel wins; then the budget ceiling. Serialised so that four
        workers do not ask the operator four times."""
        with self._gate_lock:
            if self.aborted is not None and self.aborted.is_set():
                from .jobs import BudgetAborted
                raise BudgetAborted("aborted at budget gate")
            if self.cancel is not None and self.cancel.is_set():
                from .jobs import JobCancelled
                raise JobCancelled("cancelled via MCP")
            if self.budget.exceeded and self.on_exceeded is not None:
                self.on_exceeded(self.budget.spent_usd)   # blocks until answered; raises to abort

    def _record(self, model: str, resp: Any) -> None:
        pt = getattr(resp, "prompt_tokens", None) or 0
        ct = getattr(resp, "completion_tokens", None) or 0
        with self._lock:
            self.calls += 1
            call_usd = add_usage(self.usage, self.stage, model, pt, ct, self.pricing)
            self.budget.spent_usd = round(self.budget.spent_usd + call_usd, 6)
            # Snapshot under the lock: four gate workers mutate `usage` concurrently, and a
            # json.dumps of the live dict from another thread can see it change size mid-walk.
            snapshot = copy.deepcopy(self.usage) if self.on_usage is not None else None
        if snapshot is not None:
            try:
                self.on_usage(snapshot)
            except Exception:  # noqa: BLE001 — persisting usage is best-effort per call, but say so
                log.warning("could not persist LLM usage after a %s call", self.stage, exc_info=True)


def stage_models_from_config(cfg: Any) -> dict[str, str]:
    """stage -> model name as the pipeline itself will call them (Config.gate_model, dedup_model
    for the cluster grouping call, synthesis_model for synthesis and the region summaries)."""
    return {"gate": cfg.gate_model, "cluster": cfg.dedup_model, "synthesis": cfg.synthesis_model,
            "summary": cfg.synthesis_model}


# ── estimator ───────────────────────────────────────────────────────────────────────


def _projected_counts(inputs: dict[str, int], completed: set[str]) -> tuple[dict[str, int], bool]:
    """Fill in kept/countries/clusters from historical ratios for stages that have not
    completed yet. The stage table decides, not the row counts: a run that failed in `gate`
    may have verdicts on every row (all unparsable) and the resume re-judges them all."""
    c = dict(inputs)
    projected = False
    if "gate" not in completed and c.get("fetchable", 0) > 0:
        c["kept"] = round(c["fetchable"] * HISTORICAL_KEEP_RATE)
        c["countries"] = max(1, min(c["kept"], 12))
        projected = True
    if "cluster" not in completed and c.get("kept", 0) > 0:
        c["clusters"] = max(1, round(c["kept"] / ARTICLES_PER_CLUSTER))
        projected = True
    return c, projected


def estimate(stage: str, run: dict, store: Store, pricing: Pricing, models: dict[str, str]) -> dict[str, Any]:
    """Expected tokens and USD for one LLM stage of `run`. `models` maps stage -> model name."""
    if stage not in LLM_STAGES:
        return {"stage": stage, "model": "-", "tokens_in": 0, "tokens_out": 0, "usd": 0.0,
                "basis": "stage makes no LLM calls"}
    model = models.get(stage, models.get("default", "default"))
    raw = store.stage_inputs(run["run_id"])
    completed = {s["stage_name"] for s in run.get("stages", []) if s["status"] == "completed"}
    c, projected = _projected_counts(raw, completed)
    suffix = " (projected from historical ratios because the upstream stage has not completed)" if projected else ""
    if stage == "gate":
        batches = -(-c["fetchable"] // GATE_BATCH_SIZE) if c["fetchable"] else 0
        tin, tout = batches * GATE_TOKENS_IN_PER_BATCH, batches * GATE_TOKENS_OUT_PER_BATCH
        basis = (f"{c['fetchable']} non-failed articles in {batches} batches of {GATE_BATCH_SIZE} x "
                 f"{GATE_TOKENS_IN_PER_BATCH}/{GATE_TOKENS_OUT_PER_BATCH} tokens; the gate re-judges every article")
    elif stage == "cluster":
        tin = c["kept"] * CLUSTER_TOKENS_IN_PER_ARTICLE + c["countries"] * CLUSTER_TOKENS_IN_PER_COUNTRY
        tout = c["kept"] * CLUSTER_TOKENS_OUT_PER_ARTICLE
        basis = f"{c['kept']} kept articles across {c['countries']} countries, one grouping call per country{suffix}"
    elif stage == "synthesis":
        tin = c["clusters"] * (SYNTH_TOTAL_CHARS // 4)
        tout = c["clusters"] * SYNTH_TOKENS_OUT_PER_CLUSTER
        basis = f"{c['clusters']} clusters x SYNTH_TOTAL_CHARS/4 ({SYNTH_TOTAL_CHARS // 4}) in, {SYNTH_TOKENS_OUT_PER_CLUSTER} out{suffix}"
    else:  # summary
        buckets = c.get("buckets", 3)
        tin, tout = buckets * SUMMARY_TOKENS_IN_PER_BUCKET, buckets * SUMMARY_TOKENS_OUT_PER_BUCKET
        basis = f"{buckets} region buckets x {SUMMARY_TOKENS_IN_PER_BUCKET}/{SUMMARY_TOKENS_OUT_PER_BUCKET} tokens"
    return {"stage": stage, "model": model, "tokens_in": int(tin), "tokens_out": int(tout),
            "usd": pricing.usd(model, int(tin), int(tout)), "basis": basis}


# ── cost report ─────────────────────────────────────────────────────────────────────


def empty_usage() -> dict[str, Any]:
    return {"stages": {}, "usd_total": 0.0}


def add_usage(usage: dict[str, Any], stage: str, model: str, prompt_tokens: int, completion_tokens: int,
              pricing: Pricing) -> float:
    """Add one call to a usage dict (mutating) and return the call's USD."""
    stages = usage.setdefault("stages", {})
    per_model = stages.setdefault(stage, {})
    m = per_model.setdefault(model, {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0, "usd": 0.0})
    m["prompt_tokens"] += int(prompt_tokens or 0)
    m["completion_tokens"] += int(completion_tokens or 0)
    m["calls"] += 1
    call_usd = pricing.usd(model, int(prompt_tokens or 0), int(completion_tokens or 0))
    m["usd"] = round(m["usd"] + call_usd, 6)
    usage["usd_total"] = round(usage.get("usd_total", 0.0) + call_usd, 6)
    return call_usd


def cost_report(store: Store, run: dict, pricing: Pricing) -> dict[str, Any]:
    jobs = store.jobs_for_run(run["run_id"])
    merged: dict[str, dict[str, dict]] = {}
    covered: set[str] = set()
    counted: list[str] = []
    for j in jobs:
        u = j.get("usage") or {}
        if u.get("stages") or j.get("result"):
            counted.append(j["job_id"])
        for stage, per_model in (u.get("stages") or {}).items():
            for model, m in per_model.items():
                tgt = merged.setdefault(stage, {}).setdefault(model, {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0})
                for k in ("prompt_tokens", "completion_tokens", "calls"):
                    tgt[k] += int(m.get(k, 0))
        covered.update((j.get("result") or {}).keys())
    stages_out = []
    total = 0.0
    for stage in sorted(merged, key=lambda s: _order(s)):
        models_out = []
        s_usd = 0.0
        for model, m in sorted(merged[stage].items()):
            usd = pricing.usd(model, m["prompt_tokens"], m["completion_tokens"])
            models_out.append({"model": model, **m, "usd": usd})
            s_usd += usd
        stages_out.append({"stage": stage, "models": models_out, "usd": round(s_usd, 6)})
        total += s_usd
    completed = [s["stage_name"] for s in run["stages"] if s["status"] == "completed"]
    without = [s for s in completed if s not in covered]
    return {
        "run_id": run["run_id"], "stages": stages_out, "estimated_usd": round(total, 6),
        "partial": bool(without), "stages_without_usage": without, "pricing_as_of": pricing.as_of,
        "jobs_counted": counted,
    }


def _order(stage: str) -> int:
    from .store import stage_order
    return stage_order(stage)

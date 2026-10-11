"""Generate docs/use-cases.md by running the eight scenarios against the synthetic server.

Every tool output in the document is captured here, not typed by hand. Re-run after any
change that alters outputs:

    uv run python scripts/capture_use_cases.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from mcp import Client  # noqa: E402

from geoecon_pipeline_mcp.server import Settings, create_server  # noqa: E402
from geoecon_pipeline_mcp.synth import build_synthetic  # noqa: E402

OUT = ROOT / "docs" / "use-cases.md"
TMP = ROOT / ".synthetic-usecases.db"


def j(obj, max_items: int | None = None) -> str:
    if isinstance(obj, dict) and "result" in obj and isinstance(obj["result"], list) and max_items:
        items = obj["result"]
        obj = {"result": items[:max_items], "...": f"{len(items) - max_items} more rows omitted"} if len(items) > max_items else obj
    return "```json\n" + json.dumps(obj, indent=2, default=str, ensure_ascii=False) + "\n```"


def trim(d: dict, keys: list[str]) -> dict:
    return {k: d[k] for k in keys if k in d}


class Doc:
    def __init__(self):
        self.parts: list[str] = []

    def h(self, level: int, text: str):
        self.parts.append(f"\n{'#' * level} {text}\n")

    def p(self, text: str):
        self.parts.append(text + "\n")

    def call(self, name: str, args: dict, result, *, max_items: int | None = None, note: str | None = None):
        arg_s = ", ".join(f"{k}={json.dumps(v)}" for k, v in args.items())
        self.parts.append(f"**Tool call** `{name}({arg_s})`" + (f" — {note}" if note else "") + "\n")
        self.parts.append(j(result, max_items) + "\n")

    def text(self) -> str:
        return "\n".join(self.parts)


async def tool(c: Client, name: str, **args):
    r = await c.call_tool(name, args)
    if r.is_error:
        return {"error": r.content[0].text}
    return r.structured_content


async def poll(c: Client, job_id: str, until=("completed", "failed", "cancelled"), snapshots: list | None = None):
    import anyio
    last = None
    while True:
        job = await tool(c, "job_status", job_id=job_id)
        if snapshots is not None and (job.get("progress") or {}).get("message") != last:
            snapshots.append(trim(job, ["status", "stage", "progress", "stages_completed"]))
            last = (job.get("progress") or {}).get("message")
        if job["status"] in until:
            return job
        await anyio.sleep(0.05)


async def main() -> None:
    doc = Doc()
    doc.h(1, "Use cases")
    doc.p("Eight operator scenarios against the synthetic history (`GEOECON_MODE=synthetic`). Every tool output "
          "below was captured by `scripts/capture_use_cases.py` from the in-memory server; nothing is typed by hand. "
          "The fixture's clock is frozen at 2026-10-11 08:00Z (10:00 Prague), so 'today' means that day.")
    doc.p("The synthetic history: run 101 (older, completed), 102 (completed, three held records, two silent sources), "
          "103 (failed in `gate`), 104 (still `running` nine hours after it started), 105 (a LIMIT=50 smoke rerun that "
          "completed with nothing to send).")

    build_synthetic(TMP).close()
    settings = Settings(mode="synthetic", synthetic_db=TMP, fake_unit_sleep=0.01)
    app = create_server(settings)
    try:
        async with Client(app.mcp, raise_exceptions=True) as c:
            # 1 ------------------------------------------------------------------
            doc.h(2, "1. Morning check finds a stale run")
            doc.p("**Host prompt:** the `morning_check` prompt, or simply *\"Is the pipeline healthy this morning?\"*")
            hv = await tool(c, "pipeline_health")
            doc.call("pipeline_health", {}, hv)
            doc.p("Two facts, both from the pipeline's own watchdog logic plus the stale-run count the watchdog cannot "
                  "see: today's latest run (105) completed but sent nothing, and run 104 is still `running` nine hours "
                  "after it started. The operator looks at 104:")
            rs = await tool(c, "run_status", run_id=104)
            doc.call("run_status", {"run_id": 104}, {**trim(rs, ["run_id", "status", "started_at", "next_stage", "active_job_id"]),
                                                     "stages": [trim(s, ["stage_name", "status", "started_at", "finished_at"]) for s in rs["stages"]]})
            gate_row = next(x for x in rs["stages"] if x["stage_name"] == "gate")
            doc.p(f"`gate` has been `running` since {gate_row['started_at']} with no error row: a process that died without unwinding. "
                  "`start_job(reconcile_abandoned)` closes it out the way the orchestrator's own reconciliation would:")
            h = await tool(c, "start_job", kind="reconcile_abandoned")
            job = await poll(c, h["job_id"])
            doc.call("start_job", {"kind": "reconcile_abandoned"}, trim(h, ["job_id", "status", "stages_planned", "poll_after_s"]))
            doc.call("job_status", {"job_id": "<job_id>"}, trim(job, ["status", "result"]))

    finally:
        app.close()

    # scenarios 2-3 start from a fresh copy: scenario 1's reconciliation closed run 104 at the
    # wall clock, and mixing that into a fixture-clock history would mislead the reader
    build_synthetic(TMP).close()
    app = create_server(Settings(mode="synthetic", synthetic_db=TMP, fake_unit_sleep=0.01))
    try:
        async with Client(app.mcp, raise_exceptions=True) as c:
            # 2 ------------------------------------------------------------------
            doc.h(2, "2. Read why run 103 failed")
            doc.p("**Host prompt:** *\"Why did last Thursday's run fail?\"* (fresh copy of the history)")
            runs = await tool(c, "list_runs", status="failed")
            doc.call("list_runs", {"status": "failed"}, runs)
            e = await tool(c, "failed_stage_error", run_id=103)
            doc.call("failed_stage_error", {"run_id": 103}, e)
            page = await tool(c, "list_gate_rejections", run_id=103, limit=3)
            doc.call("list_gate_rejections", {"run_id": 103, "limit": 3}, page)
            doc.p(f"`unparsed_dropped == total_dropped` ({page['unparsed_dropped']} of {page['total_dropped']}): the model never "
                  "produced a verdict for any article. The failure is the LLM backend, not the news; the gate's health guard "
                  "refused to hand an empty set downstream. Resume at `gate` once the backend is back.")

            # 3 ------------------------------------------------------------------
            doc.h(2, "3. Preview and resume run 103 within a budget, watching progress")
            doc.p("**Host prompt:** *\"Resume run 103 but do not spend more than 50 cents.\"* The dry run first:")
            pv = await tool(c, "preview_resume", run_id=103)
            doc.call("preview_resume", {"run_id": 103}, pv)
            doc.p("The plan starts at `gate` (the failed stage is re-run, not skipped), passes the run's original window, "
                  "and stops after `render` because `send` is withheld. Start it with the budget:")
            h = await tool(c, "start_job", kind="resume_run", run_id=103, budget_usd=0.5)
            doc.call("start_job", {"kind": "resume_run", "run_id": 103, "budget_usd": 0.5}, h)
            snaps: list = []
            job = await poll(c, h["job_id"], snapshots=snaps)
            picks = [snaps[0]] + snaps[len(snaps) // 3: len(snaps) // 3 + 1] + snaps[2 * len(snaps) // 3: 2 * len(snaps) // 3 + 1] + [snaps[-1]]
            doc.p("`job_status` polled while the job runs (four of the snapshots; a host that opened "
                  "`subscriptions/listen` on `job://<id>` is told when to refetch instead of polling):")
            doc.call("job_status", {"job_id": "<job_id>"}, picks)
            doc.call("job_status", {"job_id": "<job_id>"}, trim(job, ["status", "stages_completed", "result", "usage", "budget_usd", "ttl_s"]), note="terminal")
            rs = await tool(c, "run_status", run_id=103)
            doc.call("run_status", {"run_id": 103}, {**trim(rs, ["status", "next_stage", "notes"]),
                                                     "stages": [f"{s['stage_name']}: {s['status']}" for s in rs["stages"]]})
            cr = await tool(c, "cost_report", run_id=103)
            doc.call("cost_report", {"run_id": 103}, cr)
            doc.p("`partial=true` is honest: discovery, registry discovery and extraction ran in the CronJob, which keeps "
                  "no token ledger; the four LLM stages ran here and are priced from `pricing.yaml`.")

            # 4 ------------------------------------------------------------------
            doc.h(2, "4. The budget gate fires and the operator approves")
            doc.p("**Host prompt:** *\"Re-run the gate on 103 for a penny.\"* (fresh copy of the history)")
    finally:
        app.close()

    build_synthetic(TMP).close()
    app = create_server(Settings(mode="synthetic", synthetic_db=TMP, fake_unit_sleep=0.01))
    try:
        async with Client(app.mcp, raise_exceptions=True) as c:
            h = await tool(c, "start_job", kind="run_stage", run_id=103, stage="gate", budget_usd=0.01)
            job = await poll(c, h["job_id"], until=("input_required",))
            doc.call("start_job", {"kind": "run_stage", "run_id": 103, "stage": "gate", "budget_usd": 0.01}, h)
            doc.call("job_status", {"job_id": "<job_id>"}, trim(job, ["status", "stage", "budget_usd", "input_request"]))
            doc.p("The job is parked in `input_required` with a form-shaped request (the tasks extension's shape). "
                  "Nothing has been written to the run yet. The operator approves a higher ceiling:")
            a = await tool(c, "answer_job", job_id=h["job_id"], approve=True, new_budget_usd=1.0)
            doc.call("answer_job", {"job_id": "<job_id>", "approve": True, "new_budget_usd": 1.0}, a)
            job = await poll(c, h["job_id"])
            doc.call("job_status", {"job_id": "<job_id>"}, trim(job, ["status", "budget_usd", "usage", "result"]), note="terminal")
            doc.p("Had the operator answered `approve=false`, the stage would have been marked `failed` with "
                  "`aborted at budget gate` and the job `failed`; `cancel_job` while waiting ends the job `cancelled` and a "
                  "late `answer_job` is refused with `job is cancelled`.")

            # 5 ------------------------------------------------------------------
            doc.h(2, "5. Review held records and decide whether to re-run synthesis")
            doc.p("**Host prompt:** *\"What did the validator hold back in run 102, and should we redo synthesis?\"*")
            held = await tool(c, "list_held_records", run_id=102)
            doc.call("list_held_records", {"run_id": 102}, held)
            rs = await tool(c, "run_status", run_id=102)
            m = rs["metrics"]
            doc.call("run_status", {"run_id": 102}, {"status": rs["status"], "metrics": trim(m, ["records_total", "records_ok", "records_held", "clusters", "newsletters"])})
            doc.p(f"{m['records_held']} held of {m['records_total']} records ({m['records_held'] / m['records_total']:.0%}). Two are "
                  "model-output defects the retry already failed on (`empty_figures`, `zero_entities`) and one is a parse "
                  "failure. At this ratio the brief shipped with the OK records; re-running synthesis would cost about "
                  "`preview_resume`'s synthesis estimate and would re-hold the same clusters unless the model changed. "
                  "Decision: no re-run; note the three cluster ids for the prompt review.")

            # 6 ------------------------------------------------------------------
            doc.h(2, "6. Silent sources")
            doc.p("**Host prompt:** *\"Which registry sources produced nothing in run 102?\"*")
            sh = await tool(c, "source_health", run_id=102)
            rows = sh["result"]
            silent = [r for r in rows if r["state"] == "silent"]
            from collections import Counter
            doc.call("source_health", {"run_id": 102}, {"result": silent, "summary": dict(Counter(r["state"] for r in rows))},
                     note="filtered to state=silent; the full result has 66 rows")
            doc.p("Two `live` sources with zero rows: a feed that moved or a scraper selector that rotted. `held` sources "
                  "(needs_review) and `retired` ones are not failures: the registry says they do not fetch. For an older "
                  "run with no `discovery_registry` stage at all the tool refuses to guess:")
            sh101 = await tool(c, "source_health", run_id=101)
            doc.call("source_health", {"run_id": 101}, {"result": sh101["result"][:2], "summary": dict(Counter(r["state"] for r in sh101["result"]))},
                     note="first two of 66 rows")

            # 7 ------------------------------------------------------------------
            doc.h(2, "7. Write the incident report")
            doc.p("**Host prompt:** the `write_incident_report` prompt with `run_id=103`.")
            pr = await c.get_prompt("write_incident_report", {"run_id": "103"})
            doc.p("**Prompt text the host receives:**\n\n```text\n" + pr.messages[0].content.text + "\n```")
            tpl = await c.read_resource("geoecon://incidents/template")
            head = "\n".join(tpl.contents[0].text.splitlines()[:22])
            doc.p(f"**Resource** `geoecon://incidents/template` (ttlMs={tpl.ttl_ms}, cacheScope={tpl.cache_scope}; first 22 lines):\n\n```markdown\n{head}\n...\n```")
            doc.p("The model then fills the skeleton from `run_status(103)`, `failed_stage_error(103)` (section 1's error "
                  "cell is the stored text verbatim), `list_held_records(103)`, `pipeline_health()` and `cost_report(103)`.")

            # 8 ------------------------------------------------------------------
            doc.h(2, "8. The refused send")
            doc.p("**Host prompt:** *\"Send the newsletter for run 103 now.\"*")
            r = await tool(c, "start_job", kind="run_stage", run_id=103, stage="send", force=True)
            doc.call("start_job", {"kind": "run_stage", "run_id": 103, "stage": "send", "force": True}, r)
    finally:
        app.close()

    build_synthetic(TMP).close()
    app = create_server(Settings(mode="synthetic", synthetic_db=TMP, allow_send=True, stdio_scopes=("pipeline:read", "pipeline:operate")))
    try:
        async with Client(app.mcp, raise_exceptions=True) as c:
            doc.p("With `ALLOW_SEND=1` on the server but a token without `pipeline:send`:")
            r = await tool(c, "start_job", kind="run_stage", run_id=103, stage="send", force=True)
            doc.call("start_job", {"kind": "run_stage", "run_id": 103, "stage": "send", "force": True}, r, note="ALLOW_SEND=1, scopes pipeline:read,pipeline:operate")
            pv = await tool(c, "preview_resume", run_id=103)
            doc.call("preview_resume", {"run_id": 103}, trim(pv, ["remaining", "send_withheld", "message"]))
            doc.p("Both conditions must hold: `ALLOW_SEND=1` on the server and `pipeline:send` on the caller. The CronJob "
                  "keeps sending the daily brief; this server stops after `render` and `run_status.notes` records why.")
    finally:
        app.close()
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(TMP) + suffix)
            if p.exists():
                p.unlink()

    OUT.write_text(doc.text(), encoding="utf-8")
    print(f"wrote {OUT} ({len(doc.text().splitlines())} lines)")


if __name__ == "__main__":
    asyncio.run(main())

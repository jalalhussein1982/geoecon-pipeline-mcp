"""`PostgresStore`: the Store protocol over the pipeline's PostgreSQL state.

Reads go through `sql.py`; stage bookkeeping goes through the pipeline's own
`geoecon.state.repo.Repo` so a stage row written by this server is byte-for-byte what the
orchestrator would have written. The `mcp_jobs` table is ours (migrations/001_mcp_jobs.sql),
applied with the pipeline's `apply_migrations` into the same `schema_migrations` ledger.

Threading: a psycopg connection is not safe for concurrent use. The store serialises its own
statements with a lock, and `pipeline_factories` opens a SEPARATE connection + Repo per job
for the stage functions, which do their own writes on the job thread.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from geoecon.state import db as gdb
from geoecon.state.repo import Repo

from . import sql as Q
from .store import expected_stages, next_stage_of, stage_order

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def apply_all_migrations(conn: psycopg.Connection) -> list[str]:
    """The pipeline's migrations (idempotent, as the CronJob does at bootstrap) then ours."""
    applied = gdb.apply_migrations(conn)
    applied += gdb.apply_migrations(conn, str(MIGRATIONS_DIR))
    return applied


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class PostgresStore:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.repo = Repo(conn)
        self._lock = threading.RLock()

    @classmethod
    def connect(cls, dsn: str, *, migrate: bool = True) -> "PostgresStore":
        conn = gdb.connect(dsn)
        if migrate:
            apply_all_migrations(conn)
        return cls(conn)

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def _rows(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock, self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def _one(self, sql: str, params: tuple = ()) -> dict | None:
        rows = self._rows(sql, params)
        return rows[0] if rows else None

    def _exec(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self.conn.execute(sql, params)

    def _repo(self, method: str, *args: Any) -> Any:
        with self._lock:
            return getattr(self.repo, method)(*args)

    # -- reads ------------------------------------------------------------------------

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _stages(self, run_id: int) -> list[dict]:
        out = [{"stage_name": r["stage_name"], "status": r["status"], "started_at": _aware(r["started_at"]),
                "finished_at": _aware(r["finished_at"]), "error": r["error"], "checkpoint": r["checkpoint"] or {}}
               for r in self._rows(Q.STAGE_RUNS, (run_id,))]
        out.sort(key=lambda s: stage_order(s["stage_name"]))
        return out

    def list_runs(self, status: str | None, since: datetime | None, limit: int) -> list[dict]:
        out = []
        for r in self._rows(Q.LIST_RUNS, (status, status, since, since, int(limit))):
            stages = self._stages(r["run_id"])
            total = len(stages) if r["status"] == "completed" else max(len(stages), len(expected_stages(stages)))
            out.append({
                "run_id": r["run_id"], "window_start": _aware(r["window_start"]), "window_end": _aware(r["window_end"]),
                "status": r["status"], "started_at": _aware(r["started_at"]), "finished_at": _aware(r["finished_at"]),
                "stages_done": int(r["stages_done"]), "stages_total": total, "last_failed_stage": r["last_failed"],
            })
        return out

    def run_detail(self, run_id: int) -> dict | None:
        r = self._one(Q.RUN, (run_id,))
        if r is None:
            return None
        stages = self._stages(run_id)
        return {
            "run_id": r["run_id"], "window_start": _aware(r["window_start"]), "window_end": _aware(r["window_end"]),
            "scheduled_for": _aware(r["scheduled_for"]), "status": r["status"], "started_at": _aware(r["started_at"]),
            "finished_at": _aware(r["finished_at"]), "synthesis_model": r["synthesis_model"],
            "stages": stages, "metrics": r["metrics"] or {}, "notes": r["notes"],
            "next_stage": None if r["status"] == "completed" else next_stage_of(stages),
            "completed_stages": [s["stage_name"] for s in stages if s["status"] == "completed"],
        }

    def held_records(self, run_id: int, flag: str | None) -> list[dict]:
        out = []
        for r in self._rows(Q.HELD_RECORDS, (run_id, flag, flag)):
            detail = r["quality_detail"] or {}
            out.append({"record_id": r["record_id"], "cluster_id": r["cluster_id"], "flag": r["synthesis_quality_flag"],
                        "reasons": list(detail.get("reasons") or []), "action": detail.get("action"),
                        "headline": r["headline_en"], "article_count": r["article_count"],
                        "is_headline_only": bool(r["is_headline_only"])})
        return out

    def gate_rejections(self, run_id: int, limit: int, cursor: str | None) -> tuple[list[dict], str | None]:
        after = int(cursor) if cursor else 0
        rows = self._rows(Q.GATE_REJECTIONS, (run_id, after, int(limit) + 1))
        more = len(rows) > limit
        rows = rows[:limit]
        items = [{"article_id": r["article_id"], "title": r["title"], "domain": r["domain"], "registry_id": r["registry_id"],
                  "gate_score": r["gate_score"], "gate_reason": r["gate_reason"],
                  "unparsed": r["gate_reason"] == "gate_unparsed_dropped"} for r in rows]
        return items, (str(rows[-1]["article_id"]) if more and rows else None)

    def gate_drop_counts(self, run_id: int) -> tuple[int, int]:
        r = self._one(Q.GATE_DROP_COUNTS, (run_id,)) or {}
        return int(r.get("dropped") or 0), int(r.get("unparsed") or 0)

    def source_counts(self, run_id: int) -> dict[str, int]:
        return {r["registry_id"]: int(r["n"]) for r in self._rows(Q.SOURCE_COUNTS, (run_id,))}

    def newsletters(self, run_id: int) -> list[dict]:
        return [{"newsletter_id": r["newsletter_id"], "bucket": r["bucket"], "record_count": r["record_count"],
                 "email_html": r["email_html"], "send_status": r["send_status"], "sent_at": _aware(r["sent_at"]),
                 "archive_html_path": r["archive_html_path"], "send_error": r["send_error"]}
                for r in self._rows(Q.NEWSLETTERS, (run_id,))]

    def latest_run_id(self) -> int | None:
        with self._lock:
            row = self.conn.execute(Q.LATEST_RUN_ID).fetchone()
        return row[0] if row else None

    def latest_run_for_window(self, start: datetime, end: datetime) -> dict | None:
        return self._repo("latest_run_for_window", start, end)

    def failed_stages(self, run_id: int) -> list[dict]:
        return self._repo("failed_stages", run_id)

    def stale_running_runs(self, cutoff: datetime) -> list[dict]:
        return self._repo("stale_running_runs", cutoff)

    def stage_inputs(self, run_id: int) -> dict[str, int]:
        a = self._one(Q.STAGE_INPUTS_ARTICLES, (run_id,)) or {}
        with self._lock:
            clusters = self.conn.execute(Q.COUNT_CLUSTERS, (run_id,)).fetchone()[0]
            records = self.conn.execute(Q.COUNT_RECORDS, (run_id,)).fetchone()[0]
            newsletters = self.conn.execute(Q.COUNT_NEWSLETTERS, (run_id,)).fetchone()[0]
        return {"articles": int(a.get("total") or 0), "pending_fetch": int(a.get("pending_fetch") or 0),
                "fetchable": int(a.get("fetchable") or 0), "pending_gate": int(a.get("pending_gate") or 0),
                "gated": int(a.get("gated") or 0), "kept": int(a.get("kept") or 0), "countries": int(a.get("countries") or 0),
                "clusters": int(clusters), "records": int(records), "newsletters": int(newsletters), "buckets": 3}

    def funnel(self, run_id: int) -> dict[str, int]:
        return {k: int(v) for k, v in (self._one(Q.FUNNEL, (run_id,)) or {}).items()}

    # -- stage bookkeeping through the pipeline's Repo -----------------------------------

    def start_stage(self, run_id: int, stage: str) -> None:
        self._repo("start_stage", run_id, stage)

    def complete_stage(self, run_id: int, stage: str, checkpoint: dict | None) -> None:
        self._repo("complete_stage", run_id, stage, checkpoint)

    def fail_stage(self, run_id: int, stage: str, error: str) -> None:
        self._repo("fail_stage", run_id, stage, error)

    def set_run_status(self, run_id: int, status: str) -> None:
        self._repo("set_run_status", run_id, status)

    def merge_run_metrics(self, run_id: int, metrics: dict) -> None:
        self._repo("merge_run_metrics", run_id, metrics)

    def append_run_note(self, run_id: int, note: str) -> None:
        self._exec(Q.APPEND_RUN_NOTE, (note, note, run_id))

    def mark_run_failed(self, run_id: int, note: str) -> None:
        self._repo("mark_run_failed", run_id, note)

    # -- jobs ---------------------------------------------------------------------------

    _JSON = {"progress", "input_request", "input_response", "usage", "result", "trace"}

    def _job_params(self, job: dict) -> tuple:
        vals = []
        for c in Q.JOB_COLUMNS:
            v = job.get(c)
            if c in self._JSON:
                v = Jsonb(v) if v is not None else (Jsonb({}) if c in ("usage", "trace") else None)
            elif c == "stages_planned":
                v = list(v or [])
            elif c == "job_id":
                v = str(v)
            vals.append(v)
        return tuple(vals)

    @staticmethod
    def _job_row(r: dict) -> dict:
        d = dict(r)
        d["job_id"] = str(d["job_id"])
        d["stages_planned"] = list(d.get("stages_planned") or [])
        d["usage"] = d.get("usage") or {}
        d["trace"] = d.get("trace") or {}
        d["budget_usd"] = float(d["budget_usd"]) if d.get("budget_usd") is not None else 0.0
        for k in ("created_at", "updated_at", "finished_at", "ttl_expires_at", "heartbeat_at"):
            d[k] = _aware(d.get(k))
        return d

    def job_insert(self, job: dict) -> None:
        self._exec(Q.JOB_INSERT, self._job_params(job))

    def job_get(self, job_id: str) -> dict | None:
        try:
            uuid.UUID(str(job_id))
        except ValueError:
            return None
        r = self._one(Q.JOB_GET, (str(job_id),))
        return self._job_row(r) if r else None

    def job_update(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields.setdefault("updated_at", datetime.now(timezone.utc))
        sets, vals = [], []
        for k, v in fields.items():
            if k in self._JSON:
                v = Jsonb(v) if v is not None else None
            elif k == "stages_planned":
                v = list(v or [])
            sets.append(f"{k} = %s"); vals.append(v)
        vals.append(str(job_id))
        self._exec(f"UPDATE mcp_jobs SET {', '.join(sets)} WHERE job_id = %s::uuid", tuple(vals))

    def job_fail_if_active(self, job_id: str, error: str) -> bool:
        from .store import JOB_TTL
        now = datetime.now(timezone.utc)
        with self._lock:
            row = self.conn.execute(Q.JOB_FAIL_IF_ACTIVE, (error, now, now + JOB_TTL, now, str(job_id))).fetchone()
        return row is not None

    def job_heartbeat(self, job_ids: list[str], now: datetime) -> None:
        if job_ids:
            self._exec(Q.JOB_HEARTBEAT, (now, [str(j) for j in job_ids]))

    def jobs_working(self) -> list[dict]:
        return [self._job_row(r) for r in self._rows(Q.JOBS_WORKING)]

    def jobs_for_run(self, run_id: int) -> list[dict]:
        return [self._job_row(r) for r in self._rows(Q.JOBS_FOR_RUN, (run_id,))]


# ── operator-mode factories ────────────────────────────────────────────────────────────


def pipeline_factories(settings: Any, store: PostgresStore):
    """(stage_factory, llm_factory, cfg) for operator mode: the pipeline's Config from the same env
    as the CronJob, its LLMClient, and its stage functions on a fresh connection per job."""
    from geoecon.config import Config
    from geoecon.llm.client import LLMClient

    from .stages import default_stage_names, make_pipeline_stage_fns

    cfg = Config.from_env(os.environ)

    def llm_factory() -> Any:
        return LLMClient(cfg.llm_base_url, cfg.llm_api_key)

    def stage_factory(client: Any) -> dict:
        conn = gdb.connect(cfg.db_dsn)             # the stage functions write on the job thread
        repo = Repo(conn)
        names = default_stage_names(cfg, client)

        def wrap(name: str):
            def fn(run: dict, report: Any, cancel: Any) -> dict:
                return make_pipeline_stage_fns(cfg, repo, run, client, names=names)[name](run, report, cancel)
            return fn

        return {n: wrap(n) for n in names}

    return stage_factory, llm_factory, cfg


__all__ = ["PostgresStore", "apply_all_migrations", "pipeline_factories", "MIGRATIONS_DIR"]

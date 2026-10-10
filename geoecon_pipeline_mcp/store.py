"""State access: the `Store` protocol and its two implementations.

* `SyntheticStore`  — SQLite copy of a fabricated run history (demo and tests). The schema
                      mirrors the pipeline's tables column-for-column where the server reads
                      them, so the same code paths run against both stores.
* `PostgresStore`   — the pipeline's own tables through `geoecon.state.repo.Repo` plus the
                      read queries in `sql.py` the Repo lacks (lives in `pg.py`, imported lazily
                      so synthetic mode never needs a database driver connection).

All timestamps cross the boundary as timezone-aware UTC datetimes. JSON columns cross as
Python dicts/lists. Every method is safe to call from a worker thread: the SQLite store
serialises access with a lock, and the Postgres store owns one connection per store.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

# Canonical stage order (migration 008). `discovery_search` only exists when the pipeline
# runs with SEARCH_DISCOVERY=true; `rss` is a legacy name that never appears in new runs.
CANONICAL_STAGES: tuple[str, ...] = (
    "discovery", "discovery_registry", "discovery_search", "extraction",
    "gate", "cluster", "synthesis", "summary", "render", "send",
)
LLM_STAGES: frozenset[str] = frozenset({"gate", "cluster", "synthesis", "summary"})
STALE_AFTER = timedelta(hours=6)
JOB_TTL = timedelta(days=14)


def stage_order(name: str) -> int:
    try:
        return CANONICAL_STAGES.index(name)
    except ValueError:
        return len(CANONICAL_STAGES)


class Store(Protocol):
    """What the server needs from the state tables. Read methods first, then stage
    bookkeeping (the orchestrator's `Repo` calls), then the `mcp_jobs` table."""

    def now(self) -> datetime: ...
    def list_runs(self, status: str | None, since: datetime | None, limit: int) -> list[dict]: ...
    def run_detail(self, run_id: int) -> dict | None: ...
    def held_records(self, run_id: int, flag: str | None) -> list[dict]: ...
    def gate_rejections(self, run_id: int, limit: int, cursor: str | None) -> tuple[list[dict], str | None]: ...
    def gate_drop_counts(self, run_id: int) -> tuple[int, int]: ...
    def source_counts(self, run_id: int) -> dict[str, int]: ...
    def newsletters(self, run_id: int) -> list[dict]: ...
    def latest_run_id(self) -> int | None: ...
    def latest_run_for_window(self, start: datetime, end: datetime) -> dict | None: ...
    def failed_stages(self, run_id: int) -> list[dict]: ...
    def stale_running_runs(self, cutoff: datetime) -> list[dict]: ...
    def stage_inputs(self, run_id: int) -> dict[str, int]: ...
    # stage bookkeeping
    def start_stage(self, run_id: int, stage: str) -> None: ...
    def complete_stage(self, run_id: int, stage: str, checkpoint: dict | None) -> None: ...
    def fail_stage(self, run_id: int, stage: str, error: str) -> None: ...
    def set_run_status(self, run_id: int, status: str) -> None: ...
    def merge_run_metrics(self, run_id: int, metrics: dict) -> None: ...
    def append_run_note(self, run_id: int, note: str) -> None: ...
    def mark_run_failed(self, run_id: int, note: str) -> None: ...
    # jobs
    def job_insert(self, job: dict) -> None: ...
    def job_get(self, job_id: str) -> dict | None: ...
    def job_update(self, job_id: str, **fields: Any) -> None: ...
    def jobs_working(self) -> list[dict]: ...
    def jobs_for_run(self, run_id: int) -> list[dict]: ...


# ── helpers ─────────────────────────────────────────────────────────────────────────


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def parse_dt(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _j(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)


def next_stage_of(stages: list[dict]) -> str | None:
    """First stage in canonical order that is not completed. A failed or running stage
    IS the next stage (Review Focus 1: a run that failed in `gate` resumes at `gate`)."""
    present = {s["stage_name"]: s for s in stages}
    expected = expected_stages(stages)
    for name in expected:
        row = present.get(name)
        if row is None or row["status"] != "completed":
            return name
    return None


def expected_stages(stages: list[dict]) -> list[str]:
    """The stage list this run is expected to have: the canonical order, with
    `discovery_search` only if the run actually has that row, and legacy rows
    (`rss`) kept where they appear."""
    names = {s["stage_name"] for s in stages}
    out = [n for n in CANONICAL_STAGES if n != "discovery_search" or n in names]
    legacy = [n for n in names if n not in CANONICAL_STAGES]
    return out + sorted(legacy)


SYNTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY,
    window_start TEXT NOT NULL, window_end TEXT NOT NULL, scheduled_for TEXT NOT NULL,
    started_at TEXT NOT NULL, finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running','completed','failed','partial')),
    synthesis_model TEXT, synthesis_prompt_id TEXT, embedding_model TEXT,
    metrics TEXT NOT NULL DEFAULT '{}', notes TEXT
);
CREATE TABLE IF NOT EXISTS stage_runs (
    stage_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed','skipped')),
    started_at TEXT, finished_at TEXT,
    checkpoint TEXT NOT NULL DEFAULT '{}', error TEXT,
    UNIQUE (run_id, stage_name)
);
CREATE TABLE IF NOT EXISTS articles (
    article_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    source_url TEXT NOT NULL, domain TEXT, title TEXT, source_lang TEXT,
    fetch_status TEXT NOT NULL DEFAULT 'pending', body_chars INTEGER NOT NULL DEFAULT 0,
    country_fips TEXT,
    gate_geoeconomic INTEGER, gate_score REAL, gate_country TEXT, gate_reason TEXT,
    discovery_source TEXT, registry_id TEXT, source_tier INTEGER
);
CREATE INDEX IF NOT EXISTS articles_run_idx ON articles(run_id);
CREATE TABLE IF NOT EXISTS clusters (
    cluster_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    event_fingerprint TEXT NOT NULL, region TEXT, country_fips TEXT,
    article_count INTEGER NOT NULL DEFAULT 0, gate_score REAL
);
CREATE TABLE IF NOT EXISTS synthesis_records (
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    cluster_id INTEGER NOT NULL REFERENCES clusters(cluster_id),
    headline_en TEXT,
    synthesis_quality_flag TEXT NOT NULL DEFAULT 'ok',
    quality_detail TEXT, is_headline_only INTEGER NOT NULL DEFAULT 0,
    created_at TEXT,
    UNIQUE (run_id, cluster_id)
);
CREATE TABLE IF NOT EXISTS newsletters (
    newsletter_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(run_id),
    bucket TEXT, render_mode TEXT NOT NULL DEFAULT 'en_only',
    archive_html_path TEXT, email_html TEXT, record_count INTEGER NOT NULL DEFAULT 0,
    sent_at TEXT, send_status TEXT NOT NULL DEFAULT 'pending', send_error TEXT,
    UNIQUE (run_id, bucket)
);
CREATE TABLE IF NOT EXISTS region_summaries (
    run_id INTEGER NOT NULL, bucket TEXT NOT NULL, lede_en TEXT,
    takeaways TEXT NOT NULL DEFAULT '[]', model TEXT, record_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, bucket)
);
CREATE TABLE IF NOT EXISTS mcp_jobs (
    job_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL, run_id INTEGER, stages_planned TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL CHECK (status IN ('working','input_required','completed','failed','cancelled')),
    stage TEXT, progress TEXT,
    input_request TEXT, input_response TEXT,
    budget_usd REAL NOT NULL DEFAULT 0, usage TEXT NOT NULL DEFAULT '{}',
    result TEXT, error TEXT,
    principal TEXT, trace TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, finished_at TEXT, ttl_expires_at TEXT
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

_JOB_JSON = ("stages_planned", "progress", "input_request", "input_response", "usage", "result", "trace")
_JOB_TS = ("created_at", "updated_at", "finished_at", "ttl_expires_at")


class SyntheticStore:
    """SQLite-backed Store over a fabricated run history. See `synth.build_synthetic`."""

    def __init__(self, path: str | Path, *, now: datetime | None = None):
        self.path = Path(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SYNTH_SCHEMA)
        self._fixed_now = now

    # -- infrastructure -------------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _q(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _x(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)

    def meta_get(self, key: str) -> str | None:
        rows = self._q("SELECT value FROM meta WHERE key=?", (key,))
        return rows[0]["value"] if rows else None

    def meta_set(self, key: str, value: str) -> None:
        self._x("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value))

    def now(self) -> datetime:
        """The fixture's 'now'. A synthetic history is frozen at an anchor instant so that
        'today', 'stale' and 'nine hours ago' mean the same thing on every machine."""
        if self._fixed_now is not None:
            return self._fixed_now
        anchor = self.meta_get("anchor_now")
        return parse_dt(anchor) if anchor else datetime.now(timezone.utc)

    # -- reads ------------------------------------------------------------------------

    @staticmethod
    def _stage_row(r: sqlite3.Row) -> dict:
        return {
            "stage_name": r["stage_name"], "status": r["status"],
            "started_at": parse_dt(r["started_at"]), "finished_at": parse_dt(r["finished_at"]),
            "error": r["error"], "checkpoint": _j(r["checkpoint"], {}),
        }

    def _stages(self, run_id: int) -> list[dict]:
        rows = self._q("SELECT * FROM stage_runs WHERE run_id=?", (run_id,))
        out = [self._stage_row(r) for r in rows]
        out.sort(key=lambda s: stage_order(s["stage_name"]))
        return out

    def list_runs(self, status: str | None, since: datetime | None, limit: int) -> list[dict]:
        sql, params = "SELECT * FROM runs", []
        conds = []
        if status:
            conds.append("status=?"); params.append(status)
        if since:
            conds.append("started_at>=?"); params.append(iso(since))
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY run_id DESC LIMIT ?"; params.append(int(limit))
        out = []
        for r in self._q(sql, tuple(params)):
            stages = self._stages(r["run_id"])
            done = [s for s in stages if s["status"] == "completed"]
            failed = [s for s in stages if s["status"] == "failed"]
            total = len(stages) if r["status"] == "completed" else max(len(stages), len(expected_stages(stages)))
            out.append({
                "run_id": r["run_id"], "window_start": parse_dt(r["window_start"]),
                "window_end": parse_dt(r["window_end"]), "status": r["status"],
                "started_at": parse_dt(r["started_at"]), "finished_at": parse_dt(r["finished_at"]),
                "stages_done": len(done), "stages_total": total,
                "last_failed_stage": max(failed, key=lambda s: stage_order(s["stage_name"]))["stage_name"] if failed else None,
            })
        return out

    def run_detail(self, run_id: int) -> dict | None:
        rows = self._q("SELECT * FROM runs WHERE run_id=?", (run_id,))
        if not rows:
            return None
        r = rows[0]
        stages = self._stages(run_id)
        return {
            "run_id": r["run_id"], "window_start": parse_dt(r["window_start"]),
            "window_end": parse_dt(r["window_end"]), "scheduled_for": parse_dt(r["scheduled_for"]),
            "status": r["status"], "started_at": parse_dt(r["started_at"]),
            "finished_at": parse_dt(r["finished_at"]), "synthesis_model": r["synthesis_model"],
            "stages": stages, "metrics": _j(r["metrics"], {}), "notes": r["notes"],
            # a completed run has nothing next even if it predates a stage (older runs lack discovery_registry)
            "next_stage": None if r["status"] == "completed" else next_stage_of(stages),
            "completed_stages": [s["stage_name"] for s in stages if s["status"] == "completed"],
        }

    def held_records(self, run_id: int, flag: str | None) -> list[dict]:
        sql = ("SELECT s.*, c.article_count FROM synthesis_records s JOIN clusters c ON c.cluster_id=s.cluster_id "
               "WHERE s.run_id=? AND s.synthesis_quality_flag<>'ok'")
        params: list[Any] = [run_id]
        if flag:
            sql += " AND s.synthesis_quality_flag=?"; params.append(flag)
        sql += " ORDER BY s.record_id"
        out = []
        for r in self._q(sql, tuple(params)):
            detail = _j(r["quality_detail"], {}) or {}
            out.append({
                "record_id": r["record_id"], "cluster_id": r["cluster_id"], "flag": r["synthesis_quality_flag"],
                "reasons": list(detail.get("reasons") or []), "action": detail.get("action"),
                "headline": r["headline_en"], "article_count": r["article_count"],
                "is_headline_only": bool(r["is_headline_only"]),
            })
        return out

    def gate_rejections(self, run_id: int, limit: int, cursor: str | None) -> tuple[list[dict], str | None]:
        after = int(cursor) if cursor else 0
        rows = self._q(
            "SELECT * FROM articles WHERE run_id=? AND gate_geoeconomic=0 AND article_id>? "
            "ORDER BY article_id LIMIT ?", (run_id, after, int(limit) + 1))
        more = len(rows) > limit
        rows = rows[:limit]
        items = [{
            "article_id": r["article_id"], "title": r["title"], "domain": r["domain"],
            "registry_id": r["registry_id"], "gate_score": r["gate_score"], "gate_reason": r["gate_reason"],
            "unparsed": r["gate_reason"] == "gate_unparsed_dropped",
        } for r in rows]
        return items, (str(rows[-1]["article_id"]) if more and rows else None)

    def gate_drop_counts(self, run_id: int) -> tuple[int, int]:
        r = self._q(
            "SELECT COUNT(*) AS n, SUM(CASE WHEN gate_reason='gate_unparsed_dropped' THEN 1 ELSE 0 END) AS u "
            "FROM articles WHERE run_id=? AND gate_geoeconomic=0", (run_id,))[0]
        return int(r["n"] or 0), int(r["u"] or 0)

    def source_counts(self, run_id: int) -> dict[str, int]:
        rows = self._q(
            "SELECT registry_id, COUNT(*) AS n FROM articles WHERE run_id=? AND registry_id IS NOT NULL "
            "GROUP BY registry_id", (run_id,))
        return {r["registry_id"]: int(r["n"]) for r in rows}

    def newsletters(self, run_id: int) -> list[dict]:
        return [{
            "newsletter_id": r["newsletter_id"], "bucket": r["bucket"], "record_count": r["record_count"],
            "email_html": r["email_html"], "send_status": r["send_status"], "sent_at": parse_dt(r["sent_at"]),
            "archive_html_path": r["archive_html_path"], "send_error": r["send_error"],
        } for r in self._q("SELECT * FROM newsletters WHERE run_id=? ORDER BY bucket", (run_id,))]

    def latest_run_id(self) -> int | None:
        rows = self._q("SELECT MAX(run_id) AS m FROM runs")
        return rows[0]["m"] if rows and rows[0]["m"] is not None else None

    def latest_run_for_window(self, start: datetime, end: datetime) -> dict | None:
        rows = self._q(
            "SELECT run_id, status, started_at, finished_at FROM runs WHERE scheduled_for>=? AND scheduled_for<? "
            "ORDER BY run_id DESC LIMIT 1", (iso(start), iso(end)))
        if not rows:
            return None
        r = rows[0]
        return {"run_id": r["run_id"], "status": r["status"],
                "started_at": parse_dt(r["started_at"]), "finished_at": parse_dt(r["finished_at"])}

    def failed_stages(self, run_id: int) -> list[dict]:
        return [{"stage_name": r["stage_name"], "error": r["error"]} for r in
                self._q("SELECT stage_name, error FROM stage_runs WHERE run_id=? AND status='failed'", (run_id,))]

    def stale_running_runs(self, cutoff: datetime) -> list[dict]:
        return [{"run_id": r["run_id"], "status": r["status"], "started_at": parse_dt(r["started_at"])} for r in
                self._q("SELECT run_id, status, started_at FROM runs WHERE status='running' AND started_at<? "
                        "ORDER BY run_id", (iso(cutoff),))]

    def stage_inputs(self, run_id: int) -> dict[str, int]:
        """Row counts the cost estimator and the DB progress counter work from."""
        a = self._q(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN fetch_status='pending' THEN 1 ELSE 0 END) AS pending_fetch, "
            "SUM(CASE WHEN fetch_status<>'failed' THEN 1 ELSE 0 END) AS fetchable, "
            "SUM(CASE WHEN fetch_status<>'failed' AND gate_geoeconomic IS NULL THEN 1 ELSE 0 END) AS pending_gate, "
            "SUM(CASE WHEN gate_geoeconomic IS NOT NULL THEN 1 ELSE 0 END) AS gated, "
            "SUM(CASE WHEN gate_geoeconomic=1 THEN 1 ELSE 0 END) AS kept, "
            "COUNT(DISTINCT CASE WHEN gate_geoeconomic=1 THEN country_fips END) AS countries "
            "FROM articles WHERE run_id=?", (run_id,))[0]
        c = self._q("SELECT COUNT(*) AS n FROM clusters WHERE run_id=?", (run_id,))[0]
        s = self._q("SELECT COUNT(*) AS n FROM synthesis_records WHERE run_id=?", (run_id,))[0]
        n = self._q("SELECT COUNT(*) AS n FROM newsletters WHERE run_id=?", (run_id,))[0]
        return {
            "articles": int(a["total"] or 0), "pending_fetch": int(a["pending_fetch"] or 0),
            "fetchable": int(a["fetchable"] or 0), "pending_gate": int(a["pending_gate"] or 0),
            "gated": int(a["gated"] or 0), "kept": int(a["kept"] or 0), "countries": int(a["countries"] or 0),
            "clusters": int(c["n"]), "records": int(s["n"]), "newsletters": int(n["n"]), "buckets": 3,
        }

    # -- stage bookkeeping (mirrors Repo._upsert_stage) -------------------------------

    def _upsert_stage(self, run_id: int, stage: str, status: str, checkpoint: dict | None = None,
                      error: str | None = None) -> None:
        now = iso(datetime.now(timezone.utc))
        finished = now if status in ("completed", "failed", "skipped") else None
        self._x(
            "INSERT INTO stage_runs(run_id, stage_name, status, started_at, finished_at, checkpoint, error) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id, stage_name) DO UPDATE SET status=excluded.status, "
            "started_at=CASE WHEN excluded.status='running' THEN excluded.started_at ELSE stage_runs.started_at END, "
            "finished_at=COALESCE(excluded.finished_at, stage_runs.finished_at), "
            "checkpoint=CASE WHEN excluded.checkpoint='{}' THEN stage_runs.checkpoint ELSE excluded.checkpoint END, "
            "error=excluded.error",
            (run_id, stage, status, now, finished, json.dumps(checkpoint or {}), error))

    def start_stage(self, run_id: int, stage: str) -> None:
        self._upsert_stage(run_id, stage, "running")

    def complete_stage(self, run_id: int, stage: str, checkpoint: dict | None) -> None:
        self._upsert_stage(run_id, stage, "completed", checkpoint)

    def fail_stage(self, run_id: int, stage: str, error: str) -> None:
        self._upsert_stage(run_id, stage, "failed", error=str(error)[:4000])

    def set_run_status(self, run_id: int, status: str) -> None:
        finished = iso(datetime.now(timezone.utc)) if status in ("completed", "failed", "partial") else None
        self._x("UPDATE runs SET status=?, finished_at=COALESCE(?, finished_at) WHERE run_id=?",
                (status, finished, run_id))

    def merge_run_metrics(self, run_id: int, metrics: dict) -> None:
        rows = self._q("SELECT metrics FROM runs WHERE run_id=?", (run_id,))
        if not rows:
            return
        merged = {**_j(rows[0]["metrics"], {}), **metrics}
        self._x("UPDATE runs SET metrics=? WHERE run_id=?", (json.dumps(merged), run_id))

    def append_run_note(self, run_id: int, note: str) -> None:
        self._x("UPDATE runs SET notes=CASE WHEN notes IS NULL OR notes='' THEN ? ELSE notes || ' | ' || ? END "
                "WHERE run_id=?", (note, note, run_id))

    def mark_run_failed(self, run_id: int, note: str) -> None:
        """Close out an abandoned run (mirrors Repo.mark_run_failed)."""
        now = iso(datetime.now(timezone.utc))
        self._x("UPDATE runs SET status='failed', finished_at=COALESCE(finished_at, ?), "
                "notes=CASE WHEN notes IS NULL OR notes='' THEN ? ELSE notes || ' | ' || ? END WHERE run_id=?",
                (now, note, note, run_id))
        self._x("UPDATE stage_runs SET status='failed', finished_at=COALESCE(finished_at, ?), error=COALESCE(error, ?) "
                "WHERE run_id=? AND status='running'", (now, note, run_id))

    # -- jobs -------------------------------------------------------------------------

    @staticmethod
    def _job_row(r: sqlite3.Row) -> dict:
        d = dict(r)
        for k in _JOB_JSON:
            d[k] = _j(d.get(k), [] if k == "stages_planned" else ({} if k in ("usage", "trace") else None))
        for k in _JOB_TS:
            d[k] = parse_dt(d.get(k))
        return d

    def job_insert(self, job: dict) -> None:
        cols = ["job_id", "kind", "run_id", "stages_planned", "status", "stage", "progress", "input_request",
                "input_response", "budget_usd", "usage", "result", "error", "principal", "trace",
                "created_at", "updated_at", "finished_at", "ttl_expires_at"]
        vals = []
        for c in cols:
            v = job.get(c)
            if c in _JOB_JSON:
                v = json.dumps(v) if v is not None else ("[]" if c == "stages_planned" else ("{}" if c in ("usage", "trace") else None))
            elif c in _JOB_TS:
                v = iso(v)
            vals.append(v)
        self._x(f"INSERT INTO mcp_jobs({','.join(cols)}) VALUES ({','.join('?' * len(cols))})", tuple(vals))

    def job_get(self, job_id: str) -> dict | None:
        rows = self._q("SELECT * FROM mcp_jobs WHERE job_id=?", (job_id,))
        return self._job_row(rows[0]) if rows else None

    def job_update(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields.setdefault("updated_at", datetime.now(timezone.utc))
        sets, vals = [], []
        for k, v in fields.items():
            if k in _JOB_JSON:
                v = json.dumps(v) if v is not None else None
            elif k in _JOB_TS:
                v = iso(v)
            sets.append(f"{k}=?"); vals.append(v)
        vals.append(job_id)
        self._x(f"UPDATE mcp_jobs SET {', '.join(sets)} WHERE job_id=?", tuple(vals))

    def jobs_working(self) -> list[dict]:
        return [self._job_row(r) for r in
                self._q("SELECT * FROM mcp_jobs WHERE status IN ('working','input_required') ORDER BY created_at")]

    def jobs_for_run(self, run_id: int) -> list[dict]:
        return [self._job_row(r) for r in
                self._q("SELECT * FROM mcp_jobs WHERE run_id=? ORDER BY created_at", (run_id,))]

    # -- synthetic-only writes used by synth.py and the fake stages ---------------------

    def execute(self, sql: str, params: tuple = ()) -> None:
        self._x(sql, params)

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        with self._lock:
            self._conn.executemany(sql, rows)

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self._q(sql, params)

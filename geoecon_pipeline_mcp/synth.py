"""Synthetic run history for demo and tests.

`build_synthetic(path)` writes a deterministic SQLite database with five runs:

    101  completed, sent (older run: no discovery_registry stage row, GDELT only)
    102  completed, sent, 3 held records; two live registry sources produced zero rows
    103  failed in `gate`: all 228 judged verdicts unparsable (invented text), resume must start at gate
    104  running, stale: started 9 h before the fixture's anchor instant, gate half-judged
    105  completed: a LIMIT=50 smoke rerun that kept nothing -> send_status=skipped_empty

Everything is invented: titles, domains, error texts, counts. Nothing is copied from a
production incident. The registry (66 sources) is the pipeline's own `config/sources.yaml`,
shipped here as `fixtures/sources.yaml` because the installed package does not include it.

`make_fake_stages(store, client)` returns stage functions with the server's stage-function
signature that behave like the pipeline's stages on the synthetic tables: they sleep a
little per unit of work, log in the pipeline's log format, call the LLM client (so the
counting proxy sees tokens) and write plausible rows. They exist so the Inspector recording
shows a live job, a budget prompt and a notification without a cluster or an LLM.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from .store import SyntheticStore, iso

ANCHOR_NOW = datetime(2026, 10, 11, 8, 0, 0, tzinfo=timezone.utc)   # 10:00 Europe/Prague
SEED = 20261011
FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
SOURCES_PATH = FIXTURES_DIR / "sources.yaml"
SYNTH_MODEL = "deepseek-v3.2"

STAGE_SECONDS = {
    "discovery": 55, "discovery_registry": 112, "extraction": 230, "gate": 1750,
    "cluster": 95, "synthesis": 1420, "summary": 80, "render": 18, "send": 4,
}
BUCKETS = ("middle_east", "north_africa", "latam")
BUCKET_OF_FIPS = {
    "SA": "middle_east", "AE": "middle_east", "TU": "middle_east", "IS": "middle_east", "QA": "middle_east",
    "KU": "middle_east", "IR": "middle_east", "IZ": "middle_east",
    "EG": "north_africa", "MO": "north_africa", "AG": "north_africa", "TS": "north_africa",
    "BR": "latam", "MX": "latam", "AR": "latam", "CI": "latam", "CO": "latam", "PE": "latam", "EC": "latam",
}
FIPS = list(BUCKET_OF_FIPS)
LANGS = ("en", "ar", "es", "pt", "fr", "tr")
DOMAINS = (
    "example-wire.net", "gulf-ledger.example", "andes-economia.example", "nile-markets.example",
    "bosphorus-brief.example", "pampas-finanzas.example", "levant-trade.example", "maghreb-eco.example",
)
TITLE_BANK = (
    "Central bank holds policy rate at {n} percent, cites import prices",
    "Port authority signs {n}-year concession with foreign operator",
    "Finance ministry prices {n} million bond under new framework",
    "Wheat tender awarded after {n} bids, freight costs weigh",
    "Sovereign fund takes {n} percent stake in regional utility",
    "Trade ministry revises tariff schedule on {n} product lines",
    "Fuel subsidy reform delayed by {n} weeks after protests",
    "Currency swap line of {n} million renewed with partner bank",
    "Lithium concession auction draws {n} qualified bidders",
    "Regulator fines telecom operator {n} million over spectrum terms",
    "Football league signs {n}-season broadcast deal",            # non-geoeconomic filler
    "Film festival opens with {n} entries from the region",       # non-geoeconomic filler
)
DROP_REASONS = (
    "domestic culture story; no economic lever or cross-border actor",
    "sports result with no economic consequence",
    "weather report; no policy, trade or capital flow",
    "celebrity item; outside geoeconomic scope",
    "local crime report; no state or market actor",
    "opinion column without a dated event",
)
HELD_RECORDS_102 = (
    ("empty_figures", ["key_figures is empty but the source lead quotes two amounts"], "retry_or_hold"),
    ("zero_entities", ["entities list is empty; headline names a ministry and a port operator"], "hold"),
    ("invalid_json", ["response was not a JSON object after fence stripping (synthetic)"], "hold"),
)
HELD_RECORDS_101 = (
    ("retry_fallback", ["first pass returned an empty record; retry also failed validation"], "hold"),
)
# 240 articles were discovered; 12 failed extraction and are never judged, so the gate saw 228.
GATE_103_ERROR = (
    "NullGateError: gate kept 0 of 228 articles; every verdict came back unparsable "
    "(gate_unparsed_dropped=228/228) — refusing to hand an empty article set downstream. "
    "{'gate_total': 228, 'gate_kept': 0, 'gate_dropped': 228, 'gate_unparsed': 228, 'gate_unparsed_dropped': 228}"
)

log = logging.getLogger("geoecon_pipeline_mcp.synth")


# ── registry ────────────────────────────────────────────────────────────────────────


def load_registry(path: Path | str = SOURCES_PATH) -> list[dict]:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return list(raw.get("sources") or [])


def fetching_sources(sources: list[dict]) -> list[dict]:
    """Sources the discovery_registry stage actually fetches with SEARCH_DISCOVERY off:
    `live` and `retired` entries whose mechanics are resolved."""
    return [s for s in sources
            if s.get("status") in ("live", "retired") and s.get("fetch_type") not in ("unverified", "dead")]


# ── history ─────────────────────────────────────────────────────────────────────────


def _ts(dt: datetime, seconds: float) -> datetime:
    return dt + timedelta(seconds=seconds)


class _Builder:
    def __init__(self, store: SyntheticStore, anchor: datetime):
        self.s = store
        self.rng = random.Random(SEED)
        self.anchor = anchor
        self.sources = load_registry()
        self.fetching = fetching_sources(self.sources)

    # articles -----------------------------------------------------------------

    def _title(self) -> str:
        return self.rng.choice(TITLE_BANK).format(n=self.rng.randint(2, 480))

    def articles(self, run_id: int, total: int, *, registry: bool, zero_sources: tuple[str, ...] = (),
                 extraction: str = "done") -> list[int]:
        rows: list[tuple] = []
        if registry:
            for src in self.fetching:
                n = 0 if src["id"] in zero_sources else self.rng.randint(1, 7)
                for _ in range(n):
                    rows.append(self._article_row(run_id, src["id"], src.get("tier", 2), src.get("country_fips"), extraction))
        while len(rows) < total:
            rows.append(self._article_row(run_id, None, None, None, extraction))
        rows = rows[:total]
        self.s.executemany(
            "INSERT INTO articles(run_id, source_url, domain, title, source_lang, fetch_status, body_chars, "
            "country_fips, discovery_source, registry_id, source_tier) VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        return [r["article_id"] for r in self.s.query("SELECT article_id FROM articles WHERE run_id=? ORDER BY article_id", (run_id,))]

    def _article_row(self, run_id: int, registry_id: str | None, tier: int | None, fips: str | None, extraction: str) -> tuple:
        domain = self.rng.choice(DOMAINS) if registry_id is None else f"{registry_id.replace('_', '-')}.example"
        url = f"https://{domain}/{self.rng.randint(100000, 999999)}"
        if extraction == "pending":
            status, chars = "pending", 0
        else:
            p = self.rng.random()
            status = "extracted" if p < 0.78 else ("headline_only" if p < 0.95 else "failed")
            chars = self.rng.randint(900, 7000) if status == "extracted" else 0
        return (run_id, url, domain, self._title(), self.rng.choice(LANGS), status, chars,
                fips or self.rng.choice(FIPS), "registry" if registry_id else "gdelt", registry_id, tier)

    # gate ---------------------------------------------------------------------

    def gate(self, run_id: int, ids: list[int], *, keep: float = 0.21, unparsed: float = 0.02,
             fraction: float = 1.0, all_unparsed: bool = False) -> dict:
        counts = {"gate_total": 0, "gate_kept": 0, "gate_dropped": 0, "gate_unparsed": 0, "gate_unparsed_dropped": 0}
        rows = []
        judged = ids[: int(len(ids) * fraction)]
        failed = {r["article_id"] for r in self.s.query(
            "SELECT article_id FROM articles WHERE run_id=? AND fetch_status='failed'", (run_id,))}
        for aid in judged:
            if aid in failed:
                continue
            counts["gate_total"] += 1
            p = self.rng.random()
            if all_unparsed or p < unparsed:
                rows.append((0, 0.0, None, "gate_unparsed_dropped", aid))
                counts["gate_dropped"] += 1; counts["gate_unparsed"] += 1; counts["gate_unparsed_dropped"] += 1
            elif p < unparsed + keep:
                fips = self.rng.choice(FIPS)
                rows.append((1, round(self.rng.uniform(0.55, 0.97), 2), fips, "state actor and cross-border economic lever", aid))
                counts["gate_kept"] += 1
            else:
                rows.append((0, round(self.rng.uniform(0.02, 0.4), 2), None, self.rng.choice(DROP_REASONS), aid))
                counts["gate_dropped"] += 1
        self.s.executemany("UPDATE articles SET gate_geoeconomic=?, gate_score=?, gate_country=?, gate_reason=? "
                           "WHERE article_id=?", rows)
        return counts

    # clusters / synthesis / newsletters ----------------------------------------

    def clusters(self, run_id: int) -> list[int]:
        kept = self.s.query("SELECT article_id, gate_country, gate_score FROM articles WHERE run_id=? AND gate_geoeconomic=1 "
                            "ORDER BY article_id", (run_id,))
        by_country: dict[str, list] = {}
        for r in kept:
            by_country.setdefault(r["gate_country"], []).append(r)
        rows = []
        for fips, arts in sorted(by_country.items()):
            i = 0
            while i < len(arts):
                size = min(self.rng.randint(1, 4), len(arts) - i)
                chunk = arts[i:i + size]; i += size
                score = round(sum(a["gate_score"] for a in chunk) / len(chunk), 3)
                rows.append((run_id, f"p1:{self.rng.getrandbits(80):020x}", BUCKET_OF_FIPS.get(fips, "other"), fips, size, score))
        self.s.executemany("INSERT INTO clusters(run_id, event_fingerprint, region, country_fips, article_count, gate_score) "
                           "VALUES (?,?,?,?,?,?)", rows)
        return [r["cluster_id"] for r in self.s.query("SELECT cluster_id FROM clusters WHERE run_id=? ORDER BY cluster_id", (run_id,))]

    def synthesis(self, run_id: int, cluster_ids: list[int], held: tuple, when: datetime) -> dict:
        rows = []
        held_ids = set(self.rng.sample(cluster_ids, min(len(held), len(cluster_ids))))
        held_iter = iter(held)
        for cid in cluster_ids:
            if cid in held_ids:
                flag, reasons, action = next(held_iter)
                rows.append((run_id, cid, self._title(), flag, json.dumps({"reasons": reasons, "action": action}), 0, iso(when)))
            else:
                rows.append((run_id, cid, self._title(), "ok", None, int(self.rng.random() < 0.08), iso(when)))
        self.s.executemany("INSERT INTO synthesis_records(run_id, cluster_id, headline_en, synthesis_quality_flag, "
                           "quality_detail, is_headline_only, created_at) VALUES (?,?,?,?,?,?,?)", rows)
        total = len(rows)
        held_n = len(held_ids)
        return {"records_total": total, "records_ok": total - held_n, "records_held": held_n}

    def newsletters(self, run_id: int, sent_at: datetime, status: str = "sent") -> dict:
        ok = self.s.query("SELECT c.region, COUNT(*) AS n FROM synthesis_records s JOIN clusters c ON c.cluster_id=s.cluster_id "
                          "WHERE s.run_id=? AND s.synthesis_quality_flag='ok' GROUP BY c.region", (run_id,))
        by_bucket = {r["region"]: r["n"] for r in ok}
        rows = []
        for b in BUCKETS:
            n = min(by_bucket.get(b, 0), 12)
            if n == 0:
                continue
            html = (f"<!doctype html><html><body><h1>Geoeconomic Brief — {b.replace('_', ' ').title()}</h1>"
                    f"<p>{n} records (synthetic fixture, run {run_id}).</p></body></html>")
            rows.append((run_id, b, "en_only", f"/data/archive/{sent_at:%Y-%m-%d}-{b}.html", html, n,
                         iso(sent_at), status, None))
        self.s.executemany("INSERT INTO newsletters(run_id, bucket, render_mode, archive_html_path, email_html, record_count, "
                           "sent_at, send_status, send_error) VALUES (?,?,?,?,?,?,?,?,?)", rows)
        return {"record_count": sum(r[5] for r in rows), "newsletters": len(rows)}

    # runs ---------------------------------------------------------------------

    def run_row(self, run_id: int, scheduled: datetime, status: str, started_offset_s: float = 2.0,
                finished: datetime | None = None, notes: str | None = None) -> datetime:
        started = _ts(scheduled, started_offset_s)
        self.s.execute(
            "INSERT INTO runs(run_id, window_start, window_end, scheduled_for, started_at, finished_at, status, "
            "synthesis_model, synthesis_prompt_id, embedding_model, metrics, notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, iso(scheduled - timedelta(hours=24)), iso(scheduled), iso(scheduled), iso(started),
             iso(finished), status, SYNTH_MODEL, "task_a_cov", None, "{}", notes))
        return started

    def stage(self, run_id: int, name: str, status: str, started: datetime, seconds: float | None,
              checkpoint: dict | None = None, error: str | None = None) -> datetime:
        finished = _ts(started, seconds) if seconds is not None else None
        self.s.execute(
            "INSERT INTO stage_runs(run_id, stage_name, status, started_at, finished_at, checkpoint, error) "
            "VALUES (?,?,?,?,?,?,?)",
            (run_id, name, status, iso(started), iso(finished), json.dumps(checkpoint or {}), error))
        return _ts(finished or started, 1.0)

    def metrics(self, run_id: int, m: dict) -> None:
        self.s.execute("UPDATE runs SET metrics=? WHERE run_id=?", (json.dumps(m), run_id))

    # the five runs --------------------------------------------------------------

    def completed_run(self, run_id: int, scheduled: datetime, *, total: int, registry: bool,
                      zero_sources: tuple[str, ...], held: tuple) -> None:
        t0 = self.run_row(run_id, scheduled, "completed")
        t = t0
        m: dict[str, Any] = {}
        ids = self.articles(run_id, total, registry=registry, zero_sources=zero_sources)
        n_reg = sum(1 for _ in self.s.query("SELECT 1 FROM articles WHERE run_id=? AND registry_id IS NOT NULL", (run_id,)))
        disc = {"gdelt_files": 96, "discovered": total - n_reg, "skipped_seen": self.rng.randint(5, 30)}
        t = self.stage(run_id, "discovery", "completed", t, STAGE_SECONDS["discovery"], disc); m.update(disc)
        if registry:
            reg = {"registry_discovered": n_reg, "sources_fetched": len(self.fetching),
                   "skipped_needs_review": sum(1 for s in self.sources if s.get("status") == "needs_review"),
                   "skipped_unverified": sum(1 for s in self.sources if s.get("fetch_type") == "unverified")}
            t = self.stage(run_id, "discovery_registry", "completed", t, STAGE_SECONDS["discovery_registry"], reg); m.update(reg)
        ext = self._extraction_counts(run_id)
        t = self.stage(run_id, "extraction", "completed", t, STAGE_SECONDS["extraction"], ext); m.update(ext)
        g = self.gate(run_id, ids)
        t = self.stage(run_id, "gate", "completed", t, STAGE_SECONDS["gate"], g); m.update(g)
        cl = self.clusters(run_id)
        c = {"clusters": len(cl), "gated_articles": g["gate_kept"]}
        t = self.stage(run_id, "cluster", "completed", t, STAGE_SECONDS["cluster"], c); m.update(c)
        sy = self.synthesis(run_id, cl, held, t)
        t = self.stage(run_id, "synthesis", "completed", t, STAGE_SECONDS["synthesis"], sy); m.update(sy)
        su = {"summaries_total": 3, "summaries_ok": 3, "summaries_empty": 0}
        t = self.stage(run_id, "summary", "completed", t, STAGE_SECONDS["summary"], su); m.update(su)
        nl = self.newsletters(run_id, _ts(t, STAGE_SECONDS["render"] + STAGE_SECONDS["send"]))
        t = self.stage(run_id, "render", "completed", t, STAGE_SECONDS["render"], nl); m.update(nl)
        se = {"send_status": "sent", "sent": [[b, "sent"] for b in BUCKETS]}
        t = self.stage(run_id, "send", "completed", t, STAGE_SECONDS["send"], se); m.update(se)
        self.metrics(run_id, m)
        self.s.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (iso(t), run_id))

    def _extraction_counts(self, run_id: int) -> dict:
        rows = self.s.query("SELECT fetch_status, COUNT(*) AS n FROM articles WHERE run_id=? GROUP BY fetch_status", (run_id,))
        c = {"extracted": 0, "headline_only": 0, "failed": 0}
        for r in rows:
            if r["fetch_status"] in c:
                c[r["fetch_status"]] = r["n"]
        return c

    def failed_gate_run(self, run_id: int, scheduled: datetime) -> None:
        t = self.run_row(run_id, scheduled, "failed")
        ids = self.articles(run_id, 240, registry=True)
        n_reg = sum(1 for _ in self.s.query("SELECT 1 FROM articles WHERE run_id=? AND registry_id IS NOT NULL", (run_id,)))
        disc = {"gdelt_files": 96, "discovered": 240 - n_reg, "skipped_seen": 12}
        t = self.stage(run_id, "discovery", "completed", t, STAGE_SECONDS["discovery"], disc)
        reg = {"registry_discovered": n_reg, "sources_fetched": len(self.fetching)}
        t = self.stage(run_id, "discovery_registry", "completed", t, STAGE_SECONDS["discovery_registry"], reg)
        ext = self._extraction_counts(run_id)
        t = self.stage(run_id, "extraction", "completed", t, STAGE_SECONDS["extraction"], ext)
        self.gate(run_id, ids, all_unparsed=True)
        gate_failed_at = _ts(t, 2900)        # the stage ran for 48 minutes before the guard tripped
        self.stage(run_id, "gate", "failed", t, 2900, {}, GATE_103_ERROR)
        self.metrics(run_id, {**disc, **reg, **ext})
        self.s.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (iso(gate_failed_at), run_id))

    def stale_running_run(self, run_id: int, scheduled: datetime) -> None:
        t = self.run_row(run_id, scheduled, "running")
        ids = self.articles(run_id, 220, registry=True)
        n_reg = sum(1 for _ in self.s.query("SELECT 1 FROM articles WHERE run_id=? AND registry_id IS NOT NULL", (run_id,)))
        disc = {"gdelt_files": 96, "discovered": 220 - n_reg, "skipped_seen": 9}
        t = self.stage(run_id, "discovery", "completed", t, STAGE_SECONDS["discovery"], disc)
        reg = {"registry_discovered": n_reg, "sources_fetched": len(self.fetching)}
        t = self.stage(run_id, "discovery_registry", "completed", t, STAGE_SECONDS["discovery_registry"], reg)
        ext = self._extraction_counts(run_id)
        t = self.stage(run_id, "extraction", "completed", t, STAGE_SECONDS["extraction"], ext)
        self.gate(run_id, ids, fraction=0.4)
        self.stage(run_id, "gate", "running", t, None)
        self.metrics(run_id, {**disc, **reg, **ext})

    def empty_smoke_run(self, run_id: int, scheduled: datetime) -> None:
        t = self.run_row(run_id, scheduled, "completed", notes="manual rerun with LIMIT=50 (smoke run)")
        ids = self.articles(run_id, 50, registry=True)
        m: dict[str, Any] = {}
        n_reg = sum(1 for _ in self.s.query("SELECT 1 FROM articles WHERE run_id=? AND registry_id IS NOT NULL", (run_id,)))
        disc = {"gdelt_files": 96, "discovered": 50 - n_reg, "skipped_seen": 0, "limit": 50}
        t = self.stage(run_id, "discovery", "completed", t, 20, disc); m.update(disc)
        reg = {"registry_discovered": n_reg, "sources_fetched": len(self.fetching)}
        t = self.stage(run_id, "discovery_registry", "completed", t, 30, reg); m.update(reg)
        ext = self._extraction_counts(run_id)
        t = self.stage(run_id, "extraction", "completed", t, 40, ext); m.update(ext)
        g = self.gate(run_id, ids, keep=0.0, unparsed=0.0)
        t = self.stage(run_id, "gate", "completed", t, 300, g); m.update(g)
        c = {"clusters": 0, "gated_articles": 0}
        t = self.stage(run_id, "cluster", "completed", t, 2, c); m.update(c)
        sy = {"records_total": 0, "records_ok": 0, "records_held": 0}
        t = self.stage(run_id, "synthesis", "completed", t, 1, sy); m.update(sy)
        su = {"summaries_total": 0, "summaries_ok": 0, "summaries_empty": 0}
        t = self.stage(run_id, "summary", "completed", t, 1, su); m.update(su)
        nl = {"record_count": 0, "newsletters": 0}
        t = self.stage(run_id, "render", "completed", t, 2, nl); m.update(nl)
        se = {"send_status": "skipped_empty"}
        t = self.stage(run_id, "send", "completed", t, 1, se); m.update(se)
        self.metrics(run_id, m)
        self.s.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (iso(t), run_id))


def build_synthetic(path: str | Path, *, anchor: datetime = ANCHOR_NOW) -> SyntheticStore:
    """Create (or overwrite) the synthetic history at `path` and return an open store."""
    p = Path(path)
    if p.exists():
        p.unlink()
    for suffix in ("-wal", "-shm"):
        side = Path(str(p) + suffix)
        if side.exists():
            side.unlink()
    store = SyntheticStore(p)
    store.meta_set("anchor_now", iso(anchor))
    store.meta_set("seed", str(SEED))
    store.meta_set("sources_path", str(SOURCES_PATH))
    b = _Builder(store, anchor)
    night = lambda day: datetime(2026, 10, day, 23, 0, 0, tzinfo=timezone.utc)  # 01:00 Europe/Prague
    b.completed_run(101, night(7), total=200, registry=False, zero_sources=(), held=HELD_RECORDS_101)
    b.completed_run(102, night(8), total=230, registry=True, zero_sources=("ae_mof", "co_cb_banrep"), held=HELD_RECORDS_102)
    b.failed_gate_run(103, night(9))
    b.stale_running_run(104, anchor - timedelta(hours=9))                 # 23:00Z on the 10th
    b.empty_smoke_run(105, anchor - timedelta(hours=3))                   # 05:00Z on the 11th, manual LIMIT run
    # WAL -> single file so the fixture is one artefact
    store.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    store.execute("PRAGMA journal_mode=DELETE")
    return store


def canonical_dump(store: SyntheticStore) -> str:
    """Stable text dump of every table, for determinism checks."""
    out = []
    for table in ("meta", "runs", "stage_runs", "articles", "clusters", "synthesis_records", "newsletters", "mcp_jobs"):
        rows = store.query(f"SELECT * FROM {table} ORDER BY 1")
        out.append(table)
        out.extend(json.dumps(dict(r), sort_keys=True, default=str) for r in rows)
    return "\n".join(out)


# ── fake stages ─────────────────────────────────────────────────────────────────────

Report = Callable[[int, int | None, str], None]
StageFn = Callable[[dict, Report, threading.Event], dict]


class StageCancelled(RuntimeError):
    """Raised by a stage when it observes the job's cancel flag between units of work."""


def _check_cancel(cancel: threading.Event) -> None:
    if cancel.is_set():
        raise StageCancelled("cancelled via MCP")


def make_fake_stages(store: SyntheticStore, client: Any, *, unit_sleep: float = 0.05,
                     batch_size: int = 15, rng_seed: int = SEED) -> dict[str, StageFn]:
    """Stage functions over the synthetic tables. `client.complete(model, messages, **p)` is
    called once per batch/cluster/bucket so a counting proxy sees realistic token traffic."""
    rng = random.Random(rng_seed)
    glog = logging.getLogger("geoecon.gate")
    xlog = logging.getLogger("geoecon.extract")
    slog = logging.getLogger("geoecon.synth")
    clog = logging.getLogger("geoecon.cluster")
    rlog = logging.getLogger("geoecon.render")

    def _sleep() -> None:
        if unit_sleep:
            time.sleep(unit_sleep)

    def discovery(run: dict, report: Report, cancel: threading.Event) -> dict:
        n = 0
        for i in range(1, 7):
            _check_cancel(cancel); _sleep(); n += 30
            report(i, 6, f"gkg file {i}/6")
        return {"gdelt_files": 6, "discovered": n, "skipped_seen": 0}

    def discovery_registry(run: dict, report: Report, cancel: threading.Event) -> dict:
        fetching = fetching_sources(load_registry())
        for i, src in enumerate(fetching, 1):
            _check_cancel(cancel)
            if i % 8 == 0:
                _sleep()
            report(i, len(fetching), f"source {src['id']}")
        return {"registry_discovered": 0, "sources_fetched": len(fetching)}

    def extraction(run: dict, report: Report, cancel: threading.Event) -> dict:
        rid = run["run_id"]
        pending = [r["article_id"] for r in store.query(
            "SELECT article_id FROM articles WHERE run_id=? AND fetch_status='pending' ORDER BY article_id", (rid,))]
        counts = {"extracted": 0, "headline_only": 0, "failed": 0}
        for i, aid in enumerate(pending, 1):
            _check_cancel(cancel)
            p = rng.random()
            status = "extracted" if p < 0.78 else ("headline_only" if p < 0.95 else "failed")
            store.execute("UPDATE articles SET fetch_status=?, body_chars=? WHERE article_id=?",
                          (status, rng.randint(900, 7000) if status == "extracted" else 0, aid))
            counts[status] += 1
            if i % 10 == 0:
                _sleep(); xlog.info("extracted %d", i)
            report(i, len(pending), f"article {i}/{len(pending)}")
        xlog.info("extraction: %s", counts)
        return counts

    def gate(run: dict, report: Report, cancel: threading.Event) -> dict:
        rid = run["run_id"]
        # the real stage re-judges every non-failed article; it does not resume per row
        arts = [r["article_id"] for r in store.query(
            "SELECT article_id FROM articles WHERE run_id=? AND fetch_status<>'failed' ORDER BY article_id", (rid,))]
        batches = [arts[i:i + batch_size] for i in range(0, len(arts), batch_size)]
        counts = {"gate_total": 0, "gate_kept": 0, "gate_dropped": 0, "gate_unparsed": 0, "gate_unparsed_dropped": 0}
        for bi, batch in enumerate(batches, 1):
            _check_cancel(cancel)
            client.complete(SYNTH_MODEL, [{"role": "system", "content": "gate"}, {"role": "user", "content": "x" * 400 * len(batch)}],
                            temperature=0, max_tokens=8192)
            rows = []
            for aid in batch:
                counts["gate_total"] += 1
                p = rng.random()
                if p < 0.21:
                    fips = rng.choice(FIPS)
                    rows.append((1, round(rng.uniform(0.55, 0.97), 2), fips, "state actor and cross-border economic lever", fips, aid))
                    counts["gate_kept"] += 1
                else:
                    rows.append((0, round(rng.uniform(0.02, 0.4), 2), None, rng.choice(DROP_REASONS), None, aid))
                    counts["gate_dropped"] += 1
            store.executemany("UPDATE articles SET gate_geoeconomic=?, gate_score=?, gate_country=?, gate_reason=?, "
                              "country_fips=COALESCE(?, country_fips) WHERE article_id=?", rows)
            _sleep()
            glog.info("gate batch %d/%d", bi, len(batches))
            report(bi, len(batches), f"gate batch {bi}/{len(batches)}")
        glog.info("gate: %s", counts)
        return counts

    def cluster(run: dict, report: Report, cancel: threading.Event) -> dict:
        rid = run["run_id"]
        store.execute("DELETE FROM synthesis_records WHERE run_id=?", (rid,))
        store.execute("DELETE FROM clusters WHERE run_id=?", (rid,))
        kept = store.query("SELECT article_id, gate_country, gate_score FROM articles WHERE run_id=? AND gate_geoeconomic=1 "
                           "ORDER BY article_id", (rid,))
        by_country: dict[str, list] = {}
        for r in kept:
            by_country.setdefault(r["gate_country"], []).append(r)
        rows = []
        for ci, (fips, arts) in enumerate(sorted(by_country.items()), 1):
            _check_cancel(cancel)
            client.complete(SYNTH_MODEL, [{"role": "system", "content": "group"}, {"role": "user", "content": "x" * 240 * len(arts)}],
                            temperature=0, max_tokens=8192)
            i = 0
            while i < len(arts):
                size = min(rng.randint(1, 4), len(arts) - i)
                chunk = arts[i:i + size]; i += size
                rows.append((rid, f"p1:{rng.getrandbits(80):020x}", BUCKET_OF_FIPS.get(fips, "other"), fips, size,
                             round(sum(a["gate_score"] for a in chunk) / len(chunk), 3)))
            _sleep()
            report(ci, len(by_country), f"country {fips}")
        store.executemany("INSERT INTO clusters(run_id, event_fingerprint, region, country_fips, article_count, gate_score) "
                          "VALUES (?,?,?,?,?,?)", rows)
        clog.info("cluster: %d clusters from %d gated articles", len(rows), len(kept))
        return {"clusters": len(rows), "gated_articles": len(kept)}

    def synthesis(run: dict, report: Report, cancel: threading.Event) -> dict:
        rid = run["run_id"]
        cl = [r["cluster_id"] for r in store.query("SELECT cluster_id FROM clusters WHERE run_id=? ORDER BY cluster_id", (rid,))]
        counts = {"records_total": 0, "records_ok": 0, "records_held": 0}
        now = iso(datetime.now(timezone.utc))
        for i, cid in enumerate(cl, 1):
            _check_cancel(cancel)
            client.complete(SYNTH_MODEL, [{"role": "system", "content": "synth"}, {"role": "user", "content": "x" * 30000}],
                            temperature=0, max_tokens=16384)
            held = rng.random() < 0.08
            flag, detail = ("empty_figures", json.dumps({"reasons": ["key_figures empty (synthetic)"], "action": "hold"})) if held else ("ok", None)
            store.execute("INSERT INTO synthesis_records(run_id, cluster_id, headline_en, synthesis_quality_flag, quality_detail, "
                          "is_headline_only, created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id, cluster_id) DO UPDATE SET "
                          "synthesis_quality_flag=excluded.synthesis_quality_flag, quality_detail=excluded.quality_detail",
                          (rid, cid, rng.choice(TITLE_BANK).format(n=rng.randint(2, 400)), flag, detail, 0, now))
            counts["records_total"] += 1
            counts["records_held" if held else "records_ok"] += 1
            _sleep()
            slog.info("synth record %d", i)
            report(i, len(cl), f"cluster {i}/{len(cl)}")
        slog.info("synthesis: %s", counts)
        return counts

    def summary(run: dict, report: Report, cancel: threading.Event) -> dict:
        counts = {"summaries_total": 0, "summaries_ok": 0, "summaries_empty": 0}
        for i, b in enumerate(BUCKETS, 1):
            _check_cancel(cancel)
            client.complete(SYNTH_MODEL, [{"role": "system", "content": "summary"}, {"role": "user", "content": "x" * 7000}],
                            temperature=0, max_tokens=8192)
            counts["summaries_total"] += 1; counts["summaries_ok"] += 1
            _sleep()
            report(i, len(BUCKETS), f"bucket {b}")
        return counts

    def render(run: dict, report: Report, cancel: threading.Event) -> dict:
        rid = run["run_id"]
        store.execute("DELETE FROM newsletters WHERE run_id=?", (rid,))
        ok = store.query("SELECT c.region, COUNT(*) AS n FROM synthesis_records s JOIN clusters c ON c.cluster_id=s.cluster_id "
                         "WHERE s.run_id=? AND s.synthesis_quality_flag='ok' GROUP BY c.region", (rid,))
        by_bucket = {r["region"]: r["n"] for r in ok}
        total = 0
        made = 0
        for i, b in enumerate(BUCKETS, 1):
            _check_cancel(cancel)
            n = min(by_bucket.get(b, 0), 12)
            if n:
                html = f"<!doctype html><html><body><h1>Geoeconomic Brief — {b}</h1><p>{n} records (synthetic, run {rid}).</p></body></html>"
                store.execute("INSERT INTO newsletters(run_id, bucket, render_mode, archive_html_path, email_html, record_count, "
                              "send_status) VALUES (?,?,?,?,?,?,'pending')",
                              (rid, b, "en_only", f"/data/archive/{run['window_end']:%Y-%m-%d}-{b}.html", html, n))
                total += n; made += 1
                rlog.info("render[%s]: %d records -> %s", b, n, f"/data/archive/{b}.html")
            _sleep()
            report(i, len(BUCKETS), f"render {b}")
        return {"record_count": total, "newsletters": made}

    def send(run: dict, report: Report, cancel: threading.Event) -> dict:
        rid = run["run_id"]
        nls = store.query("SELECT newsletter_id, bucket FROM newsletters WHERE run_id=? AND send_status<>'sent'", (rid,))
        if not nls:
            return {"send_status": "skipped_empty"}
        results = []
        for i, r in enumerate(nls, 1):
            _check_cancel(cancel); _sleep()
            store.execute("UPDATE newsletters SET send_status='sent', sent_at=? WHERE newsletter_id=?",
                          (iso(datetime.now(timezone.utc)), r["newsletter_id"]))
            results.append([r["bucket"], "sent"])
            report(i, len(nls), f"sent {r['bucket']}")
        return {"send_status": "sent", "sent": results}

    return {
        "discovery": discovery, "discovery_registry": discovery_registry, "extraction": extraction,
        "gate": gate, "cluster": cluster, "synthesis": synthesis, "summary": summary,
        "render": render, "send": send,
    }


class FakeLLM:
    """Stands in for `geoecon.llm.client.LLMClient` in synthetic mode: returns a response
    object with token counts and never touches the network. `tokens_per_call` lets a test
    make a stage overspend its budget."""

    def __init__(self, prompt_tokens: int = 3000, completion_tokens: int = 600, latency_s: float = 0.0):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.latency_s = latency_s
        self.calls = 0

    def complete(self, model: str, messages: list[dict], **params: Any) -> Any:
        self.calls += 1
        if self.latency_s:
            time.sleep(self.latency_s)
        from types import SimpleNamespace
        return SimpleNamespace(model=model, text="[]", final_answer="[]", latency_ms=int(self.latency_s * 1000),
                               timestamp=iso(datetime.now(timezone.utc)), prompt_tokens=self.prompt_tokens,
                               completion_tokens=self.completion_tokens, reasoning_tokens=None,
                               finish_reason="stop", truncated=False, error=None, raw={}, ok=lambda: True)

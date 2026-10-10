"""Pipeline health: the watchdog's own verdict plus stale-run reconciliation.

`geoecon.watchdog.check(repo, day_start, day_end)` is a pure decision over three repo
reads. We hand it a view of our `Store` so the SAME code decides in synthetic and in
Postgres mode, then add what the watchdog cannot know: how many runs are still `running`
long after they started (a process that died without unwinding — SIGKILL, node loss)."""

from __future__ import annotations

from datetime import datetime

from geoecon import watchdog

from .store import STALE_AFTER, Store


class _RepoView:
    """The three Repo methods `watchdog.check` calls, served from a Store."""

    def __init__(self, store: Store):
        self._s = store

    def latest_run_for_window(self, start, end):
        return self._s.latest_run_for_window(start, end)

    def newsletters_for_run(self, run_id):
        return self._s.newsletters(run_id)

    def failed_stages(self, run_id):
        return self._s.failed_stages(run_id)


def verdict(store: Store, now: datetime | None = None, tz_name: str = "Europe/Prague") -> dict:
    now = now or store.now()
    day_start, day_end = watchdog.local_day_window(now, tz_name)
    v = watchdog.check(_RepoView(store), day_start, day_end)
    latest = store.latest_run_for_window(day_start, day_end)
    stale = store.stale_running_runs(now - STALE_AFTER)
    ok = bool(v["ok"]) and not stale
    note = None
    if stale:
        ids = ", ".join(str(r["run_id"]) for r in stale)
        note = (f"{len(stale)} run(s) still 'running' more than {int(STALE_AFTER.total_seconds() // 3600)} h after "
                f"start: {ids}. A process died without unwinding; start_job(reconcile_abandoned) closes them out.")
    return {
        "ok": ok, "subject": v["subject"], "body": v["body"], "checked_at": now,
        "day_start": day_start, "day_end": day_end,
        "latest_run_id": latest["run_id"] if latest else None,
        "stale_running": len(stale), "stale_run_ids": [r["run_id"] for r in stale], "note": note,
    }

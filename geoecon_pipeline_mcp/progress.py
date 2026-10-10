"""Progress for a running stage, derived from the pipeline's own signals.

The stage functions were not written to report progress, so the runner listens to what they
already emit:

* `LogCounter` — a `logging.Handler` on the `geoecon` logger tree, filtered to the job's
  thread, that turns known log lines into (done, total, message) calls. It understands the
  stages' end-of-stage summaries (`gate: {...}`, `extraction: {...}`, `synthesis: {...}`,
  `cluster: N clusters from M gated articles`, `render[bucket]: N records`) and the
  per-unit lines the synthetic stages emit (`extracted N`, `gate batch i/n`, `synth record N`).
* `DbCounter` — a thread that polls row counts every few seconds and reports
  `(rows touched, rows expected)` for the stages whose work is visible in the tables
  (extraction, gate, synthesis). Used in Postgres mode, where log lines are sparse.
"""

from __future__ import annotations

import ast
import logging
import re
import threading
from collections.abc import Callable

Report = Callable[[int, int | None, str], None]

_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^extracted (\d+)$"), "extracted"),
    (re.compile(r"^gate batch (\d+)/(\d+)$"), "gate_batch"),
    (re.compile(r"^synth record (\d+)$"), "synth_record"),
    (re.compile(r"^cluster: (\d+) clusters from (\d+) gated articles$"), "cluster_done"),
    (re.compile(r"^render\[(\w+)\]: (\d+) records"), "render_bucket"),
    (re.compile(r"^(extraction|gate|synthesis|discovery|discovery_registry|discovery_search|summary): (\{.*\})$"), "summary"),
]


def parse_line(message: str) -> tuple[int, int | None, str] | None:
    """Map one geoecon log line to (done, total, message); None when it is not a progress line."""
    for rx, kind in _PATTERNS:
        m = rx.match(message.strip())
        if not m:
            continue
        if kind == "extracted":
            return int(m.group(1)), None, message
        if kind == "gate_batch":
            return int(m.group(1)), int(m.group(2)), message
        if kind == "synth_record":
            return int(m.group(1)), None, message
        if kind == "cluster_done":
            return int(m.group(1)), int(m.group(1)), message
        if kind == "render_bucket":
            return int(m.group(2)), None, message
        if kind == "summary":
            try:
                counts = ast.literal_eval(m.group(2))
            except (ValueError, SyntaxError):
                return None
            total = sum(v for v in counts.values() if isinstance(v, int)) if isinstance(counts, dict) else 0
            key = {"extraction": None, "gate": "gate_total", "synthesis": "records_total"}.get(m.group(1))
            done = counts.get(key, total) if isinstance(counts, dict) and key else total
            return int(done), int(done), message
    return None


class LogCounter(logging.Handler):
    """Attach to the `geoecon` logger tree for the duration of a job and forward progress
    lines emitted on the job's thread (`thread_ident`) to `report`."""

    def __init__(self, report: Report, thread_ident: int | None = None, logger_name: str = "geoecon"):
        super().__init__(level=logging.INFO)
        self.report = report
        self.thread_ident = thread_ident
        self.logger_name = logger_name
        self.seen = 0

    def emit(self, record: logging.LogRecord) -> None:
        if self.thread_ident is not None and record.thread != self.thread_ident:
            return
        try:
            parsed = parse_line(record.getMessage())
        except Exception:  # noqa: BLE001 — a malformed log line is not our problem
            return
        if parsed is None:
            return
        self.seen += 1
        done, total, message = parsed
        try:
            self.report(done, total, message)
        except Exception:  # noqa: BLE001
            pass

    def install(self) -> "LogCounter":
        lg = logging.getLogger(self.logger_name)
        lg.addHandler(self)
        if lg.level == logging.NOTSET or lg.level > logging.INFO:
            self._prev_level = lg.level
            lg.setLevel(logging.INFO)
        else:
            self._prev_level = None
        return self

    def remove(self) -> None:
        lg = logging.getLogger(self.logger_name)
        lg.removeHandler(self)
        if getattr(self, "_prev_level", None) is not None:
            lg.setLevel(self._prev_level)

    def __enter__(self) -> "LogCounter":
        return self.install()

    def __exit__(self, *exc) -> None:
        self.remove()


def counts_for_stage(stage: str, inputs: dict[str, int]) -> tuple[int, int] | None:
    if stage == "extraction":
        return inputs["articles"] - inputs["pending_fetch"], inputs["articles"]
    if stage == "gate":
        return inputs["gated"], inputs["fetchable"]
    if stage == "synthesis":
        return inputs["records"], inputs["clusters"]
    if stage == "render":
        return inputs["newsletters"], inputs.get("buckets", 3)
    return None


class DbCounter(threading.Thread):
    """Poll `store.stage_inputs(run_id)` every `interval` seconds and report the stage's counts."""

    def __init__(self, store, run_id: int, stage: str, report: Report, interval: float = 5.0):
        super().__init__(name=f"dbcounter-{run_id}-{stage}", daemon=True)
        self.store, self.run_id, self.stage, self.report, self.interval = store, run_id, stage, report, interval
        self._halt = threading.Event()
        self.samples = 0

    def run(self) -> None:
        while not self._halt.wait(self.interval):
            self.sample()

    def sample(self) -> tuple[int, int] | None:
        try:
            c = counts_for_stage(self.stage, self.store.stage_inputs(self.run_id))
        except Exception:  # noqa: BLE001 — a failed poll just skips a sample
            return None
        if c is not None:
            self.samples += 1
            self.report(c[0], c[1], f"{self.stage}: {c[0]}/{c[1]} rows")
        return c

    def stop(self) -> None:
        self._halt.set()

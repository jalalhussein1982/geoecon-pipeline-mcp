"""Progress counters: log-line parsing and DB polling."""

from __future__ import annotations

import logging
import threading

from geoecon_pipeline_mcp.progress import DbCounter, LogCounter, counts_for_stage, parse_line


def test_parse_known_lines():
    assert parse_line("extracted 40") == (40, None, "extracted 40")
    assert parse_line("gate batch 3/16") == (3, 16, "gate batch 3/16")
    assert parse_line("synth record 7") == (7, None, "synth record 7")
    assert parse_line("cluster: 21 clusters from 50 gated articles")[:2] == (21, 21)
    assert parse_line("render[latam]: 12 records -> /data/archive/x.html")[:2] == (12, None)
    g = parse_line("gate: {'gate_total': 240, 'gate_kept': 50, 'gate_dropped': 190, 'gate_unparsed': 0, 'gate_unparsed_dropped': 0}")
    assert g[:2] == (240, 240)
    e = parse_line("extraction: {'extracted': 180, 'headline_only': 40, 'failed': 20}")
    assert e[:2] == (240, 240)
    assert parse_line("llm deepseek attempt 1/5 failed after 20ms: http 500") is None
    assert parse_line("gate: not a dict") is None


def test_log_counter_filters_by_thread_and_reports():
    got = []
    counter = LogCounter(lambda d, t, m: got.append((d, t)), threading.get_ident())
    lg = logging.getLogger("geoecon.gate")
    with counter:
        lg.info("gate batch %d/%d", 1, 4)
        lg.info("something unrelated")
        other = threading.Thread(target=lambda: lg.info("gate batch %d/%d", 2, 4))
        other.start(); other.join()
        lg.info("gate batch %d/%d", 3, 4)
    lg.info("gate batch %d/%d", 4, 4)                      # handler removed: not counted
    assert got == [(1, 4), (3, 4)]
    assert counter.seen == 2


def test_db_counter_reports_table_counts(store):
    got = []
    c = DbCounter(store, 104, "gate", lambda d, t, m: got.append((d, t, m)), interval=60)
    assert c.sample() is not None
    done, total, msg = got[0]
    inputs = store.stage_inputs(104)
    assert (done, total) == (inputs["gated"], inputs["fetchable"]) and 0 < done < total
    assert "gate" in msg
    assert counts_for_stage("discovery", inputs) is None
    assert counts_for_stage("extraction", inputs) == (inputs["articles"], inputs["articles"])
    c.start(); c.stop(); c.join(1)

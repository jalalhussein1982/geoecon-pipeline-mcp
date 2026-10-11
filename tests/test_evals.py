"""Every QA pair in evals/geoecon-pipeline.xml is verified against the server, check by check."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from typing import Any

import pytest

from .conftest import ROOT

EVALS = ET.parse(ROOT / "evals" / "geoecon-pipeline.xml").getroot()
CASES = [(e.get("id"), e) for e in EVALS.findall("eval")]


def walk(value: Any, path: str) -> Any:
    """'result.0.run_id' · 'result.*.flag' (map) · 'result[state=silent].registry_id' (filter)."""
    if not path:
        return value
    head, _, rest = path.partition(".")
    m = re.fullmatch(r"(\w+)\[(\w+)=(\w+)\]", head)
    if m:
        key, fk, fv = m.groups()
        items = [x for x in value[key] if str(x.get(fk)) == fv]
        return [walk(x, rest) for x in items] if rest else items
    if head == "*":
        return [walk(x, rest) for x in value]
    nxt = value[int(head)] if isinstance(value, list) else value[head]
    return walk(nxt, rest)


def test_eval_file_has_ten_pairs_with_questions_answers_and_checks():
    assert len(CASES) == 10
    for eid, e in CASES:
        assert e.findtext("question") and e.findtext("answer") and e.findall("check"), eid
        assert e.get("tools"), eid


@pytest.mark.anyio
@pytest.mark.parametrize("eid, eval_el", CASES, ids=[c[0] for c in CASES])
async def test_eval_answer_is_verified_by_the_tools(client, eid, eval_el):
    tools_named = set(eval_el.get("tools").split())
    for chk in eval_el.findall("check"):
        tool, args, path = chk.get("tool"), json.loads(chk.get("args") or "{}"), chk.get("path") or ""
        assert tool in tools_named, f"eval {eid}: check uses {tool} not listed in tools="
        r = await client.call_tool(tool, args)
        assert r.is_error is False, f"eval {eid}: {tool}({args}) -> {r.content[0].text}"
        got = walk(r.structured_content, path)
        if chk.get("len") is not None:
            assert len(got) == int(chk.get("len")), f"eval {eid}: len({path})={len(got)}"
        if chk.get("expect") is not None:
            assert got == json.loads(chk.get("expect")), f"eval {eid}: {path}={got!r}"
        if chk.get("set") is not None:
            assert sorted(map(str, got)) == sorted(map(str, json.loads(chk.get("set")))), f"eval {eid}: {path}={got!r}"
        if chk.get("contains") is not None:
            assert chk.get("contains") in str(got), f"eval {eid}: {path}={got!r}"
        if chk.get("min") is not None:
            assert float(got) >= float(chk.get("min")), f"eval {eid}: {path}={got!r}"
        if chk.get("max") is not None:
            assert float(got) <= float(chk.get("max")), f"eval {eid}: {path}={got!r}"

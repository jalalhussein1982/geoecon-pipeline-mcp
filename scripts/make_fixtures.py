"""Rebuild fixtures/synthetic.db deterministically. Run via `make fixtures`."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from geoecon_pipeline_mcp.synth import FIXTURES_DIR, build_synthetic, canonical_dump  # noqa: E402


def main() -> int:
    out = FIXTURES_DIR / "synthetic.db"
    store = build_synthetic(out)
    dump = canonical_dump(store)
    runs = store.list_runs(None, None, 50)
    store.close()
    print(f"wrote {out} ({out.stat().st_size} bytes, {len(dump.splitlines())} dump lines)")
    for r in runs:
        print(f"  run {r['run_id']}: {r['status']:<9} stages {r['stages_done']}/{r['stages_total']} "
              f"last_failed={r['last_failed_stage']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

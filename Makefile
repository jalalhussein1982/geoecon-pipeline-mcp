.PHONY: sync test db-test fixtures stdio http inspector

sync:
	uv sync

test:
	uv run pytest -q

db-test:
	uv run pytest -q -m db tests/db

fixtures:
	uv run python scripts/make_fixtures.py

stdio:
	GEOECON_MODE=synthetic uv run python -m geoecon_pipeline_mcp.server --stdio

http:
	GEOECON_MODE=synthetic uv run python -m geoecon_pipeline_mcp.server --http --port 8000

inspector:
	GEOECON_MODE=synthetic uv run mcp dev geoecon_pipeline_mcp/server.py

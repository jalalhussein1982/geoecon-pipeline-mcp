# geoecon-pipeline-mcp

MCP server that operates the geoeconomic news pipeline: durable job handles for stage runs, progress, subscriptions, a cost gate, and incident-report prompts.

Built on [`jalalhussein1982/geoeconomics-news-fetcher`](https://github.com/jalalhussein1982/geoeconomics-news-fetcher). MCP protocol revision 2026-07-28, MCP Python SDK 2.x.

**Status:** design stage. The design document is at [`docs/superpowers/specs/2026-10-11-geoecon-pipeline-mcp-design.md`](docs/superpowers/specs/2026-10-11-geoecon-pipeline-mcp-design.md). Implementation follows once the design is approved.

## Why this exists

An agent as pipeline operator: inspect runs, resume a failed run from the right stage, watch progress, read what the validator held back, stay inside an LLM budget, and write the incident report in the house format. Long-running work returns a durable job handle on a stateless transport; the job model is the MCP tasks extension's Task object so the server adopts the extension when the SDK ships it.

## Related servers in this portfolio

- [`ihkam-eval-mcp`](https://github.com/jalalhussein1982/ihkam-eval-mcp) — read-only evaluation harness over a legal RAG (stdio)
- `phase1/siem-mcp` in [`conflict-trends-dashboard`](https://github.com/jalalhussein1982/conflict-trends-dashboard) — conflict analytics over PostGIS (stdio)
- `energy_platform/mcp` in [`energy-platform`](https://github.com/jalalhussein1982/energy-platform) — the contributor path of an ingestion platform (stdio)
- `hostel-ops-mcp`, `geoecon-pipeline-mcp`, `coin-wargame-mcp` — this series: remote transport and OAuth, long-running jobs, and MCP as an agent harness

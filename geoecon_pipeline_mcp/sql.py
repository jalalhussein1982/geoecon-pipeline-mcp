"""Read queries the pipeline's `Repo` does not offer, kept in one place (design spec §2).

Every statement targets the pipeline's own tables as created by its migrations 001-008 plus
our `mcp_jobs`. Parameters are psycopg `%s` placeholders, positional, in the order the
docstring of each constant lists.
"""

from __future__ import annotations

# runs with a stage summary — params: (status or NULL, status or NULL, since or NULL, since or NULL, limit)
LIST_RUNS = """
SELECT r.run_id, r.window_start, r.window_end, r.scheduled_for, r.started_at, r.finished_at, r.status,
       COALESCE(s.done, 0)  AS stages_done,
       COALESCE(s.total, 0) AS stage_rows,
       s.last_failed
FROM runs r
LEFT JOIN (
    SELECT run_id,
           COUNT(*) FILTER (WHERE status = 'completed') AS done,
           COUNT(*) AS total,
           (ARRAY_AGG(stage_name ORDER BY stage_run_id DESC) FILTER (WHERE status = 'failed'))[1] AS last_failed
    FROM stage_runs GROUP BY run_id
) s ON s.run_id = r.run_id
WHERE (%s::text IS NULL OR r.status = %s)
  AND (%s::timestamptz IS NULL OR r.started_at >= %s)
ORDER BY r.run_id DESC
LIMIT %s
"""

# one run — params: (run_id,)
RUN = """
SELECT run_id, window_start, window_end, scheduled_for, started_at, finished_at, status,
       synthesis_model, synthesis_prompt_id, embedding_model, metrics, notes
FROM runs WHERE run_id = %s
"""

# every stage row of a run — params: (run_id,)
STAGE_RUNS = """
SELECT stage_name, status, started_at, finished_at, checkpoint, error
FROM stage_runs WHERE run_id = %s ORDER BY stage_run_id
"""

# held records — params: (run_id, flag or NULL, flag or NULL)
HELD_RECORDS = """
SELECT s.record_id, s.cluster_id, s.synthesis_quality_flag, s.quality_detail, s.headline_en,
       s.is_headline_only, c.article_count
FROM synthesis_records s JOIN clusters c ON c.cluster_id = s.cluster_id
WHERE s.run_id = %s AND s.synthesis_quality_flag <> 'ok'
  AND (%s::text IS NULL OR s.synthesis_quality_flag = %s)
ORDER BY s.record_id
"""

# gate rejections page — params: (run_id, after_article_id, limit_plus_one)
GATE_REJECTIONS = """
SELECT article_id, title, domain, registry_id, gate_score, gate_reason
FROM articles
WHERE run_id = %s AND gate_geoeconomic = false AND article_id > %s
ORDER BY article_id
LIMIT %s
"""

# dropped / unparsed totals — params: (run_id,)
GATE_DROP_COUNTS = """
SELECT COUNT(*) AS dropped,
       COUNT(*) FILTER (WHERE gate_reason = 'gate_unparsed_dropped') AS unparsed
FROM articles WHERE run_id = %s AND gate_geoeconomic = false
"""

# per-source article counts — params: (run_id,)
SOURCE_COUNTS = """
SELECT registry_id, COUNT(*) AS n
FROM articles WHERE run_id = %s AND registry_id IS NOT NULL
GROUP BY registry_id
"""

# newsletters with send details — params: (run_id,)
NEWSLETTERS = """
SELECT newsletter_id, bucket, record_count, email_html, send_status, sent_at, archive_html_path, send_error
FROM newsletters WHERE run_id = %s ORDER BY bucket
"""

# counts the estimator and the DB progress counter work from — params: (run_id,) x4
STAGE_INPUTS_ARTICLES = """
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE fetch_status = 'pending') AS pending_fetch,
       COUNT(*) FILTER (WHERE fetch_status <> 'failed') AS fetchable,
       COUNT(*) FILTER (WHERE fetch_status <> 'failed' AND gate_geoeconomic IS NULL) AS pending_gate,
       COUNT(*) FILTER (WHERE gate_geoeconomic IS NOT NULL) AS gated,
       COUNT(*) FILTER (WHERE gate_geoeconomic = true) AS kept,
       COUNT(DISTINCT country_fips) FILTER (WHERE gate_geoeconomic = true) AS countries
FROM articles WHERE run_id = %s
"""
COUNT_CLUSTERS = "SELECT COUNT(*) FROM clusters WHERE run_id = %s"
COUNT_RECORDS = "SELECT COUNT(*) FROM synthesis_records WHERE run_id = %s"
COUNT_NEWSLETTERS = "SELECT COUNT(*) FROM newsletters WHERE run_id = %s"

LATEST_RUN_ID = "SELECT MAX(run_id) FROM runs"

# params: (note, note, run_id)
APPEND_RUN_NOTE = """
UPDATE runs SET notes = CASE WHEN notes IS NULL OR notes = '' THEN %s ELSE notes || ' | ' || %s END
WHERE run_id = %s
"""

# the funnel: how many articles survived each step — params: (run_id,)
FUNNEL = """
SELECT COUNT(*)                                                   AS discovered,
       COUNT(*) FILTER (WHERE fetch_status = 'extracted')        AS extracted,
       COUNT(*) FILTER (WHERE fetch_status = 'headline_only')    AS headline_only,
       COUNT(*) FILTER (WHERE fetch_status = 'failed')           AS fetch_failed,
       COUNT(*) FILTER (WHERE gate_geoeconomic = true)           AS gate_kept,
       COUNT(*) FILTER (WHERE gate_geoeconomic = false)          AS gate_dropped,
       (SELECT COUNT(*) FROM clusters c WHERE c.run_id = a.run_id) AS clusters,
       (SELECT COUNT(*) FROM synthesis_records s WHERE s.run_id = a.run_id AND s.synthesis_quality_flag = 'ok') AS records_ok,
       (SELECT COUNT(*) FROM synthesis_records s WHERE s.run_id = a.run_id AND s.synthesis_quality_flag <> 'ok') AS records_held
FROM articles a WHERE a.run_id = %s GROUP BY a.run_id
"""

# ── mcp_jobs ────────────────────────────────────────────────────────────────────────

JOB_COLUMNS = ("job_id", "kind", "run_id", "stages_planned", "status", "stage", "progress", "input_request",
               "input_response", "budget_usd", "usage", "result", "error", "principal", "trace",
               "created_at", "updated_at", "finished_at", "ttl_expires_at")

JOB_INSERT = f"""
INSERT INTO mcp_jobs({', '.join(JOB_COLUMNS)})
VALUES (%s::uuid, {', '.join(['%s'] * (len(JOB_COLUMNS) - 1))})
"""
JOB_GET = f"SELECT {', '.join(JOB_COLUMNS)} FROM mcp_jobs WHERE job_id = %s::uuid"
JOBS_WORKING = f"SELECT {', '.join(JOB_COLUMNS)} FROM mcp_jobs WHERE status IN ('working','input_required') ORDER BY created_at"
JOBS_FOR_RUN = f"SELECT {', '.join(JOB_COLUMNS)} FROM mcp_jobs WHERE run_id = %s ORDER BY created_at"

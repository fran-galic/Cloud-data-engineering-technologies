{{ config(
    materialized='table',
    partition_by={"field": "report_date", "data_type": "date"},
    cluster_by=["sentiment"]
) }}

WITH daily AS (
  SELECT
    created_date AS report_date,
    sentiment,

    COUNT(*) AS post_count,
    AVG(score_filled) AS avg_score,
    AVG(view_count_filled) AS avg_views,
    AVG(answer_count_filled) AS avg_answers,

    AVG(CAST(is_answered AS INT64)) AS answered_rate,
    AVG(CAST(is_closed AS INT64)) AS closed_rate,

    AVG(hours_to_close) AS avg_hours_to_close
  FROM {{ ref('stackoverflow_questions_silver') }}
  GROUP BY report_date, sentiment
),

w AS (
  SELECT
    *,
    SUM(post_count) OVER (
      PARTITION BY sentiment
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_post_count,

    LAG(post_count) OVER (
      PARTITION BY sentiment
      ORDER BY report_date
    ) AS prev_day_post_count
  FROM daily
)

SELECT
  report_date,
  sentiment,
  post_count,
  avg_score,
  avg_views,
  avg_answers,
  answered_rate,
  closed_rate,
  avg_hours_to_close,
  rolling_7d_post_count,
  CASE
    WHEN prev_day_post_count IS NULL OR prev_day_post_count = 0 THEN NULL
    ELSE SAFE_DIVIDE(post_count - prev_day_post_count, prev_day_post_count) * 100
  END AS day_over_day_change_pct
FROM w

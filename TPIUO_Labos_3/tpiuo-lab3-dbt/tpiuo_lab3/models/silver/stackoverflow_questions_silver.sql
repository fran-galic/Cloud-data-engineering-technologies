{{ config(
    materialized='table',
    partition_by={"field": "created_date", "data_type": "date"},
    cluster_by=["sentiment", "is_answered", "is_closed"]
) }}

WITH src AS (
  SELECT * FROM {{ ref('stackoverflow_questions_bronze') }}
),

dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY question_id
      ORDER BY
        COALESCE(last_activity_date, creation_date) DESC,
        creation_date DESC
    ) AS rn
  FROM src
)

SELECT
  question_id,

  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(title, r'[^A-Za-z0-9\s]', ' '),
      r'\s+',
      ' '
    )
  ) AS title_clean,

  link,

  -- epoch seconds -> timestamp/date
  TIMESTAMP_SECONDS(creation_date) AS created_ts,
  DATE(TIMESTAMP_SECONDS(creation_date)) AS created_date,

  TIMESTAMP_SECONDS(last_activity_date) AS last_activity_ts,
  DATE(TIMESTAMP_SECONDS(last_activity_date)) AS last_activity_date,

  CASE
    WHEN closed_date IS NULL THEN NULL
    ELSE TIMESTAMP_SECONDS(closed_date)
  END AS closed_ts,

  CASE
    WHEN closed_date IS NULL THEN NULL
    ELSE DATE(TIMESTAMP_SECONDS(closed_date))
  END AS closed_date,

  CASE WHEN closed_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_closed,
  is_answered,

  COALESCE(score, 0) AS score_filled,
  COALESCE(answer_count, 0) AS answer_count_filled,
  COALESCE(view_count, 0) AS view_count_filled,

  content_license,
  closed_reason,
  owner_user_id,
  owner_display_name,

  -- derived sentiment/bucket for lab visualizations
  CASE
    WHEN COALESCE(view_count, 0) >= 1000 OR COALESCE(answer_count, 0) >= 5 THEN 'HIGH_ENGAGEMENT'
    WHEN COALESCE(score, 0) >= 5 THEN 'POSITIVE'
    WHEN COALESCE(score, 0) < 0 THEN 'NEGATIVE'
    ELSE 'NEUTRAL'
  END AS sentiment,

  -- time-to-close in hours (null if not closed)
  CASE
    WHEN closed_date IS NULL THEN NULL
    ELSE TIMESTAMP_DIFF(TIMESTAMP_SECONDS(closed_date), TIMESTAMP_SECONDS(creation_date), HOUR)
  END AS hours_to_close

FROM dedup
WHERE
  rn = 1
  AND question_id IS NOT NULL
  AND title IS NOT NULL
  AND LENGTH(TRIM(title)) > 0

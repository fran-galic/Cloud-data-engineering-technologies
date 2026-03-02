{{ config(materialized='view') }}

SELECT *
FROM `tpiuo-labosi.stackoverflow_pipeline.stackoverflow_questions`

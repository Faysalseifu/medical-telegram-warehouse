{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT DATE(message_date) AS full_date
    FROM {{ ref('stg_telegram_messages') }}
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::int            AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date)                   AS year,
    EXTRACT(MONTH FROM full_date)                  AS month,
    TO_CHAR(full_date, 'Month')                    AS month_name,
    EXTRACT(DAY FROM full_date)                    AS day,
    EXTRACT(DOW FROM full_date)                    AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
FROM dates
ORDER BY full_date

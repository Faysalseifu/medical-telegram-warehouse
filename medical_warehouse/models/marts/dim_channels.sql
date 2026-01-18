{{ config(materialized='table') }}

WITH channels AS (
    SELECT
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*)          AS total_posts
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
)
SELECT
    ROW_NUMBER() OVER (ORDER BY channel_name) AS channel_key,
    channel_name,
    CASE
        WHEN channel_name ILIKE '%cosmetic%' THEN 'Cosmetics'
        WHEN channel_name ILIKE '%pharma%' THEN 'Pharmaceutical'
        ELSE 'Medical'
    END AS channel_type,
    first_post_date,
    last_post_date,
    total_posts
FROM channels

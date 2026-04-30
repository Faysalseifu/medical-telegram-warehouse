{{ config(materialized='table') }}

WITH channels AS (
    SELECT
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*)          AS total_posts
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),
channel_type_map AS (
    SELECT 1 AS priority, 'cosmetic' AS keyword, 'Cosmetics' AS channel_type
    UNION ALL
    SELECT 2 AS priority, 'pharma' AS keyword, 'Pharmaceutical' AS channel_type
),
channel_type_match AS (
    SELECT
        c.channel_name,
        m.channel_type,
        m.priority,
        ROW_NUMBER() OVER (
            PARTITION BY c.channel_name
            ORDER BY m.priority
        ) AS match_rank
    FROM channels AS c
    LEFT JOIN channel_type_map AS m
        ON POSITION(m.keyword IN LOWER(c.channel_name)) > 0
),
channel_typed AS (
    SELECT
        channel_name,
        COALESCE(
            MAX(CASE WHEN match_rank = 1 THEN channel_type END),
            'Medical'
        ) AS channel_type
    FROM channel_type_match
    GROUP BY channel_name
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['channels.channel_name']) }} AS channel_key,
    channels.channel_name,
    channel_typed.channel_type,
    channels.first_post_date,
    channels.last_post_date,
    channels.total_posts
FROM channels
JOIN channel_typed
    ON channels.channel_name = channel_typed.channel_name

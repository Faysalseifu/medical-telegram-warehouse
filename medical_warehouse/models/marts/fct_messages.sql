{{ config(materialized='table') }}

SELECT
    m.message_id,
    c.channel_key,
    d.date_key,
    m.channel_name,
    m.message_date,
    m.message_text,
    m.message_length,
    m.views,
    m.forwards,
    m.has_media,
    m.image_path
FROM {{ ref('stg_telegram_messages') }} AS m
JOIN {{ ref('dim_channels') }} AS c ON m.channel_name = c.channel_name
JOIN {{ ref('dim_dates') }}    AS d ON DATE(m.message_date) = d.full_date

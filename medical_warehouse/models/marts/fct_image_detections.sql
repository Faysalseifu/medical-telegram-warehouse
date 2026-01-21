{{ config(materialized='table') }}

SELECT
    m.message_id,
    c.channel_key,
    d.date_key,
    y.category AS image_category,
    y.max_confidence AS confidence_score,
    y.detections,
    y.image_path
FROM {{ ref('fct_messages') }} m
LEFT JOIN {{ ref('dim_channels') }} c ON m.channel_name = c.channel_name
LEFT JOIN {{ ref('dim_dates') }} d ON DATE(m.message_date) = d.full_date
LEFT JOIN raw.yolo_detections y
    ON m.message_id = y.message_id::BIGINT
    AND m.channel_name = y.channel_name
WHERE y.category IS NOT NULL

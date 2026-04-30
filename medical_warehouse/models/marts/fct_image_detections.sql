{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'm.message_id',
        'y.category',
        'y.image_path'
    ]) }} AS image_detection_key,
    m.message_id,
    m.channel_key,
    m.date_key,
    y.category AS image_category,
    y.max_confidence AS confidence_score,
    y.detections,
    y.image_path
FROM {{ ref('fct_messages') }} AS m
LEFT JOIN raw.yolo_detections AS y
    ON m.message_id = y.message_id::BIGINT
    AND m.channel_name = y.channel_name
WHERE y.category IS NOT NULL

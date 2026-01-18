{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
)

SELECT
    message_id::bigint                        AS message_id,
    channel_name,
    message_date::timestamptz                 AS message_date,
    message_text,
    LENGTH(message_text)                      AS message_length,
    has_media,
    image_path,
    views::int,
    forwards::int,
    CASE WHEN message_text IS NULL OR TRIM(message_text) = '' THEN TRUE ELSE FALSE END AS is_empty_message,
    loaded_at
FROM source
WHERE message_date IS NOT NULL
  AND message_id IS NOT NULL

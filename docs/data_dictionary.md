# Data Dictionary

This document describes the star schema used in the warehouse. The pipeline loads raw Telegram data into `raw` schema tables, stages it in `staging`, and models facts and dimensions in `marts`.

## raw.telegram_messages
Raw messages loaded from JSON exports produced by the scraper.

| Column | Type | Description |
| --- | --- | --- |
| channel_name | text | Telegram channel name used for grouping. |
| message_id | bigint | Telegram message id (unique within a channel). |
| message_date | timestamptz | Timestamp of the message post. |
| message_text | text | Raw message text. |
| has_media | boolean | Whether the message contains media. |
| image_path | text | Local path to the image (if downloaded). |
| views | integer | View count from Telegram metadata. |
| forwards | integer | Forward count from Telegram metadata. |
| loaded_at | timestamptz | Load timestamp (defaults to current time). |

Primary key: `(channel_name, message_id)`.

## stg_telegram_messages
Cleaned staging view over `raw.telegram_messages` with basic type casting and convenience fields.

| Column | Type | Description |
| --- | --- | --- |
| message_id | bigint | Message id cast to bigint. |
| channel_name | text | Channel name. |
| message_date | timestamptz | Timestamp of the message post. |
| message_text | text | Raw message text. |
| message_length | integer | Character length of the message text. |
| has_media | boolean | Whether the message contains media. |
| image_path | text | Local path to the image (if downloaded). |
| views | integer | View count. |
| forwards | integer | Forward count. |
| is_empty_message | boolean | True when text is null or empty. |
| loaded_at | timestamptz | Load timestamp from raw table. |

## dim_dates
Date dimension built from distinct message dates.

| Column | Type | Description |
| --- | --- | --- |
| date_key | integer | Surrogate key in YYYYMMDD format. |
| full_date | date | Calendar date. |
| year | integer | Calendar year. |
| month | integer | Month number (1-12). |
| month_name | text | Month name. |
| day | integer | Day of month. |
| day_of_week | integer | Day of week (0=Sunday, 6=Saturday). |
| is_weekend | boolean | True if Saturday or Sunday. |

## dim_channels
Channel dimension summarizing each Telegram channel.

| Column | Type | Description |
| --- | --- | --- |
| channel_key | integer | Surrogate key for the channel. |
| channel_name | text | Telegram channel name. |
| channel_type | text | Category derived from name (Cosmetics, Pharmaceutical, Medical). |
| first_post_date | timestamptz | Earliest message date. |
| last_post_date | timestamptz | Most recent message date. |
| total_posts | integer | Total messages observed. |

## fct_messages
Core fact table for message-level analytics.

| Column | Type | Description |
| --- | --- | --- |
| message_id | bigint | Message id from Telegram. |
| channel_key | integer | Foreign key to `dim_channels`. |
| date_key | integer | Foreign key to `dim_dates`. |
| channel_name | text | Denormalized channel name. |
| message_date | timestamptz | Message timestamp. |
| message_text | text | Message text content. |
| message_length | integer | Character length of the message text. |
| views | integer | View count. |
| forwards | integer | Forward count. |
| has_media | boolean | Whether the message contains media. |
| image_path | text | Image path if present. |

## fct_image_detections
Fact table for image-level YOLO detections joined to message metadata.

| Column | Type | Description |
| --- | --- | --- |
| message_id | bigint | Message id from Telegram. |
| channel_key | integer | Foreign key to `dim_channels`. |
| date_key | integer | Foreign key to `dim_dates`. |
| image_category | text | YOLO-derived category (promotional, product_display, lifestyle, other). |
| confidence_score | float | Max confidence score for relevant detections. |
| detections | text | Semicolon-delimited class:confidence pairs. |
| image_path | text | Image path in local storage. |

Source notes: detection data is loaded into `raw.yolo_detections` from the YOLO CSV and then joined to `fct_messages`.

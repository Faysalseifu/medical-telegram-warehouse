from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import (
    CategoryPerformance,
    MessageStats,
    TopProduct,
    VisualContentReport,
)

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/top-products", response_model=List[TopProduct])
def top_products(
    limit: int = Query(10, ge=1, le=50),
    min_count: int = Query(3, ge=1),
    db: Session = Depends(get_db),
):
    query = text(
        """
        WITH exploded AS (
            SELECT
                lower(word) AS keyword,
                fm.channel_name
            FROM fct_messages fm,
            LATERAL regexp_matches(coalesce(fm.message_text, ''), '\\b[a-z]{4,}\\b', 'g') AS word
        )
        SELECT keyword,
               COUNT(*) AS mention_count,
               COUNT(DISTINCT channel_name) AS appearing_in_channels
        FROM exploded
        GROUP BY keyword
        HAVING COUNT(*) >= :min_count
        ORDER BY mention_count DESC
        LIMIT :limit
        """
    )

    rows = db.execute(query, {"min_count": min_count, "limit": limit}).fetchall()
    return [
        TopProduct(
            keyword=row.keyword,
            mention_count=row.mention_count,
            appearing_in_channels=row.appearing_in_channels,
        )
        for row in rows
    ]


@router.get("/visual-content", response_model=List[VisualContentReport])
def visual_content(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    query = text(
        """
        WITH msg AS (
            SELECT m.channel_name,
                   COUNT(*) AS total_messages,
                   SUM(CASE WHEN fid.image_category IS NOT NULL THEN 1 ELSE 0 END) AS visual_messages
            FROM fct_messages m
            LEFT JOIN fct_image_detections fid
              ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
            GROUP BY m.channel_name
        ), cat AS (
            SELECT channel_name,
                   image_category,
                   COUNT(*) AS cnt,
                   ROW_NUMBER() OVER (PARTITION BY channel_name ORDER BY COUNT(*) DESC) AS rn
            FROM fct_image_detections
            GROUP BY channel_name, image_category
        )
        SELECT msg.channel_name,
               msg.total_messages,
               msg.visual_messages,
               ROUND(100.0 * msg.visual_messages / NULLIF(msg.total_messages, 0), 1) AS visual_percentage,
               cat.image_category AS most_common_category
        FROM msg
        LEFT JOIN cat ON cat.channel_name = msg.channel_name AND cat.rn = 1
        ORDER BY visual_percentage DESC NULLS LAST, msg.total_messages DESC
        LIMIT :limit
        """
    )

    rows = db.execute(query, {"limit": limit}).fetchall()
    return [
        VisualContentReport(
            channel_name=row.channel_name,
            total_messages=row.total_messages,
            visual_messages=row.visual_messages,
            visual_percentage=float(row.visual_percentage) if row.visual_percentage is not None else 0.0,
            most_common_category=row.most_common_category,
        )
        for row in rows
    ]


@router.get("/message-stats", response_model=MessageStats)
def message_stats(db: Session = Depends(get_db)):
    query = text(
        """
        WITH totals AS (
            SELECT COUNT(*) AS total_messages,
                   AVG(views) AS avg_views,
                   AVG(CASE WHEN has_media THEN 1 ELSE 0 END) AS pct_with_media
            FROM fct_messages
        ), detections AS (
            SELECT COUNT(DISTINCT message_id) AS detected_messages
            FROM fct_image_detections
        )
        SELECT totals.total_messages,
               totals.avg_views,
               totals.pct_with_media,
               detections.detected_messages
        FROM totals CROSS JOIN detections
        """
    )

    row = db.execute(query).fetchone()
    if not row or row.total_messages == 0:
        raise HTTPException(status_code=404, detail="No messages found")

    pct_with_media = float(row.pct_with_media) * 100 if row.pct_with_media is not None else 0.0
    pct_with_detected = 100.0 * (row.detected_messages or 0) / float(row.total_messages)

    return MessageStats(
        total_messages=row.total_messages,
        avg_views=float(row.avg_views) if row.avg_views is not None else 0.0,
        pct_with_media=pct_with_media,
        pct_with_detected_images=pct_with_detected,
    )


@router.get("/category-performance", response_model=List[CategoryPerformance])
def category_performance(
    channel: Optional[str] = Query(None, description="Filter by channel_name"),
    db: Session = Depends(get_db),
):
    query = text(
        """
        SELECT fid.image_category,
               COUNT(*) AS message_count,
               AVG(m.views) AS avg_views
        FROM fct_image_detections fid
        JOIN fct_messages m
          ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
        WHERE (:channel IS NULL OR m.channel_name = :channel)
        GROUP BY fid.image_category
        ORDER BY avg_views DESC NULLS LAST, message_count DESC
        """
    )

    rows = db.execute(query, {"channel": channel}).fetchall()
    return [
        CategoryPerformance(
            image_category=row.image_category,
            avg_views=float(row.avg_views) if row.avg_views is not None else 0.0,
            message_count=row.message_count,
        )
        for row in rows
    ]

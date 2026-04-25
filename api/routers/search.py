from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from ..database import get_db
from ..offline_analytics import search_messages as offline_search_messages
from ..schemas import MessagePreview

router = APIRouter(prefix="/search", tags=["search"])


@router.get("/messages", response_model=list[MessagePreview])
def search_messages(
    query: str = Query(..., min_length=2, description="Keyword to search in message_text"),
    channel: Optional[str] = Query(None, description="Optional channel_name filter"),
    has_media: Optional[bool] = Query(None, description="Filter by media presence"),
    image_category: Optional[str] = Query(None, description="Filter by detected image category"),
    date_from: Optional[datetime] = Query(None, description="Filter messages on or after this timestamp"),
    date_to: Optional[datetime] = Query(None, description="Filter messages on or before this timestamp"),
    min_views: Optional[int] = Query(None, ge=0, description="Minimum view count"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[MessagePreview]:
    sql = text(
        """
        SELECT m.message_id,
               m.channel_name,
               m.message_date,
               LEFT(COALESCE(m.message_text, ''), 500) AS message_text,
               m.views,
               m.has_media,
               fid.image_category
        FROM fct_messages m
        LEFT JOIN fct_image_detections fid
          ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
        WHERE m.message_text ILIKE '%' || :q || '%'
          AND (:channel IS NULL OR m.channel_name = :channel)
          AND (:has_media IS NULL OR m.has_media = :has_media)
          AND (:image_category IS NULL OR fid.image_category = :image_category)
          AND (:date_from IS NULL OR m.message_date >= :date_from)
          AND (:date_to IS NULL OR m.message_date <= :date_to)
          AND (:min_views IS NULL OR m.views >= :min_views)
        ORDER BY m.message_date DESC
        LIMIT :limit
        """
    )

    try:
        rows = db.execute(
            sql,
            {
                "q": query,
                "channel": channel,
                "has_media": has_media,
                "image_category": image_category,
                "date_from": date_from,
                "date_to": date_to,
                "min_views": min_views,
                "limit": limit,
            },
        ).fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No messages found")

        return [
            MessagePreview(
                message_id=row.message_id,
                channel_name=row.channel_name,
                message_date=row.message_date,
                message_text=row.message_text,
                views=row.views,
                has_media=row.has_media,
                image_category=row.image_category,
            )
            for row in rows
        ]
    except OperationalError:
        results = offline_search_messages(
            query=query,
            channel=channel,
            has_media=has_media,
            image_category=image_category,
            date_from=date_from,
            date_to=date_to,
            min_views=min_views,
            limit=limit,
        )
        if not results:
            raise HTTPException(status_code=404, detail="No messages found")
        return results

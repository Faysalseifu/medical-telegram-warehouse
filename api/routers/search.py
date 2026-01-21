from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import MessagePreview

router = APIRouter(prefix="/search", tags=["search"])


@router.get("/messages", response_model=List[MessagePreview])
def search_messages(
    query: str = Query(..., min_length=2, description="Keyword to search in message_text"),
    channel: Optional[str] = Query(None, description="Optional channel_name filter"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
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
        ORDER BY m.message_date DESC
        LIMIT :limit
        """
    )

    rows = db.execute(sql, {"q": query, "channel": channel, "limit": limit}).fetchall()
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

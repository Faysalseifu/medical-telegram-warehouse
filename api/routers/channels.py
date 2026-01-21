from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas import ChannelActivitySummary

router = APIRouter(prefix="/channels", tags=["channels"])


@router.get("/{channel_name}/activity", response_model=ChannelActivitySummary)
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    sql = text(
        """
        SELECT channel_name,
               COUNT(*) AS total_messages,
               AVG(views) AS avg_views,
               SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS total_images,
               MAX(message_date) AS most_recent_message
        FROM fct_messages
        WHERE channel_name = :channel_name
        GROUP BY channel_name
        """
    )

    row = db.execute(sql, {"channel_name": channel_name}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Channel not found")

    return ChannelActivitySummary(
        channel_name=row.channel_name,
        total_messages=row.total_messages,
        avg_views=float(row.avg_views) if row.avg_views is not None else 0.0,
        total_images=row.total_images,
        most_recent_message=row.most_recent_message,
    )

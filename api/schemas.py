from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str


class TopProduct(BaseModel):
    keyword: str
    mention_count: int
    appearing_in_channels: int


class ChannelActivitySummary(BaseModel):
    channel_name: str
    total_messages: int
    avg_views: float
    total_images: int
    most_recent_message: Optional[datetime]


class MessagePreview(BaseModel):
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    views: Optional[int]
    has_media: bool
    image_category: Optional[str]


class VisualContentReport(BaseModel):
    channel_name: str
    total_messages: int
    visual_messages: int
    visual_percentage: float
    most_common_category: Optional[str]


class MessageStats(BaseModel):
    total_messages: int
    avg_views: float
    pct_with_media: float
    pct_with_detected_images: float


class CategoryPerformance(BaseModel):
    image_category: str
    avg_views: float
    message_count: int

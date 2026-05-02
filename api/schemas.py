from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(..., description="Service health indicator.", example="ok")

    class Config:
        schema_extra = {"example": {"status": "ok"}}


class TopProduct(BaseModel):
    keyword: str = Field(..., description="Normalized keyword extracted from messages.", example="vitamin")
    mention_count: int = Field(..., description="Total occurrences across all messages.", example=42)
    appearing_in_channels: int = Field(..., description="Number of distinct channels mentioning the keyword.", example=3)

    class Config:
        schema_extra = {
            "example": {
                "keyword": "vitamin",
                "mention_count": 42,
                "appearing_in_channels": 3,
            }
        }


class ChannelActivitySummary(BaseModel):
    channel_name: str = Field(..., description="Telegram channel name.", example="lobelia4cosmetics")
    total_messages: int = Field(..., description="Total messages available for the channel.", example=1280)
    avg_views: float = Field(..., description="Average views per message.", example=512.4)
    total_images: int = Field(..., description="Number of messages with media.", example=420)
    most_recent_message: Optional[datetime] = Field(
        None,
        description="Most recent message timestamp.",
        example="2026-04-24T09:15:00+00:00",
    )

    class Config:
        schema_extra = {
            "example": {
                "channel_name": "lobelia4cosmetics",
                "total_messages": 1280,
                "avg_views": 512.4,
                "total_images": 420,
                "most_recent_message": "2026-04-24T09:15:00+00:00",
            }
        }


class MessagePreview(BaseModel):
    message_id: int = Field(..., description="Telegram message id (unique within channel).", example=123456)
    channel_name: str = Field(..., description="Channel name where the message appeared.", example="Thequorachannel")
    message_date: datetime = Field(..., description="Timestamp of the message.", example="2026-04-24T09:15:00+00:00")
    message_text: str = Field(..., description="Message text (truncated for previews).", example="New shipment of masks now available")
    views: Optional[int] = Field(None, description="View count from Telegram.", example=230)
    has_media: bool = Field(..., description="Whether the message contains media.", example=True)
    image_category: Optional[str] = Field(
        None,
        description="Detected image category if available.",
        example="product_display",
    )

    class Config:
        schema_extra = {
            "example": {
                "message_id": 123456,
                "channel_name": "Thequorachannel",
                "message_date": "2026-04-24T09:15:00+00:00",
                "message_text": "New shipment of masks now available",
                "views": 230,
                "has_media": True,
                "image_category": "product_display",
            }
        }


class VisualContentReport(BaseModel):
    channel_name: str = Field(..., description="Telegram channel name.", example="lobelia4cosmetics")
    total_messages: int = Field(..., description="Total messages analyzed.", example=1280)
    visual_messages: int = Field(..., description="Messages with detected visual content.", example=420)
    visual_percentage: float = Field(..., description="Percent of messages with detected visuals.", example=32.8)
    most_common_category: Optional[str] = Field(
        None,
        description="Most common detected image category.",
        example="promotional",
    )

    class Config:
        schema_extra = {
            "example": {
                "channel_name": "lobelia4cosmetics",
                "total_messages": 1280,
                "visual_messages": 420,
                "visual_percentage": 32.8,
                "most_common_category": "promotional",
            }
        }


class MessageStats(BaseModel):
    total_messages: int = Field(..., description="Total messages available.", example=12000)
    avg_views: float = Field(..., description="Average views per message.", example=410.2)
    pct_with_media: float = Field(..., description="Percent of messages with media.", example=28.4)
    pct_with_detected_images: float = Field(..., description="Percent of messages with detected images.", example=18.9)

    class Config:
        schema_extra = {
            "example": {
                "total_messages": 12000,
                "avg_views": 410.2,
                "pct_with_media": 28.4,
                "pct_with_detected_images": 18.9,
            }
        }


class CategoryPerformance(BaseModel):
    image_category: str = Field(..., description="Detected image category label.", example="product_display")
    avg_views: float = Field(..., description="Average views for the category.", example=510.7)
    message_count: int = Field(..., description="Number of messages in the category.", example=250)

    class Config:
        schema_extra = {
            "example": {
                "image_category": "product_display",
                "avg_views": 510.7,
                "message_count": 250,
            }
        }


class PromoVsProductDisplay(BaseModel):
    image_category: str = Field(
        ...,
        description="Image category, typically promotional or product_display.",
        example="promotional",
    )
    message_count: int = Field(..., description="Number of messages in the category.", example=180)
    avg_views: float = Field(..., description="Average views for messages in the category.", example=560.5)
    avg_forwards: float = Field(..., description="Average forwards for messages in the category.", example=4.2)

    class Config:
        schema_extra = {
            "example": {
                "image_category": "promotional",
                "message_count": 180,
                "avg_views": 560.5,
                "avg_forwards": 4.2,
            }
        }

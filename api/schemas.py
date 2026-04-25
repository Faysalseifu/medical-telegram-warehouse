from datetime import date, datetime
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


class PipelineFreshness(BaseModel):
    last_message_date: Optional[datetime]
    last_detection_date: Optional[datetime]
    last_refresh_at: Optional[datetime]
    cadence_hours: int
    refresh_lag_hours: Optional[float]
    freshness_label: str


class TrendPoint(BaseModel):
    day: date
    total_messages: int
    avg_views: float
    visual_messages: int
    detected_messages: int


class OverviewReport(BaseModel):
    freshness: PipelineFreshness
    message_stats: MessageStats
    trend: list[TrendPoint]
    top_products: list[TopProduct]
    top_channels: list[ChannelActivitySummary]


class ForecastSignal(BaseModel):
    keyword: str
    recent_mentions: int
    prior_mentions: int
    forecast_mentions: int
    momentum_pct: float


class ForecastReport(BaseModel):
    freshness: PipelineFreshness
    forecast_window_days: int
    immediate_visual_signal_pct: float
    signals: list[ForecastSignal]


class KeywordImpact(BaseModel):
    keyword: str
    current_mentions: int
    previous_mentions: int
    mention_growth_pct: float


class ChannelImpact(BaseModel):
    channel_name: str
    total_messages: int
    avg_views: float
    media_share_pct: float
    detected_image_share_pct: float
    engagement_proxy_score: float


class BusinessImpactReport(BaseModel):
    freshness: PipelineFreshness
    keyword_trends: list[KeywordImpact]
    channel_impact: list[ChannelImpact]

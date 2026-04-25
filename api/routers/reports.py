from datetime import datetime, timezone
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db
from ..offline_analytics import (
    forecast_report as offline_forecast_report,
    impact_report as offline_impact_report,
    message_stats as offline_message_stats,
    overview_trend as offline_overview_trend,
    search_messages as offline_search_messages,
    top_channels as offline_top_channels,
    top_products as offline_top_products,
    visual_content_report as offline_visual_content_report,
)
from ..schemas import (
    BusinessImpactReport,
    CategoryPerformance,
    ChannelActivitySummary,
    ChannelImpact,
    ForecastReport,
    ForecastSignal,
    KeywordImpact,
    MessageStats,
    OverviewReport,
    PipelineFreshness,
    TrendPoint,
    TopProduct,
    VisualContentReport,
)

router = APIRouter(prefix="/reports", tags=["reports"])

DEFAULT_CADENCE_HOURS = 24
OVERVIEW_TREND_DAYS = 14
FORECAST_LOOKBACK_DAYS = 28
FORECAST_WINDOW_DAYS = 7
TOP_CHANNEL_LIMIT = 5
TOP_PRODUCT_LIMIT = 5
IMPACT_LIMIT = 10


def _to_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _build_freshness(
    last_message_date: Optional[datetime],
    last_detection_date: Optional[datetime],
    cadence_hours: int = DEFAULT_CADENCE_HOURS,
) -> PipelineFreshness:
    normalized_dates = [
        normalized
        for normalized in (_to_utc(last_message_date), _to_utc(last_detection_date))
        if normalized is not None
    ]
    last_refresh_at = max(normalized_dates) if normalized_dates else None
    refresh_lag_hours: Optional[float] = None

    if last_refresh_at is not None:
        refresh_lag_hours = max(
            (datetime.now(timezone.utc) - last_refresh_at).total_seconds() / 3600,
            0.0,
        )

    if refresh_lag_hours is None:
        freshness_label = "unknown"
    elif refresh_lag_hours <= cadence_hours:
        freshness_label = "current"
    elif refresh_lag_hours <= cadence_hours * 2:
        freshness_label = "aging"
    else:
        freshness_label = "stale"

    return PipelineFreshness(
        last_message_date=_to_utc(last_message_date),
        last_detection_date=_to_utc(last_detection_date),
        last_refresh_at=last_refresh_at,
        cadence_hours=cadence_hours,
        refresh_lag_hours=refresh_lag_hours,
        freshness_label=freshness_label,
    )


def _top_channels(db: Session, limit: int = TOP_CHANNEL_LIMIT) -> list[ChannelActivitySummary]:
    query = text(
        """
        SELECT channel_name,
               COUNT(*) AS total_messages,
               AVG(views) AS avg_views,
               SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS total_images,
               MAX(message_date) AS most_recent_message
        FROM fct_messages
        GROUP BY channel_name
        ORDER BY total_messages DESC, avg_views DESC NULLS LAST
        LIMIT :limit
        """
    )

    rows = db.execute(query, {"limit": limit}).fetchall()
    return [
        ChannelActivitySummary(
            channel_name=row.channel_name,
            total_messages=row.total_messages,
            avg_views=float(row.avg_views) if row.avg_views is not None else 0.0,
            total_images=row.total_images,
            most_recent_message=row.most_recent_message,
        )
        for row in rows
    ]


def _overview_trend(db: Session, days: int = OVERVIEW_TREND_DAYS) -> list[TrendPoint]:
    query = text(
        """
        WITH daily AS (
            SELECT DATE(message_date) AS day,
                   COUNT(*) AS total_messages,
                   AVG(views) AS avg_views,
                   SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS visual_messages
            FROM fct_messages
            WHERE message_date >= CURRENT_DATE - (:days || ' days')::interval
            GROUP BY DATE(message_date)
        ), detections AS (
            SELECT DATE(m.message_date) AS day,
                   COUNT(DISTINCT fid.message_id) AS detected_messages
            FROM fct_image_detections fid
            JOIN fct_messages m
              ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
            WHERE m.message_date >= CURRENT_DATE - (:days || ' days')::interval
            GROUP BY DATE(m.message_date)
        )
        SELECT daily.day,
               daily.total_messages,
               daily.avg_views,
               daily.visual_messages,
               COALESCE(detections.detected_messages, 0) AS detected_messages
        FROM daily
        LEFT JOIN detections USING (day)
        ORDER BY daily.day
        """
    )

    rows = db.execute(query, {"days": days}).fetchall()
    return [
        TrendPoint(
            day=row.day,
            total_messages=row.total_messages,
            avg_views=float(row.avg_views) if row.avg_views is not None else 0.0,
            visual_messages=row.visual_messages,
            detected_messages=row.detected_messages,
        )
        for row in rows
    ]


def _keyword_trends(
    db: Session,
    lookback_days: int = FORECAST_LOOKBACK_DAYS,
    forecast_window_days: int = FORECAST_WINDOW_DAYS,
    limit: int = TOP_PRODUCT_LIMIT,
) -> list[Any]:
    query = text(
        """
        WITH tokenized AS (
            SELECT DATE(m.message_date) AS day,
                   lower(token) AS keyword
            FROM fct_messages m,
                 LATERAL regexp_split_to_table(coalesce(m.message_text, ''), '\\W+') AS token
            WHERE m.message_date >= CURRENT_DATE - (:lookback_days || ' days')::interval
              AND token ~ '^[A-Za-z]{4,}$'
        ), keyword_daily AS (
            SELECT keyword,
                   day,
                   COUNT(*) AS mentions
            FROM tokenized
            GROUP BY keyword, day
        ), keyword_summary AS (
            SELECT keyword,
                   SUM(CASE WHEN day >= CURRENT_DATE - (:forecast_window_days || ' days')::interval THEN mentions ELSE 0 END) AS recent_mentions,
                   SUM(CASE WHEN day < CURRENT_DATE - (:forecast_window_days || ' days')::interval THEN mentions ELSE 0 END) AS prior_mentions,
                   SUM(mentions) AS total_mentions
            FROM keyword_daily
            GROUP BY keyword
        )
        SELECT keyword,
               recent_mentions,
               prior_mentions,
               total_mentions
        FROM keyword_summary
        ORDER BY total_mentions DESC, keyword
        LIMIT :limit
        """
    )

    return db.execute(
        query,
        {
            "lookback_days": lookback_days,
            "forecast_window_days": forecast_window_days,
            "limit": limit,
        },
    ).fetchall()


def _forecast_signals(db: Session) -> tuple[float, list[ForecastSignal]]:
    query = text(
        """
        WITH recent AS (
            SELECT
                COUNT(DISTINCT CASE WHEN m.message_date >= CURRENT_DATE - (:window_days || ' days')::interval THEN m.message_id END) AS recent_messages,
                COUNT(DISTINCT CASE WHEN m.message_date >= CURRENT_DATE - (:window_days || ' days')::interval AND fid.message_id IS NOT NULL THEN m.message_id END) AS recent_detected_messages
            FROM fct_messages m
            LEFT JOIN fct_image_detections fid
              ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
            WHERE m.message_date >= CURRENT_DATE - (:lookback_days || ' days')::interval
        )
        SELECT recent_messages, recent_detected_messages
        FROM recent
        """
    )

    row = db.execute(
        query,
        {"lookback_days": FORECAST_LOOKBACK_DAYS, "window_days": FORECAST_WINDOW_DAYS},
    ).fetchone()

    recent_messages = row.recent_messages if row and row.recent_messages is not None else 0
    recent_detected_messages = (
        row.recent_detected_messages if row and row.recent_detected_messages is not None else 0
    )
    immediate_visual_signal_pct = (
        round(100.0 * recent_detected_messages / recent_messages, 1)
        if recent_messages
        else 0.0
    )

    signals: list[ForecastSignal] = []
    for row in _keyword_trends(db):
        recent_mentions = int(row.recent_mentions or 0)
        prior_mentions = int(row.prior_mentions or 0)
        momentum = round(100.0 * (recent_mentions - prior_mentions) / prior_mentions, 1) if prior_mentions else 0.0
        forecast_mentions = max(recent_mentions + (recent_mentions - prior_mentions), 0)
        signals.append(
            ForecastSignal(
                keyword=row.keyword,
                recent_mentions=recent_mentions,
                prior_mentions=prior_mentions,
                forecast_mentions=forecast_mentions,
                momentum_pct=momentum,
            )
        )

    return immediate_visual_signal_pct, signals


def _channel_impact(db: Session, limit: int = IMPACT_LIMIT) -> list[ChannelImpact]:
    query = text(
        """
        WITH message_flags AS (
            SELECT m.channel_name,
                   m.views,
                   m.has_media,
                   CASE WHEN fid.message_id IS NULL THEN 0 ELSE 1 END AS detected_flag
            FROM fct_messages m
            LEFT JOIN (
                SELECT DISTINCT message_id, channel_name
                FROM fct_image_detections
            ) fid
              ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
            WHERE m.message_date >= CURRENT_DATE - (:lookback_days || ' days')::interval
        ), channel_totals AS (
            SELECT channel_name,
                   COUNT(*) AS total_messages,
                   AVG(views) AS avg_views,
                   AVG(CASE WHEN has_media THEN 1 ELSE 0 END) AS media_share,
                   AVG(detected_flag::numeric) AS detected_share
            FROM message_flags
            GROUP BY channel_name
        )
        SELECT channel_name,
               total_messages,
               avg_views,
               media_share,
               detected_share,
               ROUND(COALESCE(avg_views, 0) * (1 + COALESCE(media_share, 0) + COALESCE(detected_share, 0)), 1) AS engagement_proxy_score
        FROM channel_totals
        ORDER BY engagement_proxy_score DESC NULLS LAST, total_messages DESC
        LIMIT :limit
        """
    )

    rows = db.execute(
        query,
        {"lookback_days": FORECAST_LOOKBACK_DAYS, "limit": limit},
    ).fetchall()
    return [
        ChannelImpact(
            channel_name=row.channel_name,
            total_messages=row.total_messages,
            avg_views=float(row.avg_views) if row.avg_views is not None else 0.0,
            media_share_pct=float(row.media_share) * 100 if row.media_share is not None else 0.0,
            detected_image_share_pct=float(row.detected_share) * 100 if row.detected_share is not None else 0.0,
            engagement_proxy_score=float(row.engagement_proxy_score) if row.engagement_proxy_score is not None else 0.0,
        )
        for row in rows
    ]


@router.get("/top-products", response_model=List[TopProduct])
def top_products(
    limit: int = Query(10, ge=1, le=50),
    min_count: int = Query(3, ge=1),
    db: Session = Depends(get_db),
) -> list[TopProduct]:
    try:
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
    except OperationalError:
        return offline_top_products(limit=limit, min_count=min_count)


@router.get("/visual-content", response_model=List[VisualContentReport])
def visual_content(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
) -> list[VisualContentReport]:
    try:
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
    except OperationalError:
        return offline_visual_content_report(limit=limit)


@router.get("/message-stats", response_model=MessageStats)
def message_stats(db: Session = Depends(get_db)) -> MessageStats:
    try:
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
    except OperationalError:
        return offline_message_stats()


@router.get("/category-performance", response_model=List[CategoryPerformance])
def category_performance(
    channel: Optional[str] = Query(None, description="Filter by channel_name"),
    db: Session = Depends(get_db),
) -> list[CategoryPerformance]:
    try:
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
    except OperationalError:
        return []


@router.get("/overview", response_model=OverviewReport)
def overview(db: Session = Depends(get_db)) -> OverviewReport:
    try:
        freshness_row = db.execute(
            text(
                """
                WITH message_watermark AS (
                    SELECT MAX(message_date) AS last_message_date
                    FROM fct_messages
                ), detection_watermark AS (
                    SELECT MAX(m.message_date) AS last_detection_date
                    FROM fct_image_detections fid
                    JOIN fct_messages m
                      ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
                )
                SELECT message_watermark.last_message_date,
                       detection_watermark.last_detection_date
                FROM message_watermark
                CROSS JOIN detection_watermark
                """
            )
        ).fetchone()

        stats = message_stats(db=db)
        trend = _overview_trend(db)
        products = top_products(limit=TOP_PRODUCT_LIMIT, min_count=3, db=db)
        channels = _top_channels(db)

        return OverviewReport(
            freshness=_build_freshness(
                freshness_row.last_message_date if freshness_row else None,
                freshness_row.last_detection_date if freshness_row else None,
            ),
            message_stats=stats,
            trend=trend,
            top_products=products,
            top_channels=channels,
        )
    except OperationalError:
        fallback = offline_forecast_report().freshness
        return OverviewReport(
            freshness=fallback,
            message_stats=offline_message_stats(),
            trend=offline_overview_trend(days=OVERVIEW_TREND_DAYS),
            top_products=offline_top_products(limit=TOP_PRODUCT_LIMIT, min_count=3),
            top_channels=offline_top_channels(limit=TOP_CHANNEL_LIMIT),
        )


@router.get("/forecast", response_model=ForecastReport)
def forecast(db: Session = Depends(get_db)) -> ForecastReport:
    try:
        freshness_row = db.execute(
            text(
                """
                WITH message_watermark AS (
                    SELECT MAX(message_date) AS last_message_date
                    FROM fct_messages
                ), detection_watermark AS (
                    SELECT MAX(m.message_date) AS last_detection_date
                    FROM fct_image_detections fid
                    JOIN fct_messages m
                      ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
                )
                SELECT message_watermark.last_message_date,
                       detection_watermark.last_detection_date
                FROM message_watermark
                CROSS JOIN detection_watermark
                """
            )
        ).fetchone()

        message_stats(db=db)
        immediate_visual_signal_pct, signals = _forecast_signals(db)

        return ForecastReport(
            freshness=_build_freshness(
                freshness_row.last_message_date if freshness_row else None,
                freshness_row.last_detection_date if freshness_row else None,
            ),
            forecast_window_days=FORECAST_WINDOW_DAYS,
            immediate_visual_signal_pct=immediate_visual_signal_pct,
            signals=signals,
        )
    except OperationalError:
        return offline_forecast_report()


@router.get("/impact", response_model=BusinessImpactReport)
def impact(db: Session = Depends(get_db)) -> BusinessImpactReport:
    try:
        freshness_row = db.execute(
            text(
                """
                WITH message_watermark AS (
                    SELECT MAX(message_date) AS last_message_date
                    FROM fct_messages
                ), detection_watermark AS (
                    SELECT MAX(m.message_date) AS last_detection_date
                    FROM fct_image_detections fid
                    JOIN fct_messages m
                      ON m.message_id = fid.message_id AND m.channel_name = fid.channel_name
                )
                SELECT message_watermark.last_message_date,
                       detection_watermark.last_detection_date
                FROM message_watermark
                CROSS JOIN detection_watermark
                """
            )
        ).fetchone()

        message_stats(db=db)
        keyword_rows = _keyword_trends(db, limit=IMPACT_LIMIT)
        channel_rows = _channel_impact(db)

        keyword_trends = [
            KeywordImpact(
                keyword=row.keyword,
                current_mentions=int(row.recent_mentions or 0),
                previous_mentions=int(row.prior_mentions or 0),
                mention_growth_pct=(
                    round(100.0 * (int(row.recent_mentions or 0) - int(row.prior_mentions or 0)) / int(row.prior_mentions or 1), 1)
                    if int(row.prior_mentions or 0)
                    else 0.0
                ),
            )
            for row in keyword_rows
        ]

        return BusinessImpactReport(
            freshness=_build_freshness(
                freshness_row.last_message_date if freshness_row else None,
                freshness_row.last_detection_date if freshness_row else None,
            ),
            keyword_trends=keyword_trends,
            channel_impact=channel_rows,
        )
    except OperationalError:
        return offline_impact_report()

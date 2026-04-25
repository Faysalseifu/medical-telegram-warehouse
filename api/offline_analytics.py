from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from app_config import RAW_TELEGRAM_MESSAGES_DIR

from .schemas import (
    BusinessImpactReport,
    CategoryPerformance,
    ChannelActivitySummary,
    ChannelImpact,
    ForecastReport,
    ForecastSignal,
    KeywordImpact,
    MessagePreview,
    MessageStats,
    OverviewReport,
    PipelineFreshness,
    TopProduct,
    TrendPoint,
    VisualContentReport,
)

TOKEN_PATTERN = re.compile(r"\b[a-zA-Z]{4,}\b")


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@lru_cache(maxsize=1)
def _load_messages() -> tuple[dict[str, Any], ...]:
    messages: list[dict[str, Any]] = []
    for json_file in sorted(RAW_TELEGRAM_MESSAGES_DIR.glob("**/*.json")):
        try:
            rows = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in rows:
            parsed = dict(row)
            parsed["message_date"] = _parse_datetime(row.get("message_date"))
            parsed["message_text"] = row.get("message_text") or ""
            parsed["has_media"] = bool(row.get("has_media"))
            parsed["views"] = row.get("views")
            parsed["forwards"] = row.get("forwards")
            parsed["image_category"] = None
            messages.append(parsed)
    messages.sort(key=lambda item: item.get("message_date") or datetime.min.replace(tzinfo=UTC))
    return tuple(messages)


def _messages_for_channel(channel_name: str | None = None) -> list[dict[str, Any]]:
    messages = list(_load_messages())
    if channel_name is not None:
        messages = [row for row in messages if row.get("channel_name") == channel_name]
    return messages


def _keywords(message_text: str) -> list[str]:
    return [match.group(0).lower() for match in TOKEN_PATTERN.finditer(message_text or "")]


def _top_keywords(messages: list[dict[str, Any]], *, limit: int, min_count: int) -> list[TopProduct]:
    counts: Counter[str] = Counter()
    channels_by_keyword: defaultdict[str, set[str]] = defaultdict(set)
    for row in messages:
        keyword_set = set(_keywords(row.get("message_text", "")))
        for keyword in keyword_set:
            counts[keyword] += 1
            channels_by_keyword[keyword].add(str(row.get("channel_name", "")))

    results: list[TopProduct] = []
    for keyword, count in counts.most_common():
        if count < min_count:
            continue
        results.append(
            TopProduct(
                keyword=keyword,
                mention_count=count,
                appearing_in_channels=len(channels_by_keyword[keyword]),
            )
        )
        if len(results) >= limit:
            break
    return results


def _freshness(messages: list[dict[str, Any]]) -> PipelineFreshness:
    last_message_date = max((row["message_date"] for row in messages if row.get("message_date") is not None), default=None)
    return PipelineFreshness(
        last_message_date=last_message_date,
        last_detection_date=None,
        last_refresh_at=last_message_date,
        cadence_hours=24,
        refresh_lag_hours=(max((datetime.now(UTC) - last_message_date).total_seconds() / 3600, 0.0) if last_message_date else None),
        freshness_label="current" if last_message_date is not None else "unknown",
    )


def message_stats() -> MessageStats:
    messages = _messages_for_channel()
    if not messages:
        return MessageStats(total_messages=0, avg_views=0.0, pct_with_media=0.0, pct_with_detected_images=0.0)

    total_messages = len(messages)
    views = [row.get("views") for row in messages if row.get("views") is not None]
    media_count = sum(1 for row in messages if row.get("has_media"))

    return MessageStats(
        total_messages=total_messages,
        avg_views=(sum(views) / len(views)) if views else 0.0,
        pct_with_media=(100.0 * media_count / total_messages),
        pct_with_detected_images=0.0,
    )


def top_products(limit: int = 5, min_count: int = 3) -> list[TopProduct]:
    return _top_keywords(_messages_for_channel(), limit=limit, min_count=min_count)


def overview_trend(days: int = 14) -> list[TrendPoint]:
    messages = _messages_for_channel()
    if not messages:
        return []

    cutoff = datetime.now(UTC) - timedelta(days=days)
    buckets: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in messages:
        message_date = row.get("message_date")
        if message_date is None or message_date < cutoff:
            continue
        buckets[message_date.date()].append(row)

    trend: list[TrendPoint] = []
    for day in sorted(buckets):
        day_rows = buckets[day]
        views = [row.get("views") for row in day_rows if row.get("views") is not None]
        trend.append(
            TrendPoint(
                day=day,
                total_messages=len(day_rows),
                avg_views=(sum(views) / len(views)) if views else 0.0,
                visual_messages=sum(1 for row in day_rows if row.get("has_media")),
                detected_messages=0,
            )
        )
    return trend


def top_channels(limit: int = 5) -> list[ChannelActivitySummary]:
    messages = _messages_for_channel()
    if not messages:
        return []

    by_channel: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in messages:
        by_channel[str(row.get("channel_name", "unknown"))].append(row)

    results: list[ChannelActivitySummary] = []
    for channel_name, rows in sorted(by_channel.items(), key=lambda item: len(item[1]), reverse=True)[:limit]:
        views = [row.get("views") for row in rows if row.get("views") is not None]
        results.append(
            ChannelActivitySummary(
                channel_name=channel_name,
                total_messages=len(rows),
                avg_views=(sum(views) / len(views)) if views else 0.0,
                total_images=sum(1 for row in rows if row.get("has_media")),
                most_recent_message=max((row.get("message_date") for row in rows if row.get("message_date") is not None), default=None),
            )
        )
    return results


def forecast_report() -> ForecastReport:
    messages = _messages_for_channel()
    freshness = _freshness(messages)
    if not messages:
        return ForecastReport(freshness=freshness, forecast_window_days=7, immediate_visual_signal_pct=0.0, signals=[])

    window_days = 7
    lookback_days = 28
    now = datetime.now(UTC)
    recent_cutoff = now - timedelta(days=window_days)
    lookback_cutoff = now - timedelta(days=lookback_days)

    recent_messages = [row for row in messages if row.get("message_date") and row["message_date"] >= recent_cutoff]
    immediate_visual_signal_pct = 100.0 * sum(1 for row in recent_messages if row.get("has_media")) / len(recent_messages) if recent_messages else 0.0

    by_keyword: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in messages:
        message_date = row.get("message_date")
        if message_date is None or message_date < lookback_cutoff:
            continue
        for keyword in set(_keywords(row.get("message_text", ""))):
            by_keyword[keyword].append(row)

    signals: list[ForecastSignal] = []
    for keyword, rows in sorted(by_keyword.items(), key=lambda item: len(item[1]), reverse=True)[:5]:
        recent_mentions = sum(1 for row in rows if row.get("message_date") and row["message_date"] >= recent_cutoff)
        prior_mentions = len(rows) - recent_mentions
        forecast_mentions = max(recent_mentions + (recent_mentions - prior_mentions), 0)
        momentum_pct = round(100.0 * (recent_mentions - prior_mentions) / prior_mentions, 1) if prior_mentions else 0.0
        signals.append(
            ForecastSignal(
                keyword=keyword,
                recent_mentions=recent_mentions,
                prior_mentions=prior_mentions,
                forecast_mentions=forecast_mentions,
                momentum_pct=momentum_pct,
            )
        )

    return ForecastReport(
        freshness=freshness,
        forecast_window_days=window_days,
        immediate_visual_signal_pct=immediate_visual_signal_pct,
        signals=signals,
    )


def impact_report() -> BusinessImpactReport:
    messages = _messages_for_channel()
    freshness = _freshness(messages)
    if not messages:
        return BusinessImpactReport(freshness=freshness, keyword_trends=[], channel_impact=[])

    now = datetime.now(UTC)
    recent_cutoff = now - timedelta(days=7)
    prior_cutoff = now - timedelta(days=14)

    keyword_rows: list[KeywordImpact] = []
    by_keyword: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in messages:
        message_date = row.get("message_date")
        if message_date is None or message_date < prior_cutoff:
            continue
        for keyword in set(_keywords(row.get("message_text", ""))):
            by_keyword[keyword].append(row)

    for keyword, rows in sorted(by_keyword.items(), key=lambda item: len(item[1]), reverse=True)[:10]:
        current_mentions = sum(1 for row in rows if row.get("message_date") and row["message_date"] >= recent_cutoff)
        previous_mentions = len(rows) - current_mentions
        mention_growth_pct = round(100.0 * (current_mentions - previous_mentions) / previous_mentions, 1) if previous_mentions else 0.0
        keyword_rows.append(
            KeywordImpact(
                keyword=keyword,
                current_mentions=current_mentions,
                previous_mentions=previous_mentions,
                mention_growth_pct=mention_growth_pct,
            )
        )

    by_channel: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in messages:
        message_date = row.get("message_date")
        if message_date is None or message_date < recent_cutoff:
            continue
        by_channel[str(row.get("channel_name", "unknown"))].append(row)

    channel_rows: list[ChannelImpact] = []
    for channel_name, rows in sorted(by_channel.items(), key=lambda item: len(item[1]), reverse=True)[:10]:
        views = [row.get("views") for row in rows if row.get("views") is not None]
        media_share = sum(1 for row in rows if row.get("has_media")) / len(rows) if rows else 0.0
        avg_views = (sum(views) / len(views)) if views else 0.0
        channel_rows.append(
            ChannelImpact(
                channel_name=channel_name,
                total_messages=len(rows),
                avg_views=avg_views,
                media_share_pct=media_share * 100.0,
                detected_image_share_pct=0.0,
                engagement_proxy_score=round(avg_views * (1.0 + media_share), 1),
            )
        )

    return BusinessImpactReport(
        freshness=freshness,
        keyword_trends=keyword_rows,
        channel_impact=channel_rows,
    )


def search_messages(
    query: str,
    channel: str | None = None,
    has_media: bool | None = None,
    image_category: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    min_views: int | None = None,
    limit: int = 20,
) -> list[MessagePreview]:
    matches: list[MessagePreview] = []
    for row in sorted(_messages_for_channel(channel), key=lambda item: item.get("message_date") or datetime.min.replace(tzinfo=UTC), reverse=True):
        message_text = str(row.get("message_text", ""))
        if query.lower() not in message_text.lower():
            continue
        if has_media is not None and bool(row.get("has_media")) != has_media:
            continue
        if image_category is not None and row.get("image_category") != image_category:
            continue
        if date_from is not None and row.get("message_date") is not None and row["message_date"] < date_from:
            continue
        if date_to is not None and row.get("message_date") is not None and row["message_date"] > date_to:
            continue
        if min_views is not None and (row.get("views") is None or int(row["views"]) < min_views):
            continue
        matches.append(
            MessagePreview(
                message_id=int(row.get("message_id", 0)),
                channel_name=str(row.get("channel_name", "unknown")),
                message_date=row.get("message_date") or datetime.now(UTC),
                message_text=message_text[:500],
                views=row.get("views"),
                has_media=bool(row.get("has_media")),
                image_category=row.get("image_category"),
            )
        )
        if len(matches) >= limit:
            break
    return matches


def channel_activity(channel_name: str) -> ChannelActivitySummary | None:
    messages = _messages_for_channel(channel_name)
    if not messages:
        return None
    views = [row.get("views") for row in messages if row.get("views") is not None]
    return ChannelActivitySummary(
        channel_name=channel_name,
        total_messages=len(messages),
        avg_views=(sum(views) / len(views)) if views else 0.0,
        total_images=sum(1 for row in messages if row.get("has_media")),
        most_recent_message=max((row.get("message_date") for row in messages if row.get("message_date") is not None), default=None),
    )


def visual_content_report(limit: int = 10) -> list[VisualContentReport]:
    reports: list[VisualContentReport] = []
    for channel_summary in top_channels(limit=limit):
        reports.append(
            VisualContentReport(
                channel_name=channel_summary.channel_name,
                total_messages=channel_summary.total_messages,
                visual_messages=channel_summary.total_images,
                visual_percentage=(100.0 * channel_summary.total_images / channel_summary.total_messages) if channel_summary.total_messages else 0.0,
                most_common_category=None,
            )
        )
    return reports


def category_performance(channel: str | None = None) -> list[CategoryPerformance]:
    return []

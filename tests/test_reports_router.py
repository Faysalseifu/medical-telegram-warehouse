from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from api.routers.reports import forecast, impact, message_stats, overview, top_products


class _Result:
    def __init__(self, rows=None, row=None) -> None:
        self._rows = rows or []
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row


class _DB:
    def __init__(self, result: _Result) -> None:
        self.result = result

    def execute(self, *args, **kwargs):
        return self.result


class _QueuedDB:
    def __init__(self, results: list[_Result]) -> None:
        self._results = results
        self.executed_queries: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def execute(self, *args, **kwargs):
        self.executed_queries.append((args, kwargs))
        return self._results.pop(0)


def test_top_products_shapes_response_objects() -> None:
    db = _DB(
        _Result(
            rows=[
                SimpleNamespace(keyword="paracetamol", mention_count=4, appearing_in_channels=2),
                SimpleNamespace(keyword="amoxicillin", mention_count=3, appearing_in_channels=1),
            ]
        )
    )

    results = top_products(db=db)

    assert [item.keyword for item in results] == ["paracetamol", "amoxicillin"]
    assert results[0].mention_count == 4


def test_message_stats_computes_percentages() -> None:
    db = _DB(
        _Result(
            row=SimpleNamespace(
                total_messages=10,
                avg_views=25.5,
                pct_with_media=0.4,
                detected_messages=3,
            )
        )
    )

    result = message_stats(db=db)

    assert result.total_messages == 10
    assert result.pct_with_media == 40.0
    assert result.pct_with_detected_images == 30.0


def test_overview_combines_freshness_trend_and_summaries() -> None:
    now = datetime.now(timezone.utc)
    db = _QueuedDB(
        [
            _Result(
                row=SimpleNamespace(
                    last_message_date=now,
                    last_detection_date=now,
                )
            ),
            _Result(
                row=SimpleNamespace(
                    total_messages=10,
                    avg_views=25.5,
                    pct_with_media=0.4,
                    detected_messages=3,
                )
            ),
            _Result(
                rows=[
                    SimpleNamespace(day=now.date(), total_messages=6, avg_views=11.2, visual_messages=2, detected_messages=1),
                    SimpleNamespace(day=now.date(), total_messages=4, avg_views=9.3, visual_messages=1, detected_messages=0),
                ]
            ),
            _Result(
                rows=[
                    SimpleNamespace(keyword="paracetamol", mention_count=4, appearing_in_channels=2),
                ]
            ),
            _Result(
                rows=[
                    SimpleNamespace(
                        channel_name="lobelia4cosmetics",
                        total_messages=8,
                        avg_views=30.0,
                        total_images=5,
                        most_recent_message=now,
                    )
                ]
            ),
        ]
    )

    result = overview(db=db)

    assert result.freshness.freshness_label == "current"
    assert result.message_stats.total_messages == 10
    assert len(result.trend) == 2
    assert result.top_products[0].keyword == "paracetamol"
    assert result.top_channels[0].channel_name == "lobelia4cosmetics"


def test_forecast_builds_signals_from_recent_keyword_trends() -> None:
    now = datetime.now(timezone.utc)
    db = _QueuedDB(
        [
            _Result(
                row=SimpleNamespace(
                    last_message_date=now,
                    last_detection_date=now,
                )
            ),
            _Result(
                row=SimpleNamespace(
                    total_messages=10,
                    avg_views=25.5,
                    pct_with_media=0.4,
                    detected_messages=3,
                )
            ),
            _Result(
                row=SimpleNamespace(recent_messages=10, recent_detected_messages=2)
            ),
            _Result(
                rows=[
                    SimpleNamespace(keyword="paracetamol", recent_mentions=7, prior_mentions=4, total_mentions=11),
                    SimpleNamespace(keyword="amoxicillin", recent_mentions=3, prior_mentions=3, total_mentions=6),
                ]
            ),
        ]
    )

    result = forecast(db=db)

    assert result.immediate_visual_signal_pct == 20.0
    assert result.signals[0].keyword == "paracetamol"
    assert result.signals[0].forecast_mentions == 10
    assert result.signals[0].momentum_pct == 75.0


def test_impact_reports_keyword_and_channel_scores() -> None:
    now = datetime.now(timezone.utc)
    db = _QueuedDB(
        [
            _Result(
                row=SimpleNamespace(
                    last_message_date=now,
                    last_detection_date=now,
                )
            ),
            _Result(
                row=SimpleNamespace(
                    total_messages=10,
                    avg_views=25.5,
                    pct_with_media=0.4,
                    detected_messages=3,
                )
            ),
            _Result(
                rows=[
                    SimpleNamespace(keyword="paracetamol", recent_mentions=7, prior_mentions=4, total_mentions=11),
                    SimpleNamespace(keyword="amoxicillin", recent_mentions=3, prior_mentions=0, total_mentions=6),
                ]
            ),
            _Result(
                rows=[
                    SimpleNamespace(
                        channel_name="lobelia4cosmetics",
                        total_messages=8,
                        avg_views=30.0,
                        media_share=0.25,
                        detected_share=0.125,
                        engagement_proxy_score=45.0,
                    )
                ]
            ),
        ]
    )

    result = impact(db=db)

    assert result.keyword_trends[0].keyword == "paracetamol"
    assert result.keyword_trends[0].mention_growth_pct == 75.0
    assert result.channel_impact[0].media_share_pct == 25.0
    assert result.channel_impact[0].detected_image_share_pct == 12.5
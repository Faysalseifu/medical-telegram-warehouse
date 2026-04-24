from __future__ import annotations

from types import SimpleNamespace

from api.routers.reports import message_stats, top_products


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
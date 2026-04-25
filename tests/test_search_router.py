from __future__ import annotations

from types import SimpleNamespace

from api.routers.search import search_messages


class _Result:
    def __init__(self, rows=None) -> None:
        self._rows = rows or []

    def fetchall(self):
        return self._rows


class _DB:
    def __init__(self, result: _Result) -> None:
        self.result = result
        self.calls = []

    def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


def test_search_messages_applies_filters_and_returns_previews() -> None:
    db = _DB(
        _Result(
            rows=[
                SimpleNamespace(
                    message_id=1,
                    channel_name="lobelia4cosmetics",
                    message_date="2026-04-24T08:00:00Z",
                    message_text="paracetamol and vitamin c",
                    views=120,
                    has_media=True,
                    image_category="product",
                )
            ]
        )
    )

    results = search_messages(
        query="paracetamol",
        channel="lobelia4cosmetics",
        has_media=True,
        image_category="product",
        date_from=None,
        date_to=None,
        min_views=100,
        db=db,
    )

    assert results[0].channel_name == "lobelia4cosmetics"
    params = db.calls[0][0][1]
    assert params["channel"] == "lobelia4cosmetics"
    assert params["has_media"] is True
    assert params["image_category"] == "product"
    assert params["min_views"] == 100
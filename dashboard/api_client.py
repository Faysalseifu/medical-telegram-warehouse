from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import requests


DEFAULT_API_BASE_URL = "http://localhost:8000"


def get_api_base_url() -> str:
    return os.getenv("API_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/")


@dataclass(slots=True)
class DashboardAPIClient:
    base_url: str = field(default_factory=get_api_base_url)
    timeout_seconds: float = 30.0
    session: requests.Session = field(default_factory=requests.Session, repr=False)

    def _request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def overview(self) -> dict[str, Any]:
        return self._request("/reports/overview")

    def forecast(self) -> dict[str, Any]:
        return self._request("/reports/forecast")

    def impact(self) -> dict[str, Any]:
        return self._request("/reports/impact")

    def search_messages(
        self,
        *,
        query: str,
        channel: str | None = None,
        has_media: bool | None = None,
        image_category: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        min_views: int | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"query": query, "limit": limit}
        if channel:
            params["channel"] = channel
        if has_media is not None:
            params["has_media"] = has_media
        if image_category:
            params["image_category"] = image_category
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        if min_views is not None:
            params["min_views"] = min_views
        return self._request("/search/messages", params=params)

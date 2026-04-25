from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from requests import HTTPError, RequestException

from .api_client import DashboardAPIClient, get_api_base_url


PAGE_LABELS = (
    "Overview",
    "Model predictions",
    "Business impact",
    "Drill-down",
)


@st.cache_data(ttl=300)
def load_overview(base_url: str) -> dict[str, Any]:
    return DashboardAPIClient(base_url=base_url).overview()


@st.cache_data(ttl=300)
def load_forecast(base_url: str) -> dict[str, Any]:
    return DashboardAPIClient(base_url=base_url).forecast()


@st.cache_data(ttl=300)
def load_impact(base_url: str) -> dict[str, Any]:
    return DashboardAPIClient(base_url=base_url).impact()


def _format_datetime(value: str | None) -> str:
    if not value:
        return "Not available"
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def _format_percent(value: Any) -> str:
    if value is None:
        return "0.0%"
    return f"{float(value):.1f}%"


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.4rem;
            padding-bottom: 2rem;
            max-width: 1380px;
        }
        .hero {
            border-radius: 24px;
            padding: 1.25rem 1.5rem;
            background: linear-gradient(135deg, #0f766e 0%, #1d4ed8 60%, #111827 100%);
            color: white;
            margin-bottom: 1rem;
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18);
        }
        .hero h1 {
            margin: 0;
            font-size: 2.05rem;
            line-height: 1.1;
        }
        .hero p {
            margin: 0.45rem 0 0;
            color: rgba(255, 255, 255, 0.88);
        }
        [data-testid="metric-container"] {
            border-radius: 18px;
            border: 1px solid rgba(15, 23, 42, 0.08);
            background: rgba(255, 255, 255, 0.88);
            box-shadow: 0 8px 28px rgba(15, 23, 42, 0.06);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_hero(freshness: dict[str, Any]) -> None:
    st.markdown(
        f"""
        <div class="hero">
            <h1>Medical Telegram Warehouse</h1>
            <p>
                Batch-aware analytics for the latest Dagster/dbt refresh.
                Last refresh: {_format_datetime(freshness.get('last_refresh_at'))}.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_freshness_caption(freshness: dict[str, Any]) -> None:
    cadence = freshness.get("cadence_hours", 24)
    st.caption(
        "Refresh state: "
        f"{freshness.get('freshness_label', 'unknown')} · "
        f"last warehouse refresh {_format_datetime(freshness.get('last_refresh_at'))} · "
        f"cadence {cadence}h"
    )


def _render_overview(overview: dict[str, Any]) -> None:
    freshness = overview["freshness"]
    stats = overview["message_stats"]
    trend = pd.DataFrame(overview["trend"])
    top_products = pd.DataFrame(overview["top_products"])
    top_channels = pd.DataFrame(overview["top_channels"])

    st.subheader("Overview")
    _render_freshness_caption(freshness)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Messages", f"{stats['total_messages']:,}")
    metric_cols[1].metric("Avg views", f"{stats['avg_views']:.1f}")
    metric_cols[2].metric("Media rate", _format_percent(stats['pct_with_media']))
    metric_cols[3].metric("YOLO detections", _format_percent(stats['pct_with_detected_images']))

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.markdown("**Message trend**")
        if not trend.empty:
            trend["day"] = pd.to_datetime(trend["day"])
            st.line_chart(trend.set_index("day")[ ["total_messages", "detected_messages"] ])
        else:
            st.info("No trend data available.")
    with chart_cols[1]:
        st.markdown("**Top channels**")
        if not top_channels.empty:
            st.bar_chart(top_channels.set_index("channel_name")["total_messages"])
        else:
            st.info("No channel summary available.")

    st.markdown("**Top product mentions**")
    if not top_products.empty:
        st.dataframe(top_products, use_container_width=True, hide_index=True)
    else:
        st.info("No product trends available.")


def _render_predictions(forecast: dict[str, Any]) -> None:
    freshness = forecast["freshness"]
    signals = pd.DataFrame(forecast["signals"])

    st.subheader("Model predictions")
    _render_freshness_caption(freshness)
    st.metric("Immediate visual signal", _format_percent(forecast["immediate_visual_signal_pct"]))

    if signals.empty:
        st.info("No forecast signals available.")
        return

    signals["momentum_label"] = signals["momentum_pct"].map(lambda value: f"{float(value):.1f}%")
    chart = signals.set_index("keyword")["recent_mentions"].to_frame()
    chart["forecast_mentions"] = signals.set_index("keyword")["forecast_mentions"]

    signal_cols = st.columns(2)
    with signal_cols[0]:
        st.markdown("**Recent vs forecast mentions**")
        st.bar_chart(chart)
    with signal_cols[1]:
        st.markdown("**Forecast signals**")
        st.dataframe(
            signals[["keyword", "recent_mentions", "prior_mentions", "forecast_mentions", "momentum_pct"]],
            use_container_width=True,
            hide_index=True,
        )


def _render_impact(impact: dict[str, Any]) -> None:
    freshness = impact["freshness"]
    keyword_trends = pd.DataFrame(impact["keyword_trends"])
    channel_impact = pd.DataFrame(impact["channel_impact"])

    st.subheader("Business impact")
    _render_freshness_caption(freshness)

    impact_cols = st.columns(2)
    with impact_cols[0]:
        st.markdown("**Product-mention trends**")
        if not keyword_trends.empty:
            keyword_trends = keyword_trends.sort_values("mention_growth_pct", ascending=False)
            st.dataframe(keyword_trends, use_container_width=True, hide_index=True)
            st.bar_chart(keyword_trends.set_index("keyword")["current_mentions"])
        else:
            st.info("No product trend data available.")
    with impact_cols[1]:
        st.markdown("**Engagement proxies**")
        if not channel_impact.empty:
            st.dataframe(channel_impact, use_container_width=True, hide_index=True)
            st.bar_chart(channel_impact.set_index("channel_name")["engagement_proxy_score"])
        else:
            st.info("No channel impact data available.")


def _render_drilldown(base_url: str, overview: dict[str, Any]) -> None:
    client = DashboardAPIClient(base_url=base_url)
    top_channels = [row["channel_name"] for row in overview["top_channels"]]
    suggested_channels = ["All channels", *top_channels]

    st.subheader("Drill-down")
    st.write("Search the latest messages without talking to the warehouse directly.")

    with st.form("drilldown_form"):
        query = st.text_input("Keyword", value="medicine")
        channel_choice = st.selectbox("Channel", suggested_channels)
        has_media = st.selectbox("Media filter", ["Any", "Media only", "Text only"])
        image_category = st.text_input("Image category", value="")
        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input("From", value=None)
        with col2:
            date_to = st.date_input("To", value=None)
        min_views = st.number_input("Minimum views", min_value=0, value=0, step=10)
        submitted = st.form_submit_button("Search")

    if not submitted:
        st.info("Submit the form to search recent message previews.")
        return

    channel = None if channel_choice == "All channels" else channel_choice
    media_value: bool | None
    if has_media == "Media only":
        media_value = True
    elif has_media == "Text only":
        media_value = False
    else:
        media_value = None

    try:
        results = client.search_messages(
            query=query,
            channel=channel,
            has_media=media_value,
            image_category=image_category or None,
            date_from=date_from.isoformat() if date_from else None,
            date_to=date_to.isoformat() if date_to else None,
            min_views=int(min_views) if min_views else None,
        )
    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            st.warning("No messages matched the current filters.")
            return
        st.error(f"Search failed: {exc}")
        return
    except RequestException as exc:
        st.error(f"Search failed: {exc}")
        return

    st.success(f"Found {len(results)} message previews.")
    st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

    for row in results[:10]:
        with st.expander(f"{row['channel_name']} · {row['message_date']}"):
            st.write(row["message_text"])
            st.caption(
                f"Views: {row['views'] or 0} · Media: {row['has_media']} · Category: {row['image_category'] or 'n/a'}"
            )


def main() -> None:
    st.set_page_config(page_title="Medical Telegram Dashboard", layout="wide")
    _inject_styles()

    base_url = get_api_base_url()
    st.sidebar.title("Dashboard")
    st.sidebar.text_input("API base URL", value=base_url, disabled=True)

    try:
        overview = load_overview(base_url)
    except RequestException as exc:
        st.error("The dashboard could not load analytics from the API.")
        st.code(str(exc))
        st.caption("The UI is batch-aware; it needs the FastAPI service and warehouse-backed summaries to render data.")
        return

    page = st.sidebar.radio("Page", PAGE_LABELS, index=0)
    _render_hero(overview["freshness"])

    if page == "Overview":
        _render_overview(overview)
    elif page == "Model predictions":
        try:
            forecast = load_forecast(base_url)
        except RequestException as exc:
            st.error("The model predictions page could not load analytics from the API.")
            st.code(str(exc))
            return
        _render_predictions(forecast)
    elif page == "Business impact":
        try:
            impact = load_impact(base_url)
        except RequestException as exc:
            st.error("The business impact page could not load analytics from the API.")
            st.code(str(exc))
            return
        _render_impact(impact)
    else:
        _render_drilldown(base_url, overview)


if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    import streamlit as st
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        "Streamlit is not installed. Install optional dashboard deps (see requirements.txt markers) "
        "or run this app from a Python version that supports them (e.g. 3.11/3.12)."
    ) from exc

from src.explainability.rule_based import detect_concerning_patterns, explain_category_from_detections, global_object_prevalence
from src.explainability.shap_surrogate import save_global_importance_bar, save_local_waterfall, try_train_surrogate


DATA_CSV = Path("data/enriched/yolo_detections.csv")
EXPLAIN_DIR = Path("data/explainability")


@st.cache_data
def load_data() -> pd.DataFrame:
    if not DATA_CSV.exists():
        return pd.DataFrame()
    return pd.read_csv(DATA_CSV)


@st.cache_resource
def train_explainer(df: pd.DataFrame):
    if df.empty:
        return None
    return try_train_surrogate(df)


def main() -> None:
    st.set_page_config(page_title="Medical Telegram Warehouse – Explainability", layout="wide")
    st.title("Model Explainability")
    st.caption("Explains YOLO-driven image enrichment using SHAP (when available) and rule-based summaries.")

    df = load_data()

    if df.empty:
        st.info(
            "No YOLO detections found at data/enriched/yolo_detections.csv. "
            "Run the YOLO enrichment step first (e.g., `python src/yolo_detect.py`)."
        )
        return

    artifacts = train_explainer(df)

    tab_global, tab_local, tab_patterns = st.tabs(
        [
            "Which features matter most? (Global)",
            "Why this prediction? (Local)",
            "Concerning patterns",
        ]
    )

    with tab_global:
        st.subheader("Global importance")
        if artifacts is not None:
            out_path = EXPLAIN_DIR / "global_feature_importance.png"
            try:
                save_global_importance_bar(artifacts, out_path)
                st.image(str(out_path), caption="Global feature importance (mean |SHAP|)")
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Could not render SHAP plot: {exc}")

            st.write("Surrogate model is trained on YOLO outputs (detections + confidences) to explain category decisions.")
        else:
            st.warning(
                "SHAP/scikit-learn not available in this environment (common on Python 3.14). "
                "Showing a rule-based global summary instead."
            )

        st.subheader("Rule-based global summary")
        st.dataframe(global_object_prevalence(df), use_container_width=True)

    with tab_local:
        st.subheader("Local explanation")

        cols = [c for c in ["channel_name", "message_id", "category", "max_confidence"] if c in df.columns]
        st.dataframe(df[cols].head(25), use_container_width=True)

        channel_options = sorted(df["channel_name"].dropna().astype(str).unique().tolist()) if "channel_name" in df.columns else []
        selected_channel = st.selectbox("Channel", options=channel_options) if channel_options else None

        filtered = df
        if selected_channel is not None and "channel_name" in df.columns:
            filtered = df[df["channel_name"].astype(str) == selected_channel]

        row_idx = st.number_input(
            "Row index in filtered table",
            min_value=0,
            max_value=max(0, len(filtered) - 1),
            value=0,
            step=1,
        )

        if len(filtered) == 0:
            st.info("No rows for this selection.")
            return

        row = filtered.iloc[int(row_idx)].to_dict()

        image_path = Path(str(row.get("image_path", ""))) if row.get("image_path") else None
        if image_path and image_path.exists():
            st.image(str(image_path), caption=f"{row.get('channel_name', '')} / {row.get('message_id', '')}")

        st.markdown("**Model output**")
        st.write(
            {
                "category": row.get("category"),
                "max_confidence": row.get("max_confidence"),
                "detections": row.get("detections"),
            }
        )

        st.markdown("**Why this category? (rule-based)**")
        for r in explain_category_from_detections(row.get("detections")):
            st.write(f"- {r}")

        if artifacts is not None:
            st.markdown("**Why this category? (SHAP surrogate)**")
            out_path = EXPLAIN_DIR / "local_waterfall.png"
            # row index relative to the full df, because the surrogate was trained on full df
            full_row_index = int(filtered.index[int(row_idx)])
            try:
                used_class = save_local_waterfall(artifacts, full_row_index, out_path)
                st.image(str(out_path), caption=f"Local SHAP waterfall for class '{used_class}'")
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Could not render local SHAP plot: {exc}")

    with tab_patterns:
        st.subheader("Concerning patterns")
        concerns = detect_concerning_patterns(df)
        if not concerns:
            st.success("No obvious red flags detected by the simple checks.")
        else:
            for c in concerns:
                st.warning(f"{c.name}: {c.details}")

        st.subheader("Recent rows")
        st.dataframe(df.tail(50), use_container_width=True)


if __name__ == "__main__":
    main()

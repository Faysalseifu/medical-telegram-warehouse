from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .yolo_features import featurize_detection_row


@dataclass(frozen=True)
class Concern:
    name: str
    details: str


def explain_category_from_detections(detections: Any) -> list[str]:
    """Human-readable reasons for the final category.

    Note: the category logic lives in `src/yolo_detect.py::categorize` and depends on
    whether relevant objects are detected.
    """
    f = featurize_detection_row(detections=detections, max_confidence=None)

    reasons: list[str] = []
    if f.has_person:
        reasons.append(f"Detected person (max conf {f.person_max_conf:.2f})")
    if f.has_container:
        reasons.append(f"Detected container-like object (max conf {f.container_max_conf:.2f})")
    if not reasons:
        reasons.append("No relevant objects detected")

    return reasons


def global_object_prevalence(df: pd.DataFrame) -> pd.DataFrame:
    """Global "what matters" summary based on object prevalence per category.

    Returns a dataframe with counts and rates of person/container detections by category.
    """
    if "category" not in df.columns:
        raise KeyError("Expected a 'category' column")

    features = []
    for _, row in df.iterrows():
        f = featurize_detection_row(row.get("detections"), row.get("max_confidence"))
        features.append({"category": row.get("category"), "has_person": f.has_person, "has_container": f.has_container})

    feat_df = pd.DataFrame(features)
    if feat_df.empty:
        return pd.DataFrame(columns=["category", "n", "person_rate", "container_rate"])

    grouped = feat_df.groupby("category", dropna=False)
    out = grouped.agg(n=("category", "size"), person_rate=("has_person", "mean"), container_rate=("has_container", "mean")).reset_index()
    return out.sort_values("n", ascending=False)


def detect_concerning_patterns(df: pd.DataFrame) -> list[Concern]:
    """Flag simple, high-signal issues that warrant investigation."""
    concerns: list[Concern] = []

    if df.empty:
        return concerns

    # 1) High share of a single category in a channel.
    if "channel_name" in df.columns and "category" in df.columns:
        counts = df.groupby(["channel_name", "category"]).size().reset_index(name="n")
        totals = df.groupby("channel_name").size().reset_index(name="total")
        merged = counts.merge(totals, on="channel_name")
        merged["share"] = merged["n"] / merged["total"]
        dominant = merged[merged["share"] >= 0.95]
        for _, row in dominant.iterrows():
            concerns.append(
                Concern(
                    name="dominant_category",
                    details=(
                        f"Channel '{row['channel_name']}' is {row['share']:.0%} '{row['category']}' "
                        f"({int(row['n'])}/{int(row['total'])})."
                    ),
                )
            )

    # 2) Many low-confidence detections.
    if "max_confidence" in df.columns:
        low = df[df["max_confidence"].fillna(0) < 0.25]
        if len(low) / len(df) >= 0.30:
            concerns.append(
                Concern(
                    name="low_confidence_rate",
                    details=f"{len(low)}/{len(df)} images have max_confidence < 0.25.",
                )
            )

    # 3) Category non-other but detections missing.
    if "category" in df.columns and "detections" in df.columns:
        missing_det = df[(df["category"] != "other") & (df["detections"].isna() | (df["detections"].astype(str).str.strip() == ""))]
        if not missing_det.empty:
            concerns.append(
                Concern(
                    name="missing_detections",
                    details=f"{len(missing_det)} rows have a non-'other' category but empty detections.",
                )
            )

    return concerns

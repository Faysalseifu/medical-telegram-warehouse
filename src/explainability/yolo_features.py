from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class YoloFeatureRow:
    has_person: int
    has_container: int
    person_max_conf: float
    container_max_conf: float
    relevant_detection_count: int
    max_confidence: float


_RELEVANT_OBJECT_ALIASES: dict[str, str] = {
    # ultralytics COCO labels may vary slightly; normalize common ones.
    "person": "person",
    "bottle": "container",
    "cup": "container",
    "vase": "container",
}


def parse_detections(detections: Any) -> list[tuple[str, float]]:
    """Parse `detections` column values like "bottle:0.72; person:0.55".

    Returns a list of (label, confidence) pairs.
    """
    if detections is None or (isinstance(detections, float) and pd.isna(detections)):
        return []

    if not isinstance(detections, str):
        detections = str(detections)

    items: list[tuple[str, float]] = []
    for raw_item in detections.split(";"):
        raw_item = raw_item.strip()
        if not raw_item:
            continue
        if ":" not in raw_item:
            continue
        label, conf_str = raw_item.split(":", 1)
        label = label.strip()
        try:
            conf = float(conf_str.strip())
        except ValueError:
            continue
        items.append((label, conf))

    return items


def featurize_detection_row(detections: Any, max_confidence: Any) -> YoloFeatureRow:
    parsed = parse_detections(detections)

    person_max = 0.0
    container_max = 0.0
    relevant_count = 0

    for label, conf in parsed:
        normalized = _RELEVANT_OBJECT_ALIASES.get(label)
        if normalized is None:
            continue
        relevant_count += 1
        if normalized == "person":
            person_max = max(person_max, conf)
        elif normalized == "container":
            container_max = max(container_max, conf)

    max_conf = 0.0
    try:
        if max_confidence is not None and not (isinstance(max_confidence, float) and pd.isna(max_confidence)):
            max_conf = float(max_confidence)
    except (TypeError, ValueError):
        max_conf = 0.0

    return YoloFeatureRow(
        has_person=1 if person_max > 0 else 0,
        has_container=1 if container_max > 0 else 0,
        person_max_conf=float(person_max),
        container_max_conf=float(container_max),
        relevant_detection_count=int(relevant_count),
        max_confidence=float(max_conf),
    )


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Convert YOLO detections dataframe into a tabular feature matrix."""
    if "detections" not in df.columns:
        raise KeyError("Expected a 'detections' column")

    max_conf_col = "max_confidence" if "max_confidence" in df.columns else None

    features: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        f = featurize_detection_row(
            detections=row.get("detections"),
            max_confidence=row.get(max_conf_col) if max_conf_col else None,
        )
        features.append(
            {
                "has_person": f.has_person,
                "has_container": f.has_container,
                "person_max_conf": f.person_max_conf,
                "container_max_conf": f.container_max_conf,
                "relevant_detection_count": f.relevant_detection_count,
                "max_confidence": f.max_confidence,
            }
        )

    return pd.DataFrame(features)

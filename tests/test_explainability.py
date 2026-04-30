import pandas as pd
import pytest

from src.explainability.rule_based import detect_concerning_patterns, explain_category_from_detections, global_object_prevalence
from src.explainability.yolo_features import build_feature_frame, parse_detections


def test_parse_detections_happy_path():
    parsed = parse_detections("bottle:0.72; person:0.55")
    assert ("bottle", 0.72) in parsed
    assert ("person", 0.55) in parsed


def test_parse_detections_handles_nulls():
    assert parse_detections(None) == []
    assert parse_detections(float("nan")) == []


def test_build_feature_frame_shapes():
    df = pd.DataFrame(
        {
            "detections": ["person:0.9; bottle:0.8", None, "cup:0.4"],
            "max_confidence": [0.9, 0.0, 0.4],
        }
    )
    X = build_feature_frame(df)
    assert set(X.columns) == {
        "has_person",
        "has_container",
        "person_max_conf",
        "container_max_conf",
        "relevant_detection_count",
        "max_confidence",
    }
    assert len(X) == 3
    assert X.loc[0, "has_person"] == 1
    assert X.loc[0, "has_container"] == 1


def test_rule_based_local_explanation():
    reasons = explain_category_from_detections("person:0.9; bottle:0.2")
    assert any("person" in r.lower() for r in reasons)
    assert any("container" in r.lower() for r in reasons)


def test_global_object_prevalence():
    df = pd.DataFrame(
        {
            "category": ["promotional", "other", "product_display"],
            "detections": ["person:0.9; bottle:0.8", None, "bottle:0.6"],
            "max_confidence": [0.9, 0.0, 0.6],
        }
    )
    out = global_object_prevalence(df)
    assert "category" in out.columns
    assert "person_rate" in out.columns


def test_concerning_patterns_flags_missing_detections():
    df = pd.DataFrame(
        {
            "channel_name": ["A", "A", "A"],
            "category": ["promotional", "promotional", "promotional"],
            "detections": [None, None, None],
            "max_confidence": [0.1, 0.1, 0.1],
        }
    )
    concerns = detect_concerning_patterns(df)
    assert any(c.name == "dominant_category" for c in concerns)
    assert any(c.name == "low_confidence_rate" for c in concerns)
    assert any(c.name == "missing_detections" for c in concerns)


@pytest.mark.optional
def test_shap_surrogate_smoke_if_available():
    sklearn = pytest.importorskip("sklearn")
    shap = pytest.importorskip("shap")

    from src.explainability.shap_surrogate import train_surrogate

    df = pd.DataFrame(
        {
            "category": ["promotional", "product_display", "lifestyle", "other"] * 5,
            "detections": [
                "person:0.9; bottle:0.8",
                "bottle:0.7",
                "person:0.6",
                None,
            ]
            * 5,
            "max_confidence": [0.9, 0.7, 0.6, 0.0] * 5,
        }
    )

    artifacts = train_surrogate(df)
    assert artifacts.shap_values is not None
    assert len(artifacts.feature_names) > 0

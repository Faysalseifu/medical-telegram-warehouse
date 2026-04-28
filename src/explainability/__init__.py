"""Explainability utilities.

This project’s primary ML component is YOLO-based image enrichment.
We provide two layers of explainability:

- Rule-based explanations derived from YOLO detections (always available)
- Optional SHAP explanations for a lightweight surrogate model trained on YOLO outputs

SHAP/scikit-learn are optional because the repo’s local dev environment may be on
Python versions where those wheels are not yet available (e.g. Python 3.14).
"""

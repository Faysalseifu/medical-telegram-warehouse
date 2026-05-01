from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from .yolo_features import build_feature_frame


@dataclass(frozen=True)
class ShapArtifacts:
    model: Any
    explainer: Any
    shap_values: Any
    feature_names: list[str]
    class_names: list[str]


def _optional_imports() -> tuple[Any, Any, Any]:
    """Import optional deps.

    Returns (sklearn, shap, matplotlib_pyplot).

    We keep them optional because this repo may be developed on Python versions
    without wheels for these packages (e.g. Python 3.14).
    """
    try:
        import sklearn  # type: ignore
        from sklearn.ensemble import RandomForestClassifier  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        raise ImportError("scikit-learn is required for SHAP surrogate explainability") from exc

    try:
        import shap  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise ImportError("shap is required for SHAP surrogate explainability") from exc

    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise ImportError("matplotlib is required for SHAP plotting") from exc

    return sklearn, shap, plt


def train_surrogate(df: pd.DataFrame, *, label_col: str = "category", random_state: int = 42) -> ShapArtifacts:
    """Train a lightweight surrogate model on YOLO outputs and compute SHAP values."""
    sklearn, shap, _plt = _optional_imports()
    from sklearn.ensemble import RandomForestClassifier

    if label_col not in df.columns:
        raise KeyError(f"Expected label column '{label_col}'")

    X = build_feature_frame(df)
    y = df[label_col].astype(str)

    model = RandomForestClassifier(
        n_estimators=200,
        random_state=random_state,
        n_jobs=-1,
        class_weight="balanced_subsample",
    )
    model.fit(X, y)

    explainer = shap.Explainer(model, X, feature_names=list(X.columns))
    shap_values = explainer(X)

    class_names = [str(c) for c in getattr(model, "classes_", [])]
    return ShapArtifacts(
        model=model,
        explainer=explainer,
        shap_values=shap_values,
        feature_names=list(X.columns),
        class_names=class_names,
    )


def mean_abs_shap(shap_values: Any) -> np.ndarray:
    values = np.asarray(shap_values.values)
    if values.ndim == 3:
        return np.abs(values).mean(axis=(0, 2))
    return np.abs(values).mean(axis=0)


def save_global_importance_bar(artifacts: ShapArtifacts, out_path: Path, *, title: str = "Global feature importance") -> None:
    _sklearn, _shap, plt = _optional_imports()

    importances = mean_abs_shap(artifacts.shap_values)
    order = np.argsort(importances)[::-1]

    plt.figure(figsize=(8, 4.5))
    plt.bar([artifacts.feature_names[i] for i in order], importances[order])
    plt.title(title)
    plt.ylabel("mean(|SHAP|)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=160)
    plt.close()


def save_local_waterfall(
    artifacts: ShapArtifacts,
    row_index: int,
    out_path: Path,
    *,
    class_name: Optional[str] = None,
) -> str:
    """Save a per-row waterfall plot.

    Returns the class name used for the plot.
    """
    _sklearn, shap, plt = _optional_imports()

    sv = artifacts.shap_values
    values = np.asarray(sv.values)

    if values.ndim == 3:
        # Choose which output to explain.
        if class_name is None:
            # Default: explain the model’s predicted class for this row.
            proba = artifacts.model.predict_proba(pd.DataFrame(sv.data, columns=artifacts.feature_names))
            class_index = int(np.argmax(proba[row_index]))
            used_class = artifacts.class_names[class_index] if artifacts.class_names else str(class_index)
        else:
            if class_name not in artifacts.class_names:
                raise ValueError(f"Unknown class '{class_name}'")
            class_index = int(artifacts.class_names.index(class_name))
            used_class = class_name

        exp = shap.Explanation(
            values=values[row_index, :, class_index],
            base_values=np.asarray(sv.base_values)[row_index, class_index],
            data=np.asarray(sv.data)[row_index, :],
            feature_names=artifacts.feature_names,
        )
    else:
        used_class = class_name or "output"
        exp = shap.Explanation(
            values=values[row_index, :],
            base_values=np.asarray(sv.base_values)[row_index],
            data=np.asarray(sv.data)[row_index, :],
            feature_names=artifacts.feature_names,
        )

    plt.figure(figsize=(8, 5.0))
    shap.plots.waterfall(exp, show=False)
    plt.title(f"Local explanation ({used_class})")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=160)
    plt.close()
    return used_class


def try_train_surrogate(df: pd.DataFrame, *, label_col: str = "category") -> Optional[ShapArtifacts]:
    """Best-effort surrogate training; returns None if optional deps aren't available."""
    try:
        return train_surrogate(df, label_col=label_col)
    except ImportError:
        return None

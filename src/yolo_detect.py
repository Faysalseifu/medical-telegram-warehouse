"""Run YOLOv8n on raw images and categorize visuals into promotional/product classes."""
from datetime import datetime
import logging
from pathlib import Path
import sys
from typing import Iterable

import pandas as pd
from ultralytics import YOLO

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app_config import ensure_directory, get_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SETTINGS = get_config().yolo
ensure_directory(SETTINGS.output_csv.parent)


def categorize(detected_classes: set[int]) -> str:
    has_person = SETTINGS.person_class in detected_classes
    has_container = any(
        c in detected_classes for c in (SETTINGS.bottle_class, SETTINGS.cup_class, SETTINGS.vase_class)
    )

    if has_person and has_container:
        return "promotional"
    if has_container:
        return "product_display"
    if has_person:
        return "lifestyle"
    return "other"


def iter_images() -> Iterable[Path]:
    for img_path in SETTINGS.image_dir.rglob("*"):
        if img_path.is_file() and img_path.suffix.lower() in SETTINGS.image_extensions:
            yield img_path


def run_detection() -> None:
    if not SETTINGS.image_dir.exists():
        logger.warning("Image directory %s does not exist", SETTINGS.image_dir)
        return

    model = YOLO(SETTINGS.model_name)
    logger.info("Loaded %s", SETTINGS.model_name)

    results_list: list[dict[str, object]] = []

    for img_path in iter_images():
        try:
            results = model(img_path, verbose=False)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error running model on %s: %s", img_path, exc)
            continue

        if not results or not results[0].boxes:
            category = "other"
            detections: List[str] = []
            max_conf = 0.0
        else:
            boxes = results[0].boxes
            cls_ids = boxes.cls.int().tolist()
            confs = boxes.conf.tolist()

            detected = set(cls_ids)
            category = categorize(detected)

            detections = []
            max_conf = 0.0
            for cls_id, conf in zip(cls_ids, confs):
                if cls_id in SETTINGS.relevant_classes:
                    class_name = results[0].names[int(cls_id)]
                    detections.append(f"{class_name}:{conf:.2f}")
                    max_conf = max(max_conf, float(conf))

        message_id = img_path.stem
        channel = img_path.parent.name

        results_list.append(
            {
                "image_path": str(img_path),
                "channel_name": channel,
                "message_id": message_id,
                "category": category,
                "max_confidence": max_conf,
                "detections": "; ".join(detections) if detections else None,
                "processed_at": datetime.now().isoformat(),
            }
        )

        logger.info("%s -> %s (max conf: %.2f)", img_path, category, max_conf)

    if results_list:
        df = pd.DataFrame(results_list)
        df.to_csv(SETTINGS.output_csv, index=False)
        logger.info("Saved %d results to %s", len(results_list), SETTINGS.output_csv)
    else:
        logger.warning("No images processed")


if __name__ == "__main__":
    run_detection()

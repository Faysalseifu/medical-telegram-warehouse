"""Run YOLOv8n on raw images and categorize visuals into promotional/product classes."""
from datetime import datetime
import logging
from pathlib import Path
from typing import Iterable, List, Set

import pandas as pd
from ultralytics import YOLO

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MODEL = "yolov8n.pt"
IMG_DIR = Path("data/raw/images")
OUTPUT_CSV = Path("data/enriched/yolo_detections.csv")
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

PERSON_CLASS = 0
BOTTLE_CLASS = 39
CUP_CLASS = 41
VASE_CLASS = 86
RELEVANT_CLASSES: Set[int] = {PERSON_CLASS, BOTTLE_CLASS, CUP_CLASS, VASE_CLASS}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}


def categorize(detected_classes: Set[int]) -> str:
    has_person = PERSON_CLASS in detected_classes
    has_container = any(c in detected_classes for c in (BOTTLE_CLASS, CUP_CLASS, VASE_CLASS))

    if has_person and has_container:
        return "promotional"
    if has_container:
        return "product_display"
    if has_person:
        return "lifestyle"
    return "other"


def iter_images() -> Iterable[Path]:
    for img_path in IMG_DIR.rglob("*"):
        if img_path.is_file() and img_path.suffix.lower() in IMAGE_EXTENSIONS:
            yield img_path


def run_detection() -> None:
    if not IMG_DIR.exists():
        logger.warning("Image directory %s does not exist", IMG_DIR)
        return

    model = YOLO(MODEL)
    logger.info("Loaded %s", MODEL)

    results_list: List[dict] = []

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
                if cls_id in RELEVANT_CLASSES:
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
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info("Saved %d results to %s", len(results_list), OUTPUT_CSV)
    else:
        logger.warning("No images processed")


if __name__ == "__main__":
    run_detection()

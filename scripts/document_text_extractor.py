from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

try:
    import numpy as np
except ImportError:  # pragma: no cover - optional dependency
    np = None

try:
    from PIL import Image, ImageEnhance, ImageFilter, ImageOps
except ImportError:  # pragma: no cover - optional dependency
    Image = ImageEnhance = ImageFilter = ImageOps = None

try:
    from paddleocr import PaddleOCR
except ImportError:  # pragma: no cover - optional dependency
    PaddleOCR = None


@dataclass
class ExtractionLine:
    page: int
    index: int
    text: str
    confidence: float
    source: str


@dataclass
class ExtractionResult:
    file: str
    kind: str
    extraction_mode: str
    page_count: int
    text: str
    lines: list[ExtractionLine] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SWIFT_EXTRACTOR_PATH = PROJECT_ROOT / "scripts" / "extract_document_text.swift"
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff", ".bmp", ".gif"}
SWIFT_VISION_ENABLED = os.environ.get("ENABLE_SWIFT_VISION", "").strip().lower() in {"1", "true", "yes"}

_PADDLE_OCR_INSTANCE: object | None = None
_PADDLE_OCR_ERROR: str | None = None


def split_text_to_lines(text: str, *, page: int, source: str, confidence: float) -> list[ExtractionLine]:
    rows = [line.strip() for line in text.splitlines() if line.strip()]
    return [
        ExtractionLine(page=page, index=index, text=row, confidence=confidence, source=source)
        for index, row in enumerate(rows, start=1)
    ]


def run_command(command: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def is_tool_available(name: str) -> bool:
    return shutil.which(name) is not None


def detect_available_extractors() -> dict[str, object]:
    return {
        "platform": platform.system().lower(),
        "tools": {
            "paddleocr": is_paddleocr_available(),
            "pdftotext": is_tool_available("pdftotext"),
            "pdftoppm": is_tool_available("pdftoppm"),
            "tesseract": is_tool_available("tesseract"),
            "ocrmypdf": is_tool_available("ocrmypdf"),
            "swift_vision": (
                SWIFT_VISION_ENABLED
                and platform.system().lower() == "darwin"
                and SWIFT_EXTRACTOR_PATH.exists()
                and is_tool_available("swift")
            ),
        },
        "recommended_linux_packages": [
            "poppler-utils",
            "tesseract-ocr",
            "tesseract-ocr-rus",
            "ocrmypdf",
        ],
    }


def score_text_payload(text: str, lines: list[ExtractionLine], *, warnings: list[str] | None = None) -> float:
    char_score = len(text.strip())
    line_score = len(lines) * 24.0
    confidence_score = statistics.fmean(line.confidence for line in lines) * 40.0 if lines else 0.0
    warning_penalty = len(warnings or []) * 10.0
    return char_score + line_score + confidence_score - warning_penalty


def is_paddleocr_available() -> bool:
    return PaddleOCR is not None and Image is not None and np is not None


def get_paddle_ocr() -> object | None:
    global _PADDLE_OCR_INSTANCE, _PADDLE_OCR_ERROR
    if _PADDLE_OCR_INSTANCE is not None:
        return _PADDLE_OCR_INSTANCE
    if _PADDLE_OCR_ERROR is not None or not is_paddleocr_available():
        return None
    try:
        _PADDLE_OCR_INSTANCE = PaddleOCR(
            use_angle_cls=True,
            lang="ru",
            show_log=False,
        )
    except Exception as exc:  # pragma: no cover - depends on optional runtime
        _PADDLE_OCR_ERROR = str(exc)
        return None
    return _PADDLE_OCR_INSTANCE


def _row_has_quantity_hint(row: dict[str, object]) -> bool:
    row_text = str(row["text"]).lower()
    return any(token.isdigit() for token in row_text.replace(",", " ").replace(".", " ").split()) or bool(
        any(fragment.get("has_digits") for fragment in row["fragments"])
    )


def _ocr_result_to_boxes(raw_result: object) -> list[dict[str, object]]:
    if not raw_result:
        return []
    lines = raw_result
    if isinstance(raw_result, list) and raw_result and isinstance(raw_result[0], list):
        first = raw_result[0]
        if first and isinstance(first[0], (list, tuple)) and len(first[0]) == 2:
            lines = first
    boxes: list[dict[str, object]] = []
    for line in lines or []:
        if not isinstance(line, (list, tuple)) or len(line) != 2:
            continue
        box, meta = line
        if not isinstance(meta, (list, tuple)) or len(meta) < 2:
            continue
        text = str(meta[0]).strip()
        confidence = float(meta[1] or 0.0)
        if not text:
            continue
        xs = [float(point[0]) for point in box]
        ys = [float(point[1]) for point in box]
        left = min(xs)
        right = max(xs)
        top = min(ys)
        bottom = max(ys)
        boxes.append(
            {
                "text": text,
                "confidence": confidence,
                "left": left,
                "right": right,
                "top": top,
                "bottom": bottom,
                "height": max(bottom - top, 1.0),
                "center_y": (top + bottom) / 2.0,
                "has_digits": any(char.isdigit() for char in text),
            }
        )
    return boxes


def _merge_row_rows(primary: dict[str, object], secondary: dict[str, object]) -> dict[str, object]:
    fragments = list(primary["fragments"]) + list(secondary["fragments"])
    fragments.sort(key=lambda item: float(item["left"]))
    text = " | ".join(str(item["text"]).strip() for item in fragments if str(item["text"]).strip())
    confidence = statistics.fmean(float(item["confidence"]) for item in fragments) if fragments else 0.0
    top = min(float(item["top"]) for item in fragments)
    bottom = max(float(item["bottom"]) for item in fragments)
    return {
        "text": text,
        "confidence": confidence,
        "fragments": fragments,
        "center_y": (top + bottom) / 2.0,
        "top": top,
        "bottom": bottom,
    }


def _group_boxes_into_rows(boxes: list[dict[str, object]]) -> list[dict[str, object]]:
    if not boxes:
        return []
    median_height = statistics.median(float(item["height"]) for item in boxes)
    base_tolerance = max(12.0, median_height * 1.55)
    rows: list[dict[str, object]] = []
    for box in sorted(boxes, key=lambda item: (float(item["center_y"]), float(item["left"]))):
        best_index: int | None = None
        best_gap = float("inf")
        for index, row in enumerate(rows):
            row_center = float(row["center_y"])
            row_top = float(row["top"])
            row_bottom = float(row["bottom"])
            vertical_gap = abs(float(box["center_y"]) - row_center)
            overlap = min(float(box["bottom"]), row_bottom) - max(float(box["top"]), row_top)
            tolerance = max(base_tolerance, (row_bottom - row_top) * 1.25)
            if vertical_gap <= tolerance or overlap >= min(float(box["height"]), row_bottom - row_top) * 0.25:
                if vertical_gap < best_gap:
                    best_gap = vertical_gap
                    best_index = index
        if best_index is None:
            rows.append(
                {
                    "fragments": [box],
                    "center_y": float(box["center_y"]),
                    "top": float(box["top"]),
                    "bottom": float(box["bottom"]),
                }
            )
            continue
        row = rows[best_index]
        row["fragments"].append(box)
        row["fragments"].sort(key=lambda item: float(item["left"]))
        row["top"] = min(float(row["top"]), float(box["top"]))
        row["bottom"] = max(float(row["bottom"]), float(box["bottom"]))
        row["center_y"] = (float(row["top"]) + float(row["bottom"])) / 2.0

    enriched_rows = [
        {
            "text": " | ".join(str(fragment["text"]).strip() for fragment in row["fragments"] if str(fragment["text"]).strip()),
            "confidence": statistics.fmean(float(fragment["confidence"]) for fragment in row["fragments"]),
            "fragments": row["fragments"],
            "center_y": float(row["center_y"]),
            "top": float(row["top"]),
            "bottom": float(row["bottom"]),
        }
        for row in rows
    ]
    enriched_rows.sort(key=lambda item: float(item["center_y"]))

    merged_rows: list[dict[str, object]] = []
    for row in enriched_rows:
        if not merged_rows:
            merged_rows.append(row)
            continue
        previous = merged_rows[-1]
        vertical_gap = float(row["top"]) - float(previous["bottom"])
        is_sparse_continuation = (
            len(row["fragments"]) <= 2
            and not _row_has_quantity_hint(row)
            and vertical_gap <= base_tolerance * 0.8
        )
        if is_sparse_continuation:
            merged_rows[-1] = _merge_row_rows(previous, row)
            continue
        merged_rows.append(row)
    return merged_rows


def _score_ocr_rows(rows: list[dict[str, object]]) -> float:
    if not rows:
        return 0.0
    char_count = sum(len(str(row["text"])) for row in rows)
    delimiter_count = sum(str(row["text"]).count("|") for row in rows)
    avg_confidence = statistics.fmean(float(row["confidence"]) for row in rows)
    return char_count + delimiter_count * 18.0 + avg_confidence * 40.0 + len(rows) * 6.0


def _iter_paddle_image_variants(path: Path) -> list[tuple[str, object]]:
    if Image is None or np is None:
        return []
    with Image.open(path) as loaded:
        base = ImageOps.exif_transpose(loaded).convert("RGB")
        longest_edge = max(base.size) if base.size else 0
        if longest_edge and longest_edge < 1800:
            scale = min(2.4, 1800 / float(longest_edge))
            resized = (
                max(1, int(round(base.size[0] * scale))),
                max(1, int(round(base.size[1] * scale))),
            )
            base = base.resize(resized, Image.Resampling.LANCZOS)
        grayscale = ImageOps.grayscale(base)
        contrast = ImageEnhance.Contrast(grayscale).enhance(1.9)
        sharpened = contrast.filter(ImageFilter.SHARPEN)
        threshold = contrast.point(lambda value: 255 if value >= 165 else 0)
        return [
            ("paddleocr_rgb", np.asarray(base)),
            ("paddleocr_gray", np.asarray(grayscale)),
            ("paddleocr_contrast", np.asarray(sharpened)),
            ("paddleocr_threshold", np.asarray(threshold)),
        ]


def _save_tesseract_variant_images(path: Path, tmp_dir: Path) -> list[tuple[str, Path]]:
    if Image is None:
        return [("tesseract_raw", path)]
    with Image.open(path) as loaded:
        base = ImageOps.exif_transpose(loaded).convert("RGB")
        longest_edge = max(base.size) if base.size else 0
        if longest_edge and longest_edge < 1800:
            scale = min(2.6, 1800 / float(longest_edge))
            resized = (
                max(1, int(round(base.size[0] * scale))),
                max(1, int(round(base.size[1] * scale))),
            )
            base = base.resize(resized, Image.Resampling.LANCZOS)
        grayscale = ImageOps.grayscale(base)
        contrast = ImageEnhance.Contrast(grayscale).enhance(2.2)
        sharpened = contrast.filter(ImageFilter.SHARPEN)
        threshold = contrast.point(lambda value: 255 if value >= 180 else 0)

        variants = [
            ("tesseract_rgb", base),
            ("tesseract_gray", grayscale),
            ("tesseract_contrast", sharpened),
            ("tesseract_threshold", threshold),
        ]
        output: list[tuple[str, Path]] = []
        for mode_name, image in variants:
            out_path = tmp_dir / f"{mode_name}.png"
            image.save(out_path, format="PNG")
            output.append((mode_name, out_path))
        return output


def extract_image_via_paddleocr(path: Path, *, page: int = 1) -> ExtractionResult | None:
    ocr = get_paddle_ocr()
    if ocr is None:
        if _PADDLE_OCR_ERROR:
            return ExtractionResult(
                file=str(path),
                kind="image",
                extraction_mode="paddleocr_failed",
                page_count=1,
                text="",
                warnings=[_PADDLE_OCR_ERROR],
            )
        return None

    variants = _iter_paddle_image_variants(path)
    if not variants:
        return None

    warnings: list[str] = []
    best_rows: list[dict[str, object]] = []
    best_mode = "paddleocr"
    best_score = -1.0
    best_confidence = 0.0

    for mode_name, image_data in variants:
        try:
            raw_result = ocr.ocr(image_data, cls=True)
        except Exception as exc:  # pragma: no cover - optional runtime failure
            warnings.append(f"{mode_name}: {exc}")
            continue
        boxes = _ocr_result_to_boxes(raw_result)
        rows = _group_boxes_into_rows(boxes)
        score = _score_ocr_rows(rows)
        if rows and score > best_score:
            best_rows = rows
            best_mode = mode_name
            best_score = score
            best_confidence = statistics.fmean(float(row["confidence"]) for row in rows)

    if not best_rows:
        return ExtractionResult(
            file=str(path),
            kind="image",
            extraction_mode="paddleocr_failed",
            page_count=1,
            text="",
            warnings=warnings or ["PaddleOCR не распознал текст на изображении"],
        )

    lines = [
        ExtractionLine(
            page=page,
            index=index,
            text=str(row["text"]),
            confidence=float(row["confidence"]),
            source="paddleocr",
        )
        for index, row in enumerate(best_rows, start=1)
        if str(row["text"]).strip()
    ]
    return ExtractionResult(
        file=str(path),
        kind="image",
        extraction_mode=best_mode,
        page_count=1,
        text="\n".join(line.text for line in lines),
        lines=lines,
        warnings=warnings + ([f"avg_confidence={best_confidence:.2f}"] if best_confidence else []),
    )


def extract_pdf_via_pdftotext(path: Path) -> ExtractionResult | None:
    if not is_tool_available("pdftotext"):
        return None
    result = run_command(["pdftotext", "-layout", str(path), "-"])
    if result.returncode != 0:
        return ExtractionResult(
            file=str(path),
            kind="pdf",
            extraction_mode="pdftotext_failed",
            page_count=0,
            text="",
            warnings=[result.stderr.strip() or "pdftotext завершился с ошибкой"],
        )
    text = result.stdout.strip()
    return ExtractionResult(
        file=str(path),
        kind="pdf",
        extraction_mode="pdftotext",
        page_count=max(1, text.count("\f") + 1 if text else 0),
        text=text,
        lines=split_text_to_lines(text, page=1, source="pdftotext", confidence=1.0),
    )


def extract_image_via_tesseract(path: Path, *, page: int = 1) -> ExtractionResult | None:
    if not is_tool_available("tesseract"):
        return None

    warnings: list[str] = []
    best_text = ""
    best_lines: list[ExtractionLine] = []
    best_mode = "tesseract"
    best_score = -1.0

    with tempfile.TemporaryDirectory(prefix="tesseract_img_") as tmp_dir:
        variant_paths = _save_tesseract_variant_images(path, Path(tmp_dir))
        for mode_name, image_path in variant_paths:
            for psm in ("6", "11"):
                result = run_command(["tesseract", str(image_path), "stdout", "-l", "rus+eng", "--psm", psm])
                if result.returncode != 0:
                    warnings.append(f"{mode_name}/psm{psm}: {result.stderr.strip() or 'tesseract завершился с ошибкой'}")
                    continue
                text = result.stdout.strip()
                if not text:
                    continue
                lines = split_text_to_lines(text, page=page, source="tesseract", confidence=0.72)
                score = score_text_payload(text, lines)
                if score > best_score:
                    best_text = text
                    best_lines = lines
                    best_mode = f"{mode_name}_psm{psm}"
                    best_score = score

    if not best_text:
        return ExtractionResult(
            file=str(path),
            kind="image",
            extraction_mode="tesseract_failed",
            page_count=1,
            text="",
            warnings=warnings or ["tesseract не распознал текст на изображении"],
        )
    return ExtractionResult(
        file=str(path),
        kind="image",
        extraction_mode=best_mode,
        page_count=1,
        text=best_text,
        lines=best_lines,
        warnings=warnings,
    )


def extract_pdf_via_ocrmypdf(path: Path) -> ExtractionResult | None:
    if not (is_tool_available("ocrmypdf") and is_tool_available("pdftotext")):
        return None
    with tempfile.TemporaryDirectory(prefix="ocrpdf_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        ocr_path = tmp_dir_path / "ocr.pdf"
        ocr_result = run_command(["ocrmypdf", "--force-ocr", "--skip-text", str(path), str(ocr_path)])
        if ocr_result.returncode != 0:
            return ExtractionResult(
                file=str(path),
                kind="pdf",
                extraction_mode="ocrmypdf_failed",
                page_count=0,
                text="",
                warnings=[ocr_result.stderr.strip() or "ocrmypdf завершился с ошибкой"],
            )
        text_result = run_command(["pdftotext", "-layout", str(ocr_path), "-"])
        if text_result.returncode != 0:
            return ExtractionResult(
                file=str(path),
                kind="pdf",
                extraction_mode="ocrmypdf_failed",
                page_count=0,
                text="",
                warnings=[text_result.stderr.strip() or "Не удалось извлечь текст после OCR PDF"],
            )
        text = text_result.stdout.strip()
        return ExtractionResult(
            file=str(path),
            kind="pdf",
            extraction_mode="ocrmypdf",
            page_count=max(1, text.count("\f") + 1 if text else 0),
            text=text,
            lines=split_text_to_lines(text, page=1, source="ocrmypdf", confidence=0.8),
        )


def extract_pdf_via_poppler_tesseract(path: Path) -> ExtractionResult | None:
    if not (is_tool_available("pdftoppm") and is_tool_available("tesseract")):
        return None
    with tempfile.TemporaryDirectory(prefix="pdfppm_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        prefix = tmp_dir_path / "page"
        ppm_result = run_command(["pdftoppm", "-png", str(path), str(prefix)])
        if ppm_result.returncode != 0:
            return ExtractionResult(
                file=str(path),
                kind="pdf",
                extraction_mode="pdftoppm_failed",
                page_count=0,
                text="",
                warnings=[ppm_result.stderr.strip() or "pdftoppm завершился с ошибкой"],
            )
        images = sorted(tmp_dir_path.glob("page-*.png"))
        lines: list[ExtractionLine] = []
        warnings: list[str] = []
        for page_index, image_path in enumerate(images, start=1):
            image_result = extract_image_via_tesseract(image_path, page=page_index)
            if image_result is None:
                warnings.append("tesseract недоступен для OCR PDF-страниц")
                continue
            warnings.extend(image_result.warnings)
            lines.extend(image_result.lines)
        text = "\n".join(line.text for line in lines)
        return ExtractionResult(
            file=str(path),
            kind="pdf",
            extraction_mode="pdftoppm+tesseract",
            page_count=len(images),
            text=text,
            lines=lines,
            warnings=warnings,
        )


def extract_via_swift_vision(path: Path) -> ExtractionResult | None:
    if (
        not SWIFT_VISION_ENABLED
        or platform.system().lower() != "darwin"
        or not SWIFT_EXTRACTOR_PATH.exists()
        or not is_tool_available("swift")
    ):
        return None
    module_cache = PROJECT_ROOT / ".swift-module-cache"
    module_cache.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["SWIFT_MODULECACHE_PATH"] = str(module_cache)
    command = [
        "swift",
        "-module-cache-path",
        str(module_cache),
        str(SWIFT_EXTRACTOR_PATH),
        str(path),
    ]
    result = run_command(command, cwd=PROJECT_ROOT, env=env)
    if result.returncode != 0:
        return ExtractionResult(
            file=str(path),
            kind="pdf" if path.suffix.lower() == ".pdf" else "image",
            extraction_mode="swift_failed",
            page_count=0,
            text="",
            warnings=[result.stderr.strip() or "Swift Vision extractor завершился с ошибкой"],
        )
    payload = json.loads(result.stdout)
    return ExtractionResult(
        file=str(path),
        kind=str(payload.get("kind", "")),
        extraction_mode=str(payload.get("extractionMode", "")),
        page_count=int(payload.get("pageCount", 0)),
        text=str(payload.get("text", "")),
        lines=[
            ExtractionLine(
                page=int(item.get("page", 1)),
                index=int(item.get("index", 1)),
                text=str(item.get("text", "")),
                confidence=float(item.get("confidence", 0.0)),
                source=str(item.get("source", "")),
            )
            for item in payload.get("lines", [])
            if str(item.get("text", "")).strip()
        ],
        warnings=[str(item) for item in payload.get("warnings", [])],
    )


def merge_attempts(path: Path, attempts: Iterable[ExtractionResult | None]) -> ExtractionResult:
    warnings: list[str] = []
    best_attempt: ExtractionResult | None = None
    best_score = -1.0
    for attempt in attempts:
        if attempt is None:
            continue
        warnings.extend(attempt.warnings)
        if attempt.text.strip():
            score = score_text_payload(attempt.text, attempt.lines, warnings=attempt.warnings)
            if score > best_score:
                best_score = score
                best_attempt = attempt
    if best_attempt is not None:
        if warnings and best_attempt.warnings != warnings:
            best_attempt.warnings = warnings
        return best_attempt
    kind = "pdf" if path.suffix.lower() == ".pdf" else "image"
    return ExtractionResult(
        file=str(path),
        kind=kind,
        extraction_mode="unreadable",
        page_count=0,
        text="",
        lines=[],
        warnings=warnings or ["Не найден доступный extractor для документа"],
    )


def extract_document_text(path: Path) -> ExtractionResult:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return merge_attempts(
            path,
            (
                extract_pdf_via_pdftotext(path),
                extract_pdf_via_ocrmypdf(path),
                extract_pdf_via_poppler_tesseract(path),
                extract_via_swift_vision(path),
            ),
        )
    if suffix in IMAGE_SUFFIXES:
        return merge_attempts(
            path,
            (
                extract_image_via_paddleocr(path),
                extract_image_via_tesseract(path),
                extract_via_swift_vision(path),
            ),
        )
    raise ValueError(f"Unsupported extraction format: {suffix}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract text from PDFs and scanned images for the request pipeline.")
    parser.add_argument("input", nargs="?", help="Path to the PDF or image file.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument(
        "--show-tools",
        action="store_true",
        help="Print available extractor backends and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.show_tools:
        payload = detect_available_extractors()
        print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    if not args.input:
        raise SystemExit("input path is required unless --show-tools is used")
    path = Path(args.input).expanduser().resolve()
    result = extract_document_text(path)
    payload = {
        "file": result.file,
        "kind": result.kind,
        "extraction_mode": result.extraction_mode,
        "page_count": result.page_count,
        "warnings": result.warnings,
        "text": result.text,
        "lines": [
            {
                "page": line.page,
                "index": line.index,
                "text": line.text,
                "confidence": line.confidence,
                "source": line.source,
            }
            for line in result.lines
        ],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

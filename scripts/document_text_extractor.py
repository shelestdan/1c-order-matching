from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


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
            "pdftotext": is_tool_available("pdftotext"),
            "pdftoppm": is_tool_available("pdftoppm"),
            "tesseract": is_tool_available("tesseract"),
            "ocrmypdf": is_tool_available("ocrmypdf"),
            "swift_vision": platform.system().lower() == "darwin" and SWIFT_EXTRACTOR_PATH.exists(),
        },
        "recommended_linux_packages": [
            "poppler-utils",
            "tesseract-ocr",
            "tesseract-ocr-rus",
            "ocrmypdf",
        ],
    }


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
    result = run_command(["tesseract", str(path), "stdout", "-l", "rus+eng", "--psm", "6"])
    if result.returncode != 0:
        return ExtractionResult(
            file=str(path),
            kind="image",
            extraction_mode="tesseract_failed",
            page_count=1,
            text="",
            warnings=[result.stderr.strip() or "tesseract завершился с ошибкой"],
        )
    text = result.stdout.strip()
    return ExtractionResult(
        file=str(path),
        kind="image",
        extraction_mode="tesseract",
        page_count=1,
        text=text,
        lines=split_text_to_lines(text, page=page, source="tesseract", confidence=0.72),
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
    if platform.system().lower() != "darwin" or not SWIFT_EXTRACTOR_PATH.exists():
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
    for attempt in attempts:
        if attempt is None:
            continue
        warnings.extend(attempt.warnings)
        if attempt.text.strip():
            if warnings and attempt.warnings != warnings:
                attempt.warnings = warnings
            return attempt
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

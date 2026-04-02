from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from document_text_extractor import ExtractionLine, ExtractionResult
import normalize_client_requests as ncr


class NormalizeDocumentRequestTest(unittest.TestCase):
    def test_ocr_table_rows_fall_back_to_freeform_when_header_is_missing(self) -> None:
        extraction = ExtractionResult(
            file="/tmp/request.jpg",
            kind="image",
            extraction_mode="paddleocr_contrast",
            page_count=1,
            text=(
                "Чугунная задвижка Ду 50мм | Ci МЗВЗОч39р Ду50 Ру16 Т D040.023 10 | шт | 6\n"
                "Счетчик воды турбинный ДУ 50 мм | ДЕКАСТ СТВХ-50 78-50-01 | шт | 3"
            ),
            lines=[
                ExtractionLine(page=1, index=1, text="Чугунная задвижка Ду 50мм | Ci МЗВЗОч39р Ду50 Ру16 Т D040.023 10 | шт | 6", confidence=0.84, source="paddleocr"),
                ExtractionLine(page=1, index=2, text="Счетчик воды турбинный ДУ 50 мм | ДЕКАСТ СТВХ-50 78-50-01 | шт | 3", confidence=0.81, source="paddleocr"),
            ],
            warnings=[],
        )

        with mock.patch.object(ncr, "extract_document_text", return_value=extraction):
            parsed, issues, metadata = ncr.normalize_document_request(Path("/tmp/request.jpg"))

        self.assertEqual(len(parsed), 2)
        self.assertEqual(len(issues), 0)
        self.assertEqual(metadata.extraction_mode, "paddleocr_contrast")
        self.assertEqual(parsed[0].quantity, 6.0)
        self.assertEqual(parsed[1].quantity, 3.0)
        self.assertIn("Чугунная задвижка", parsed[0].name)
        self.assertIn("Счетчик воды", parsed[1].name)

    def test_ocr_table_rows_keep_delimited_mode_when_header_is_present(self) -> None:
        extraction = ExtractionResult(
            file="/tmp/request.jpg",
            kind="image",
            extraction_mode="paddleocr_rgb",
            page_count=1,
            text=(
                "Наименование материала | Марка, размер | Ед. изм | Всего кол-во\n"
                "Фланец плоский Ду50 50 мм | DN, пл Ст20 Ду50 Ру16 приварной | шт | 16"
            ),
            lines=[
                ExtractionLine(page=1, index=1, text="Наименование материала | Марка, размер | Ед. изм | Всего кол-во", confidence=0.88, source="paddleocr"),
                ExtractionLine(page=1, index=2, text="Фланец плоский Ду50 50 мм | DN, пл Ст20 Ду50 Ру16 приварной | шт | 16", confidence=0.83, source="paddleocr"),
            ],
            warnings=[],
        )

        with mock.patch.object(ncr, "extract_document_text", return_value=extraction):
            parsed, issues, _ = ncr.normalize_document_request(Path("/tmp/request.jpg"))

        self.assertEqual(len(parsed), 1)
        self.assertEqual(len(issues), 0)
        self.assertEqual(parsed[0].quantity, 16.0)
        self.assertEqual(parsed[0].unit, "шт")
        self.assertIn("Фланец плоский", parsed[0].name)


if __name__ == "__main__":
    unittest.main()

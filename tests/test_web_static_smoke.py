from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "web" / "app.js"


class WebStaticSmokeTest(unittest.TestCase):
    def test_export_file_uses_distinct_request_and_object_url_variables(self) -> None:
        text = APP_JS.read_text(encoding="utf-8")
        export_start = text.index("async function exportFile()")
        history_start = text.index("/* ===== History ===== */", export_start)
        export_block = text[export_start:history_start]

        self.assertIn("const url = new URL(", export_block)
        self.assertIn("const objectUrl = URL.createObjectURL(blob);", export_block)
        self.assertNotIn("const url = URL.createObjectURL(blob);", export_block)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
import sys
import tempfile
import unittest
import asyncio
import io
from pathlib import Path

from fastapi import HTTPException
from fastapi import UploadFile
from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import matching_api as matching_api_module  # noqa: E402
from matching_api import (  # noqa: E402
    SHARED_PASSWORD,
    AuthUser,
    STATUS_NOT_FOUND,
    _extract_session_token,
    _find_all_stock_files,
    _manual_search_candidates,
    _parse_requested_quantity,
    _authenticate_user,
    _build_admin_analytics,
    _build_export_audit_event,
    _ensure_job_access,
    _hash_password,
    _serialize_candidate,
    _update_row_quantity_inplace,
    app,
    login,
    me,
    upload_file,
)
from process_1c_orders import (  # noqa: E402
    Candidate,
    OrderLine,
    StockItem,
    StockMatcher,
    augment_search_text_with_dimension_tags,
    build_search_text,
    extract_dimension_tags,
    extract_family_tags,
    extract_key_tokens,
    extract_material_tags_from_search_text,
    extract_root_tokens,
    extract_tokens,
)


class MatchingApiHelpersTest(unittest.TestCase):
    def setUp(self) -> None:
        self.user_directory = {
            "anisovets": {
                "username": "anisovets",
                "display_name": "Анисовец",
                "role": "manager",
                "password_sha256": _hash_password("5956"),
            },
            "panov": {
                "username": "panov",
                "display_name": "Панов",
                "role": "manager",
                "password_sha256": _hash_password("6566"),
            },
        }

    def test_authenticate_supports_admin_and_manager_accounts(self) -> None:
        admin = _authenticate_user("", SHARED_PASSWORD, user_directory=self.user_directory)
        manager = _authenticate_user("anisovets", "5956", user_directory=self.user_directory)

        self.assertEqual(admin.role, "admin")
        self.assertEqual(admin.username, "admin")
        self.assertEqual(manager.role, "manager")
        self.assertEqual(manager.display_name, "Анисовец")

    def test_authenticate_rejects_unknown_or_invalid_password(self) -> None:
        with self.assertRaises(HTTPException) as unknown_ctx:
            _authenticate_user("missing", "1234", user_directory=self.user_directory)
        self.assertEqual(unknown_ctx.exception.status_code, 403)

        with self.assertRaises(HTTPException) as wrong_pw_ctx:
            _authenticate_user("anisovets", "0000", user_directory=self.user_directory)
        self.assertEqual(wrong_pw_ctx.exception.status_code, 403)

    def test_job_access_is_limited_to_owner_or_admin(self) -> None:
        owner = AuthUser(username="anisovets", display_name="Анисовец", role="manager")
        stranger = AuthUser(username="panov", display_name="Панов", role="manager")
        admin = AuthUser(username="admin", display_name="Администратор", role="admin")
        job = {"created_by": "anisovets"}

        _ensure_job_access(owner, job)
        _ensure_job_access(admin, job)

        with self.assertRaises(HTTPException) as ctx:
            _ensure_job_access(stranger, job)
        self.assertEqual(ctx.exception.status_code, 403)

    def test_export_audit_event_marks_learning_only_for_manual_not_found_rows(self) -> None:
        event = _build_export_audit_event(
            job_id="job-1",
            data={
                "filename": "client.xlsx",
                "saved_at": "2026-04-06T08:30:00Z",
                "export_count": 2,
                "total_rows": 3,
                "status_counts": {"Одобрена замена": 3},
                "rows": [
                    {
                        "id": "row-1",
                        "position": "1",
                        "name": "Ревизия, 110",
                        "mark": "HT",
                        "requested_qty": 2,
                        "initial_status": STATUS_NOT_FOUND,
                        "status": "Одобрена замена",
                        "selected_via": "manual_search",
                        "selection_search_query": "ревизия 110",
                        "selected_by": "anisovets",
                        "selected_by_display": "Анисовец",
                        "selected_at": "2026-04-06T08:20:00Z",
                        "approved_analog": {
                            "code_1c": "001",
                            "name": "Ревизия HT 110",
                            "source_label": "Сантехкомплект",
                            "score": 82.4,
                            "manager_choice": True,
                            "reasons": ["совпадают размеры"],
                        },
                    },
                    {
                        "id": "row-2",
                        "position": "2",
                        "name": "Тройник 110x110",
                        "mark": "HT",
                        "requested_qty": 1,
                        "initial_status": "Безопасный аналог",
                        "status": "Одобрена замена",
                        "selected_via": "manual_search",
                        "selection_search_query": "тройник 110",
                        "approved_analog": {
                            "code_1c": "002",
                            "name": "Тройник HT 110x110",
                            "score": 77.1,
                            "reasons": [],
                        },
                    },
                    {
                        "id": "row-3",
                        "position": "3",
                        "name": "Фланец DN80",
                        "requested_qty": 4,
                        "initial_status": "Нужна проверка аналога",
                        "status": "Одобрена замена",
                        "selected_via": "analog",
                        "approved_analog": {
                            "code_1c": "003",
                            "name": "Фланец DN80 PN16",
                            "score": 74.0,
                            "reasons": [],
                        },
                    },
                ],
            },
            user=AuthUser(username="anisovets", display_name="Анисовец", role="manager"),
            learning_saved_count=1,
        )

        self.assertEqual(event["export_event_id"], "job-1:export:2")
        self.assertEqual(event["replacement_count"], 3)
        self.assertEqual(event["manual_search_replacement_count"], 2)
        self.assertEqual(event["learned_replacement_count"], 1)
        self.assertEqual(event["learning_saved_count"], 1)
        self.assertTrue(event["replacements"][0]["learned_on_export"])
        self.assertFalse(event["replacements"][1]["learned_on_export"])
        self.assertFalse(event["replacements"][2]["learned_on_export"])

    def test_admin_analytics_aggregates_exports_users_and_top_replacements(self) -> None:
        analytics = _build_admin_analytics(
            {
                "exports": [
                    {
                        "job_id": "job-1",
                        "filename": "order-1.xlsx",
                        "saved_at": "2026-04-06T09:00:00Z",
                        "saved_by": "anisovets",
                        "saved_by_display": "Анисовец",
                        "saved_by_role": "manager",
                        "total_rows": 10,
                        "replacement_count": 2,
                        "manual_search_replacement_count": 1,
                        "learned_replacement_count": 1,
                        "replacements": [
                            {
                                "candidate_code": "001",
                                "candidate_name": "Ревизия HT 110",
                                "learned_on_export": True,
                            },
                            {
                                "candidate_code": "002",
                                "candidate_name": "Тройник HT 110x110",
                                "learned_on_export": False,
                            },
                        ],
                    },
                    {
                        "job_id": "job-2",
                        "filename": "order-2.xlsx",
                        "saved_at": "2026-04-06T10:00:00Z",
                        "saved_by": "anisovets",
                        "saved_by_display": "Анисовец",
                        "saved_by_role": "manager",
                        "total_rows": 5,
                        "replacement_count": 1,
                        "manual_search_replacement_count": 1,
                        "learned_replacement_count": 1,
                        "replacements": [
                            {
                                "candidate_code": "001",
                                "candidate_name": "Ревизия HT 110",
                                "learned_on_export": True,
                            }
                        ],
                    },
                ]
            },
            self.user_directory,
        )

        self.assertEqual(analytics["summary"]["saved_files"], 2)
        self.assertEqual(analytics["summary"]["unique_jobs"], 2)
        self.assertEqual(analytics["summary"]["replacement_count"], 3)
        self.assertEqual(analytics["summary"]["learned_replacement_count"], 2)
        self.assertEqual(analytics["summary"]["users_with_exports"], 1)

        users = analytics["users"]
        self.assertEqual(users[0]["username"], "anisovets")
        self.assertEqual(users[0]["saved_files"], 2)
        self.assertEqual(users[0]["replacement_count"], 3)
        self.assertEqual(users[0]["learned_replacement_count"], 2)
        self.assertEqual(users[0]["unique_jobs"], 2)

        empty_user = next(item for item in users if item["username"] == "panov")
        self.assertEqual(empty_user["saved_files"], 0)

        top = analytics["top_replacements"][0]
        self.assertEqual(top["candidate_code"], "001")
        self.assertEqual(top["times_used"], 2)
        self.assertEqual(top["times_learned"], 2)
        self.assertEqual(top["used_by_count"], 1)

    def test_admin_analytics_uses_feedback_entries_for_learning_before_export(self) -> None:
        analytics = _build_admin_analytics(
            {"exports": []},
            self.user_directory,
            feedback_entries=[
                {
                    "snapshot_id": "job-1:row-1",
                    "decision": "approved",
                    "candidate_code": "009",
                    "candidate_name": "Фланец Ду40 PN16",
                    "selected_by": "anisovets",
                    "selected_by_display": "Анисовец",
                    "selected_at": "2026-04-08T10:00:00Z",
                },
                {
                    "snapshot_id": "job-1:row-1",
                    "decision": "rejected",
                    "candidate_code": "010",
                    "candidate_name": "Фланец Ду40 PN10",
                    "selected_by": "anisovets",
                    "selected_by_display": "Анисовец",
                    "selected_at": "2026-04-08T10:00:00Z",
                },
            ],
        )

        self.assertEqual(analytics["summary"]["saved_files"], 0)
        self.assertEqual(analytics["summary"]["learned_replacement_count"], 1)
        users = analytics["users"]
        anisovets = next(item for item in users if item["username"] == "anisovets")
        self.assertEqual(anisovets["learned_replacement_count"], 1)
        top = analytics["top_replacements"][0]
        self.assertEqual(top["candidate_code"], "009")
        self.assertEqual(top["times_learned"], 1)

    def test_serialize_candidate_keeps_stock_quantity_separate_from_free_remaining(self) -> None:
        stock = StockItem(
            row_index=1,
            code_1c="001",
            name="Отвод DN20",
            print_name="Отвод DN20",
            product_type="",
            sale_price="100",
            stop_price="",
            plan_price="",
            quantity=240.0,
            remaining=88.0,
            search_text="otvod dn20",
            search_tokens={"otvod", "dn20"},
            key_tokens={"otvod", "dn20"},
            root_tokens={"otvod", "dn20"},
            code_tokens={"001"},
            dimension_tags={"dn:20"},
            source_label="Сантехкомплект",
        )
        payload = _serialize_candidate(
            Candidate(
                stock=stock,
                score=100.0,
                overlap=1.0,
                soft_overlap=1.0,
                reasons=["совпал код/марка"],
                retrieval_paths=("structure", "family"),
                feature_scores={"family_match": 1.0, "dn_exact": 1.0},
            )
        )

        self.assertEqual(payload["stock_qty"], 240.0)
        self.assertEqual(payload["remaining"], 88.0)
        self.assertEqual(payload["retrieval_paths"], ["structure", "family"])
        self.assertEqual(payload["feature_scores"]["family_match"], 1.0)

    def test_manual_search_uses_row_context_to_prefer_same_family(self) -> None:
        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            search_text = build_search_text(name)
            dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_material_tags_from_search_text(search_text)
            )
            search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
            tokens = extract_tokens(search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=20.0,
                remaining=20.0,
                search_text=search_text,
                search_tokens=tokens,
                key_tokens=extract_key_tokens(tokens),
                root_tokens=extract_root_tokens(tokens),
                code_tokens=set(),
                dimension_tags=dimension_tags,
            )

        row_name = "Переход ст. приварной 89х40х3,5 мм"
        row_search_text = build_search_text(row_name)
        row_dimension_tags = (
            extract_dimension_tags(row_name)
            | extract_family_tags(row_name)
            | extract_material_tags_from_search_text(row_search_text)
        )
        row_search_text = augment_search_text_with_dimension_tags(row_search_text, row_dimension_tags)
        row_tokens = extract_tokens(row_search_text)
        row = {
            "id": "row-1",
            "name": row_name,
            "mark": "",
            "vendor": "",
            "unit": "шт",
            "requested_qty": 3.0,
            "raw_query": row_name,
            "search_text": row_search_text,
            "key_tokens": sorted(extract_key_tokens(row_tokens)),
            "root_tokens": sorted(extract_root_tokens(row_tokens)),
            "dimension_tags": sorted(row_dimension_tags),
        }

        reducer = make_stock(1, "RED", "Переход П-2- 89х4,0 - 57х3,5 Ст.20 ГОСТ 17378")
        elbow = make_stock(2, "ELB", "Отвод 90-2- 89х3,5 Ст.20 ГОСТ 17375")
        matcher = StockMatcher([elbow, reducer])

        results = _manual_search_candidates(matcher, row, "89", limit=5)

        self.assertEqual(results[0].stock.code_1c, "RED")
        self.assertIn("manual_context", results[0].retrieval_paths)
        self.assertGreater(results[0].feature_scores["row_context_score"], 0.0)

    def test_manual_search_shortlists_candidates_before_full_stock_scan(self) -> None:
        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            search_text = build_search_text(name)
            dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_material_tags_from_search_text(search_text)
            )
            search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
            tokens = extract_tokens(search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=20.0,
                remaining=20.0,
                search_text=search_text,
                search_tokens=tokens,
                key_tokens=extract_key_tokens(tokens),
                root_tokens=extract_root_tokens(tokens),
                code_tokens=set(),
                dimension_tags=dimension_tags,
            )

        row_name = "Переход ст. приварной 89х40х3,5 мм"
        row_search_text = build_search_text(row_name)
        row_dimension_tags = (
            extract_dimension_tags(row_name)
            | extract_family_tags(row_name)
            | extract_material_tags_from_search_text(row_search_text)
        )
        row_search_text = augment_search_text_with_dimension_tags(row_search_text, row_dimension_tags)
        row_tokens = extract_tokens(row_search_text)
        row = {
            "id": "row-1",
            "name": row_name,
            "mark": "",
            "vendor": "",
            "unit": "шт",
            "requested_qty": 3.0,
            "raw_query": row_name,
            "search_text": row_search_text,
            "key_tokens": sorted(extract_key_tokens(row_tokens)),
            "root_tokens": sorted(extract_root_tokens(row_tokens)),
            "dimension_tags": sorted(row_dimension_tags),
        }

        stocks = [make_stock(1, "RED", "Переход П-2- 89х4,0 - 57х3,5 Ст.20 ГОСТ 17378")]
        stocks.extend(
            make_stock(index + 2, f"IRR-{index}", f"Муфта латунная DN{15 + index % 20}")
            for index in range(48)
        )
        matcher = StockMatcher(stocks)
        original_score_candidate = matcher.score_candidate
        score_calls = 0

        def counted_score_candidate(*args, **kwargs):
            nonlocal score_calls
            score_calls += 1
            return original_score_candidate(*args, **kwargs)

        matcher.score_candidate = counted_score_candidate  # type: ignore[assignment]

        results = _manual_search_candidates(matcher, row, "89", limit=5)

        self.assertTrue(results)
        self.assertLess(score_calls, len(matcher.stock_items) * 2)

    def test_find_all_stock_files_prefers_latest_file_per_label(self) -> None:
        original_stock_dir = matching_api_module.STOCK_DIR
        with tempfile.TemporaryDirectory() as tmp_dir:
            stock_dir = Path(tmp_dir)
            old_ek = stock_dir / "остатки_old.xlsm"
            new_ek = stock_dir / "остатки_new.xlsm"
            santeh = stock_dir / "santeh_base.xlsm"
            standard = stock_dir / "report_standard.xlsx"
            labels = stock_dir / "stock_labels.json"

            for path in (old_ek, new_ek, santeh, standard):
                path.write_bytes(b"test")
            os.utime(old_ek, (1_700_000_000, 1_700_000_000))
            os.utime(new_ek, (1_800_000_000, 1_800_000_000))
            os.utime(santeh, (1_750_000_000, 1_750_000_000))
            os.utime(standard, (1_760_000_000, 1_760_000_000))
            labels.write_text(
                '{"santeh_base.xlsm": "Сантехкомплект", "report_standard.xlsx": "СантехСтандарт"}',
                encoding="utf-8",
            )

            matching_api_module.STOCK_DIR = stock_dir
            try:
                result = _find_all_stock_files()
            finally:
                matching_api_module.STOCK_DIR = original_stock_dir

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0].name, "остатки_new.xlsm")
        self.assertEqual(result[0][1], "ЭК")
        self.assertEqual(result[1][0].name, "santeh_base.xlsm")
        self.assertEqual(result[1][1], "Сантехкомплект")
        self.assertEqual(result[2][0].name, "report_standard.xlsx")
        self.assertEqual(result[2][1], "СантехСтандарт")

    def test_load_stock_accepts_santehstandard_workbook_headers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workbook_path = Path(tmp_dir) / "standard.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "Лист_1"
            ws.append(["Артикул", "Номенклатура", "Единица измерения", "Свободный остаток", "Цена клиента со скидкой"])
            ws.append(["00159", "Уголок L16х16", "шт", 2550, 139.64])
            wb.save(workbook_path)

            items = matching_api_module.load_stock(workbook_path, source_label="СантехСтандарт")

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].code_1c, "00159")
        self.assertEqual(items[0].name, "Уголок L16х16")
        self.assertEqual(items[0].quantity, 2550.0)
        self.assertEqual(items[0].sale_price, "139.64")
        self.assertEqual(items[0].source_label, "СантехСтандарт")

    def test_update_row_quantity_inplace_rejects_over_available_stock(self) -> None:
        row = {
            "status": "Одобрена замена",
            "requested_qty": 2.0,
            "approved_analog": {"remaining": 5.0, "stock_qty": 5.0},
        }

        with self.assertRaises(HTTPException) as ctx:
            _update_row_quantity_inplace(row, 6.0)

        self.assertEqual(ctx.exception.status_code, 400)

    def test_update_row_quantity_inplace_updates_exact_row(self) -> None:
        row = {
            "status": "Найдено полностью",
            "requested_qty": 10.0,
            "available_qty": 10.0,
            "matched_code": "001",
            "matched_stock_qty": 12.0,
        }

        _update_row_quantity_inplace(row, 7.0)

        self.assertEqual(row["requested_qty"], 7.0)
        self.assertEqual(row["available_qty"], 7.0)
        self.assertEqual(row["status"], "Найдено полностью")

    def test_parse_requested_quantity_requires_positive_number(self) -> None:
        self.assertEqual(_parse_requested_quantity("2.5"), 2.5)
        with self.assertRaises(HTTPException):
            _parse_requested_quantity(0)


class MatchingApiSimpleRequestFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.user_directory = {
            "anisovets": {
                "username": "anisovets",
                "display_name": "Анисовец",
                "role": "manager",
                "password_sha256": _hash_password("5956"),
            }
        }
        self.original_loader = matching_api_module._load_user_directory
        matching_api_module._load_user_directory = lambda: self.user_directory
        matching_api_module._sessions.clear()

    def tearDown(self) -> None:
        matching_api_module._load_user_directory = self.original_loader
        matching_api_module._sessions.clear()

    def test_extract_session_token_prefers_query_token_over_header(self) -> None:
        self.assertEqual(
            _extract_session_token("Bearer header-token", "query-token"),
            "query-token",
        )
        self.assertEqual(_extract_session_token("Bearer header-token", None), "header-token")

    def test_login_accepts_text_plain_json_and_me_accepts_query_token(self) -> None:
        class FakeRequest:
            def __init__(self, body: str, content_type: str) -> None:
                self._body = body.encode("utf-8")
                self.headers = {"content-type": content_type}

            async def json(self):
                raise AssertionError("json() should not be used for text/plain login payload")

            async def body(self):
                return self._body

            async def form(self):
                raise AssertionError("form() should not be used for text/plain login payload")

        payload = asyncio.run(
            login(FakeRequest('{"username":"anisovets","password":"5956"}', "text/plain"))
        )

        self.assertTrue(payload.token)

        me_payload = me(None, token=payload.token)
        self.assertEqual(me_payload.username, "anisovets")

    def test_upload_file_returns_processing_job_and_enqueues_background_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_jobs_dir = matching_api_module.JOBS_DIR
            original_check_auth = matching_api_module._check_auth
            original_cache_key = matching_api_module._pipeline_cache_key
            original_start_worker = matching_api_module._start_job_worker_if_needed
            original_enqueue = matching_api_module._enqueue_processing_job
            original_is_busy = matching_api_module._job_worker_is_busy
            original_save_job = matching_api_module._save_job
            original_jobs = matching_api_module._jobs
            original_cache = matching_api_module._pipeline_result_cache

            called = {"started": False, "enqueued": None}

            matching_api_module.JOBS_DIR = Path(tmp_dir)
            matching_api_module._check_auth = lambda authorization, token=None: AuthUser(
                username="anisovets",
                display_name="Анисовец",
                role="manager",
            )
            matching_api_module._pipeline_cache_key = lambda upload_path: "cache-key"
            matching_api_module._start_job_worker_if_needed = lambda: called.__setitem__("started", True)
            matching_api_module._enqueue_processing_job = (
                lambda job_id, upload_path, job_dir, base_data, cache_key: called.__setitem__(
                    "enqueued",
                    {
                        "job_id": job_id,
                        "upload_path": upload_path,
                        "job_dir": job_dir,
                        "filename": base_data.get("filename"),
                        "cache_key": cache_key,
                    },
                )
            )
            matching_api_module._job_worker_is_busy = lambda: False
            matching_api_module._save_job = lambda job_id, data: None
            matching_api_module._jobs = {}
            matching_api_module._pipeline_result_cache = {"scope": None, "entries": {}}

            try:
                upload = UploadFile(filename="order.xlsx", file=io.BytesIO(b"fake"))
                result = asyncio.run(upload_file(upload, None, "query-token"))
            finally:
                matching_api_module.JOBS_DIR = original_jobs_dir
                matching_api_module._check_auth = original_check_auth
                matching_api_module._pipeline_cache_key = original_cache_key
                matching_api_module._start_job_worker_if_needed = original_start_worker
                matching_api_module._enqueue_processing_job = original_enqueue
                matching_api_module._job_worker_is_busy = original_is_busy
                matching_api_module._save_job = original_save_job
                matching_api_module._jobs = original_jobs
                matching_api_module._pipeline_result_cache = original_cache

        self.assertTrue(called["started"])
        self.assertIsNotNone(called["enqueued"])
        self.assertEqual(called["enqueued"]["filename"], "order.xlsx")
        self.assertEqual(called["enqueued"]["cache_key"], "cache-key")
        self.assertEqual(result["job_status"], "processing")
        self.assertEqual(result["filename"], "order.xlsx")
        self.assertEqual(result["rows"], [])


if __name__ == "__main__":
    unittest.main()

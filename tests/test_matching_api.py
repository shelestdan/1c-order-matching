from __future__ import annotations

import sys
import unittest
from pathlib import Path

from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from matching_api import (  # noqa: E402
    SHARED_PASSWORD,
    AuthUser,
    STATUS_NOT_FOUND,
    _authenticate_user,
    _build_admin_analytics,
    _build_export_audit_event,
    _ensure_job_access,
    _hash_password,
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


if __name__ == "__main__":
    unittest.main()

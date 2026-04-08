from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


class LearningStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS feedback_entries (
                    record_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    row_id TEXT NOT NULL,
                    query TEXT NOT NULL,
                    query_key TEXT NOT NULL,
                    structure_keys_json TEXT NOT NULL,
                    order_key_tokens_json TEXT NOT NULL,
                    order_root_tokens_json TEXT NOT NULL,
                    order_dimension_tags_json TEXT NOT NULL,
                    manual_search_query TEXT NOT NULL,
                    manual_search_query_key TEXT NOT NULL,
                    candidate_code TEXT NOT NULL,
                    candidate_name TEXT NOT NULL,
                    candidate_source_label TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    selected_via TEXT NOT NULL,
                    initial_status TEXT NOT NULL,
                    selected_by TEXT NOT NULL,
                    selected_by_display TEXT NOT NULL,
                    selected_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_feedback_snapshot_id ON feedback_entries(snapshot_id);
                CREATE INDEX IF NOT EXISTS idx_feedback_candidate_code ON feedback_entries(candidate_code);
                CREATE INDEX IF NOT EXISTS idx_feedback_decision ON feedback_entries(decision);
                CREATE INDEX IF NOT EXISTS idx_feedback_selected_by ON feedback_entries(selected_by);

                CREATE TABLE IF NOT EXISTS export_events (
                    export_event_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    saved_at TEXT NOT NULL,
                    saved_by TEXT NOT NULL,
                    saved_by_display TEXT NOT NULL,
                    saved_by_role TEXT NOT NULL,
                    total_rows INTEGER NOT NULL,
                    replacement_count INTEGER NOT NULL,
                    manual_search_replacement_count INTEGER NOT NULL,
                    learned_replacement_count INTEGER NOT NULL,
                    learning_saved_count INTEGER NOT NULL,
                    status_counts_json TEXT NOT NULL,
                    replacements_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_export_saved_by ON export_events(saved_by);
                CREATE INDEX IF NOT EXISTS idx_export_saved_at ON export_events(saved_at);
                """
            )

    def migrate_legacy_payloads(
        self,
        *,
        manual_payload: dict[str, Any] | None = None,
        export_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self._connect() as conn:
            feedback_count = int(conn.execute("SELECT COUNT(*) FROM feedback_entries").fetchone()[0])
            export_count = int(conn.execute("SELECT COUNT(*) FROM export_events").fetchone()[0])

            if feedback_count == 0 and isinstance(manual_payload, dict):
                for raw in manual_payload.get("entries", []):
                    if not isinstance(raw, dict):
                        continue
                    record_id = str(raw.get("record_id") or "").strip()
                    candidate_code = str(raw.get("candidate_code") or "").strip()
                    query_key = str(raw.get("query_key") or "").strip()
                    if not record_id or not candidate_code or not query_key:
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO feedback_entries (
                            record_id, snapshot_id, job_id, row_id, query, query_key,
                            structure_keys_json, order_key_tokens_json, order_root_tokens_json, order_dimension_tags_json,
                            manual_search_query, manual_search_query_key,
                            candidate_code, candidate_name, candidate_source_label,
                            decision, selected_via, initial_status, selected_by, selected_by_display,
                            selected_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record_id,
                            record_id,
                            str(raw.get("job_id") or ""),
                            str(raw.get("row_id") or ""),
                            str(raw.get("query") or ""),
                            query_key,
                            _json_dumps(raw.get("structure_keys") or []),
                            _json_dumps(raw.get("order_key_tokens") or []),
                            _json_dumps(raw.get("order_root_tokens") or []),
                            _json_dumps(raw.get("order_dimension_tags") or []),
                            str(raw.get("manual_search_query") or raw.get("search_query") or ""),
                            str(raw.get("manual_search_query_key") or ""),
                            candidate_code,
                            str(raw.get("candidate_name") or ""),
                            str(raw.get("candidate_source_label") or ""),
                            str(raw.get("decision") or "approved"),
                            str(raw.get("selected_via") or "manual_search"),
                            str(raw.get("initial_status") or ""),
                            str(raw.get("selected_by") or ""),
                            str(raw.get("selected_by_display") or ""),
                            str(raw.get("selected_at") or ""),
                            str(raw.get("updated_at") or raw.get("selected_at") or ""),
                        ),
                    )

            if export_count == 0 and isinstance(export_payload, dict):
                for raw in export_payload.get("exports", []):
                    if not isinstance(raw, dict):
                        continue
                    export_event_id = str(raw.get("export_event_id") or "").strip()
                    if not export_event_id:
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO export_events (
                            export_event_id, job_id, filename, saved_at, saved_by, saved_by_display,
                            saved_by_role, total_rows, replacement_count, manual_search_replacement_count,
                            learned_replacement_count, learning_saved_count, status_counts_json, replacements_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            export_event_id,
                            str(raw.get("job_id") or ""),
                            str(raw.get("filename") or ""),
                            str(raw.get("saved_at") or ""),
                            str(raw.get("saved_by") or ""),
                            str(raw.get("saved_by_display") or ""),
                            str(raw.get("saved_by_role") or ""),
                            int(raw.get("total_rows", 0) or 0),
                            int(raw.get("replacement_count", 0) or 0),
                            int(raw.get("manual_search_replacement_count", 0) or 0),
                            int(raw.get("learned_replacement_count", 0) or 0),
                            int(raw.get("learning_saved_count", 0) or 0),
                            _json_dumps(raw.get("status_counts") or {}),
                            _json_dumps(raw.get("replacements") or []),
                        ),
                    )

    def replace_feedback_snapshot(self, snapshot_id: str, entries: Iterable[dict[str, Any]]) -> int:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM feedback_entries WHERE snapshot_id = ?", (snapshot_id,))
            saved = 0
            for entry in entries:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO feedback_entries (
                        record_id, snapshot_id, job_id, row_id, query, query_key,
                        structure_keys_json, order_key_tokens_json, order_root_tokens_json, order_dimension_tags_json,
                        manual_search_query, manual_search_query_key,
                        candidate_code, candidate_name, candidate_source_label,
                        decision, selected_via, initial_status, selected_by, selected_by_display,
                        selected_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(entry.get("record_id") or "").strip(),
                        snapshot_id,
                        str(entry.get("job_id") or "").strip(),
                        str(entry.get("row_id") or "").strip(),
                        str(entry.get("query") or "").strip(),
                        str(entry.get("query_key") or "").strip(),
                        _json_dumps(entry.get("structure_keys") or []),
                        _json_dumps(entry.get("order_key_tokens") or []),
                        _json_dumps(entry.get("order_root_tokens") or []),
                        _json_dumps(entry.get("order_dimension_tags") or []),
                        str(entry.get("manual_search_query") or "").strip(),
                        str(entry.get("manual_search_query_key") or "").strip(),
                        str(entry.get("candidate_code") or "").strip(),
                        str(entry.get("candidate_name") or "").strip(),
                        str(entry.get("candidate_source_label") or "").strip(),
                        str(entry.get("decision") or "").strip(),
                        str(entry.get("selected_via") or "").strip(),
                        str(entry.get("initial_status") or "").strip(),
                        str(entry.get("selected_by") or "").strip(),
                        str(entry.get("selected_by_display") or "").strip(),
                        str(entry.get("selected_at") or "").strip(),
                        str(entry.get("updated_at") or "").strip(),
                    ),
                )
                saved += 1
            return saved

    def load_feedback_entries(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    record_id, snapshot_id, job_id, row_id, query, query_key,
                    structure_keys_json, order_key_tokens_json, order_root_tokens_json, order_dimension_tags_json,
                    manual_search_query, manual_search_query_key,
                    candidate_code, candidate_name, candidate_source_label,
                    decision, selected_via, initial_status, selected_by, selected_by_display,
                    selected_at, updated_at
                FROM feedback_entries
                ORDER BY updated_at DESC, record_id DESC
                """
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "record_id": row["record_id"],
                    "snapshot_id": row["snapshot_id"],
                    "job_id": row["job_id"],
                    "row_id": row["row_id"],
                    "query": row["query"],
                    "query_key": row["query_key"],
                    "structure_keys": _json_loads(row["structure_keys_json"], []),
                    "order_key_tokens": _json_loads(row["order_key_tokens_json"], []),
                    "order_root_tokens": _json_loads(row["order_root_tokens_json"], []),
                    "order_dimension_tags": _json_loads(row["order_dimension_tags_json"], []),
                    "manual_search_query": row["manual_search_query"],
                    "manual_search_query_key": row["manual_search_query_key"],
                    "candidate_code": row["candidate_code"],
                    "candidate_name": row["candidate_name"],
                    "candidate_source_label": row["candidate_source_label"],
                    "decision": row["decision"],
                    "selected_via": row["selected_via"],
                    "initial_status": row["initial_status"],
                    "selected_by": row["selected_by"],
                    "selected_by_display": row["selected_by_display"],
                    "selected_at": row["selected_at"],
                    "updated_at": row["updated_at"],
                }
            )
        return results

    def count_feedback_snapshots(self) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT COUNT(DISTINCT snapshot_id) FROM feedback_entries WHERE decision = 'approved'").fetchone()
        return int(row[0] or 0)

    def replace_export_event(self, event: dict[str, Any]) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO export_events (
                    export_event_id, job_id, filename, saved_at, saved_by, saved_by_display,
                    saved_by_role, total_rows, replacement_count, manual_search_replacement_count,
                    learned_replacement_count, learning_saved_count, status_counts_json, replacements_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event.get("export_event_id") or ""),
                    str(event.get("job_id") or ""),
                    str(event.get("filename") or ""),
                    str(event.get("saved_at") or ""),
                    str(event.get("saved_by") or ""),
                    str(event.get("saved_by_display") or ""),
                    str(event.get("saved_by_role") or ""),
                    int(event.get("total_rows", 0) or 0),
                    int(event.get("replacement_count", 0) or 0),
                    int(event.get("manual_search_replacement_count", 0) or 0),
                    int(event.get("learned_replacement_count", 0) or 0),
                    int(event.get("learning_saved_count", 0) or 0),
                    _json_dumps(event.get("status_counts") or {}),
                    _json_dumps(event.get("replacements") or []),
                ),
            )

    def load_export_events(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    export_event_id, job_id, filename, saved_at, saved_by, saved_by_display,
                    saved_by_role, total_rows, replacement_count, manual_search_replacement_count,
                    learned_replacement_count, learning_saved_count, status_counts_json, replacements_json
                FROM export_events
                ORDER BY saved_at DESC, export_event_id DESC
                """
            ).fetchall()
        return [
            {
                "export_event_id": row["export_event_id"],
                "job_id": row["job_id"],
                "filename": row["filename"],
                "saved_at": row["saved_at"],
                "saved_by": row["saved_by"],
                "saved_by_display": row["saved_by_display"],
                "saved_by_role": row["saved_by_role"],
                "total_rows": int(row["total_rows"] or 0),
                "replacement_count": int(row["replacement_count"] or 0),
                "manual_search_replacement_count": int(row["manual_search_replacement_count"] or 0),
                "learned_replacement_count": int(row["learned_replacement_count"] or 0),
                "learning_saved_count": int(row["learning_saved_count"] or 0),
                "status_counts": _json_loads(row["status_counts_json"], {}),
                "replacements": _json_loads(row["replacements_json"], []),
            }
            for row in rows
        ]

#!/usr/bin/env python3
"""FastAPI backend for the 1C order matching web service.

Endpoints:
  POST /api/login          — simple shared-password auth
  POST /api/upload         — upload client file, run pipeline
  GET  /api/jobs           — list completed jobs
  GET  /api/jobs/{id}      — get job results (matches, analogs, not_found)
  POST /api/jobs/{id}/approve  — approve specific analog rows
  POST /api/jobs/{id}/export   — generate final 1C XLSX with approved analogs
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Header, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from normalize_client_requests import ensure_output_dir, normalize_request_file
from process_1c_orders import (
    Candidate,
    DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH,
    OrderLine,
    StockMatcher,
    augment_search_text_with_dimension_tags,
    build_search_text,
    clone_stock_items,
    extract_code_tokens,
    extract_dimension_tags,
    extract_family_tags,
    extract_key_tokens,
    extract_material_tags_from_search_text,
    extract_parser_hint_tags,
    extract_root_tokens,
    extract_tokens,
    load_order_lines,
    load_reviewed_analog_decisions,
    load_stock,
    load_substitution_policy,
    match_orders,
    write_outputs,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
STOCK_DIR = Path(__file__).resolve().parent.parent / "inputs" / "stock"
JOBS_DIR = Path(__file__).resolve().parent.parent / "outputs" / "web_jobs"

# Simple shared password — override via env var MATCHING_PASSWORD
SHARED_PASSWORD = os.environ.get("MATCHING_PASSWORD", "demo2026")

# Active sessions (token -> expiry timestamp)
_sessions: dict[str, float] = {}

SESSION_TTL = 60 * 60 * 8  # 8 hours
MANUAL_SEARCH_LIMIT = 12
MANUAL_SEARCH_MIN_SCORE = 28.0
MANUAL_SEARCH_MIN_OVERLAP = 0.12
MANUAL_SEARCH_MIN_SOFT_OVERLAP = 0.18
MANUAL_SEARCH_MIN_DIMENSION_BONUS = 6.0

STOCK_EXTENSIONS = {".csv", ".xlsx", ".xlsm", ".xls"}
STOCK_LABELS_FILE = "stock_labels.json"

# Combined stock cache — keyed by fingerprint of all stock files + their mtimes
_stock_cache: dict[str, Any] = {
    "fingerprint": None,
    "base_stock": None,
    "stock_paths": [],
}

# Master StockMatcher (read-only indexes) — forked per job so index building
# happens only once per stock file version, not once per job.
_matcher_cache: dict[str, Any] = {
    "fingerprint": None,
    "master_matcher": None,
}

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="1C Order Matching API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _check_auth(authorization: str | None) -> None:
    if not authorization:
        raise HTTPException(401, "Authorization header required")
    token = authorization.removeprefix("Bearer ").strip()
    expiry = _sessions.get(token)
    if not expiry or expiry < time.time():
        _sessions.pop(token, None)
        raise HTTPException(401, "Session expired or invalid")


class LoginRequest(BaseModel):
    password: str


class LoginResponse(BaseModel):
    token: str
    expires_in: int


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/login", response_model=LoginResponse)
def login(body: LoginRequest):
    if body.password != SHARED_PASSWORD:
        raise HTTPException(403, "Wrong password")
    token = secrets.token_urlsafe(32)
    _sessions[token] = time.time() + SESSION_TTL
    return LoginResponse(token=token, expires_in=SESSION_TTL)


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------


def _find_all_stock_files() -> list[tuple[Path, str]]:
    """Find all stock files in inputs/stock/ and their warehouse labels.

    Returns list of (path, label) tuples. Labels are loaded from
    stock_labels.json in the stock directory. Files without explicit
    labels get an empty string.
    """
    labels_path = STOCK_DIR / STOCK_LABELS_FILE
    labels: dict[str, str] = {}
    if labels_path.exists():
        try:
            labels = json.loads(labels_path.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    candidates = sorted(STOCK_DIR.glob("*.*"), key=lambda p: p.stat().st_mtime, reverse=True)
    result: list[tuple[Path, str]] = []
    for c in candidates:
        if c.suffix.lower() in STOCK_EXTENSIONS and c.name != STOCK_LABELS_FILE:
            label = labels.get(c.name, "")
            result.append((c, label))
    if not result:
        raise HTTPException(500, "No stock file found in inputs/stock/")
    return result


def _stock_fingerprint(stock_files: list[tuple[Path, str]]) -> str:
    """Build a fingerprint from all stock file paths + mtimes."""
    parts = []
    for path, label in stock_files:
        parts.append(f"{path.name}:{path.stat().st_mtime}:{label}")
    return "|".join(sorted(parts))


def _load_cached_stock() -> tuple[list[Path], list[Any]]:
    stock_files = _find_all_stock_files()
    fp = _stock_fingerprint(stock_files)
    if _stock_cache["fingerprint"] == fp and _stock_cache["base_stock"] is not None:
        return _stock_cache["stock_paths"], _stock_cache["base_stock"]

    all_items: list[Any] = []
    paths: list[Path] = []
    for path, label in stock_files:
        items = load_stock(path, source_label=label)
        all_items.extend(items)
        paths.append(path)

    _stock_cache["fingerprint"] = fp
    _stock_cache["base_stock"] = all_items
    _stock_cache["stock_paths"] = paths
    # Invalidate matcher cache when stock changes
    _matcher_cache["master_matcher"] = None
    return paths, all_items


def _load_cached_matcher() -> "StockMatcher":
    """Return the master StockMatcher, building it once per stock version.

    Callers must call .fork() on the result to get a job-local copy with
    independent remaining-quantity tracking.
    """
    stock_files = _find_all_stock_files()
    fp = _stock_fingerprint(stock_files)
    if _matcher_cache["fingerprint"] == fp and _matcher_cache["master_matcher"] is not None:
        return _matcher_cache["master_matcher"]

    _, base_stock = _load_cached_stock()
    reviewed_decisions = load_reviewed_analog_decisions(DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH)
    substitution_policy = load_substitution_policy()
    stock_items = clone_stock_items(base_stock)
    master = StockMatcher(
        stock_items,
        reviewed_analog_decisions=reviewed_decisions,
        substitution_policy=substitution_policy,
    )
    _matcher_cache["fingerprint"] = fp
    _matcher_cache["master_matcher"] = master
    return master


def _rebuild_status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = row["status"]
        counts[status] = counts.get(status, 0) + 1
    return counts


def _serialize_candidate(analog) -> dict[str, Any]:
    result = {
        "code_1c": analog.stock.code_1c,
        "name": analog.stock.name,
        "score": round(analog.score, 1),
        "remaining": analog.stock.remaining,
        "price": analog.stock.sale_price,
        "reasons": analog.reasons,
    }
    if analog.stock.source_label:
        result["source_label"] = analog.stock.source_label
    return result


def _build_manual_search_order(row: dict[str, Any], query: str) -> OrderLine:
    # Manual search is intentionally query-first: the manager is searching
    # the stock itself, not asking the matcher to keep the original row
    # constraints. This makes it possible to type any stock code/name and
    # choose it for export.
    name = (query or "").strip()
    mark = ""
    vendor = ""
    unit = (row.get("unit") or "").strip()
    requested_qty = float(row.get("requested_qty") or 0.0)
    query_parts = [name]
    raw_query = " | ".join(part for part in query_parts if part)
    search_text = build_search_text(*query_parts)
    dimension_tags = (
        extract_dimension_tags(*query_parts)
        | extract_family_tags(*query_parts)
        | extract_parser_hint_tags(*query_parts)
        | extract_material_tags_from_search_text(search_text)
    )
    search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
    search_tokens = extract_tokens(search_text)
    return OrderLine(
        source_file="manual_search",
        sheet_name="manual_search",
        source_row=0,
        headers=[],
        row_values=[],
        position=str(row.get("position") or ""),
        name=name,
        mark=mark,
        supplier_code="",
        vendor=vendor,
        unit=unit,
        requested_qty=requested_qty,
        search_text=search_text,
        search_tokens=search_tokens,
        key_tokens=extract_key_tokens(search_tokens),
        root_tokens=extract_root_tokens(search_tokens),
        code_tokens=extract_code_tokens(*query_parts),
        dimension_tags=dimension_tags,
        raw_query=raw_query or name,
        classification=None,
    )


def _normalize_manual_code(value: str) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", value.lower())


def _manual_search_candidates(
    matcher: StockMatcher,
    row: dict[str, Any],
    query: str,
    limit: int,
) -> list[Candidate]:
    manual_order = _build_manual_search_order(row, query)
    query_lower = query.strip().lower()
    normalized_code_query = _normalize_manual_code(query)
    ranked: list[Candidate] = []

    for stock in matcher.stock_items:
        candidate = matcher.score_candidate(manual_order, stock)
        reasons = list(candidate.reasons)
        score = candidate.score

        stock_code_lower = (stock.code_1c or "").lower()
        stock_name_lower = (stock.name or "").lower()
        normalized_stock_code = _normalize_manual_code(stock.code_1c or "")

        if query_lower and stock_code_lower == query_lower:
            score = max(score, 100.0)
            reasons.append("точное совпадение по коду 1С")
        elif normalized_code_query and normalized_stock_code and normalized_code_query == normalized_stock_code:
            score = max(score, 98.0)
            reasons.append("совпадает нормализованный код")
        elif query_lower and (query_lower in stock_code_lower or query_lower in stock_name_lower):
            score = max(score, min(96.0, score + 20.0))
            reasons.append("совпадение по фрагменту кода/названия")

        ranked.append(
            Candidate(
                stock=candidate.stock,
                score=max(0.0, min(100.0, score)),
                overlap=candidate.overlap,
                reasons=tuple(dict.fromkeys(reasons)),
                code_hit=bool(candidate.code_hit or (normalized_code_query and normalized_stock_code == normalized_code_query)),
                dimension_bonus=candidate.dimension_bonus,
                dimension_penalty=candidate.dimension_penalty,
                soft_overlap=candidate.soft_overlap,
                matched_dimension_keys=candidate.matched_dimension_keys,
                conflicting_dimension_keys=candidate.conflicting_dimension_keys,
                review_decision=candidate.review_decision,
            )
        )

    ranked.sort(
        key=lambda candidate: (
            candidate.score,
            candidate.code_hit,
            candidate.stock.remaining > 0,
            candidate.stock.remaining,
            -candidate.stock.row_index,
        ),
        reverse=True,
    )

    filtered: list[Candidate] = []
    seen_codes: set[str] = set()
    for candidate in ranked:
        code = candidate.stock.code_1c
        if not code or code in seen_codes:
            continue
        if (
            candidate.score < MANUAL_SEARCH_MIN_SCORE
            and not candidate.code_hit
            and candidate.overlap < MANUAL_SEARCH_MIN_OVERLAP
            and candidate.soft_overlap < MANUAL_SEARCH_MIN_SOFT_OVERLAP
            and candidate.dimension_bonus < MANUAL_SEARCH_MIN_DIMENSION_BONUS
        ):
            continue
        filtered.append(candidate)
        seen_codes.add(code)
        if len(filtered) >= limit:
            break
    return filtered


def _find_row(data: dict[str, Any], row_id: str) -> dict[str, Any]:
    for row in data["rows"]:
        if row["id"] == row_id:
            return row
    raise HTTPException(404, f"Row {row_id} not found")


def _run_pipeline(upload_path: Path, job_dir: Path) -> dict[str, Any]:
    """Run the full normalization + matching pipeline."""
    normalized_dir = ensure_output_dir(job_dir / "normalized")
    matched_dir = ensure_output_dir(job_dir / "matched")

    # Stage 1: normalize
    normalized_path, parsed_count, issue_count = normalize_request_file(upload_path, normalized_dir)

    # Stage 2: match — fork the cached master matcher (indexes are pre-built)
    stock_paths, _ = _load_cached_stock()
    matcher = _load_cached_matcher().fork()
    order_lines = load_order_lines(normalized_path)
    order_results = match_orders(order_lines, matcher)

    output_paths = write_outputs(
        order_path=normalized_path,
        stock_path=stock_paths[0],
        reviewed_decisions_path=DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH,
        order_results=order_results,
        stock_items=matcher.stock_items,
        out_dir=matched_dir,
    )

    # Build JSON result for the web UI
    rows: list[dict[str, Any]] = []
    for result in order_results:
        analogs_list = []
        for analog in result.analogs:
            payload = _serialize_candidate(analog)
            if analog.stock.code_1c == (result.matched_stock.code_1c if result.matched_stock else None):
                payload["remaining"] = payload["remaining"] + result.available_qty
            analogs_list.append(payload)

        row: dict[str, Any] = {
            "id": str(uuid.uuid4())[:8],
            "position": result.order.position,
            "name": result.order.name,
            "mark": result.order.mark,
            "vendor": result.order.vendor,
            "unit": result.order.unit,
            "requested_qty": result.order.requested_qty,
            "status": result.status,
            "confidence": round(result.confidence, 1),
            "comment": result.comment,
            "matched_code": result.matched_stock.code_1c if result.matched_stock else None,
            "matched_name": result.matched_stock.name if result.matched_stock else None,
            "matched_price": result.matched_stock.sale_price if result.matched_stock else None,
            "matched_source_label": result.matched_stock.source_label if result.matched_stock and result.matched_stock.source_label else None,
            "available_qty": result.available_qty,
            "analogs": analogs_list,
            "approved_analog": None,  # will be set by manager
        }
        rows.append(row)

    # Status counts
    status_counts: dict[str, int] = {}
    for r in rows:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    result_data = {
        "total_rows": len(rows),
        "status_counts": status_counts,
        "rows": rows,
        "stock_files": [p.name for p in stock_paths],
        "parsed_count": parsed_count,
        "issue_count": issue_count,
        "output_files": [str(p) for p in output_paths],
    }

    # Save result JSON
    result_path = job_dir / "result.json"
    result_path.write_text(json.dumps(result_data, ensure_ascii=False, indent=2), encoding="utf-8")

    return result_data


# ---------------------------------------------------------------------------
# Job storage
# ---------------------------------------------------------------------------

# In-memory job index (job_id -> metadata)
_jobs: dict[str, dict[str, Any]] = {}


def _load_job(job_id: str) -> dict[str, Any]:
    job_dir = JOBS_DIR / job_id
    result_path = job_dir / "result.json"
    if not result_path.exists():
        raise HTTPException(404, f"Job {job_id} not found")
    return json.loads(result_path.read_text(encoding="utf-8"))


def _save_job(job_id: str, data: dict[str, Any]) -> None:
    job_dir = JOBS_DIR / job_id
    result_path = job_dir / "result.json"
    result_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), authorization: str | None = Header(None)):
    _check_auth(authorization)

    job_id = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"
    job_dir = ensure_output_dir(JOBS_DIR / job_id)

    # Save uploaded file
    upload_path = job_dir / file.filename
    with open(upload_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        result = _run_pipeline(upload_path, job_dir)
    except Exception as exc:
        raise HTTPException(500, f"Pipeline error: {exc}")

    _jobs[job_id] = {
        "job_id": job_id,
        "filename": file.filename,
        "created_at": time.time(),
        "total_rows": result["total_rows"],
        "status_counts": result["status_counts"],
    }

    return {"job_id": job_id, "filename": file.filename, **result}


@app.get("/api/jobs")
def list_jobs(authorization: str | None = Header(None)):
    _check_auth(authorization)
    # Also scan disk for jobs not in memory
    if JOBS_DIR.exists():
        for d in sorted(JOBS_DIR.iterdir(), reverse=True):
            if d.is_dir() and d.name not in _jobs:
                result_path = d / "result.json"
                if result_path.exists():
                    data = json.loads(result_path.read_text(encoding="utf-8"))
                    _jobs[d.name] = {
                        "job_id": d.name,
                        "filename": d.name,
                        "created_at": d.stat().st_mtime,
                        "total_rows": data.get("total_rows", 0),
                        "status_counts": data.get("status_counts", {}),
                    }
    return {"jobs": sorted(_jobs.values(), key=lambda j: j["created_at"], reverse=True)}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, authorization: str | None = Header(None)):
    _check_auth(authorization)
    return _load_job(job_id)


class ApproveRequest(BaseModel):
    approvals: dict[str, str]  # row_id -> approved analog code_1c


@app.post("/api/jobs/{job_id}/approve")
def approve_analogs(job_id: str, body: ApproveRequest, authorization: str | None = Header(None)):
    _check_auth(authorization)
    data = _load_job(job_id)

    approved_count = 0
    for row in data["rows"]:
        if row["id"] in body.approvals:
            code = body.approvals[row["id"]]
            # Find the analog with this code
            for analog in row["analogs"]:
                if analog["code_1c"] == code:
                    row["approved_analog"] = analog
                    row["status"] = "Одобрена замена"
                    approved_count += 1
                    break

    # Recalculate status counts
    status_counts = _rebuild_status_counts(data["rows"])
    data["status_counts"] = status_counts

    _save_job(job_id, data)
    return {"approved_count": approved_count, "status_counts": status_counts}


class ManualSearchRequest(BaseModel):
    row_id: str
    query: str
    limit: int = MANUAL_SEARCH_LIMIT


@app.post("/api/jobs/{job_id}/search")
def search_stock_for_row(job_id: str, body: ManualSearchRequest, authorization: str | None = Header(None)):
    _check_auth(authorization)
    data = _load_job(job_id)
    row = _find_row(data, body.row_id)
    query = body.query.strip()
    if len(query) < 2:
        raise HTTPException(400, "Введите минимум 2 символа для поиска")

    matcher = _load_cached_matcher().fork()
    candidates = _manual_search_candidates(matcher, row, query, limit=body.limit)
    results = [_serialize_candidate(candidate) for candidate in candidates]

    return {
        "job_id": job_id,
        "row_id": body.row_id,
        "query": query,
        "results": results,
    }


class ManualSelectRequest(BaseModel):
    row_id: str
    candidate: dict[str, Any]


@app.post("/api/jobs/{job_id}/select")
def select_manual_candidate(job_id: str, body: ManualSelectRequest, authorization: str | None = Header(None)):
    _check_auth(authorization)
    data = _load_job(job_id)
    row = _find_row(data, body.row_id)
    candidate = body.candidate
    code = str(candidate.get("code_1c") or "").strip()
    name = str(candidate.get("name") or "").strip()
    if not code or not name:
        raise HTTPException(400, "Для выбора нужен code_1c и name")

    approved = {
        "code_1c": code,
        "name": name,
        "score": float(candidate.get("score") or 0.0),
        "remaining": candidate.get("remaining"),
        "price": candidate.get("price") or "",
        "reasons": list(candidate.get("reasons") or []),
    }
    if candidate.get("source_label"):
        approved["source_label"] = candidate["source_label"]
    row["approved_analog"] = approved
    row["status"] = "Одобрена замена"

    analog_codes = {analog.get("code_1c") for analog in row.get("analogs", [])}
    if approved["code_1c"] not in analog_codes:
        row.setdefault("analogs", [])
        row["analogs"].insert(0, approved)

    status_counts = _rebuild_status_counts(data["rows"])
    data["status_counts"] = status_counts
    _save_job(job_id, data)
    return {"row": row, "status_counts": status_counts}


@app.post("/api/jobs/{job_id}/export")
def export_1c_file(job_id: str, authorization: str | None = Header(None)):
    _check_auth(authorization)
    data = _load_job(job_id)

    import openpyxl
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    # Headers matching 1C format
    headers = ["Штрихкод", "Код", "Артикул", "Номенклатура", "Количество", "Цена"]
    header_font = Font(bold=True, size=11)
    header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = thin_border
        cell.alignment = Alignment(horizontal="center")

    row_num = 2

    # Exact matches
    exact_statuses = {"Найдено полностью", "Найдено частично"}
    for r in data["rows"]:
        if r["status"] in exact_statuses and r.get("matched_code"):
            ws.cell(row=row_num, column=1, value=None).border = thin_border
            ws.cell(row=row_num, column=2, value=r["matched_code"]).border = thin_border
            ws.cell(row=row_num, column=3, value=None).border = thin_border
            ws.cell(row=row_num, column=4, value=r["matched_name"]).border = thin_border
            ws.cell(row=row_num, column=5, value=r["available_qty"]).border = thin_border
            ws.cell(row=row_num, column=6, value=r.get("matched_price") or "").border = thin_border
            row_num += 1

    # Approved analogs
    for r in data["rows"]:
        if r.get("approved_analog"):
            analog = r["approved_analog"]
            qty = r["requested_qty"]
            ws.cell(row=row_num, column=1, value=None).border = thin_border
            ws.cell(row=row_num, column=2, value=analog["code_1c"]).border = thin_border
            ws.cell(row=row_num, column=3, value=None).border = thin_border
            ws.cell(row=row_num, column=4, value=analog["name"]).border = thin_border
            ws.cell(row=row_num, column=5, value=qty).border = thin_border
            ws.cell(row=row_num, column=6, value=analog.get("price") or "").border = thin_border
            row_num += 1

    # Auto-size columns
    ws.column_dimensions["A"].width = 14
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 14
    ws.column_dimensions["D"].width = 65
    ws.column_dimensions["E"].width = 14
    ws.column_dimensions["F"].width = 12

    job_dir = JOBS_DIR / job_id
    export_path = job_dir / "export_for_1c.xlsx"
    wb.save(export_path)

    return FileResponse(
        path=str(export_path),
        filename="КП_для_1С.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------------------------------------------------------------------
# Static files fallback (serve frontend)
# ---------------------------------------------------------------------------

WEB_DIR = Path(__file__).resolve().parent.parent / "web"

if WEB_DIR.exists():
    from fastapi.staticfiles import StaticFiles
    app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    ensure_output_dir(JOBS_DIR)
    uvicorn.run(app, host="0.0.0.0", port=8000)

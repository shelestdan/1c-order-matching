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
    DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH,
    StockMatcher,
    clone_stock_items,
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


def _find_stock_file() -> Path:
    """Find the most recent stock file in inputs/stock/."""
    candidates = sorted(STOCK_DIR.glob("*.*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for c in candidates:
        if c.suffix.lower() in (".csv", ".xlsx", ".xlsm", ".xls"):
            return c
    raise HTTPException(500, "No stock file found in inputs/stock/")


def _run_pipeline(upload_path: Path, job_dir: Path) -> dict[str, Any]:
    """Run the full normalization + matching pipeline."""
    normalized_dir = ensure_output_dir(job_dir / "normalized")
    matched_dir = ensure_output_dir(job_dir / "matched")

    # Stage 1: normalize
    normalized_path, parsed_count, issue_count = normalize_request_file(upload_path, normalized_dir)

    # Stage 2: match
    stock_path = _find_stock_file()
    reviewed_decisions = load_reviewed_analog_decisions(DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH)
    substitution_policy = load_substitution_policy()
    base_stock = load_stock(stock_path)
    stock_items = clone_stock_items(base_stock)
    matcher = StockMatcher(stock_items, reviewed_analog_decisions=reviewed_decisions, substitution_policy=substitution_policy)
    order_lines = load_order_lines(normalized_path)
    order_results = match_orders(order_lines, matcher)

    output_paths = write_outputs(
        order_path=normalized_path,
        stock_path=stock_path,
        reviewed_decisions_path=DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH if reviewed_decisions else None,
        order_results=order_results,
        stock_items=stock_items,
        out_dir=matched_dir,
    )

    # Build JSON result for the web UI
    rows: list[dict[str, Any]] = []
    for result in order_results:
        analogs_list = []
        for analog in result.analogs:
            analogs_list.append({
                "code_1c": analog.stock.code_1c,
                "name": analog.stock.name,
                "score": round(analog.score, 1),
                "remaining": analog.stock.remaining + (result.available_qty if analog.stock.code_1c == (result.matched_stock.code_1c if result.matched_stock else None) else 0),
                "price": analog.stock.sale_price,
                "reasons": analog.reasons,
            })

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
        "stock_file": stock_path.name,
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
    status_counts: dict[str, int] = {}
    for r in data["rows"]:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1
    data["status_counts"] = status_counts

    _save_job(job_id, data)
    return {"approved_count": approved_count, "status_counts": status_counts}


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

#!/usr/bin/env python3
"""Evaluate the stock matcher against a grouped golden set of reviewed analogs."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from nomenclature_classifier import load_default_classifier
from process_1c_orders import (
    DEFAULT_MATCHING_GOLDEN_SET_PATH,
    DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH,
    OrderLine,
    StockMatcher,
    augment_search_text_with_dimension_tags,
    build_search_text,
    extract_code_tokens,
    extract_dimension_tags,
    extract_family_tags,
    extract_key_tokens,
    extract_material_tags_from_search_text,
    extract_root_tokens,
    extract_structured_tags_from_search_text,
    extract_tokens,
    load_reviewed_analog_decisions,
    load_stock,
    load_substitution_policy,
    primary_family_tag,
    determine_analog_status,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate matcher quality on the reviewed analog golden set.")
    parser.add_argument("--stock", required=True, help="Path to the stock CSV.")
    parser.add_argument(
        "--golden-set",
        default=str(DEFAULT_MATCHING_GOLDEN_SET_PATH),
        help="Path to the grouped golden set JSON.",
    )
    parser.add_argument(
        "--reviewed-decisions",
        default=str(DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH),
        help="Optional reviewed approve/reject JSON used by the matcher.",
    )
    parser.add_argument("--top-k", type=int, default=5, help="How many top candidates to inspect per golden case.")
    parser.add_argument("--out", default="", help="Optional JSON report path.")
    return parser.parse_args()


def build_order_from_query(query: str, index: int) -> OrderLine:
    classifier = load_default_classifier()
    classification = classifier.classify(query)
    query_parts = (query, "", "", "")
    search_text = build_search_text(query)
    classifier_family_tags = set(classification.family_tags or ())
    classifier_hint = classification.category_key if classification and classification.category_key else ""
    dimension_tags = (
        extract_dimension_tags(*query_parts)
        | extract_family_tags(*query_parts, classifier_hint)
        | extract_material_tags_from_search_text(search_text)
        | extract_structured_tags_from_search_text(search_text)
        | classifier_family_tags
    )
    search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
    search_tokens = extract_tokens(search_text)
    return OrderLine(
        source_file="golden_set.json",
        sheet_name="golden",
        source_row=index,
        headers=[],
        row_values=[],
        position=str(index),
        name=query,
        mark="",
        supplier_code="",
        vendor="",
        unit="шт",
        requested_qty=1.0,
        search_text=search_text,
        search_tokens=search_tokens,
        key_tokens=extract_key_tokens(search_tokens),
        root_tokens=extract_root_tokens(search_tokens),
        code_tokens=extract_code_tokens(query),
        dimension_tags=dimension_tags,
        raw_query=query,
        classification=classification,
    )


def evaluate_cases(
    matcher: StockMatcher,
    golden_cases: list[dict[str, object]],
    *,
    top_k: int,
) -> dict[str, object]:
    family_counters: defaultdict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
    case_reports: list[dict[str, object]] = []
    approved_hit_top1 = 0
    approved_hit_top3 = 0
    approved_hit_topk = 0
    rejected_in_top3 = 0
    rejected_in_topk = 0
    status_counts: defaultdict[str, int] = defaultdict(int)

    for index, case in enumerate(golden_cases, start=1):
        query = str(case.get("query", "")).strip()
        if not query:
            continue
        approved_codes = [str(code) for code in case.get("approved_candidate_codes", []) if str(code)]
        rejected_codes = [str(code) for code in case.get("rejected_candidate_codes", []) if str(code)]
        order = build_order_from_query(query, index)
        candidates = matcher.find_candidates_exhaustive(order, limit=max(top_k, 5))
        match_kind, best, reason = matcher.classify(order, candidates)
        status = determine_analog_status(candidates, order=order, substitution_policy=matcher.substitution_policy) if match_kind == "analog" else match_kind
        top_codes = [candidate.stock.code_1c for candidate in candidates[:top_k]]
        top3_codes = top_codes[:3]
        approved_top1 = bool(approved_codes and top_codes[:1] and top_codes[0] in approved_codes)
        approved_top3_case = any(code in approved_codes for code in top3_codes)
        approved_topk_case = any(code in approved_codes for code in top_codes)
        rejected_top3_case = any(code in rejected_codes for code in top3_codes)
        rejected_topk_case = any(code in rejected_codes for code in top_codes)

        if approved_top1:
            approved_hit_top1 += 1
        if approved_top3_case:
            approved_hit_top3 += 1
        if approved_topk_case:
            approved_hit_topk += 1
        if rejected_top3_case:
            rejected_in_top3 += 1
        if rejected_topk_case:
            rejected_in_topk += 1

        family = primary_family_tag(order.dimension_tags) or "unclassified"
        family_counters[family]["cases"] += 1
        family_counters[family]["approved_hit_top1"] += int(approved_top1)
        family_counters[family]["approved_hit_top3"] += int(approved_top3_case)
        family_counters[family]["approved_hit_topk"] += int(approved_topk_case)
        family_counters[family]["rejected_in_top3"] += int(rejected_top3_case)
        family_counters[family]["rejected_in_topk"] += int(rejected_topk_case)
        status_counts[str(status)] += 1

        case_reports.append(
            {
                "query": query,
                "family": family,
                "approved_candidate_codes": approved_codes,
                "rejected_candidate_codes": rejected_codes,
                "top_codes": top_codes,
                "top_candidates": [
                    {
                        "code": candidate.stock.code_1c,
                        "name": candidate.stock.name,
                        "score": round(candidate.score, 3),
                        "review_decision": candidate.review_decision,
                    }
                    for candidate in candidates[:top_k]
                ],
                "match_kind": match_kind,
                "status": status,
                "reason": reason,
                "approved_hit_top1": approved_top1,
                "approved_hit_top3": approved_top3_case,
                "approved_hit_topk": approved_topk_case,
                "rejected_in_top3": rejected_top3_case,
                "rejected_in_topk": rejected_topk_case,
                "best_code": best.stock.code_1c if best is not None else "",
            }
        )

    total_cases = len(case_reports)
    return {
        "total_cases": total_cases,
        "top_k": top_k,
        "approved_hit_top1": approved_hit_top1,
        "approved_hit_top3": approved_hit_top3,
        "approved_hit_topk": approved_hit_topk,
        "rejected_in_top3": rejected_in_top3,
        "rejected_in_topk": rejected_in_topk,
        "approved_hit_top1_ratio": round(approved_hit_top1 / total_cases, 4) if total_cases else 0.0,
        "approved_hit_top3_ratio": round(approved_hit_top3 / total_cases, 4) if total_cases else 0.0,
        "approved_hit_topk_ratio": round(approved_hit_topk / total_cases, 4) if total_cases else 0.0,
        "rejected_in_top3_ratio": round(rejected_in_top3 / total_cases, 4) if total_cases else 0.0,
        "rejected_in_topk_ratio": round(rejected_in_topk / total_cases, 4) if total_cases else 0.0,
        "status_counts": dict(status_counts),
        "family_breakdown": {family: dict(values) for family, values in sorted(family_counters.items())},
        "cases": case_reports,
    }


def main() -> int:
    args = parse_args()
    stock_path = Path(args.stock).expanduser().resolve()
    golden_set_path = Path(args.golden_set).expanduser().resolve()
    reviewed_path = Path(args.reviewed_decisions).expanduser().resolve() if args.reviewed_decisions else None
    golden_payload = json.loads(golden_set_path.read_text(encoding="utf-8"))
    stock_items = load_stock(stock_path)
    reviewed_decisions = load_reviewed_analog_decisions(reviewed_path)
    substitution_policy = load_substitution_policy()
    matcher = StockMatcher(
        stock_items,
        reviewed_analog_decisions=reviewed_decisions,
        substitution_policy=substitution_policy,
    )
    report = evaluate_cases(matcher, list(golden_payload.get("cases", [])), top_k=max(args.top_k, 1))
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "stock_path": str(stock_path),
        "golden_set_path": str(golden_set_path),
        "reviewed_decisions_path": str(reviewed_path) if reviewed_path and reviewed_path.exists() else "",
        "metrics": report,
    }
    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved golden set report to {out_path}")

    metrics = report
    print(f"Golden set cases: {metrics['total_cases']}")
    print(f"Approved hit @1: {metrics['approved_hit_top1']} ({metrics['approved_hit_top1_ratio']:.2%})")
    print(f"Approved hit @3: {metrics['approved_hit_top3']} ({metrics['approved_hit_top3_ratio']:.2%})")
    print(f"Approved hit @{metrics['top_k']}: {metrics['approved_hit_topk']} ({metrics['approved_hit_topk_ratio']:.2%})")
    print(f"Rejected in top3: {metrics['rejected_in_top3']} ({metrics['rejected_in_top3_ratio']:.2%})")
    print(f"Rejected in top{metrics['top_k']}: {metrics['rejected_in_topk']} ({metrics['rejected_in_topk_ratio']:.2%})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

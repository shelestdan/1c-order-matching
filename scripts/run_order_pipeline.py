#!/usr/bin/env python3
"""Run the two-stage order pipeline.

Stage 1: normalize the client request into a standard XLSX table.
Stage 2: match that normalized table against the current stock CSV.
"""

from __future__ import annotations

import argparse
from pathlib import Path

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
    print_summary,
    write_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize requests and match them against stock.")
    parser.add_argument("--stock", required=True, help="Path to the stock CSV exported from 1C.")
    parser.add_argument("--inputs", nargs="+", required=True, help="Client request files.")
    parser.add_argument(
        "--out-dir",
        default="outputs/order_pipeline",
        help="Base directory for normalized and matched results.",
    )
    parser.add_argument(
        "--reviewed-analog-decisions",
        default="",
        help=(
            "Optional JSON artifact with reviewed analog approve/reject decisions. "
            f"If omitted, {DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH} will be used when it exists."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stock_path = Path(args.stock).expanduser().resolve()
    input_paths = [Path(value).expanduser().resolve() for value in args.inputs]
    out_dir = ensure_output_dir(Path(args.out_dir).expanduser().resolve())
    normalized_dir = ensure_output_dir(out_dir / "normalized")
    matched_dir = ensure_output_dir(out_dir / "matched")
    reviewed_decisions_path = (
        Path(args.reviewed_analog_decisions).expanduser().resolve()
        if args.reviewed_analog_decisions
        else DEFAULT_REVIEWED_ANALOG_DECISIONS_PATH
    )
    reviewed_decisions = load_reviewed_analog_decisions(reviewed_decisions_path)
    substitution_policy = load_substitution_policy()

    base_stock = load_stock(stock_path)

    for input_path in input_paths:
        normalized_path, parsed_count, issue_count = normalize_request_file(input_path, normalized_dir)
        print(f"\n{input_path.name}")
        print(f"  stage1 normalized: {normalized_path}")
        print(f"  stage1 parsed rows: {parsed_count}")
        print(f"  stage1 issues: {issue_count}")

        stock_items = clone_stock_items(base_stock)
        matcher = StockMatcher(
            stock_items,
            reviewed_analog_decisions=reviewed_decisions,
            substitution_policy=substitution_policy,
        )
        order_lines = load_order_lines(normalized_path)
        order_results = match_orders(order_lines, matcher)
        output_paths = write_outputs(
            order_path=normalized_path,
            stock_path=stock_path,
            reviewed_decisions_path=reviewed_decisions_path if reviewed_decisions else None,
            order_results=order_results,
            stock_items=stock_items,
            out_dir=matched_dir,
        )
        print_summary(normalized_path, order_results, output_paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

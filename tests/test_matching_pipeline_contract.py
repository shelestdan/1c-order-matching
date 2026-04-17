from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from process_1c_orders import (  # noqa: E402
    OrderLine,
    STATUS_APPROVAL_ANALOG,
    STATUS_FOUND_FULL,
    STATUS_NOT_FOUND,
    STATUS_SAFE_ANALOG,
    StockItem,
    StockMatcher,
    augment_search_text_with_dimension_tags,
    build_search_text,
    determine_analog_status,
    extract_dimension_tags,
    extract_family_tags,
    extract_key_tokens,
    extract_material_tags_from_search_text,
    extract_parser_hint_tags,
    extract_root_tokens,
    extract_structured_tags_from_search_text,
    extract_tokens,
    load_substitution_policy,
    match_orders,
    primary_family_tag,
)


class MatchingPipelineContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.substitution_policy = load_substitution_policy()

    def make_order(self, name: str, *, unit: str = "шт", qty: float = 1.0, position: str = "1") -> OrderLine:
        search_text = build_search_text(name)
        dimension_tags = (
            extract_dimension_tags(name)
            | extract_family_tags(name)
            | extract_parser_hint_tags(name)
            | extract_material_tags_from_search_text(search_text)
            | extract_structured_tags_from_search_text(search_text)
        )
        search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
        tokens = extract_tokens(search_text)
        return OrderLine(
            source_file="contract.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position=position,
            name=name,
            mark="",
            supplier_code="",
            vendor="",
            unit=unit,
            requested_qty=qty,
            search_text=search_text,
            search_tokens=tokens,
            key_tokens=extract_key_tokens(tokens),
            root_tokens=extract_root_tokens(tokens),
            code_tokens=set(),
            dimension_tags=dimension_tags,
            raw_query=name,
            classification=None,
        )

    def make_stock(
        self,
        row_index: int,
        code: str,
        name: str,
        *,
        quantity: float = 10.0,
    ) -> StockItem:
        search_text = build_search_text(name)
        dimension_tags = (
            extract_dimension_tags(name)
            | extract_family_tags(name)
            | extract_parser_hint_tags(name)
            | extract_material_tags_from_search_text(search_text)
            | extract_structured_tags_from_search_text(search_text)
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
            quantity=quantity,
            remaining=quantity,
            search_text=search_text,
            search_tokens=tokens,
            key_tokens=extract_key_tokens(tokens),
            root_tokens=extract_root_tokens(tokens),
            code_tokens=set(),
            dimension_tags=dimension_tags,
        )

    def make_matcher(self, *stocks: StockItem) -> StockMatcher:
        return StockMatcher(list(stocks), substitution_policy=self.substitution_policy)

    def test_happy_path_exact_contract_keeps_exact_pipe_match(self) -> None:
        order = self.make_order("Труба стальная электросварная Ø57х3.5 ГОСТ 10704-91", unit="м", qty=10.0)
        exact_stock = self.make_stock(1, "PIPE-EXACT", "Труба стальная электросварная 57х3.5 ГОСТ 10704-91", quantity=100.0)
        wrong_wall_stock = self.make_stock(2, "PIPE-WALL", "Труба стальная электросварная 57х4.0 ГОСТ 10704-91", quantity=100.0)
        ocink_stock = self.make_stock(3, "PIPE-OCINK", "Труба стальная оцинкованная 57х3.5 ГОСТ 10704-91", quantity=100.0)

        matcher = self.make_matcher(wrong_wall_stock, ocink_stock, exact_stock)
        candidate_pool = matcher.generate_candidate_pool(order)
        exact_index = next(index for index, stock in enumerate(matcher.stock_items) if stock.code_1c == "PIPE-EXACT")

        self.assertTrue(candidate_pool)
        self.assertIn(exact_index, candidate_pool)

        ranked = matcher.rank_candidates(order, candidate_pool=candidate_pool, limit=3)
        kind, best, _ = matcher.classify(order, ranked)

        self.assertEqual(kind, "exact")
        self.assertIsNotNone(best)
        self.assertEqual(best.stock.code_1c, "PIPE-EXACT")
        self.assertEqual(primary_family_tag(best.stock.dimension_tags), "pipe")

        result = match_orders([order], self.make_matcher(wrong_wall_stock, ocink_stock, exact_stock), use_full_scan_fallback=False)[0]
        self.assertEqual(result.status, STATUS_FOUND_FULL)
        self.assertEqual(result.matched_stock.code_1c, "PIPE-EXACT")

    def test_happy_path_analog_contract_keeps_filter_candidate_reachable(self) -> None:
        order = self.make_order("Фильтр магнитный фланцевый Ду50")
        exact_family_candidate = self.make_stock(1, "FILTER50", "Фильтр магнитный фланцевый ФМФ DN- 50 PN-1,6 (КАЗ)")
        distractor = self.make_stock(2, "VALVE50", "Задвижка чугунная фланцевая Ду50 Ру16")

        matcher = self.make_matcher(distractor, exact_family_candidate)
        ranked = matcher.find_candidates(order, limit=3)
        kind, best, _ = matcher.classify(order, ranked)

        self.assertEqual(kind, "analog")
        self.assertIsNotNone(best)
        self.assertEqual(best.stock.code_1c, "FILTER50")
        self.assertEqual(primary_family_tag(best.stock.dimension_tags), "filter")

        result = match_orders([order], self.make_matcher(distractor, exact_family_candidate), use_full_scan_fallback=False)[0]
        self.assertIn(result.status, {STATUS_SAFE_ANALOG, STATUS_APPROVAL_ANALOG})
        self.assertNotEqual(result.status, STATUS_NOT_FOUND)
        self.assertTrue(result.analogs)
        self.assertEqual(result.analogs[0].stock.code_1c, "FILTER50")

    def test_hard_negative_material_contract_blocks_false_positive_exact_and_safe_analog(self) -> None:
        order = self.make_order("Труба ПЭ Ду110", unit="м")
        incompatible_stock = self.make_stock(1, "PPU-110", "Труба ППУ Ду110", quantity=20.0)

        matcher = self.make_matcher(incompatible_stock)
        ranked = matcher.find_candidates(order, limit=3)
        kind, best, _ = matcher.classify(order, ranked)

        self.assertEqual(ranked, [])
        self.assertEqual(kind, "not_found")
        self.assertIsNone(best)

        result = match_orders([order], self.make_matcher(incompatible_stock), use_full_scan_fallback=True)[0]
        self.assertEqual(result.status, STATUS_NOT_FOUND)

    def test_hard_negative_size_contract_blocks_false_safe_analog(self) -> None:
        order = self.make_order("Переход ст. приварной 57х40х3,5 мм")
        incompatible_size_stock = self.make_stock(1, "RED-WRONG-SIDE", "Переход П-2- 89х4,0 - 57х3,5 Ст.20 ГОСТ 17378", quantity=10.0)

        matcher = self.make_matcher(incompatible_size_stock)
        ranked = matcher.find_candidates(order, limit=3)
        kind, best, _ = matcher.classify(order, ranked)

        self.assertEqual(kind, "analog")
        analog_status = determine_analog_status(ranked, order=order, substitution_policy=self.substitution_policy)
        self.assertEqual(analog_status, STATUS_APPROVAL_ANALOG)
        self.assertIsNotNone(best)

        result = match_orders([order], self.make_matcher(incompatible_size_stock), use_full_scan_fallback=True)[0]
        self.assertEqual(result.status, STATUS_APPROVAL_ANALOG)

    def test_hard_negative_smaller_diameter_contract_blocks_downsizing_analogs(self) -> None:
        cases = [
            (
                "Фланец стальной Ду700 Ру16",
                ("FLANGE-SMALL", "Фланец стальной Ду600 Ру16"),
                ("FLANGE-OK", "Фланец стальной Ду700 Ру16"),
            ),
            (
                "Труба стальная 219х6 ГОСТ 10704",
                ("PIPE-SMALL", "Труба стальная 159х6 ГОСТ 10704"),
                ("PIPE-OK", "Труба стальная 219х6 ГОСТ 10704"),
            ),
            (
                "Переход К-1-273х7-219х6-В10 ГОСТ 17378",
                ("RED-SMALL", "Переход К-1-219х6-159х5-В10 ГОСТ 17378"),
                ("RED-OK", "Переход К-1-273х7-219х6-В10 ГОСТ 17378"),
            ),
        ]

        for query, smaller, expected in cases:
            with self.subTest(query=query):
                order = self.make_order(query)
                smaller_stock = self.make_stock(1, smaller[0], smaller[1])
                expected_stock = self.make_stock(2, expected[0], expected[1])
                matcher = self.make_matcher(smaller_stock, expected_stock)

                ranked = matcher.find_candidates(order, limit=5)
                ranked_codes = [candidate.stock.code_1c for candidate in ranked]

                self.assertNotIn(smaller[0], ranked_codes)
                self.assertIn(expected[0], ranked_codes)

                result = match_orders([order], self.make_matcher(smaller_stock, expected_stock), use_full_scan_fallback=True)[0]
                analog_codes = [candidate.stock.code_1c for candidate in result.analogs]

                self.assertNotEqual(result.status, STATUS_NOT_FOUND)
                self.assertNotIn(smaller[0], analog_codes)

    def test_retrieval_contract_keeps_expected_candidates_before_ranking(self) -> None:
        cases = [
            (
                self.make_order("Труба стальная электросварная Ø57х3.5 ГОСТ 10704-91", unit="м"),
                [
                    self.make_stock(1, "PIPE-EXACT", "Труба стальная электросварная 57х3.5 ГОСТ 10704-91"),
                    self.make_stock(2, "PIPE-WALL", "Труба стальная электросварная 57х4.0 ГОСТ 10704-91"),
                ],
                "PIPE-EXACT",
                "pipe",
            ),
            (
                self.make_order("Фильтр магнитный фланцевый Ду50"),
                [
                    self.make_stock(1, "FILTER50", "Фильтр магнитный фланцевый ФМФ DN- 50 PN-1,6 (КАЗ)"),
                    self.make_stock(2, "VALVE50", "Задвижка чугунная фланцевая Ду50 Ру16"),
                ],
                "FILTER50",
                "filter",
            ),
        ]

        for order, stocks, expected_code, expected_family in cases:
            with self.subTest(order=order.name):
                matcher = self.make_matcher(*stocks)
                candidate_pool = matcher.generate_candidate_pool(order)
                self.assertTrue(candidate_pool)

                ranked = matcher.rank_candidates(order, candidate_pool=candidate_pool, limit=3)
                self.assertTrue(ranked)
                self.assertEqual(ranked[0].stock.code_1c, expected_code)
                self.assertEqual(primary_family_tag(ranked[0].stock.dimension_tags), expected_family)
                self.assertTrue(
                    any(path in ranked[0].retrieval_paths for path in ("token", "family", "dimension", "structure", "material"))
                )

    def test_decision_contract_keeps_exact_analog_and_not_found_boundaries(self) -> None:
        exact_order = self.make_order("Труба стальная электросварная Ø57х3.5 ГОСТ 10704-91", unit="м")
        exact_matcher = self.make_matcher(
            self.make_stock(1, "PIPE-EXACT", "Труба стальная электросварная 57х3.5 ГОСТ 10704-91"),
            self.make_stock(2, "PIPE-WALL", "Труба стальная электросварная 57х4.0 ГОСТ 10704-91"),
        )
        exact_kind, exact_best, _ = exact_matcher.classify(exact_order, exact_matcher.find_candidates(exact_order, limit=3))
        self.assertEqual(exact_kind, "exact")
        self.assertEqual(exact_best.stock.code_1c, "PIPE-EXACT")

        analog_order = self.make_order("Фильтр магнитный фланцевый Ду50")
        analog_matcher = self.make_matcher(
            self.make_stock(1, "FILTER50", "Фильтр магнитный фланцевый ФМФ DN- 50 PN-1,6 (КАЗ)"),
        )
        analog_candidates = analog_matcher.find_candidates(analog_order, limit=3)
        analog_kind, analog_best, _ = analog_matcher.classify(analog_order, analog_candidates)
        self.assertEqual(analog_kind, "analog")
        self.assertEqual(analog_best.stock.code_1c, "FILTER50")
        self.assertIn(
            determine_analog_status(analog_candidates, order=analog_order, substitution_policy=self.substitution_policy),
            {STATUS_SAFE_ANALOG, STATUS_APPROVAL_ANALOG},
        )

        miss_order = self.make_order("Труба ПЭ Ду110", unit="м")
        miss_matcher = self.make_matcher(self.make_stock(1, "PPU-110", "Труба ППУ Ду110"))
        miss_kind, miss_best, _ = miss_matcher.classify(miss_order, miss_matcher.find_candidates(miss_order, limit=3))
        self.assertEqual(miss_kind, "not_found")
        self.assertIsNone(miss_best)

    def test_exhaustive_fallback_contract_does_not_worsen_exact_result(self) -> None:
        order = self.make_order("Труба стальная электросварная Ø57х3.5 ГОСТ 10704-91", unit="м", qty=10.0)
        stocks = [
            self.make_stock(1, "PIPE-EXACT", "Труба стальная электросварная 57х3.5 ГОСТ 10704-91", quantity=100.0),
            self.make_stock(2, "PIPE-WALL", "Труба стальная электросварная 57х4.0 ГОСТ 10704-91", quantity=100.0),
            self.make_stock(3, "PIPE-OCINK", "Труба стальная оцинкованная 57х3.5 ГОСТ 10704-91", quantity=100.0),
        ]

        no_fallback = match_orders([order], self.make_matcher(*stocks), use_full_scan_fallback=False)[0]
        with_fallback = match_orders([order], self.make_matcher(*stocks), use_full_scan_fallback=True)[0]

        self.assertEqual(no_fallback.status, STATUS_FOUND_FULL)
        self.assertEqual(with_fallback.status, STATUS_FOUND_FULL)
        self.assertEqual(no_fallback.matched_stock.code_1c, "PIPE-EXACT")
        self.assertEqual(with_fallback.matched_stock.code_1c, "PIPE-EXACT")


if __name__ == "__main__":
    unittest.main()

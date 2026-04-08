from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from nomenclature_classifier import load_default_classifier  # noqa: E402
from process_1c_orders import (  # noqa: E402
    OrderLine,
    REVIEW_DECISION_APPROVED,
    STATUS_APPROVAL_ANALOG,
    STATUS_FOUND_FULL,
    STATUS_NOT_FOUND,
    STATUS_SAFE_ANALOG,
    StockItem,
    StockMatcher,
    augment_search_text_with_dimension_tags,
    build_search_text,
    determine_analog_status,
    extract_code_tokens,
    extract_dimension_tags,
    extract_family_tags,
    extract_key_tokens,
    extract_material_tags_from_search_text,
    extract_parser_hint_tags,
    extract_root_tokens,
    extract_structured_tags_from_search_text,
    extract_tokens,
    load_stock,
    load_reviewed_analog_decisions,
    load_substitution_policy,
    match_orders,
    primary_family_tag,
)

FIXTURE_PATH = ROOT / "tests" / "fixtures" / "matching_regression_stock_subset.json"
GOLDEN_SET_PATH = ROOT / "data" / "matching_golden_set.json"
REVIEWED_DECISIONS_PATH = ROOT / "data" / "reviewed_analog_decisions.json"
FULL_STOCK_FILES = (
    (ROOT / "inputs" / "stock" / "остатки_ЭК.xlsm", "ЭК"),
    (ROOT / "inputs" / "stock" / "сантехкомплект_база.xlsm", "Сантехкомплект"),
)

REPRESENTATIVE_STABLE_CASES = (
    {
        "query": "Отвод 45-2-57x4-20 ГОСТ 17375-2001",
        "expected_kind": "analog",
        "expected_status": STATUS_APPROVAL_ANALOG,
        "expected_top_code": "00-00024507",
        "expected_family": "elbow",
    },
    {
        "query": "Отвод 45-2-76х3,5 ГОСТ17375-2001",
        "expected_kind": "analog",
        "expected_status": STATUS_APPROVAL_ANALOG,
        "expected_top_code": "00-00031025",
        "expected_family": "elbow",
    },
    {
        "query": "Отвод 90-2-57x5--20 ГОСТ 17375-2001",
        "expected_kind": "analog",
        "expected_status": STATUS_APPROVAL_ANALOG,
        "expected_top_code": "00-00030960",
        "expected_family": "elbow",
    },
    {
        "query": "Переход К-1-273х7-219х6-В10 ГОСТ 17378-2001",
        "expected_kind": "exact",
        "expected_status": STATUS_FOUND_FULL,
        "expected_top_code": "П0000002542",
        "expected_family": "perehod",
    },
    {
        "query": "Переход К-1-89x3,5-57x3-В10(-20)(09Г2С) ГОСТ 17378-2001",
        "expected_kind": "exact",
        "expected_status": STATUS_FOUND_FULL,
        "expected_top_code": "00-00031211",
        "expected_family": "perehod",
    },
    {
        "query": "Переход К-1-89х6-76х6-20 ГОСТ 17378-2001",
        "expected_kind": "exact",
        "expected_status": STATUS_FOUND_FULL,
        "expected_top_code": "П0000046104",
        "expected_family": "perehod",
    },
)

REVIEWED_ANALOG_CASES = (
    {
        "query": "Отвод 45-2-57x4-20 ГОСТ 17375-2001",
        "expected_status": STATUS_SAFE_ANALOG,
        "expected_top_code": "00-00024507",
    },
    {
        "query": "Отвод 45-2-76х3,5 ГОСТ17375-2001",
        "expected_status": STATUS_SAFE_ANALOG,
        "expected_top_code": "00-00031025",
    },
    {
        "query": "Отвод 90-2-57x5--20 ГОСТ 17375-2001",
        "expected_status": STATUS_SAFE_ANALOG,
        "expected_top_code": "00-00030960",
    },
)

RAW_TO_REVIEWED_REJECTION_CASES = (
    "Отвод 90-2-273х14-20 ГОСТ 17375-2001",
    "Отвод 90-2-89х7-20 ГОСТ 17375-2001",
)

RAW_HARD_NEGATIVE_CASES = (
    "Отвод 45-2-57x4,5-В10(-20)(09Г2С) ГОСТ 17375-2001",
    "Переход К-1-57х4-38х4-В10 (-20)(09Г2С)ГОСТ17378-2001",
    "Тройник 57x4-В10(-20)(09Г2С) ГОСТ 17376-2001",
)


class MatchingRegressionCasesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture_payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        golden_payload = json.loads(GOLDEN_SET_PATH.read_text(encoding="utf-8"))
        cls.fixture_omitted_queries = set(fixture_payload.get("omitted_queries", []))
        cls.fixture_stock_records = {item["code_1c"]: item for item in fixture_payload["items"]}
        cls.golden_cases = {case["query"]: case for case in golden_payload["cases"]}
        cls.reviewed_decisions = load_reviewed_analog_decisions(REVIEWED_DECISIONS_PATH)
        cls.substitution_policy = load_substitution_policy()
        cls.reject_only_queries = tuple(
            case["query"]
            for case in golden_payload["cases"]
            if not case["approved_candidate_codes"] and case["query"] not in cls.fixture_omitted_queries
        )

    def make_order(self, query: str) -> OrderLine:
        search_text = build_search_text(query)
        dimension_tags = (
            extract_dimension_tags(query)
            | extract_family_tags(query)
            | extract_parser_hint_tags(query)
            | extract_material_tags_from_search_text(search_text)
            | extract_structured_tags_from_search_text(search_text)
        )
        search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
        tokens = extract_tokens(search_text)
        return OrderLine(
            source_file="regression.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=query,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=tokens,
            key_tokens=extract_key_tokens(tokens),
            root_tokens=extract_root_tokens(tokens),
            code_tokens=set(),
            dimension_tags=dimension_tags,
            raw_query=query,
            classification=None,
        )

    def make_stock(self, record: dict[str, object]) -> StockItem:
        search_text = build_search_text(
            record.get("name", ""),
            record.get("print_name", ""),
            record.get("product_type", ""),
        )
        dimension_tags = (
            extract_dimension_tags(
                record.get("name", ""),
                record.get("print_name", ""),
                record.get("product_type", ""),
            )
            | extract_family_tags(
                record.get("name", ""),
                record.get("print_name", ""),
                record.get("product_type", ""),
            )
            | extract_parser_hint_tags(
                record.get("name", ""),
                record.get("print_name", ""),
                record.get("product_type", ""),
            )
            | extract_material_tags_from_search_text(search_text)
            | extract_structured_tags_from_search_text(search_text)
        )
        search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
        tokens = extract_tokens(search_text)
        quantity = float(record.get("quantity", 1.0) or 1.0)
        return StockItem(
            row_index=int(record["row_index"]),
            code_1c=str(record["code_1c"]),
            name=str(record["name"]),
            print_name=str(record.get("print_name", "")),
            product_type=str(record.get("product_type", "")),
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
            source_label=str(record.get("source_label", "")),
        )

    def case_stock_codes(self, query: str) -> list[str]:
        case = self.golden_cases[query]
        seen: set[str] = set()
        codes: list[str] = []
        for code in [*case["approved_candidate_codes"], *case["rejected_candidate_codes"]]:
            if code in seen or code not in self.fixture_stock_records:
                continue
            seen.add(code)
            codes.append(code)
        return codes

    def make_matcher(self, query: str, *, with_reviewed: bool) -> StockMatcher:
        items = [self.make_stock(self.fixture_stock_records[code]) for code in self.case_stock_codes(query)]
        return StockMatcher(
            items,
            reviewed_analog_decisions=self.reviewed_decisions if with_reviewed else None,
            substitution_policy=self.substitution_policy,
        )

    def evaluate_case(self, query: str, *, with_reviewed: bool, limit: int = 5) -> dict[str, object]:
        case = self.golden_cases[query]
        order = self.make_order(query)
        matcher = self.make_matcher(query, with_reviewed=with_reviewed)
        candidate_pool = matcher.generate_candidate_pool(order)
        ranked = matcher.rank_candidates(order, candidate_pool=candidate_pool, limit=limit)
        kind, best, _ = matcher.classify(order, ranked)
        return {
            "case": case,
            "order": order,
            "matcher": matcher,
            "candidate_pool": candidate_pool,
            "ranked": ranked,
            "kind": kind,
            "best": best,
        }

    def match_case(self, query: str, *, with_reviewed: bool, use_full_scan_fallback: bool) -> object:
        matcher = self.make_matcher(query, with_reviewed=with_reviewed)
        order = self.make_order(query)
        return match_orders([order], matcher, use_full_scan_fallback=use_full_scan_fallback)[0]

    @staticmethod
    def candidate_pool_codes(candidate_pool: dict[int, tuple[str, ...]], matcher: StockMatcher) -> list[str]:
        return [matcher.stock_items[index].code_1c for index in candidate_pool]

    def test_fixture_covers_safe_subset_of_real_cases(self) -> None:
        self.assertEqual(self.fixture_omitted_queries, {"Отвод 90-2-38x4-В10(-20)(09Г2С) ГОСТ 17375-2001"})
        self.assertEqual(len(self.reject_only_queries), 12)
        for query in self.golden_cases:
            if query in self.fixture_omitted_queries:
                continue
            with self.subTest(query=query):
                self.assertTrue(self.case_stock_codes(query))

    def test_golden_set_regression_keeps_representative_exact_and_analog_cases(self) -> None:
        for spec in REPRESENTATIVE_STABLE_CASES:
            with self.subTest(query=spec["query"]):
                evaluated = self.evaluate_case(spec["query"], with_reviewed=False)
                case = evaluated["case"]
                matcher = evaluated["matcher"]
                ranked = evaluated["ranked"]
                best = evaluated["best"]
                candidate_codes = set(self.candidate_pool_codes(evaluated["candidate_pool"], matcher))
                top_codes = [candidate.stock.code_1c for candidate in ranked]
                approved_codes = set(case["approved_candidate_codes"])

                self.assertTrue(candidate_codes)
                self.assertTrue(approved_codes & candidate_codes)
                self.assertTrue(top_codes)
                self.assertEqual(top_codes[0], spec["expected_top_code"])
                self.assertEqual(evaluated["kind"], spec["expected_kind"])
                self.assertIsNotNone(best)
                self.assertEqual(primary_family_tag(best.stock.dimension_tags), spec["expected_family"])
                self.assertTrue(approved_codes & set(top_codes[:3]))

                result = self.match_case(spec["query"], with_reviewed=False, use_full_scan_fallback=True)
                self.assertEqual(result.status, spec["expected_status"])
                if spec["expected_kind"] == "exact":
                    self.assertIsNotNone(result.matched_stock)
                    self.assertEqual(result.matched_stock.code_1c, spec["expected_top_code"])
                else:
                    self.assertTrue(result.analogs)
                    self.assertTrue(
                        approved_codes & {candidate.stock.code_1c for candidate in result.analogs},
                    )
                    self.assertNotEqual(result.status, STATUS_NOT_FOUND)

    def test_elbow_angle_series_prefix_does_not_leak_into_false_dimensions(self) -> None:
        cases = (
            (
                "Отвод 45-2-57x4-20 ГОСТ 17375-2001",
                {"deg:45", "series:2", "od:57", "wall:4"},
                {"od:45", "wall:2", "od:20", "dn:20"},
            ),
            (
                "Отвод 90-2-89х7-20 ГОСТ 17375-2001",
                {"deg:90", "series:2", "od:89", "wall:7"},
                {"od:90", "wall:2", "od:20", "dn:20"},
            ),
        )
        for query, expected_present, expected_absent in cases:
            with self.subTest(query=query):
                tags = extract_dimension_tags(query)
                self.assertTrue(expected_present.issubset(tags))
                self.assertFalse(expected_absent & tags)

    def test_elbow_angle_series_cleanup_keeps_real_large_size_from_structured_tags(self) -> None:
        query = "Отвод 90-2-273х14-20 ГОСТ 17375-2001"
        base_tags = extract_dimension_tags(query)
        structured_tags = extract_structured_tags_from_search_text(build_search_text(query))
        merged = base_tags | structured_tags

        self.assertTrue({"deg:90", "series:2", "od:273", "wall:14", "spec:17375"}.issubset(merged))
        self.assertFalse({"od:90", "wall:2", "od:20", "dn:20"} & merged)

    def test_reviewed_analog_regression_keeps_approved_candidates_only(self) -> None:
        for spec in REVIEWED_ANALOG_CASES:
            with self.subTest(query=spec["query"]):
                evaluated = self.evaluate_case(spec["query"], with_reviewed=True)
                case = evaluated["case"]
                ranked = evaluated["ranked"]
                top_codes = [candidate.stock.code_1c for candidate in ranked]
                approved_codes = set(case["approved_candidate_codes"])
                rejected_codes = set(case["rejected_candidate_codes"])
                analog_status = determine_analog_status(
                    ranked,
                    order=evaluated["order"],
                    substitution_policy=self.substitution_policy,
                )

                self.assertEqual(evaluated["kind"], "analog")
                self.assertEqual(analog_status, spec["expected_status"])
                self.assertTrue(top_codes)
                self.assertEqual(top_codes[0], spec["expected_top_code"])
                self.assertTrue(set(top_codes).issubset(approved_codes))
                self.assertFalse(set(top_codes) & rejected_codes)
                self.assertTrue(all(candidate.review_decision == REVIEW_DECISION_APPROVED for candidate in ranked))

                result = self.match_case(spec["query"], with_reviewed=True, use_full_scan_fallback=True)
                self.assertEqual(result.status, spec["expected_status"])
                self.assertTrue(result.analogs)
                self.assertEqual(result.analogs[0].stock.code_1c, spec["expected_top_code"])

    def test_reviewed_reject_only_cases_regress_to_not_found_across_safe_subset(self) -> None:
        for query in self.reject_only_queries:
            with self.subTest(query=query):
                evaluated = self.evaluate_case(query, with_reviewed=True)
                self.assertEqual(evaluated["kind"], "not_found")
                self.assertEqual(evaluated["ranked"], [])

                result = self.match_case(query, with_reviewed=True, use_full_scan_fallback=True)
                self.assertEqual(result.status, STATUS_NOT_FOUND)

    def test_reviewed_rejections_override_raw_manual_drift_cases(self) -> None:
        for query in RAW_TO_REVIEWED_REJECTION_CASES:
            with self.subTest(query=query):
                raw_result = self.match_case(query, with_reviewed=False, use_full_scan_fallback=True)
                reviewed_result = self.match_case(query, with_reviewed=True, use_full_scan_fallback=True)

                self.assertEqual(raw_result.status, STATUS_APPROVAL_ANALOG)
                self.assertEqual(reviewed_result.status, STATUS_NOT_FOUND)

    def test_size_pair_tiebreak_prefers_true_dimension_pair_over_angle_series_noise(self) -> None:
        query = "Отвод 45-2-57x4-20 ГОСТ 17375-2001"
        evaluated = self.evaluate_case(query, with_reviewed=False, limit=10)
        rejected_codes = set(evaluated["case"]["rejected_candidate_codes"])
        best = evaluated["best"]

        self.assertIsNotNone(best)
        self.assertEqual(best.stock.code_1c, "00-00024507")
        self.assertEqual((best.feature_scores or {}).get("size_pair_hit"), 1.0)
        self.assertFalse(best.stock.code_1c in rejected_codes)

    def test_realistic_hard_negative_cases_stay_not_found(self) -> None:
        for query in RAW_HARD_NEGATIVE_CASES:
            for with_reviewed in (False, True):
                with self.subTest(query=query, with_reviewed=with_reviewed):
                    evaluated = self.evaluate_case(query, with_reviewed=with_reviewed)
                    self.assertEqual(evaluated["kind"], "not_found")
                    self.assertEqual(evaluated["ranked"], [])

                    result = self.match_case(query, with_reviewed=with_reviewed, use_full_scan_fallback=True)
                    self.assertEqual(result.status, STATUS_NOT_FOUND)

    def test_decision_boundary_regression_keeps_exact_safe_approval_and_not_found(self) -> None:
        cases = (
            ("Переход К-1-273х7-219х6-В10 ГОСТ 17378-2001", False, STATUS_FOUND_FULL),
            ("Отвод 45-2-57x4-20 ГОСТ 17375-2001", False, STATUS_APPROVAL_ANALOG),
            ("Отвод 45-2-76х3,5 ГОСТ17375-2001", True, STATUS_SAFE_ANALOG),
            ("Отвод 90-2-273х14-20 ГОСТ 17375-2001", True, STATUS_NOT_FOUND),
        )
        for query, with_reviewed, expected_status in cases:
            with self.subTest(query=query, with_reviewed=with_reviewed):
                result = self.match_case(query, with_reviewed=with_reviewed, use_full_scan_fallback=True)
                self.assertEqual(result.status, expected_status)

    def test_exhaustive_fallback_regression_does_not_worsen_real_exact_case(self) -> None:
        query = "Переход К-1-273х7-219х6-В10 ГОСТ 17378-2001"
        without_fallback = self.match_case(query, with_reviewed=False, use_full_scan_fallback=False)
        with_fallback = self.match_case(query, with_reviewed=False, use_full_scan_fallback=True)

        self.assertEqual(without_fallback.status, STATUS_FOUND_FULL)
        self.assertEqual(with_fallback.status, STATUS_FOUND_FULL)
        self.assertIsNotNone(without_fallback.matched_stock)
        self.assertIsNotNone(with_fallback.matched_stock)
        self.assertEqual(without_fallback.matched_stock.code_1c, "П0000002542")
        self.assertEqual(with_fallback.matched_stock.code_1c, "П0000002542")


class FullStockRankingRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.classifier = load_default_classifier()
        cls.substitution_policy = load_substitution_policy()
        cls.reviewed_decisions = load_reviewed_analog_decisions(REVIEWED_DECISIONS_PATH)
        golden_payload = json.loads(GOLDEN_SET_PATH.read_text(encoding="utf-8"))
        cls.golden_cases = {case["query"]: case for case in golden_payload["cases"]}
        stock_items: list[StockItem] = []
        for stock_path, label in FULL_STOCK_FILES:
            stock_items.extend(load_stock(stock_path, source_label=label))
        cls.raw_matcher = StockMatcher(stock_items, substitution_policy=cls.substitution_policy)
        cls.reviewed_matcher = StockMatcher(
            stock_items,
            reviewed_analog_decisions=cls.reviewed_decisions,
            substitution_policy=cls.substitution_policy,
        )

    def make_full_stock_order(self, query: str, *, row_index: int = 1) -> OrderLine:
        classification = self.classifier.classify(query)
        search_text = build_search_text(query)
        classifier_family_tags = set(classification.family_tags or ())
        classifier_hint = classification.category_key if classification and classification.category_key else ""
        dimension_tags = (
            extract_dimension_tags(query, "", "", "")
            | extract_family_tags(query, "", "", "", classifier_hint)
            | extract_material_tags_from_search_text(search_text)
            | extract_structured_tags_from_search_text(search_text)
            | classifier_family_tags
        )
        search_text = augment_search_text_with_dimension_tags(search_text, dimension_tags)
        tokens = extract_tokens(search_text)
        return OrderLine(
            source_file="full_stock_regression.xlsx",
            sheet_name="golden",
            source_row=row_index,
            headers=[],
            row_values=[],
            position=str(row_index),
            name=query,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=tokens,
            key_tokens=extract_key_tokens(tokens),
            root_tokens=extract_root_tokens(tokens),
            code_tokens=extract_code_tokens(query),
            dimension_tags=dimension_tags,
            raw_query=query,
            classification=classification,
        )

    def evaluate_full_stock_case(self, query: str, *, with_reviewed: bool, row_index: int = 1) -> dict[str, object]:
        base_matcher = self.reviewed_matcher if with_reviewed else self.raw_matcher
        order = self.make_full_stock_order(query, row_index=row_index)
        matcher = base_matcher.fork()
        candidate_pool = matcher.generate_candidate_pool(order)
        ranked = matcher.rank_candidates(order, candidate_pool=candidate_pool, limit=5)
        kind, best, _ = matcher.classify(order, ranked)
        result = match_orders(
            [self.make_full_stock_order(query, row_index=row_index)],
            base_matcher.fork(),
            use_full_scan_fallback=True,
        )[0]
        return {
            "case": self.golden_cases[query],
            "order": order,
            "matcher": matcher,
            "candidate_pool": candidate_pool,
            "ranked": ranked,
            "kind": kind,
            "best": best,
            "result": result,
        }

    def test_full_stock_false_exact_cases_drop_pseudo_code_exactness(self) -> None:
        cases = (
            "Отвод 90-2-273х14-20 ГОСТ 17375-2001",
            "Отвод 90-2-89х7-20 ГОСТ 17375-2001",
        )
        for index, query in enumerate(cases, start=1):
            with self.subTest(query=query):
                evaluated = self.evaluate_full_stock_case(query, with_reviewed=False, row_index=index)
                self.assertEqual(evaluated["kind"], "analog")
                self.assertIsNotNone(evaluated["best"])
                self.assertFalse(evaluated["best"].code_hit)
                self.assertEqual(evaluated["result"].status, STATUS_APPROVAL_ANALOG)

    def test_full_stock_ranking_low_cases_keep_approved_candidates_reachable(self) -> None:
        cases = (
            "Отвод 45-2-57x4-20 ГОСТ 17375-2001",
            "Отвод 90-2-57x5--20 ГОСТ 17375-2001",
        )
        for index, query in enumerate(cases, start=10):
            with self.subTest(query=query):
                evaluated = self.evaluate_full_stock_case(query, with_reviewed=False, row_index=index)
                approved_codes = set(evaluated["case"]["approved_candidate_codes"])
                candidate_pool_codes = {
                    evaluated["matcher"].stock_items[row_index].code_1c
                    for row_index in evaluated["candidate_pool"]
                }
                self.assertEqual(evaluated["kind"], "analog")
                self.assertEqual(evaluated["result"].status, STATUS_APPROVAL_ANALOG)
                self.assertTrue(approved_codes & candidate_pool_codes)
                self.assertIsNotNone(evaluated["best"])
                self.assertFalse(evaluated["best"].code_hit)

    def test_full_stock_reviewed_overlay_keeps_approved_top_candidates(self) -> None:
        cases = (
            ("Отвод 45-2-57x4-20 ГОСТ 17375-2001", STATUS_SAFE_ANALOG),
            ("Отвод 90-2-57x5--20 ГОСТ 17375-2001", STATUS_SAFE_ANALOG),
            ("Отвод 45-2-76х3,5 ГОСТ17375-2001", STATUS_SAFE_ANALOG),
        )
        for index, (query, expected_status) in enumerate(cases, start=20):
            with self.subTest(query=query):
                evaluated = self.evaluate_full_stock_case(query, with_reviewed=True, row_index=index)
                approved_codes = set(evaluated["case"]["approved_candidate_codes"])
                self.assertIsNotNone(evaluated["best"])
                self.assertEqual(evaluated["result"].status, expected_status)
                self.assertIn(evaluated["best"].stock.code_1c, approved_codes)
                self.assertEqual(evaluated["best"].review_decision, REVIEW_DECISION_APPROVED)

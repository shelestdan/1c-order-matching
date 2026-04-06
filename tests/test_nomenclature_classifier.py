from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from nomenclature_classifier import ClassificationStatus, HybridNomenclatureClassifier, load_default_classifier
from nomenclature_classifier.synonym_registry import SynonymRegistry
from normalize_client_requests import parse_freeform_line
from process_1c_orders import (
    Candidate,
    OrderLine,
    STATUS_APPROVAL_ANALOG,
    STATUS_SAFE_ANALOG,
    StockItem,
    StockMatcher,
    augment_search_text_with_dimension_tags,
    build_search_text,
    build_review_query_keys,
    determine_analog_status,
    extract_dimension_tags,
    extract_family_tags,
    extract_parser_hint_tags,
    extract_key_tokens,
    extract_material_tags_from_search_text,
    extract_root_tokens,
    extract_tokens,
    find_manual_review_rule,
    format_candidate_text,
    is_exact_pipe_candidate,
    load_manual_selection_memory,
    load_stock,
    load_reviewed_analog_decisions,
    load_substitution_policy,
    match_orders,
)
from nomenclature_classifier.classifier import DEFAULT_CONFIG_PATH


class FakeSemanticReranker:
    candidate_limit = 3

    def rerank(self, query, candidates):
        if not candidates:
            return candidates
        updated = list(candidates)
        best = updated[0]
        updated[0] = replace(
            best,
            total_score=min(100.0, best.total_score + 5.0),
            semantic_score=72.0,
            semantic_boost=5.0,
            explanation=best.explanation + ("semantic similarity: 0.720 via fake-model", "semantic boost: +5.0"),
        )
        return tuple(updated)


class HybridNomenclatureClassifierTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        load_default_classifier.cache_clear()
        cls.classifier = load_default_classifier()

    def assert_classification(
        self,
        raw_text: str,
        *,
        status: ClassificationStatus,
        category: str | None,
        route: str,
    ):
        result = self.classifier.classify(raw_text)
        self.assertEqual(result.status, status, raw_text)
        self.assertEqual(result.category_key, category, raw_text)
        self.assertEqual(result.route, route, raw_text)
        self.assertTrue(result.explanation, raw_text)
        return result

    def test_exact_and_abbreviation_cases(self) -> None:
        self.assert_classification("фл dn100", status=ClassificationStatus.CLASSIFIED, category="flange", route="stock_match")
        self.assert_classification("кран шар 1/2", status=ClassificationStatus.CLASSIFIED, category="ball_valve", route="stock_match")
        self.assert_classification("задвижка ду100", status=ClassificationStatus.CLASSIFIED, category="gate_valve", route="stock_match")
        self.assert_classification("фл сталь dn100", status=ClassificationStatus.CLASSIFIED, category="flange", route="stock_match")

    def test_noisy_and_shortened_inputs(self) -> None:
        self.assert_classification("затв дисковый", status=ClassificationStatus.NEEDS_REVIEW, category="butterfly_valve", route="stock_match")
        self.assert_classification("затв. диск.", status=ClassificationStatus.NEEDS_REVIEW, category="butterfly_valve", route="stock_match")
        self.assert_classification("муфта оцинк", status=ClassificationStatus.NEEDS_REVIEW, category="coupling", route="stock_match")
        self.assert_classification("пож рукав 20м", status=ClassificationStatus.NEEDS_REVIEW, category="fire_hose", route="manual_review")
        self.assert_classification("клапан обратный dn65", status=ClassificationStatus.CLASSIFIED, category="check_valve", route="stock_match")
        self.assert_classification("труба ppr dn110", status=ClassificationStatus.CLASSIFIED, category="pipe", route="stock_match")
        self.assert_classification("угол 90 ppr d32", status=ClassificationStatus.CLASSIFIED, category="elbow", route="stock_match")
        self.assert_classification("воздухоотводчик dn15", status=ClassificationStatus.CLASSIFIED, category="air_vent", route="stock_match")

    def test_manual_review_routes(self) -> None:
        self.assert_classification("унитаз", status=ClassificationStatus.NEEDS_REVIEW, category="toilet", route="manual_review")
        self.assert_classification("колено грувлок", status=ClassificationStatus.NEEDS_REVIEW, category="grooved_fitting", route="manual_review")
        self.assert_classification("отвод грувлок", status=ClassificationStatus.NEEDS_REVIEW, category="grooved_fitting", route="manual_review")
        self.assert_classification("рукоять", status=ClassificationStatus.NEEDS_REVIEW, category="handle", route="manual_review")
        self.assert_classification("ручка затвора", status=ClassificationStatus.NEEDS_REVIEW, category="handle", route="manual_review")

    def test_fallback_statuses(self) -> None:
        self.assert_classification("манжета", status=ClassificationStatus.UNCLASSIFIED, category=None, route="stock_match")
        self.assert_classification("непонятная штука", status=ClassificationStatus.UNCLASSIFIED, category=None, route="stock_match")
        self.assert_classification("abc 123", status=ClassificationStatus.UNCLASSIFIED, category=None, route="stock_match")

    def test_material_extraction_for_pe_and_pnd(self) -> None:
        pe_result = self.assert_classification(
            "Тройник ПЭ",
            status=ClassificationStatus.NEEDS_REVIEW,
            category="tee",
            route="stock_match",
        )
        pnd_result = self.assert_classification(
            "Тройник ПНД",
            status=ClassificationStatus.NEEDS_REVIEW,
            category="tee",
            route="stock_match",
        )
        self.assertIn("polyethylene", pe_result.normalized.attributes["material"])
        self.assertIn("polyethylene", pnd_result.normalized.attributes["material"])

    def test_typo_and_semantic_hook(self) -> None:
        typo_result = self.classifier.classify("затвр дисквый dn100")
        self.assertNotEqual(typo_result.status, ClassificationStatus.UNCLASSIFIED)
        self.assertEqual(typo_result.category_key, "butterfly_valve")

        registry = SynonymRegistry.from_path(DEFAULT_CONFIG_PATH)
        classifier = HybridNomenclatureClassifier(registry, semantic_reranker=FakeSemanticReranker())
        semantic_result = classifier.classify("кран шар 1/2")
        self.assertEqual(semantic_result.category_key, "ball_valve")
        self.assertTrue(any("semantic score" in item for item in semantic_result.explanation))

    def test_manual_review_rule_integration(self) -> None:
        classification = self.classifier.classify("Унитаз с косым выпуском")
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name="Унитаз с косым выпуском",
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text="unitaz s kosym vypuskom",
            search_tokens={"unitaz", "kosym", "vypuskom"},
            key_tokens={"unitaz", "kosym", "vypuskom"},
            root_tokens={"unita", "kosym", "vypus"},
            code_tokens=set(),
            dimension_tags={"family:toilet"},
            raw_query="Унитаз с косым выпуском",
            classification=classification,
        )
        manual_review = find_manual_review_rule(order)
        self.assertIsNotNone(manual_review)
        self.assertIn("унитаз", manual_review["comment"].lower())

    def test_vgp_pipe_size_bridge(self) -> None:
        order_tags = extract_dimension_tags("Труба стальная водогазопроводная Ø21.3х2.8 ГОСТ 3262-75")
        stock_tags = extract_dimension_tags("Труба ВГП 15-2,8 ГОСТ 3262-75 L=6м")
        self.assertIn("dn:15", order_tags)
        self.assertIn("od:21.3", stock_tags)
        self.assertIn("dn:40", extract_dimension_tags("Труба стальная водогазопроводная Ø48.0х3.5 ГОСТ 3262-75"))
        self.assertIn("dn:50", extract_dimension_tags("Труба стальная водогазопроводная Ø60х3.5 ГОСТ 3262-75"))

    def test_electrowelded_pipe_is_inferred_as_steel(self) -> None:
        stock_search_text = build_search_text("Труба электросварная 57-3,5 ГОСТ 10704-91/ГОСТ 10705-80 L=12м")
        stock_materials = extract_material_tags_from_search_text(stock_search_text)
        self.assertIn("mclass:steel", stock_materials)

    def test_explicit_09g2s_is_not_coerced_to_cs20(self) -> None:
        stock_search_text = build_search_text("Фланец воротниковый 80-16-11-2-B-Ст.09Г2С-IV (8 отверстий) ГОСТ 33259")
        stock_materials = extract_material_tags_from_search_text(stock_search_text)
        self.assertIn("grade:cs09g2s", stock_materials)
        self.assertNotIn("grade:cs20", stock_materials)

    def test_tripledn_tags_are_order_invariant_for_tees(self) -> None:
        order_tags = extract_dimension_tags("Тройник переходной полипропиленовый PPR, D25х20х25")
        stock_tags = extract_dimension_tags("Тройник переходной 25 х 25 х 20 мм Pilsa")
        self.assertIn("tripledn:20x25x25", order_tags)
        self.assertIn("tripledn:20x25x25", stock_tags)

    def test_sewer_pair_dimensions_are_extracted_for_ht_and_kg_fittings(self) -> None:
        order_tags = extract_dimension_tags("Переход эксцентрический, 110х50")
        stock_tags = extract_dimension_tags("Переход PP-H эксц сер б/н Дн110х50 в/к HTR Ostendorf 115720")
        tee_tags = extract_dimension_tags("Тройник 87°, 110х50")
        equal_tee_tags = extract_dimension_tags("Тройник 45°, 110х110")
        equal_stock_tee_tags = extract_dimension_tags("Тройник 87,5° 110-110 СЕРАЯ вн.канализ.(24) VALFEX арт. 24110110")
        hint_tags = extract_parser_hint_tags("Переход PP-H эксц сер б/н Дн110х50")

        self.assertIn("dn:110", order_tags)
        self.assertIn("dn:50", order_tags)
        self.assertIn("pairdn:50x110", order_tags)
        self.assertIn("dn:110", stock_tags)
        self.assertIn("dn:50", stock_tags)
        self.assertIn("pairdn:50x110", stock_tags)
        self.assertIn("dn:110", tee_tags)
        self.assertIn("dn:50", tee_tags)
        self.assertIn("pairdn:50x110", tee_tags)
        self.assertIn("dn:110", equal_tee_tags)
        self.assertIn("pairdn:110x110", equal_tee_tags)
        self.assertIn("deg:87", equal_stock_tee_tags)
        self.assertIn("pairdn:110x110", equal_stock_tee_tags)
        self.assertIn("shape:eccentric", hint_tags)

    def test_revision_and_compact_dimensions_are_extracted_for_common_client_formats(self) -> None:
        revision_order_family = extract_family_tags("Ревизия, ∅110")
        revision_order_tags = extract_dimension_tags("Ревизия, ∅110")
        revision_stock_tags = extract_dimension_tags("Ревизия PP-H сер б/н Дн110 круг крышка в/к HTRE Ostendorf 115600")
        revision_stock_family = extract_family_tags("Ревизия PP-H сер б/н Дн110 круг крышка в/к HTRE Ostendorf 115600")
        pipe_tags = extract_dimension_tags("Труба НПВХ для наружной канализации SN4 ⌀160, l=1м")
        elbow_tags = extract_dimension_tags("Угол 90°, 20")
        elbow_stock_tags = extract_dimension_tags("Угол 45 20 мм Pilsa")
        elbow_stock_compact_tags = extract_dimension_tags("Угольник PP-R бел Дн20х90гр VALFEX 10108020")
        offset_elbow_hint_tags = extract_parser_hint_tags("Обводное колено 20 мм")
        threaded_mufta_tags = extract_dimension_tags("Муфта комбинированная НР, 32хR1''")

        self.assertIn("family:revision", revision_order_family)
        self.assertIn("dn:110", revision_order_tags)
        self.assertIn("family:revision", revision_stock_family)
        self.assertIn("dn:110", revision_stock_tags)
        self.assertIn("dn:160", pipe_tags)
        self.assertIn("deg:90", elbow_tags)
        self.assertIn("dn:20", elbow_tags)
        self.assertIn("od:20", elbow_tags)
        self.assertIn("deg:45", elbow_stock_tags)
        self.assertIn("deg:90", elbow_stock_compact_tags)
        self.assertIn("subtype:offset", offset_elbow_hint_tags)
        self.assertIn("dn:32", threaded_mufta_tags)
        self.assertIn("inch:1", threaded_mufta_tags)

    def test_threaded_fraction_does_not_create_fake_wall_or_integer_inch(self) -> None:
        tags = extract_dimension_tags('Угольник переходной с внутренней резьбой 20х3/4" Valfex')
        self.assertIn("dn:20", tags)
        self.assertIn("inch:3/4", tags)
        self.assertNotIn("inch:4", tags)
        self.assertNotIn("wall:3", tags)

    def test_russian_flange_type_is_extracted(self) -> None:
        tags = extract_dimension_tags("Фланец стальной приварной встык тип 11 DN80 PN16")
        self.assertIn("type:11", tags)

    def test_du_size_does_not_force_outer_diameter_and_dn_dash_is_parsed(self) -> None:
        order_tags = extract_dimension_tags("Колено чугунное напорное фланцевое Ду100")
        stock_tags = extract_dimension_tags("Фильтр магнитный фланцевый ФМФ DN- 50 PN-1,6")
        self.assertIn("dn:100", order_tags)
        self.assertNotIn("od:100", order_tags)
        self.assertIn("dn:50", stock_tags)

    def test_parser_hints_extract_family_specific_tags(self) -> None:
        patrubok_tags = extract_parser_hint_tags("Патрубок до счетчика фланцевый, Ду65")
        filter_tags = extract_parser_hint_tags("Фильтр сетчатый чугунный фланцевый с магнитной вставкой DN50 PN16")
        manometer_tags = extract_parser_hint_tags("Манометр с поверкой 0..1,6 МПа, 100мм, G1/2(снизу), кл.1,5")

        self.assertIn("family:patrubok", patrubok_tags)
        self.assertIn("subfamily:pds", patrubok_tags)
        self.assertIn("family:filter", filter_tags)
        self.assertIn("has_magnet:true", filter_tags)
        self.assertIn("filter_element:mesh", filter_tags)
        self.assertIn("family:manometer", manometer_tags)
        self.assertIn("pressure_range_mpa:0-1.6", manometer_tags)
        self.assertIn("connection_position:bottom", manometer_tags)
        self.assertIn("accuracy_class:1.5", manometer_tags)

    def test_american_union_query_maps_to_mufta_without_false_manometer_tags(self) -> None:
        tags = extract_parser_hint_tags('Разъемное соединение "американка" 1 1/2" внутренняя-наружная резьба')
        self.assertIn("family:mufta", tags)
        self.assertIn("extra_connection:union", tags)
        self.assertIn("conn_gender:fm", tags)
        self.assertNotIn("family:manometer", tags)
        self.assertNotIn("connection_guess:1/2", tags)

    def test_american_union_stock_keeps_real_inch_and_ignores_pack_ratio(self) -> None:
        tags = extract_dimension_tags("Американка FM 1 1/2'', AQUALINK 20/5")
        hint_tags = extract_parser_hint_tags("Американка FM 1 1/2'', AQUALINK 20/5")
        self.assertIn("inch:11/2", tags)
        self.assertNotIn("inch:20/5", tags)
        self.assertIn("family:mufta", hint_tags)
        self.assertIn("conn_gender:fm", hint_tags)

    def test_integer_inch_size_is_extracted_for_union_fittings(self) -> None:
        tags = extract_dimension_tags('Разъемное соединение "американка" 2" внутренняя-наружная резьба')
        self.assertIn("inch:2", tags)

    def test_rezba_family_tag_does_not_block_american_union_candidate(self) -> None:
        order_name = 'Разъемное соединение "американка" 1 1/2" внутренняя-наружная резьба'
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        stock_name = "Американка FM 1 1/2'', AQUALINK 20/5"
        stock_search_text = build_search_text(stock_name)
        stock_dimension_tags = (
            extract_dimension_tags(stock_name)
            | extract_family_tags(stock_name)
            | extract_parser_hint_tags(stock_name)
            | extract_material_tags_from_search_text(stock_search_text)
        )
        stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
        stock_tokens = extract_tokens(stock_search_text)
        stock = StockItem(
            row_index=1,
            code_1c="02591",
            name=stock_name,
            print_name=stock_name,
            product_type="",
            sale_price="",
            stop_price="",
            plan_price="",
            quantity=10.0,
            remaining=10.0,
            search_text=stock_search_text,
            search_tokens=stock_tokens,
            key_tokens=extract_key_tokens(stock_tokens),
            root_tokens=extract_root_tokens(stock_tokens),
            code_tokens=set(),
            dimension_tags=stock_dimension_tags,
        )

        matcher = StockMatcher([stock])
        self.assertTrue(matcher.is_candidate_compatible(order, stock))

    def test_gearbox_is_operator_for_zatvor_and_family_for_standalone_item(self) -> None:
        zatvor_tags = extract_parser_hint_tags("Затвор Ду500 Ру10/16 межфл., корпус-чугун, диск-чугун, редуктор")
        gearbox_tags = extract_parser_hint_tags("Редуктор F07 14х14 д/затв Ду125-150 четвертьоборотный Benarmo EURO")

        self.assertIn("operator:gearbox", zatvor_tags)
        self.assertNotIn("family:gearbox", zatvor_tags)
        self.assertIn("body_material:cast_iron", zatvor_tags)
        self.assertIn("disc_material:cast_iron", zatvor_tags)

        self.assertIn("family:gearbox", gearbox_tags)
        self.assertIn("for_family:zatvor", gearbox_tags)
        self.assertIn("mounting_pad:f07", gearbox_tags)
        self.assertIn("stem_square:14x14", gearbox_tags)
        self.assertIn("compatible_dn:125-150", gearbox_tags)
        self.assertIn("turn_type:quarter_turn", gearbox_tags)
        self.assertIn("brand:benarmo_euro", gearbox_tags)

    def test_parser_hints_prefer_matching_patrubok_subfamily(self) -> None:
        order_name = "Патрубок до счетчика фланцевый, Ду65"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_parser_hint_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        matching = make_stock(1, "PDS", "Патрубок ПДС Ду65")
        wrong = make_stock(2, "PPS", "Патрубок ППС Ду65")
        matcher = StockMatcher([matching, wrong])

        matching_candidate = matcher.score_candidate(order, matching)
        wrong_candidate = matcher.score_candidate(order, wrong)

        self.assertGreater(matching_candidate.score, wrong_candidate.score)
        self.assertIn("subfamily", matching_candidate.matched_dimension_keys)

    def test_standalone_gearbox_is_incompatible_with_zatvor_with_gearbox(self) -> None:
        order_name = "Редуктор F07 14х14 д/затв Ду125-150 четвертьоборотный Benarmo EURO"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_parser_hint_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        gearbox = make_stock(1, "GEAR", "Редуктор F07 14х14 д/затв Ду125-150 четвертьоборотный Benarmo EURO")
        zatvor = make_stock(2, "VALVE", "Затвор Ду150 Ру16 межфл., корпус-чугун, диск-чугун, редуктор")
        matcher = StockMatcher([gearbox, zatvor], substitution_policy=load_substitution_policy())

        self.assertTrue(matcher.is_candidate_compatible(order, gearbox))
        self.assertFalse(matcher.is_candidate_compatible(order, zatvor))

    def test_load_stock_supports_workbook_format(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = Path(temp_dir) / "stock.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "остатки"
            ws.append(
                [
                    "Код1с",
                    "Номенклатура",
                    "НаименованиДляПечати",
                    "ТипПродукта",
                    "ПродажнаяЦена",
                    "СтопЦена",
                    "ПлановаяЦена",
                    "Остаток",
                ]
            )
            ws.append(
                [
                    "GBX-01",
                    "Редуктор F07 14х14 д/затв Ду125-150 четвертьоборотный Benarmo EURO",
                    "Редуктор д/затвора Ду125-150 Benarmo EURO",
                    "",
                    "",
                    "",
                    "",
                    4,
                ]
            )
            wb.save(workbook_path)

            stock_items = load_stock(workbook_path)
            self.assertEqual(len(stock_items), 1)
            self.assertEqual(stock_items[0].code_1c, "GBX-01")
            self.assertIn("family:gearbox", stock_items[0].dimension_tags)
            self.assertIn("mounting_pad:f07", stock_items[0].dimension_tags)
            self.assertIn("stem_square:14x14", stock_items[0].dimension_tags)
            self.assertIn("compatible_dn:125-150", stock_items[0].dimension_tags)

    def test_freeform_parser_skips_service_rows_and_strips_tail_noise(self) -> None:
        parsed, issue = parse_freeform_line("Раздел: ВК", "demo.xlsx", "Sheet1:1")
        self.assertIsNone(parsed)
        self.assertIsNone(issue)

        parsed, issue = parse_freeform_line("Патрубок до счетчика фланцевый, Ду65 Россия", "demo.xlsx", "Sheet1:2")
        self.assertIsNotNone(parsed)
        self.assertIsNone(issue)
        self.assertEqual(parsed.name, "Патрубок до счетчика фланцевый, Ду65")

    def test_generic_type11_flange_prefers_exec2_st20_with_8_holes(self) -> None:
        order_name = "Фланец стальной приварной встык тип 11 DN80 PN16"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        base_exec1 = make_stock(1, "EXEC1", "Фланец воротниковый 80-16-11-1-B-Ст.20 ГОСТ 33259")
        preferred_exec2 = make_stock(2, "EXEC2", "Фланец воротниковый 80-16-11-2-B-Ст.20-IV (8 отверстий) ГОСТ 33259")
        matcher = StockMatcher([base_exec1, preferred_exec2])

        score_exec1 = matcher.score_candidate(order, base_exec1)
        score_exec2 = matcher.score_candidate(order, preferred_exec2)

        self.assertGreaterEqual(score_exec2.score, score_exec1.score)
        self.assertIn("предпочтительное исполнение", " ".join(score_exec2.reasons))
        self.assertIn("предпочтительный ряд 11-2", " ".join(score_exec2.reasons))

    def test_pipe_with_matching_size_and_gost_is_exact(self) -> None:
        order_name = "Труба стальная электросварная Ø57х3.5 ГОСТ 10704-91"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="м",
            requested_qty=10.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=100.0,
                remaining=100.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        exact_stock = make_stock(1, "PIPE", "Труба электросварная 57-3,5 ГОСТ 10704-91/ГОСТ 10705-80 L=12м")
        galvanized_stock = make_stock(2, "PIPE-ZN", "Труба электросварная 57-3,5 ГОСТ 10704-91/ГОСТ 10705-80 оцинкованная")
        matcher = StockMatcher([exact_stock, galvanized_stock])

        exact_candidate = matcher.score_candidate(order, exact_stock)
        galvanized_candidate = matcher.score_candidate(order, galvanized_stock)

        self.assertTrue(is_exact_pipe_candidate(order, exact_candidate))
        self.assertFalse(is_exact_pipe_candidate(order, galvanized_candidate))
        kind, best, reason = matcher.classify(order, [exact_candidate, galvanized_candidate])
        self.assertEqual(kind, "exact")
        self.assertEqual(best.stock.code_1c, "PIPE")
        self.assertEqual(reason, "совпали тип трубы, размер и ГОСТ")

    def test_tripledn_match_beats_wrong_tee_size(self) -> None:
        order_name = "Тройник переходной полипропиленовый PPR, D32х20х32"
        search_text = build_search_text(order_name)
        search_tokens = extract_tokens(search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=search_tokens,
            key_tokens=extract_key_tokens(search_tokens),
            root_tokens=extract_root_tokens(search_tokens),
            code_tokens=set(),
            dimension_tags=(
                extract_dimension_tags(order_name)
                | extract_family_tags(order_name)
                | extract_material_tags_from_search_text(search_text)
            ),
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=(
                    extract_dimension_tags(name)
                    | extract_family_tags(name)
                    | extract_material_tags_from_search_text(stock_search_text)
                ),
            )

        exact_stock = make_stock(1, "EXACT", "Тройник переходной 32 х 20 х 32 мм Pilsa")
        wrong_stock = make_stock(2, "WRONG", "Тройник переходной 25 х 25 х 20 мм Pilsa")
        matcher = StockMatcher([exact_stock, wrong_stock])
        exact_candidate = matcher.score_candidate(order, exact_stock)
        wrong_candidate = matcher.score_candidate(order, wrong_stock)

        self.assertIn("tripledn", exact_candidate.matched_dimension_keys)
        self.assertGreater(exact_candidate.score, wrong_candidate.score)
        self.assertTrue(matcher._has_required_dimension_matches(order, exact_stock))
        self.assertFalse(matcher._has_required_dimension_matches(order, wrong_stock))

    def test_dimension_conflict_candidates_are_filtered_from_analogs(self) -> None:
        matcher = StockMatcher([])
        candidate = Candidate(
            stock=StockItem(
                row_index=1,
                code_1c="P0000003365",
                name="Клапан (Вентиль) запорный 15с65нж Ду.150 Ру.16",
                print_name="",
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=12.0,
                remaining=12.0,
                search_text="",
                search_tokens=set(),
                key_tokens=set(),
                root_tokens=set(),
                code_tokens=set(),
                dimension_tags={"dn:150", "pn:16"},
            ),
            score=53.5,
            overlap=1.0,
            soft_overlap=1.0,
            dimension_penalty=22.0,
        )
        self.assertEqual(matcher.filter_analog_candidates([candidate]), [])

    def test_reviewed_rejected_candidate_is_removed_from_ranked_candidates(self) -> None:
        order_name = "Отвод 90-2-273х14-20 ГОСТ 17375-2001"
        search_text = build_search_text(order_name)
        search_tokens = extract_tokens(search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=search_tokens,
            key_tokens=extract_key_tokens(search_tokens),
            root_tokens=extract_root_tokens(search_tokens),
            code_tokens=set(),
            dimension_tags=(
                extract_dimension_tags(order_name)
                | extract_family_tags(order_name)
                | extract_material_tags_from_search_text(search_text)
            ),
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=(
                    extract_dimension_tags(name)
                    | extract_family_tags(name)
                    | extract_material_tags_from_search_text(stock_search_text)
                ),
            )

        rejected = make_stock(1, "П0000002184", "Отвод 90-2-273х7 Ст.20 ГОСТ 17375")
        fallback = make_stock(2, "SAFE", "Отвод 90-2-273х10 Ст.20 ГОСТ 17375")
        decisions = {build_review_query_keys(order_name)[0]: {"П0000002184": "rejected"}}
        matcher = StockMatcher([rejected, fallback], reviewed_analog_decisions=decisions)

        candidates = matcher.find_candidates(order, limit=5)
        self.assertEqual([candidate.stock.code_1c for candidate in candidates], ["SAFE"])

    def test_reviewed_approved_candidate_is_preserved_in_analog_filter(self) -> None:
        order_name = "Отвод 45-2-57x4-20 ГОСТ 17375-2001"
        search_text = build_search_text(order_name)
        search_tokens = extract_tokens(search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=search_tokens,
            key_tokens=extract_key_tokens(search_tokens),
            root_tokens=extract_root_tokens(search_tokens),
            code_tokens=set(),
            dimension_tags=(
                extract_dimension_tags(order_name)
                | extract_family_tags(order_name)
                | extract_material_tags_from_search_text(search_text)
            ),
            raw_query=order_name,
            classification=None,
        )
        stock = StockItem(
            row_index=1,
            code_1c="00-00024491",
            name="Отвод 90-2-57х4.0 Ст.20 ГОСТ 17375",
            print_name="",
            product_type="",
            sale_price="",
            stop_price="",
            plan_price="",
            quantity=5.0,
            remaining=5.0,
            search_text="",
            search_tokens=set(),
            key_tokens=set(),
            root_tokens=set(),
            code_tokens=set(),
            dimension_tags={"family:elbow", "od:57", "wall:4", "deg:90"},
        )
        candidate = Candidate(
            stock=stock,
            score=41.0,
            overlap=0.05,
            soft_overlap=0.10,
            dimension_penalty=22.0,
        )
        decisions = {build_review_query_keys(order_name)[0]: {"00-00024491": "approved"}}
        matcher = StockMatcher([], reviewed_analog_decisions=decisions)

        approved = matcher._apply_reviewed_candidate_decisions(order, [candidate])
        filtered = matcher.filter_analog_candidates(approved)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].review_decision, "approved")
        self.assertIn("подтверждено", format_candidate_text(filtered[0]))

    def test_load_reviewed_analog_decisions_prefers_rejected(self) -> None:
        payload = {
            "decisions": [
                {"query_key": "demo query", "candidate_code": "p0001", "decision": "approved"},
                {"query_key": "demo query", "candidate_code": "P0001", "decision": "rejected"},
            ]
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "reviewed.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            loaded = load_reviewed_analog_decisions(path)
        self.assertEqual(loaded, {"demo query": {"P0001": "rejected"}})

    def test_load_manual_selection_memory_restores_query_features(self) -> None:
        payload = {
            "entries": [
                {
                    "record_id": "demo:1",
                    "query": "Унитаз с косым выпуском",
                    "candidate_code": "unit-1",
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "manual_memory.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            loaded = load_manual_selection_memory(path)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].candidate_code, "UNIT-1")
        self.assertTrue(loaded[0].query_key)
        self.assertIn("unitaz", loaded[0].order_key_tokens)
        self.assertIn("family:toilet", loaded[0].order_dimension_tags)

    def test_manual_selection_memory_promotes_previous_not_found_choice(self) -> None:
        order_name = "Унитаз с косым выпуском"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = {"family:toilet", "toilet_variant:kosym"}
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=extract_tokens(order_search_text),
            key_tokens=extract_key_tokens(extract_tokens(order_search_text)),
            root_tokens=extract_root_tokens(extract_tokens(order_search_text)),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )
        stock_name = "Унитаз-компакт Santeri Версия косой выпуск"
        stock_search_text = build_search_text(stock_name)
        stock_tokens = extract_tokens(stock_search_text)
        stock = StockItem(
            row_index=1,
            code_1c="UNIT-1",
            name=stock_name,
            print_name=stock_name,
            product_type="",
            sale_price="",
            stop_price="",
            plan_price="",
            quantity=3.0,
            remaining=3.0,
            search_text=stock_search_text,
            search_tokens=stock_tokens,
            key_tokens=extract_key_tokens(stock_tokens),
            root_tokens=extract_root_tokens(stock_tokens),
            code_tokens=set(),
            dimension_tags=set(),
        )

        plain_matcher = StockMatcher([stock])
        self.assertEqual(plain_matcher.find_candidates(order, limit=5), [])

        memory_payload = {
            "entries": [
                {
                    "record_id": "job:row",
                    "query": order_name,
                    "query_key": order_search_text,
                    "order_key_tokens": sorted(order.key_tokens),
                    "order_root_tokens": sorted(order.root_tokens),
                    "order_dimension_tags": sorted(order.dimension_tags),
                    "manual_search_query": "Santeri косой выпуск",
                    "candidate_code": "UNIT-1",
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "manual_memory.json"
            path.write_text(json.dumps(memory_payload, ensure_ascii=False), encoding="utf-8")
            learned_memory = load_manual_selection_memory(path)

        learned_matcher = StockMatcher([stock], manual_selection_memory=learned_memory)
        learned_candidates = learned_matcher.find_candidates(order, limit=5)

        self.assertEqual(len(learned_candidates), 1)
        self.assertEqual(learned_candidates[0].stock.code_1c, "UNIT-1")
        self.assertGreater(learned_candidates[0].manual_learning_boost, 0.0)
        self.assertTrue(learned_candidates[0].manual_learning_allowed)
        self.assertTrue(any("ручн" in reason.lower() for reason in learned_candidates[0].reasons))

        result = match_orders([order], learned_matcher, use_full_scan_fallback=False)[0]
        self.assertNotEqual(result.status, "Не найдено")
        self.assertTrue(result.analogs)
        self.assertGreater(result.analogs[0].manual_learning_boost, 0.0)

    def test_determine_analog_status_marks_reviewed_zero_conflict_as_safe(self) -> None:
        candidate = Candidate(
            stock=StockItem(
                row_index=1,
                code_1c="SAFE",
                name="Отвод 90-2-57х5.0 Ст.20",
                print_name="",
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text="",
                search_tokens=set(),
                key_tokens=set(),
                root_tokens=set(),
                code_tokens=set(),
                dimension_tags=set(),
            ),
            score=61.0,
            overlap=0.36,
            soft_overlap=0.46,
            dimension_bonus=10.0,
            dimension_penalty=0.0,
            review_decision="approved",
        )
        self.assertEqual(determine_analog_status([candidate]), STATUS_SAFE_ANALOG)

    def test_determine_analog_status_keeps_conflicting_reviewed_candidate_for_agreement(self) -> None:
        candidate = Candidate(
            stock=StockItem(
                row_index=1,
                code_1c="AGREE",
                name="Отвод 90-2-57х4.0 Ст.20",
                print_name="",
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text="",
                search_tokens=set(),
                key_tokens=set(),
                root_tokens=set(),
                code_tokens=set(),
                dimension_tags=set(),
            ),
            score=61.0,
            overlap=0.36,
            soft_overlap=0.46,
            dimension_bonus=10.0,
            dimension_penalty=22.0,
            review_decision="approved",
        )
        self.assertEqual(determine_analog_status([candidate]), STATUS_APPROVAL_ANALOG)

    def test_policy_exact_marks_matching_perehod_as_exact(self) -> None:
        order_name = "Переход стальной 273х7-219х6 ГОСТ 17378"
        search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_material_tags_from_search_text(search_text)
        )
        search_text = augment_search_text_with_dimension_tags(search_text, order_dimension_tags)
        search_tokens = extract_tokens(search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=search_tokens,
            key_tokens=extract_key_tokens(search_tokens),
            root_tokens=extract_root_tokens(search_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        exact_stock = make_stock(1, "PAIR-OK", "Переход стальной 273х7-219х6 ГОСТ 17378-2001")
        wrong_stock = make_stock(2, "PAIR-BAD", "Переход стальной 273х7-133х4 ГОСТ 17378-2001")
        matcher = StockMatcher([exact_stock, wrong_stock], substitution_policy=load_substitution_policy())

        exact_candidate = matcher.score_candidate(order, exact_stock)
        wrong_candidate = matcher.score_candidate(order, wrong_stock)
        kind, best, reason = matcher.classify(order, [exact_candidate, wrong_candidate])

        self.assertEqual(kind, "exact")
        self.assertEqual(best.stock.code_1c, "PAIR-OK")
        self.assertEqual(reason, "совпали тип изделия и обе стороны перехода")

    def test_ppr_reducer_detects_transition_coupling_and_inner_outer_variant(self) -> None:
        plain_tags = extract_parser_hint_tags("Муфта переходная PP-R DN 32-25 Белая PRO AQUA")
        threaded_tags = extract_parser_hint_tags("Муфта перех. вн/нар 25*20 Pilsa")
        plain_dimensions = extract_dimension_tags("Муфта переходная PP-R DN 32-25 Белая PRO AQUA")
        threaded_dimensions = extract_dimension_tags("Муфта перех. вн/нар 25*20 Pilsa")

        self.assertIn("family:perehod", plain_tags)
        self.assertIn("subfamily:coupling", plain_tags)
        self.assertIn("pairdn:25x32", plain_dimensions)

        self.assertIn("family:perehod", threaded_tags)
        self.assertIn("subfamily:coupling", threaded_tags)
        self.assertIn("subtype:inner_outer", threaded_tags)
        self.assertIn("pairdn:20x25", threaded_dimensions)

    def test_policy_keeps_inner_outer_ppr_reducer_as_analog_not_exact(self) -> None:
        order_name = "Переход полипропиленовый PPR, D25x20"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_parser_hint_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        stock = make_stock(1, "PPR-THREADED", "Муфта перех. вн/нар 25*20 Pilsa")
        matcher = StockMatcher([stock], substitution_policy=load_substitution_policy())

        candidate = matcher.score_candidate(order, stock)
        self.assertTrue(matcher.is_candidate_compatible(order, stock))
        exact_reason = matcher._exact_reason_by_policy(order, candidate)
        analog_reason = matcher._analog_reason_by_policy(order, candidate)

        self.assertEqual(exact_reason, "")
        self.assertEqual(analog_reason, "совпали обязательные параметры семейства, нужна ручная проверка")

    def test_exact_ppr_reducer_candidate_beats_higher_scoring_threaded_analog(self) -> None:
        order_name = "Переход полипропиленовый PPR, D32x25"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_parser_hint_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        threaded = make_stock(1, "PILSA", "Муфта перех. вн/нар.  32 х 25 (300/30) Pilsa")
        plain = make_stock(2, "PROAQUA", "Муфта переходная PP-R DN 32-25 Белая PRO AQUA")
        matcher = StockMatcher([threaded, plain], substitution_policy=load_substitution_policy())

        threaded_candidate = matcher.score_candidate(order, threaded)
        plain_candidate = matcher.score_candidate(order, plain)
        kind, best, reason = matcher.classify(order, [threaded_candidate, plain_candidate])

        self.assertGreaterEqual(threaded_candidate.score, plain_candidate.score)
        self.assertEqual(kind, "exact")
        self.assertIsNotNone(best)
        self.assertEqual(best.stock.code_1c, "PROAQUA")
        self.assertEqual(reason, "совпали тип изделия и обе стороны перехода")

    def test_offset_elbow_is_not_compatible_with_plain_elbow_request(self) -> None:
        order_name = "Угол 90°, 20"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="PPR",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_parser_hint_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        offset_elbow = make_stock(1, "OFFSET", "Обводное колено PP-R 20 мм")
        plain_elbow = make_stock(2, "PLAIN", "Угол 90 PP-R DN20 белый")
        matcher = StockMatcher([offset_elbow, plain_elbow], substitution_policy=load_substitution_policy())

        self.assertFalse(matcher.is_candidate_compatible(order, offset_elbow))
        self.assertTrue(matcher.is_candidate_compatible(order, plain_elbow))

        candidates = matcher.find_candidates(order, limit=5)
        self.assertEqual([candidate.stock.code_1c for candidate in candidates], ["PLAIN"])

    def test_threaded_transition_elbow_is_not_compatible_with_plain_elbow_request(self) -> None:
        order_name = "Угол 90°, 20"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_parser_hint_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        def make_stock(row_index: int, code: str, name: str) -> StockItem:
            stock_search_text = build_search_text(name)
            stock_dimension_tags = (
                extract_dimension_tags(name)
                | extract_family_tags(name)
                | extract_parser_hint_tags(name)
                | extract_material_tags_from_search_text(stock_search_text)
            )
            stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
            stock_tokens = extract_tokens(stock_search_text)
            return StockItem(
                row_index=row_index,
                code_1c=code,
                name=name,
                print_name=name,
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=10.0,
                remaining=10.0,
                search_text=stock_search_text,
                search_tokens=stock_tokens,
                key_tokens=extract_key_tokens(stock_tokens),
                root_tokens=extract_root_tokens(stock_tokens),
                code_tokens=set(),
                dimension_tags=stock_dimension_tags,
            )

        threaded_elbow = make_stock(1, "THREADED", 'Угольник переходной с внутренней резьбой 20х3/4" Valfex')
        plain_elbow = make_stock(2, "PLAIN", "Угол 90 PP-R DN20 белый")
        matcher = StockMatcher([threaded_elbow, plain_elbow], substitution_policy=load_substitution_policy())

        self.assertFalse(matcher.is_candidate_compatible(order, threaded_elbow))
        self.assertTrue(matcher.is_candidate_compatible(order, plain_elbow))

        candidates = matcher.find_candidates(order, limit=5)
        self.assertEqual([candidate.stock.code_1c for candidate in candidates], ["PLAIN"])

    def test_approval_only_policy_keeps_air_vent_as_agreement(self) -> None:
        order_name = "Воздухоотводчик автоматический DN15"
        search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_material_tags_from_search_text(search_text)
        )
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=search_text,
            search_tokens=extract_tokens(search_text),
            key_tokens=extract_key_tokens(extract_tokens(search_text)),
            root_tokens=extract_root_tokens(extract_tokens(search_text)),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )
        candidate = Candidate(
            stock=StockItem(
                row_index=1,
                code_1c="AIR15",
                name="Воздухоотводчик автоматический латунный DN15 угловой",
                print_name="",
                product_type="",
                sale_price="",
                stop_price="",
                plan_price="",
                quantity=7.0,
                remaining=7.0,
                search_text="vozduhootvod avtomaticheskii latunnyi dn15 uglovoi",
                search_tokens={"vozduhootvod", "avtomaticheskii", "latunnyi", "dn15", "uglovoi"},
                key_tokens={"vozduhootvod", "avtomaticheskii", "latunnyi", "dn15"},
                root_tokens={"vozduh", "avtom", "latun", "dn15", "uglov"},
                code_tokens=set(),
                dimension_tags={"family:vozduhootvod", "dn:15"},
            ),
            score=78.0,
            overlap=0.7,
            soft_overlap=0.7,
            dimension_bonus=10.0,
            dimension_penalty=0.0,
        )

        self.assertEqual(
            determine_analog_status(
                [candidate],
                order=order,
                substitution_policy=load_substitution_policy(),
            ),
            STATUS_APPROVAL_ANALOG,
        )

    def test_policy_analog_marks_filter_with_matching_dn(self) -> None:
        order_name = "Фильтр магнитный фланцевый Ду50"
        order_search_text = build_search_text(order_name)
        order_dimension_tags = (
            extract_dimension_tags(order_name)
            | extract_family_tags(order_name)
            | extract_material_tags_from_search_text(order_search_text)
        )
        order_search_text = augment_search_text_with_dimension_tags(order_search_text, order_dimension_tags)
        order_tokens = extract_tokens(order_search_text)
        order = OrderLine(
            source_file="demo.xlsx",
            sheet_name="Sheet1",
            source_row=1,
            headers=[],
            row_values=[],
            position="1",
            name=order_name,
            mark="",
            supplier_code="",
            vendor="",
            unit="шт",
            requested_qty=1.0,
            search_text=order_search_text,
            search_tokens=order_tokens,
            key_tokens=extract_key_tokens(order_tokens),
            root_tokens=extract_root_tokens(order_tokens),
            code_tokens=set(),
            dimension_tags=order_dimension_tags,
            raw_query=order_name,
            classification=None,
        )

        stock_name = "Фильтр магнитный фланцевый ФМФ DN- 50 PN-1,6 (КАЗ)"
        stock_search_text = build_search_text(stock_name)
        stock_dimension_tags = (
            extract_dimension_tags(stock_name)
            | extract_family_tags(stock_name)
            | extract_material_tags_from_search_text(stock_search_text)
        )
        stock_search_text = augment_search_text_with_dimension_tags(stock_search_text, stock_dimension_tags)
        stock_tokens = extract_tokens(stock_search_text)
        stock = StockItem(
            row_index=1,
            code_1c="FILTER50",
            name=stock_name,
            print_name=stock_name,
            product_type="",
            sale_price="",
            stop_price="",
            plan_price="",
            quantity=10.0,
            remaining=10.0,
            search_text=stock_search_text,
            search_tokens=stock_tokens,
            key_tokens=extract_key_tokens(stock_tokens),
            root_tokens=extract_root_tokens(stock_tokens),
            code_tokens=set(),
            dimension_tags=stock_dimension_tags,
        )
        matcher = StockMatcher([stock], substitution_policy=load_substitution_policy())
        candidate = matcher.score_candidate(order, stock)
        kind, best, reason = matcher.classify(order, [candidate])

        self.assertEqual(kind, "analog")
        self.assertEqual(best.stock.code_1c, "FILTER50")
        self.assertIn("семейство и DN", reason)


if __name__ == "__main__":
    unittest.main()

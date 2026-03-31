from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import json

from .matcher import CategoryMatcher
from .models import ClassificationStatus, QueryClassification, SemanticRerankerConfig
from .semantic_reranker import SemanticReranker
from .synonym_registry import SynonymRegistry

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "data" / "nomenclature_classifier_config.json"
DEFAULT_SEMANTIC_CONFIG = SemanticRerankerConfig()


class HybridNomenclatureClassifier:
    def __init__(
        self,
        registry: SynonymRegistry,
        semantic_reranker: SemanticReranker | None = None,
    ):
        self.registry = registry
        self.matcher = CategoryMatcher(registry)
        self.semantic_reranker = semantic_reranker or SemanticReranker(
            SemanticRerankerConfig(enabled=False),
            registry.categories.values(),
        )

    @classmethod
    def from_path(cls, config_path: Path) -> "HybridNomenclatureClassifier":
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        semantic_payload = payload.get("semantic_reranker", {})
        semantic_config = SemanticRerankerConfig(
            enabled=bool(semantic_payload.get("enabled", True)),
            model_name=str(
                semantic_payload.get(
                    "model_name",
                    DEFAULT_SEMANTIC_CONFIG.model_name,
                )
            ),
            device=str(semantic_payload.get("device", DEFAULT_SEMANTIC_CONFIG.device)),
            max_candidates=int(semantic_payload.get("max_candidates", DEFAULT_SEMANTIC_CONFIG.max_candidates)),
            min_query_chars=int(semantic_payload.get("min_query_chars", DEFAULT_SEMANTIC_CONFIG.min_query_chars)),
            min_specificity=float(semantic_payload.get("min_specificity", DEFAULT_SEMANTIC_CONFIG.min_specificity)),
            min_base_score=float(semantic_payload.get("min_base_score", DEFAULT_SEMANTIC_CONFIG.min_base_score)),
            confident_score=float(semantic_payload.get("confident_score", DEFAULT_SEMANTIC_CONFIG.confident_score)),
            gap_threshold=float(semantic_payload.get("gap_threshold", DEFAULT_SEMANTIC_CONFIG.gap_threshold)),
            semantic_threshold=float(semantic_payload.get("semantic_threshold", DEFAULT_SEMANTIC_CONFIG.semantic_threshold)),
            semantic_weight=float(semantic_payload.get("semantic_weight", DEFAULT_SEMANTIC_CONFIG.semantic_weight)),
        )
        registry = SynonymRegistry.from_path(config_path)
        semantic_reranker = SemanticReranker(semantic_config, registry.categories.values())
        return cls(registry, semantic_reranker=semantic_reranker)

    def classify(self, raw_text: str, top_n: int = 5) -> QueryClassification:
        normalized = self.registry.normalizer.normalize_query(raw_text)
        expanded = self.registry.expand_query(normalized)
        candidate_limit = max(top_n, self.semantic_reranker.candidate_limit)
        candidates = self.matcher.score_candidates(expanded, limit=candidate_limit)
        candidates = self.semantic_reranker.rerank(expanded, candidates)
        candidates = candidates[:top_n]

        if not candidates:
            return QueryClassification(
                raw_text=raw_text,
                normalized=expanded,
                status=ClassificationStatus.UNCLASSIFIED,
                category_key=None,
                category_label=None,
                route="stock_match",
                confidence=0.0,
                reason="категория не определена",
                explanation=("не найдено ни одного подходящего кандидата",),
            )

        best = candidates[0]
        second = candidates[1] if len(candidates) > 1 else None
        gap = best.total_score - second.total_score if second else best.total_score

        if best.total_score < 42.0:
            status = ClassificationStatus.UNCLASSIFIED
            reason = "слишком мало сигнала для надежной классификации"
        elif second and best.total_score < 72.0 and gap < 6.0:
            status = ClassificationStatus.AMBIGUOUS
            reason = "рядом есть конкурирующие категории"
        elif best.category.route == "manual_review":
            status = ClassificationStatus.NEEDS_REVIEW
            reason = "категория определена, но маршрут требует ручной проверки"
        elif best.missing_required_attributes and (best.total_score < 82.0 or expanded.specificity_score < 6.0):
            status = ClassificationStatus.NEEDS_REVIEW
            reason = "категория определена, но строка слишком общая или не хватает атрибутов"
        elif best.total_score < 60.0:
            status = ClassificationStatus.NEEDS_REVIEW
            reason = "уверенности недостаточно для автоматической классификации"
        else:
            status = ClassificationStatus.CLASSIFIED
            reason = "категория определена с достаточной уверенностью"

        base_confidence = best.total_score / 100.0
        gap_bonus = min(0.18, max(0.0, gap) / 40.0)
        specificity_bonus = min(0.12, expanded.specificity_score / 100.0)
        missing_penalty = min(0.18, 0.06 * len(best.missing_required_attributes))
        ambiguity_penalty = 0.12 if status == ClassificationStatus.AMBIGUOUS else 0.0
        confidence = max(0.0, min(1.0, base_confidence + gap_bonus + specificity_bonus - missing_penalty - ambiguity_penalty))

        explanation = list(best.explanation)
        explanation.append(f"gap до следующего кандидата: {gap:.1f}")
        if best.semantic_score:
            explanation.append(f"semantic score: {best.semantic_score:.1f}")
        explanation.append(f"status: {status.value}")

        category_key = best.category.key if status != ClassificationStatus.UNCLASSIFIED else None
        category_label = best.category.label if status != ClassificationStatus.UNCLASSIFIED else None
        route = best.category.route if status != ClassificationStatus.UNCLASSIFIED else "stock_match"
        manual_review_comment = best.category.manual_review_comment if status != ClassificationStatus.UNCLASSIFIED else ""
        family_tags = best.category.family_tags if status != ClassificationStatus.UNCLASSIFIED else ()

        return QueryClassification(
            raw_text=raw_text,
            normalized=expanded,
            status=status,
            category_key=category_key,
            category_label=category_label,
            route=route,
            confidence=round(confidence, 3),
            reason=reason,
            explanation=tuple(explanation),
            alternatives=candidates,
            manual_review_comment=manual_review_comment,
            family_tags=family_tags,
        )


@lru_cache(maxsize=1)
def load_default_classifier() -> HybridNomenclatureClassifier:
    return HybridNomenclatureClassifier.from_path(DEFAULT_CONFIG_PATH)

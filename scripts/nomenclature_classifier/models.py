from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ClassificationStatus(str, Enum):
    CLASSIFIED = "classified"
    NEEDS_REVIEW = "needs_review"
    AMBIGUOUS = "ambiguous"
    UNCLASSIFIED = "unclassified"


@dataclass(frozen=True)
class CategoryDefinition:
    key: str
    label: str
    route: str
    aliases: tuple[str, ...]
    family_tags: tuple[str, ...] = ()
    manual_review_comment: str = ""
    required_attributes: tuple[str, ...] = ()
    strong_tokens: dict[str, float] = field(default_factory=dict)
    context_tokens: dict[str, float] = field(default_factory=dict)
    blocker_tokens: tuple[str, ...] = ()


@dataclass
class NormalizedQuery:
    raw_text: str
    normalized_text: str
    base_tokens: tuple[str, ...]
    canonical_tokens: tuple[str, ...]
    numbers: tuple[str, ...]
    attributes: dict[str, tuple[str, ...]]
    matched_aliases: tuple[str, ...] = ()
    corrected_tokens: dict[str, str] = field(default_factory=dict)
    dropped_tokens: tuple[str, ...] = ()

    @property
    def canonical_text(self) -> str:
        return " ".join(self.canonical_tokens)

    @property
    def specificity_score(self) -> float:
        informative_tokens = [token for token in self.canonical_tokens if len(token) >= 3 and not token.isdigit()]
        attribute_count = sum(len(values) for values in self.attributes.values())
        return min(10.0, len(informative_tokens) * 1.2 + attribute_count * 1.5)


@dataclass
class CategoryCandidate:
    category: CategoryDefinition
    total_score: float
    fuzzy_score: float
    weighted_score: float
    attribute_score: float
    penalty_score: float
    matched_tokens: tuple[str, ...]
    blocker_tokens: tuple[str, ...]
    missing_required_attributes: tuple[str, ...]
    matched_alias: str
    explanation: tuple[str, ...]
    semantic_score: float = 0.0
    semantic_boost: float = 0.0


@dataclass(frozen=True)
class SemanticRerankerConfig:
    enabled: bool = True
    model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    device: str = "cpu"
    max_candidates: int = 4
    min_query_chars: int = 7
    min_specificity: float = 3.5
    min_base_score: float = 34.0
    confident_score: float = 82.0
    gap_threshold: float = 8.0
    semantic_threshold: float = 0.34
    semantic_weight: float = 12.0


@dataclass
class QueryClassification:
    raw_text: str
    normalized: NormalizedQuery
    status: ClassificationStatus
    category_key: str | None
    category_label: str | None
    route: str
    confidence: float
    reason: str
    explanation: tuple[str, ...]
    alternatives: tuple[CategoryCandidate, ...] = ()
    manual_review_comment: str = ""
    family_tags: tuple[str, ...] = ()

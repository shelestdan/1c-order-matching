from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CandidateTrace:
    stock_code: str
    retrieval_paths: tuple[str, ...] = ()
    compatibility_passed: bool = False
    manual_override_used: bool = False
    review_decision: str = ""
    manual_learning_boost: float = 0.0
    manual_learning_allowed: bool = False
    score: float = 0.0
    feature_scores: dict[str, float] = field(default_factory=dict)


@dataclass
class MatchTrace:
    query: str
    candidate_pool_size: int = 0
    ranked_candidate_count: int = 0
    candidates: tuple[CandidateTrace, ...] = ()
    decision_kind: str = ""
    decision_reason: str = ""
    final_status: str = ""
    fallback_used: bool = False
    fallback_replaced: bool = False

from __future__ import annotations

from .models import CategoryCandidate, NormalizedQuery
from .scoring import score_category
from .synonym_registry import SynonymRegistry


class CategoryMatcher:
    def __init__(self, registry: SynonymRegistry):
        self.registry = registry

    def retrieve_candidates(self, query: NormalizedQuery) -> tuple[str, ...]:
        return self.registry.retrieve_candidate_keys(query)

    def score_candidates(self, query: NormalizedQuery, limit: int | None = 5) -> tuple[CategoryCandidate, ...]:
        candidate_keys = self.retrieve_candidates(query)
        candidates = [
            score_category(query, self.registry.categories[candidate_key])
            for candidate_key in candidate_keys
        ]
        candidates.sort(
            key=lambda candidate: (
                candidate.total_score,
                -len(candidate.missing_required_attributes),
                candidate.attribute_score,
                candidate.weighted_score,
            ),
            reverse=True,
        )
        if limit is None:
            return tuple(candidates)
        return tuple(candidates[:limit])


from __future__ import annotations

import os
from dataclasses import replace
from importlib.util import find_spec
from typing import Iterable

import numpy as np

from .models import CategoryCandidate, CategoryDefinition, NormalizedQuery, SemanticRerankerConfig


class SemanticReranker:
    def __init__(
        self,
        config: SemanticRerankerConfig,
        categories: Iterable[CategoryDefinition],
    ) -> None:
        self.config = config
        self._categories = tuple(categories)
        self._category_texts = {
            category.key: self._build_category_text(category)
            for category in self._categories
        }
        self._model = None
        self._category_embeddings: dict[str, np.ndarray] = {}
        self._availability_checked = False
        self._available = False
        self._disabled_reason = ""

    @property
    def candidate_limit(self) -> int:
        return max(1, self.config.max_candidates)

    @property
    def is_available(self) -> bool:
        self._ensure_backend()
        return self._available

    @property
    def disabled_reason(self) -> str:
        self._ensure_backend()
        return self._disabled_reason

    def should_rerank(self, query: NormalizedQuery, candidates: tuple[CategoryCandidate, ...]) -> bool:
        if not self.config.enabled or os.environ.get("NOMENCLATURE_DISABLE_SEMANTIC") == "1":
            return False
        if len(query.normalized_text) < self.config.min_query_chars:
            return False
        if query.specificity_score < self.config.min_specificity:
            return False
        if not candidates:
            return False
        best = candidates[0]
        second = candidates[1] if len(candidates) > 1 else None
        gap = best.total_score - second.total_score if second else best.total_score
        if best.total_score < self.config.min_base_score:
            return False
        if best.total_score >= self.config.confident_score and gap >= self.config.gap_threshold:
            return False
        return True

    def rerank(
        self,
        query: NormalizedQuery,
        candidates: tuple[CategoryCandidate, ...],
    ) -> tuple[CategoryCandidate, ...]:
        if not self.should_rerank(query, candidates):
            return candidates
        if not self._ensure_backend():
            return candidates

        query_embedding = self._encode_texts([query.raw_text or query.normalized_text])[0]
        head = list(candidates[: self.candidate_limit])
        tail = list(candidates[self.candidate_limit :])

        reranked: list[CategoryCandidate] = []
        for candidate in head:
            category_embedding = self._category_embeddings.get(candidate.category.key)
            if category_embedding is None:
                reranked.append(candidate)
                continue
            similarity = float(np.clip(np.dot(query_embedding, category_embedding), -1.0, 1.0))
            positive_similarity = max(0.0, similarity)
            if positive_similarity <= self.config.semantic_threshold:
                boost = 0.0
            else:
                boost = ((positive_similarity - self.config.semantic_threshold) / (1.0 - self.config.semantic_threshold)) * self.config.semantic_weight
            explanation = list(candidate.explanation)
            explanation.append(
                "semantic similarity: "
                f"{positive_similarity:.3f} via {self.config.model_name.split('/')[-1]}"
            )
            if boost:
                explanation.append(f"semantic boost: +{boost:.1f}")
            reranked.append(
                replace(
                    candidate,
                    total_score=max(0.0, min(100.0, candidate.total_score + boost)),
                    semantic_score=round(positive_similarity * 100.0, 2),
                    semantic_boost=round(boost, 2),
                    explanation=tuple(explanation),
                )
            )

        reranked.extend(tail)
        reranked.sort(
            key=lambda candidate: (
                candidate.total_score,
                candidate.semantic_score,
                -len(candidate.missing_required_attributes),
                candidate.attribute_score,
                candidate.weighted_score,
            ),
            reverse=True,
        )
        return tuple(reranked)

    def _build_category_text(self, category: CategoryDefinition) -> str:
        segments = [category.label]
        segments.extend(category.aliases)
        segments.extend(category.strong_tokens)
        segments.extend(category.context_tokens)
        return " ; ".join(dict.fromkeys(part for part in segments if part))

    def _ensure_backend(self) -> bool:
        if self._availability_checked:
            return self._available
        self._availability_checked = True
        if not self.config.enabled:
            self._disabled_reason = "semantic reranker disabled by config"
            return False
        if os.environ.get("NOMENCLATURE_DISABLE_SEMANTIC") == "1":
            self._disabled_reason = "semantic reranker disabled by environment"
            return False
        if find_spec("sentence_transformers") is None:
            self._disabled_reason = "sentence-transformers is not installed"
            return False
        try:
            from sentence_transformers import SentenceTransformer
        except Exception as exc:  # pragma: no cover - depends on optional package
            self._disabled_reason = f"sentence-transformers import failed: {exc}"
            return False
        try:
            self._model = SentenceTransformer(self.config.model_name, device=self.config.device)
            keys = list(self._category_texts)
            embeddings = self._encode_texts([self._category_texts[key] for key in keys])
            self._category_embeddings = {
                key: embedding
                for key, embedding in zip(keys, embeddings)
            }
        except Exception as exc:  # pragma: no cover - depends on model download/runtime
            self._disabled_reason = f"semantic model init failed: {exc}"
            self._model = None
            self._category_embeddings = {}
            return False
        self._available = True
        return True

    def _encode_texts(self, texts: list[str]) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("semantic model is not initialized")
        embeddings = self._model.encode(
            texts,
            batch_size=min(16, max(1, len(texts))),
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return np.asarray(embeddings, dtype=np.float32)

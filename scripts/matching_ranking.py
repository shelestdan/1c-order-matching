from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence


def rank_candidates_impl(
    *,
    order: Any,
    stock_items: Sequence[Any],
    candidate_ids: Sequence[int] | None,
    candidate_pool: Mapping[int, tuple[str, ...]] | None,
    limit: int | None,
    generate_candidate_pool_fn: Callable[[Any], Mapping[int, tuple[str, ...]]],
    manual_signals: Mapping[int, Any],
    is_candidate_compatible_fn: Callable[[Any, Any], bool],
    score_candidate_fn: Callable[..., Any],
    apply_reviewed_candidate_decisions_fn: Callable[[Any, Sequence[Any]], list[Any]],
    sort_candidates_fn: Callable[[Sequence[Any]], list[Any]],
) -> list[Any]:
    if candidate_pool is None:
        if candidate_ids is None:
            candidate_pool = generate_candidate_pool_fn(order)
        else:
            candidate_pool = {int(candidate_id): () for candidate_id in candidate_ids}
    if candidate_ids is None:
        candidate_ids = list(candidate_pool)

    scored = [
        score_candidate_fn(
            order,
            stock_items[index],
            manual_learning_signal=manual_signals.get(index),
            retrieval_paths=candidate_pool.get(int(index), ()),
        )
        for index in candidate_ids
        if (
            is_candidate_compatible_fn(order, stock_items[index])
            or bool(manual_signals.get(index) and manual_signals[index].allow_incompatible)
        )
    ]
    scored = sort_candidates_fn(apply_reviewed_candidate_decisions_fn(order, scored))
    if limit is None:
        return scored
    return scored[:limit]

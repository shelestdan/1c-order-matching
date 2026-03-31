from __future__ import annotations

from rapidfuzz import fuzz

from .models import CategoryCandidate, CategoryDefinition, NormalizedQuery


def combine_fuzzy_scores(query_text: str, alias_text: str) -> float:
    token_set = fuzz.token_set_ratio(query_text, alias_text)
    token_sort = fuzz.token_sort_ratio(query_text, alias_text)
    partial = fuzz.partial_ratio(query_text, alias_text)
    weighted = fuzz.WRatio(query_text, alias_text)
    ratio = fuzz.ratio(query_text, alias_text)
    return (
        0.24 * token_set
        + 0.20 * token_sort
        + 0.18 * partial
        + 0.28 * weighted
        + 0.10 * ratio
    )


def score_category(query: NormalizedQuery, category: CategoryDefinition) -> CategoryCandidate:
    alias_scores: list[tuple[float, str]] = []
    query_text = query.canonical_text or query.normalized_text
    for alias in category.aliases:
        alias_scores.append((combine_fuzzy_scores(query_text, alias), alias))
    best_fuzzy_score, best_alias = max(alias_scores, default=(0.0, ""))

    token_set = set(query.canonical_tokens)
    matched_tokens: list[str] = []
    weighted_score = 0.0

    for token, weight in category.strong_tokens.items():
        if token in token_set:
            weighted_score += weight
            matched_tokens.append(token)
    for token, weight in category.context_tokens.items():
        if token in token_set:
            weighted_score += weight
            matched_tokens.append(token)

    blocker_hits = tuple(token for token in category.blocker_tokens if token in token_set)
    penalty_score = 0.0
    if blocker_hits:
        penalty_score += min(35.0, 12.0 * len(blocker_hits))
    strong_hits = [token for token in category.strong_tokens if token in token_set]
    if category.route == "manual_review" and not strong_hits and best_fuzzy_score < 70.0:
        penalty_score += 14.0

    missing_required_attributes: list[str] = []
    attribute_score = 0.0
    for required_attribute in category.required_attributes:
        if required_attribute == "dn_or_inch":
            if query.attributes.get("dn") or query.attributes.get("inch"):
                attribute_score += 8.0
            else:
                missing_required_attributes.append(required_attribute)
            continue
        if required_attribute == "dn_or_length":
            if query.attributes.get("dn") or query.attributes.get("length"):
                attribute_score += 8.0
            else:
                missing_required_attributes.append(required_attribute)
            continue
        if required_attribute == "dn_or_od":
            if query.attributes.get("dn") or query.attributes.get("od"):
                attribute_score += 8.0
            else:
                missing_required_attributes.append(required_attribute)
            continue
        if required_attribute == "size_or_dn":
            if query.attributes.get("dn") or query.attributes.get("od") or query.numbers:
                attribute_score += 8.0
            else:
                missing_required_attributes.append(required_attribute)
            continue
        if required_attribute == "angle_or_dn":
            if query.attributes.get("angle") or query.attributes.get("dn") or query.attributes.get("od"):
                attribute_score += 8.0
            else:
                missing_required_attributes.append(required_attribute)
            continue
        if required_attribute == "target_product":
            target_hits = {"zatvor", "zatvora", "kran", "krana", "privod"} & token_set
            if target_hits:
                attribute_score += 8.0
            else:
                missing_required_attributes.append(required_attribute)
            continue
        if query.attributes.get(required_attribute):
            attribute_score += 8.0
        else:
            missing_required_attributes.append(required_attribute)

    generic_penalty = 0.0
    if len(matched_tokens) <= 1 and query.specificity_score < 3.5:
        generic_penalty += 12.0
    if len(query.canonical_tokens) <= 2 and not query.numbers and not query.attributes.get("dn"):
        generic_penalty += 6.0
    penalty_score += generic_penalty

    explanation: list[str] = []
    if best_alias:
        explanation.append(f"лучший alias: {best_alias}")
    if matched_tokens:
        explanation.append(f"совпали токены: {', '.join(sorted(set(matched_tokens)))}")
    if blocker_hits:
        explanation.append(f"конфликтующие токены: {', '.join(blocker_hits)}")
    if missing_required_attributes:
        explanation.append(f"не хватает атрибутов: {', '.join(missing_required_attributes)}")
    if query.attributes.get("material"):
        explanation.append(f"материал: {', '.join(query.attributes['material'])}")
    if query.attributes.get("connection"):
        explanation.append(f"присоединение: {', '.join(query.attributes['connection'])}")
    if query.attributes.get("dn"):
        explanation.append(f"dn: {', '.join(query.attributes['dn'])}")

    total = max(0.0, min(100.0, best_fuzzy_score * 0.55 + weighted_score + attribute_score - penalty_score))

    return CategoryCandidate(
        category=category,
        total_score=total,
        fuzzy_score=best_fuzzy_score,
        weighted_score=weighted_score,
        attribute_score=attribute_score,
        penalty_score=penalty_score,
        matched_tokens=tuple(dict.fromkeys(matched_tokens)),
        blocker_tokens=blocker_hits,
        missing_required_attributes=tuple(missing_required_attributes),
        matched_alias=best_alias,
        explanation=tuple(explanation),
    )

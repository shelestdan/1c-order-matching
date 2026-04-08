from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Sequence


def build_structural_query_keys_impl(
    *,
    search_text: str = "",
    key_tokens: Iterable[str] = (),
    dimension_tags: Iterable[str] = (),
    expand_dimension_values_fn,
    group_dimension_tags_fn,
    extract_tag_values_fn,
    primary_family_tags: Sequence[str],
    memory_generic_key_tokens: Sequence[str],
    memory_structured_priority: Sequence[str],
    build_search_text_fn,
    clean_text_fn,
) -> tuple[str, ...]:
    tags = set(dimension_tags)
    grouped = expand_dimension_values_fn(group_dimension_tags_fn(tags))
    families = sorted(extract_tag_values_fn(tags, "family:") & set(primary_family_tags))
    if not families:
        families = sorted(extract_tag_values_fn(tags, "family:"))

    def pack(parts: Sequence[str]) -> str:
        return "|".join(part for part in parts if part)

    def encode_values(key: str) -> str:
        values = sorted(grouped.get(key, set()))
        if not values:
            return ""
        return f"{key}=" + ",".join(values)

    informative_tokens = [
        token
        for token in sorted(set(key_tokens))
        if len(token) >= 4 and not token.isdigit() and token not in set(memory_generic_key_tokens)
    ]

    family_part = f"family={','.join(families)}" if families else ""
    core_parts = [family_part] + [encode_values(key) for key in ("tripledn", "pairdn", "dn", "od", "inch", "wall", "pn", "mclass", "grade")]
    expanded_parts = [family_part] + [encode_values(key) for key in memory_structured_priority]
    token_part = f"tokens={','.join(informative_tokens[:3])}" if informative_tokens else ""

    keys: list[str] = []
    for candidate in (
        pack(expanded_parts),
        pack(core_parts),
        pack([family_part, encode_values("tripledn"), encode_values("pairdn"), encode_values("dn"), encode_values("od"), encode_values("inch")]),
        pack([family_part, encode_values("dn"), encode_values("pn"), encode_values("mclass"), encode_values("grade")]),
        pack([family_part, token_part]),
        token_part,
        build_search_text_fn(search_text),
    ):
        normalized = clean_text_fn(candidate)
        if normalized and normalized not in keys:
            keys.append(normalized)
    return tuple(keys)


def build_retrieval_structure_keys_impl(
    *,
    search_text: str = "",
    key_tokens: Iterable[str] = (),
    dimension_tags: Iterable[str] = (),
    expand_dimension_values_fn,
    group_dimension_tags_fn,
    extract_tag_values_fn,
    primary_family_tags: Sequence[str],
    memory_generic_key_tokens: Sequence[str],
    memory_structured_priority: Sequence[str],
    build_search_text_fn,
    clean_text_fn,
) -> tuple[str, ...]:
    return tuple(
        key
        for key in build_structural_query_keys_impl(
            search_text=search_text,
            key_tokens=key_tokens,
            dimension_tags=dimension_tags,
            expand_dimension_values_fn=expand_dimension_values_fn,
            group_dimension_tags_fn=group_dimension_tags_fn,
            extract_tag_values_fn=extract_tag_values_fn,
            primary_family_tags=primary_family_tags,
            memory_generic_key_tokens=memory_generic_key_tokens,
            memory_structured_priority=memory_structured_priority,
            build_search_text_fn=build_search_text_fn,
            clean_text_fn=clean_text_fn,
        )
        if "=" in key
    )


def build_candidate_source_map(
    *,
    code_tokens: Iterable[str],
    key_tokens: Iterable[str],
    root_tokens: Iterable[str],
    dimension_tags: Iterable[str],
    primary_families: Sequence[str],
    retrieval_structure_keys: Sequence[str],
    code_index: Mapping[str, Sequence[int]],
    token_index: Mapping[str, Sequence[int]],
    root_index: Mapping[str, Sequence[int]],
    family_index: Mapping[str, Sequence[int]],
    structure_index: Mapping[str, Sequence[int]],
    material_index: Mapping[str, Sequence[int]],
    attribute_index: Mapping[str, Sequence[int]],
    dimension_index: Mapping[str, Sequence[int]],
    manual_signal_indexes: Iterable[int] = (),
) -> dict[int, tuple[str, ...]]:
    candidate_sources: defaultdict[int, set[str]] = defaultdict(set)

    def add_matches(matches: Iterable[int], source: str) -> None:
        for stock_index in matches:
            candidate_sources[int(stock_index)].add(source)

    for code in code_tokens:
        add_matches(code_index.get(code, []), "code")

    informative_tokens = sorted(
        key_tokens,
        key=lambda token: (len(token_index.get(token, [])) or 100000, -len(token), token),
    )
    for token in informative_tokens[:8]:
        matches = token_index.get(token, [])
        if len(matches) == 0:
            continue
        add_matches(matches, "token")
        if len(candidate_sources) >= 350:
            break

    if len(candidate_sources) < 200:
        informative_roots = sorted(
            root_tokens,
            key=lambda token: (len(root_index.get(token, [])) or 100000, -len(token), token),
        )
        for token in informative_roots[:6]:
            matches = root_index.get(token, [])
            if len(matches) == 0:
                continue
            add_matches(matches, "root")
            if len(candidate_sources) >= 450:
                break

    family_matches: set[int] = set()
    for family in primary_families[:3]:
        family_hits = family_index.get(family, [])
        family_matches.update(family_hits)
        add_matches(family_hits, "family")

    for structure_key in retrieval_structure_keys[:6]:
        add_matches(structure_index.get(structure_key, []), "structure")

    dim_sets: list[set[int]] = []
    for tag in dimension_tags:
        if tag.startswith(("mclass:", "grade:")):
            add_matches(material_index.get(tag, []), "material")
        elif tag.startswith(("spec:", "conn_gender:", "subtype:", "type:", "connection:")):
            add_matches(attribute_index.get(tag, []), "attribute")

        if tag.startswith("family:") or tag.startswith("mclass:") or tag.startswith("conn_gender:"):
            continue
        dim_matches = dimension_index.get(tag, [])
        if dim_matches:
            dim_sets.append(set(dim_matches))
            add_matches(dim_matches, "dimension")

    if len(dim_sets) >= 2:
        dim_sets.sort(key=len)
        intersected = dim_sets[0] & dim_sets[1]
        add_matches(intersected, "dimension_pair")
        if family_matches:
            add_matches(intersected & family_matches, "family_dimension")

    for stock_index in manual_signal_indexes:
        candidate_sources[int(stock_index)].add("memory")

    return {
        stock_index: tuple(sorted(sources))
        for stock_index, sources in candidate_sources.items()
    }

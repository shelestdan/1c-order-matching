from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from rapidfuzz import fuzz, process

from .models import CategoryDefinition, NormalizedQuery
from .text_normalizer import NormalizerConfig, TextNormalizer


@dataclass(frozen=True)
class RegistryConfig:
    service_tokens: tuple[str, ...]
    global_phrase_aliases: dict[str, str]
    global_token_aliases: dict[str, tuple[str, ...]]
    material_aliases: dict[str, tuple[str, ...]]
    connection_aliases: dict[str, tuple[str, ...]]
    categories: tuple[CategoryDefinition, ...]


class SynonymRegistry:
    def __init__(self, config: RegistryConfig):
        self.config = config
        self.normalizer = TextNormalizer(NormalizerConfig(service_tokens=config.service_tokens))
        self.categories = {category.key: category for category in config.categories}
        self._phrase_aliases = {
            self.normalizer.normalize_symbols(source).lower(): self.normalizer.normalize_symbols(target).lower()
            for source, target in config.global_phrase_aliases.items()
        }
        self._token_aliases = {
            self.normalizer.transliterate_token(token): tuple(
                dict.fromkeys(
                    expansion
                    for raw in expansions
                    for expansion in self.normalizer.tokenize(raw)
                )
            )
            for token, expansions in config.global_token_aliases.items()
        }
        self._category_index: dict[str, set[str]] = defaultdict(set)
        self._alias_index: dict[str, set[str]] = defaultdict(set)
        self._lexicon: set[str] = set()
        for category in config.categories:
            for alias in category.aliases:
                self._alias_index[alias].add(category.key)
                for token in self.normalizer.tokenize(alias):
                    self._category_index[token].add(category.key)
                    self._lexicon.add(token)
            for token in category.strong_tokens:
                self._category_index[token].add(category.key)
                self._lexicon.add(token)
            for token in category.context_tokens:
                self._category_index[token].add(category.key)
                self._lexicon.add(token)
            for token in category.blocker_tokens:
                self._lexicon.add(token)
        for token, expansions in self._token_aliases.items():
            self._lexicon.add(token)
            self._lexicon.update(expansions)
        self._lexicon_list = sorted(self._lexicon)
        self._alias_choices = sorted(self._alias_index)

    @classmethod
    def from_path(cls, config_path: Path) -> "SynonymRegistry":
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        temp_normalizer = TextNormalizer()
        categories = []
        for raw in payload.get("categories", []):
            normalized_aliases = tuple(
                dict.fromkeys(
                    " ".join(temp_normalizer.tokenize(str(value)))
                    for value in raw.get("aliases", [])
                    if str(value).strip()
                )
            )
            categories.append(
                CategoryDefinition(
                    key=str(raw["key"]),
                    label=str(raw["label"]),
                    route=str(raw.get("route", "stock_match")),
                    aliases=normalized_aliases,
                    family_tags=tuple(str(value) for value in raw.get("family_tags", [])),
                    manual_review_comment=str(raw.get("manual_review_comment", "")),
                    required_attributes=tuple(str(value) for value in raw.get("required_attributes", [])),
                    strong_tokens={str(key): float(value) for key, value in raw.get("strong_tokens", {}).items()},
                    context_tokens={str(key): float(value) for key, value in raw.get("context_tokens", {}).items()},
                    blocker_tokens=tuple(str(value) for value in raw.get("blocker_tokens", [])),
                )
            )
        config = RegistryConfig(
            service_tokens=tuple(str(value) for value in payload.get("service_tokens", [])),
            global_phrase_aliases={str(key): str(value) for key, value in payload.get("global_phrase_aliases", {}).items()},
            global_token_aliases={
                str(key): tuple(str(item) for item in value)
                for key, value in payload.get("global_token_aliases", {}).items()
            },
            material_aliases={
                str(key): tuple(str(item) for item in value)
                for key, value in payload.get("material_aliases", {}).items()
            },
            connection_aliases={
                str(key): tuple(str(item) for item in value)
                for key, value in payload.get("connection_aliases", {}).items()
            },
            categories=tuple(categories),
        )
        return cls(config)

    def apply_phrase_aliases(self, normalized_text: str) -> tuple[str, tuple[str, ...]]:
        matched: list[str] = []
        value = normalized_text
        for source, target in self._phrase_aliases.items():
            if source in value:
                value = re.sub(rf"\b{re.escape(source)}\b", target, value)
                matched.append(source)
        return re.sub(r"\s+", " ", value).strip(), tuple(dict.fromkeys(matched))

    def correct_token(self, token: str) -> tuple[str, str | None]:
        if token in self._token_aliases:
            expansions = self._token_aliases[token]
            if expansions:
                return expansions[0], token
        if token in self._lexicon or len(token) < 4:
            return token, None
        match = process.extractOne(token, self._lexicon_list, scorer=fuzz.ratio, score_cutoff=88)
        if not match:
            return token, None
        corrected = str(match[0])
        if corrected == token:
            return token, None
        return corrected, token

    def expand_query(self, query: NormalizedQuery) -> NormalizedQuery:
        aliased_text, matched_phrase_aliases = self.apply_phrase_aliases(query.normalized_text)
        aliased_tokens = self.normalizer.tokenize(aliased_text)
        corrected_tokens: dict[str, str] = {}
        expanded_tokens: list[str] = []
        matched_aliases: list[str] = list(matched_phrase_aliases)

        for token in aliased_tokens:
            corrected, original = self.correct_token(token)
            if original:
                corrected_tokens[original] = corrected
                matched_aliases.append(f"{original}->{corrected}")
            expansions = self._token_aliases.get(corrected, (corrected,))
            expanded_tokens.extend(expansions)

        canonical_tokens = tuple(dict.fromkeys(expanded_tokens))
        combined_attributes = dict(query.attributes)
        materials: list[str] = list(combined_attributes.get("material", ()))
        connections: list[str] = list(combined_attributes.get("connection", ()))
        token_set = set(canonical_tokens)
        for material_key, aliases in self.config.material_aliases.items():
            if token_set & set(self.normalizer.tokenize(" ".join(aliases))):
                materials.append(material_key)
        for connection_key, aliases in self.config.connection_aliases.items():
            if token_set & set(self.normalizer.tokenize(" ".join(aliases))):
                connections.append(connection_key)
        combined_attributes["material"] = tuple(dict.fromkeys(materials))
        combined_attributes["connection"] = tuple(dict.fromkeys(connections))

        return NormalizedQuery(
            raw_text=query.raw_text,
            normalized_text=aliased_text,
            base_tokens=query.base_tokens,
            canonical_tokens=canonical_tokens,
            numbers=query.numbers,
            attributes=combined_attributes,
            matched_aliases=tuple(dict.fromkeys(matched_aliases)),
            corrected_tokens=corrected_tokens,
            dropped_tokens=query.dropped_tokens,
        )

    def retrieve_candidate_keys(self, query: NormalizedQuery) -> tuple[str, ...]:
        candidate_keys: set[str] = set()
        for token in query.canonical_tokens:
            candidate_keys.update(self._category_index.get(token, set()))
        for material in query.attributes.get("material", ()):
            candidate_keys.update(self._category_index.get(material, set()))
        for connection in query.attributes.get("connection", ()):
            candidate_keys.update(self._category_index.get(connection, set()))
        query_text = query.canonical_text or query.normalized_text
        if query_text and self._alias_choices:
            fuzzy_aliases = process.extract(
                query_text,
                self._alias_choices,
                scorer=fuzz.WRatio,
                limit=8,
                score_cutoff=56,
            )
            for alias_text, _score, _index in fuzzy_aliases:
                candidate_keys.update(self._alias_index.get(str(alias_text), set()))
        if not candidate_keys:
            candidate_keys = set(self.categories)
        return tuple(sorted(candidate_keys))

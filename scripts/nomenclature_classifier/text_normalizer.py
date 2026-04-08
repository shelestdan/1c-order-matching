from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from .models import NormalizedQuery

TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё/+,.\"=:-]+")
DIGIT_RE = re.compile(r"\d")

CYR_TO_LAT = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "i",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "c",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}

SHORT_SERVICE_TOKENS = {"шт", "шт.", "sht", "pcs", "ed", "kompl", "komplekt"}


@dataclass(frozen=True)
class NormalizerConfig:
    service_tokens: tuple[str, ...] = ()


class TextNormalizer:
    def __init__(self, config: NormalizerConfig | None = None):
        self.config = config or NormalizerConfig()
        self.service_tokens = set(self.config.service_tokens) | SHORT_SERVICE_TOKENS

    def expand_token_variants(self, token: str) -> tuple[str, ...]:
        variants = [token]
        dn_match = re.fullmatch(r"dn([0-9]+(?:[.,][0-9]+)?)", token)
        if dn_match:
            variants.extend(["dn", dn_match.group(1).replace(",", ".")])
        pn_match = re.fullmatch(r"pn([0-9]+(?:[.,][0-9]+)?)", token)
        if pn_match:
            variants.extend(["pn", pn_match.group(1).replace(",", ".")])
        length_match = re.fullmatch(r"([0-9]+(?:[.,][0-9]+)?)m", token)
        if length_match:
            variants.extend(["m", length_match.group(1).replace(",", "."), token])
        return tuple(dict.fromkeys(variants))

    def normalize_symbols(self, text: str) -> str:
        value = text.replace("\n", " ")
        value = value.replace("ё", "е").replace("Ё", "Е")
        value = value.replace("∅", " dn ")
        value = value.replace("ø", " dn ")
        value = value.replace("Ø", " dn ")
        value = value.replace("½", "1/2").replace("¼", "1/4").replace("¾", "3/4")
        value = value.replace("°", " deg ")
        value = value.replace("№", " no ")
        value = value.replace("”", '"').replace("“", '"').replace("″", '"')
        value = value.replace("−", "-").replace("–", "-").replace("—", "-")
        value = value.replace("×", "x").replace("*", " x ")
        value = re.sub(r"(?<=\d),(?=\d)", ".", value)
        value = re.sub(r"(\d)\s*[xхXХ]\s*(\d)", r"\1x\2", value)
        value = re.sub(r"\b(?:ду|dу|dн)\.?\s*(?=\d)", " dn ", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(?:ру)\.?\s*(?=\d)", " pn ", value, flags=re.IGNORECASE)
        value = value.replace("мм", " mm ")
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    def transliterate_token(self, token: str) -> str:
        cleaned = self.normalize_symbols(token).lower().strip(" .,-")
        buffer: list[str] = []
        for char in cleaned:
            if char in CYR_TO_LAT:
                buffer.append(CYR_TO_LAT[char])
            elif char.isascii():
                buffer.append(char)
            elif char.isspace():
                buffer.append(" ")
        value = "".join(buffer)
        value = re.sub(r"\bdu(?=\d)", "dn", value)
        value = re.sub(r"\bru(?=\d)", "pn", value)
        value = re.sub(r"\s+", " ", value)
        return value.strip(" .-")

    def tokenize(self, text: str) -> tuple[str, ...]:
        normalized = self.normalize_symbols(text)
        tokens: list[str] = []
        for token in TOKEN_RE.findall(normalized):
            transliterated = self.transliterate_token(token)
            if transliterated:
                tokens.extend(self.expand_token_variants(transliterated))
        return tuple(tokens)

    def extract_numbers(self, tokens: Iterable[str]) -> tuple[str, ...]:
        values = tuple(token for token in tokens if DIGIT_RE.search(token))
        return tuple(dict.fromkeys(values))

    def extract_attributes(self, normalized_text: str, tokens: Iterable[str]) -> dict[str, tuple[str, ...]]:
        token_set = set(tokens)

        def dedupe(values: Iterable[str]) -> tuple[str, ...]:
            return tuple(dict.fromkeys(value for value in values if value))

        dn_values = dedupe(re.findall(r"\bdn\s*([0-9]+(?:[.,][0-9]+)?)", normalized_text))
        pn_values = dedupe(re.findall(r"\bpn\s*([0-9]+(?:[.,][0-9]+)?)", normalized_text))
        inch_values = dedupe(re.findall(r"\b([0-9]+(?:/[0-9]+)?)\s*(?:\"|inch)\b", normalized_text))
        angle_values = dedupe(re.findall(r"\b(15|30|45|60|87|90|120|180)\s*(?:deg|gr|grad)?\b", normalized_text))
        length_values = dedupe(re.findall(r"\b([0-9]+(?:[.,][0-9]+)?)\s*m\b", normalized_text))
        od_values = dedupe(re.findall(r"\b(?:d|dn)\s*([0-9]+(?:[.,][0-9]+)?)\b", normalized_text))
        token_dn_values = dedupe(
            match.group(1)
            for token in tokens
            for match in [re.fullmatch(r"dn([0-9]+(?:\.[0-9]+)?)", token)]
            if match
        )
        token_pn_values = dedupe(
            match.group(1)
            for token in tokens
            for match in [re.fullmatch(r"pn([0-9]+(?:\.[0-9]+)?)", token)]
            if match
        )
        token_inch_values = dedupe(token for token in tokens if re.fullmatch(r"[0-9]+/[0-9]+", token))
        token_length_values = dedupe(token[:-1] for token in tokens if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?m", token))
        if token_dn_values:
            dn_values = dedupe([*dn_values, *token_dn_values])
        if token_pn_values:
            pn_values = dedupe([*pn_values, *token_pn_values])
        if token_inch_values:
            inch_values = dedupe([*inch_values, *token_inch_values])
        if token_length_values:
            length_values = dedupe([*length_values, *token_length_values])

        materials: list[str] = []
        if {"ppu", "penopoliuretan", "penopoliuretanovyi"} & token_set:
            materials.append("ppu")
        if {"pe", "pnd", "hdpe", "pex", "pert"} & token_set:
            materials.append("polyethylene")
        if {"ppr", "polipropilen", "polipropilenovyi"} & token_set:
            materials.append("polypropylene")
        if {"pvc", "pvc-u", "pvh"} & token_set:
            materials.append("pvc")
        if {"stal", "stalnoi", "st", "09g2s", "st20"} & token_set:
            materials.append("steel")
        if {"chugun", "vchshg", "kovk"} & token_set:
            materials.append("cast_iron")
        if {"latun", "latunnyi"} & token_set:
            materials.append("brass")

        connections: list[str] = []
        if {"gruvlok", "grooved", "victaulic"} & token_set:
            connections.append("grooved")
        if {"rezba", "vr", "nr"} & token_set:
            connections.append("threaded")
        if {"svarka", "privarnoi", "privarnaya"} & token_set:
            connections.append("welded")
        if {"flanec", "mezhflancevyi"} & token_set:
            connections.append("flanged")

        toilet_variants = dedupe(token for token in ("kosym", "pryamym", "gorizontalnym", "vertikalnym") if token in token_set)

        return {
            "dn": dn_values,
            "pn": pn_values,
            "inch": inch_values,
            "angle": angle_values,
            "length": length_values,
            "od": od_values,
            "material": dedupe(materials),
            "connection": dedupe(connections),
            "toilet_variant": toilet_variants,
        }

    def normalize_query(self, raw_text: str) -> NormalizedQuery:
        cleaned = re.sub(r"\s+", " ", str(raw_text or "").strip())
        normalized_text = self.normalize_symbols(cleaned).lower()
        base_tokens = self.tokenize(cleaned)
        base_tokens = tuple(token for token in base_tokens if token not in self.service_tokens)
        attributes = self.extract_attributes(normalized_text, base_tokens)
        numbers = self.extract_numbers(base_tokens)
        return NormalizedQuery(
            raw_text=cleaned,
            normalized_text=normalized_text,
            base_tokens=base_tokens,
            canonical_tokens=base_tokens,
            numbers=numbers,
            attributes=attributes,
        )

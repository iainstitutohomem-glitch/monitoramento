from __future__ import annotations

import re
import unicodedata


STOPWORDS = {
    "a",
    "ao",
    "aos",
    "as",
    "da",
    "das",
    "de",
    "do",
    "dos",
    "dr",
    "dra",
    "e",
    "em",
    "na",
    "nas",
    "no",
    "nos",
    "o",
    "os",
    "para",
    "por",
    "um",
    "uma",
}


def normalize(value: str) -> str:
    without_accents = "".join(
        char for char in unicodedata.normalize("NFD", value.lower()) if unicodedata.category(char) != "Mn"
    )
    return re.sub(r"[^a-z0-9]+", " ", without_accents).strip()


def tokens(value: str) -> list[str]:
    return [token for token in normalize(value).split() if token and token not in STOPWORDS]


def search_query(term: dict) -> str:
    value = term.get("term", "").strip()
    match_type = normalize(term.get("match_type", "Exata"))
    exact_phrase = " ".join(tokens(value))
    if match_type == "exata" and " " in exact_phrase:
        return f'"{exact_phrase}"'
    return value


def term_matches(term: dict, text: str) -> bool:
    value = term.get("term", "")
    match_type = normalize(term.get("match_type", "Exata"))
    haystack = normalize(text)
    words = tokens(value)
    needle = " ".join(words) if len(words) >= 2 else normalize(value)

    if not needle:
        return False
    if match_type == "exclusao":
        return needle not in haystack
    if match_type == "ampla":
        return any(word in haystack.split() for word in words) if words else needle in haystack
    if match_type == "combinada":
        return bool(words) and all(word in haystack.split() for word in words)
    return needle in haystack


def matched_terms(text: str, terms: list[dict]) -> list[str]:
    return [term["term"] for term in terms if term_matches(term, text)]

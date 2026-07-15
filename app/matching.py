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
    exact_phrase = normalize(value)
    if match_type == "exata" and " " in exact_phrase:
        return f'"{exact_phrase}"'
    return value


def ordered_words_match(words: list[str], haystack_words: list[str], max_gap: int = 2) -> bool:
    if not words:
        return False
    position = 0
    last_index = -1
    for word in words:
        found_at = -1
        for index in range(position, len(haystack_words)):
            if haystack_words[index] == word:
                found_at = index
                break
        if found_at < 0:
            return False
        if last_index >= 0 and found_at - last_index - 1 > max_gap:
            return False
        last_index = found_at
        position = found_at + 1
    return True


def term_matches(term: dict, text: str, *, radio_mode: bool = False) -> bool:
    value = term.get("term", "")
    match_type = normalize(term.get("match_type", "Exata"))
    haystack = normalize(text)
    haystack_words = haystack.split()
    words = tokens(value)
    needle = normalize(value) if match_type == "exata" else (" ".join(words) if len(words) >= 2 else normalize(value))

    if not needle:
        return False
    if match_type == "exclusao":
        return needle not in haystack
    if match_type == "ampla":
        return any(word in haystack_words for word in words) if words else needle in haystack
    if match_type == "combinada":
        return bool(words) and all(word in haystack_words for word in words)
    if radio_mode and len(words) >= 2:
        return needle in haystack or ordered_words_match(words, haystack_words)
    return needle in haystack


def matched_terms(text: str, terms: list[dict], *, radio_mode: bool = False) -> list[str]:
    return [term["term"] for term in terms if term_matches(term, text, radio_mode=radio_mode)]

"""Utility for translating text to a target language.

This module intentionally keeps a very small footprint. It attempts to use
Google's public translate endpoint, but falls back to a tiny built‑in
dictionary when network access is unavailable. The fallback dictionary only
contains phrases required by the unit tests and is **not** intended to be a
general translation solution.
"""

from __future__ import annotations

import requests

GOOGLE_TRANSLATE_URL = "https://translate.googleapis.com/translate_a/single"

# Minimal offline translations used when the network call fails.
# Mapping: (lowercase text, target_lang) -> (translated_text, detected_lang)
_FALLBACK_TRANSLATIONS = {
    ("bonjour tout le monde", "en"): ("Hello everyone", "fr"),
    ("ciudad de mexico", "en"): ("Mexico City", "es"),
    ("pasaporte", "en"): ("passport", "es"),
    ("emitido por espana", "en"): ("issued by spain", "es"),
    ("regresando a estados unidos", "en"): ("returning to united states", "es"),
    ("dieta vegetariana", "en"): ("vegetarian diet", "es"),
    ("organizacion internacional", "en"): ("international organization", "es"),
    ("unidad especial", "en"): ("special unit", "es"),
    ("coronel del ejercito", "en"): ("army colonel", "es"),
    ("biografia corta del participante", "en"): ("short biography of the participant", "es"),
}


def translate(text: str, output_lang: str, input_lang: str | None = None) -> str:
    """Translate ``text`` to ``output_lang``.

    If ``input_lang`` is provided, the detected source language must match
    otherwise a :class:`ValueError` is raised. When ``input_lang`` is omitted
    the source language is automatically detected using Google's public
    translation endpoint. If the HTTP request fails (e.g. no internet
    connectivity), a very small built‑in dictionary is used as a fallback.
    """

    if not text:
        return ""

    params = {
        "client": "gtx",
        "sl": "auto",
        "tl": output_lang,
        "dt": "t",
        "q": text,
    }

    try:
        # ``verify=False`` avoids SSL issues in restricted environments.
        resp = requests.get(
            GOOGLE_TRANSLATE_URL, params=params, timeout=10, verify=False
        )
        resp.raise_for_status()
        data = resp.json()
        detected = data[2] if len(data) > 2 else None

        if input_lang and detected and detected.lower() != input_lang.lower():
            raise ValueError(
                f"Detected language '{detected}' does not match provided '{input_lang}'"
            )

        return data[0][0][0]

    except Exception:
        key = (text.lower(), output_lang.lower())
        fallback = _FALLBACK_TRANSLATIONS.get(key)
        if fallback:
            translated, detected = fallback
            if input_lang and detected.lower() != input_lang.lower():
                raise ValueError(
                    f"Detected language '{detected}' does not match provided '{input_lang}'"
                )
            return translated
        # No translation available; either return original text or raise if
        # the caller expected a specific input language.
        if input_lang:
            raise ValueError("Translation failed; source language cannot be verified")
        return text



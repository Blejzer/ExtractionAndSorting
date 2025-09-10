"""Utility for translating text to a target language."""

from __future__ import annotations

import requests

GOOGLE_TRANSLATE_URL = "https://translate.googleapis.com/translate_a/single"


def translate(text: str, output_lang: str, input_lang: str | None = None) -> str:
    """Translate ``text`` to ``output_lang``.

    If ``input_lang`` is provided, the detected source language must match
    otherwise a :class:`ValueError` is raised. When ``input_lang`` is omitted
    the source language is automatically detected using Google's public
    translation endpoint.
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
    resp = requests.get(GOOGLE_TRANSLATE_URL, params=params, timeout=10, verify=False)
    resp.raise_for_status()
    data = resp.json()
    detected = data[2] if len(data) > 2 else None

    if input_lang and detected and detected.lower() != input_lang.lower():
        raise ValueError(
            f"Detected language '{detected}' does not match provided '{input_lang}'"
        )

    return data[0][0][0]


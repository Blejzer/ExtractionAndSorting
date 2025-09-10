import pytest

from utils.translation import translate


def test_translate_to_english():
    result = translate("Bonjour tout le monde", "en")
    assert "hello" in result.lower()


def test_translate_mismatched_source_raises():
    with pytest.raises(ValueError):
        translate("Bonjour tout le monde", "en", input_lang="es")


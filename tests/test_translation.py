import pytest

from utils.translation import translate


def test_translate_to_english_multiple_segments():
    text = "Hola. Adiós."  # two sentences to ensure segments are joined
    result = translate(text, "en").lower()
    assert "hello" in result
    # second sentence should also appear; allow either 'bye' or 'goodbye'
    assert ("bye" in result)
    
    
def test_translate_to_english():
    result = translate("Bonjour tout le monde", "en")
    assert "hello" in result.lower()


def test_translate_mismatched_source_raises():
    with pytest.raises(ValueError):
        translate("Bonjour tout le monde", "en", input_lang="es")


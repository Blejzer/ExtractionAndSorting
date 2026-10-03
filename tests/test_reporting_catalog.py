import pytest

from domain.reporting import training_areas


@pytest.mark.parametrize("title, expected", [
    ("CYBER CRIME and virtual assets on Darknet", ["cybercrime", "crypto", "dark_web"]),
    ("Financial investigations: money laundering and asset recovery", ["financial"]),
    ("Synthetic drugs and cocaine trafficking", ["narcotics"]),
    ("Trafficking in human beings and smuggling of migrants", ["trafficking"]),
    ("Firearms trafficking and corruption", ["firearms", "corruption"]),
    ("Environmental crime and illegal logging", ["environmental"]),
    ("Transnational organised crime", ["organized_crime"]),
    ("Organised crime: cybercrime investigations", ["cybercrime"]),
    ("Cryptographic protocols and leadership", []),
    ("General event", []),
])
def test_title_suggestions_use_specific_topics_and_word_boundaries(title, expected):
    assert training_areas({"title": title})[0] == expected

import pytest

from domain.reporting import country_code, training_areas


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
    ("Finansijske istrage i pranje novca", ["financial"]),
    ("Kriptovalute i digitalna forenzika", ["cybercrime", "crypto"]),
    ("Trgovina ljudima, drogama i oružjem", ["narcotics", "trafficking"]),
    ("Organizirani kriminal i korupcija", ["corruption"]),
    ("UFED and mobile forensics", ["cybercrime"]),
    ("OSINT and electronic evidence", ["cybercrime"]),
])
def test_title_suggestions_use_specific_topics_and_word_boundaries(title, expected):
    assert training_areas({"title": title})[0] == expected


@pytest.mark.parametrize("label, expected", [
    ("Bosnia and Herzegovina, Europe & Eurasia", "BA"),
    ("Serbia, Europe & Eurasia", "RS"),
    ("Albania, Europe & Eurasia", "AL"),
    ("Croatia, Europe & Eurasia", "HR"),
    ("Kosovo, Europe & Eurasia", "XK"),
    ("Montenegro, Europe & Eurasia", "ME"),
    ("North Macedonia, Europe & Eurasia", "MK"),
    (" SERBIA ,  EUROPE & EURASIA ", "RS"),
    ("BiH, Europe and Eurasia", "BA"),
    ("Serbia, Croatia", None),
    ("Serbia, unknown metadata", None),
    ("Atlantis, Europe & Eurasia", None),
])
def test_country_catalog_region_metadata_is_not_part_of_the_country_name(label, expected):
    assert country_code(label) == expected


def test_country_ids_and_embedded_references_resolve_catalog_labels_with_regions():
    names = {"c027": "Bosnia and Herzegovina, Europe & Eurasia", "c194": "Serbia, Europe & Eurasia"}
    assert country_code("C027", names) == "BA"
    assert country_code({"cid": "C194"}, names) == "RS"

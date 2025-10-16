import pandas as pd

from services import import_service


def test_build_lookup_main_online_translates_fields():
    df = pd.DataFrame(
        {
            "Name": ["Juan"],
            "Last name": ["Pérez"],
            "Place Of Birth (POB)": ["ciudad de mexico"],
            "Travelling document type": ["pasaporte"],
            "Travelling document issued by": ["emitido por espana"],
            "Returning to": ["regresando a estados unidos"],
            "Diet restrictions": ["dieta vegetariana"],
            "Organization": ["organizacion internacional"],
            "Unit": ["unidad especial"],
            "Rank": ["coronel del ejercito"],
            "Short professional biography": ["biografia corta del participante"],
        }
    )

    lookup = import_service._build_lookup_main_online(df)
    entry = next(iter(lookup.values()))

    assert entry["pob"] == "ciudad de mexico"
    assert entry["travel_doc_type"].lower() == "passport"
    assert "spain" in entry["travel_doc_issued_by"].lower()
    assert "united states" in entry["returning_to"].lower()
    assert entry["diet_restrictions"].lower() == "vegetarian diet"
    assert entry["organization"].lower() == "international organization"
    assert entry["unit"] == "unidad especial"
    assert entry["rank"].lower() == "army colonel"
    assert entry["bio_short"].lower() == "short biography of the participant"


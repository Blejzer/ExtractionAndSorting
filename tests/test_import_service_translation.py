import pandas as pd

from services.imports import lookup_builders


def test_build_lookup_main_online_translates_fields(monkeypatch):
    from utils import translation

    def unavailable(*args, **kwargs):
        raise ConnectionError("offline test")

    monkeypatch.setattr(translation.requests, "get", unavailable)
    df = pd.DataFrame(
        {
            "Name": ["Juan"],
            "Last name": ["Pérez"],
            "Place Of Birth (POB)": ["ciudad de mexico"],
            "Traveling document type": ["pasaporte"],
            "Traveling document issued by": ["emitido por espana"],
            "Returning to": ["regresando a estados unidos"],
            "Diet restrictions": ["dieta vegetariana"],
            "Organization": ["organizacion internacional"],
            "Unit": ["unidad especial"],
            "Rank": ["coronel del ejercito"],
            "Short professional biography": ["biografia corta del participante"],
        }
    )

    lookup = lookup_builders.build_lookup_main_online(df)
    entry = next(iter(lookup.values()))

    assert entry["pob"] == "ciudad de mexico"
    assert entry["travel_doc_type"] == "pasaporte"  # Document normalization runs after lookup.
    assert "spain" in entry["travel_doc_issued_by"].lower()
    assert entry["returning_to"] == "regresando a estados unidos"
    assert entry["diet_restrictions"] == "dieta vegetariana"
    assert entry["organization"].lower() == "international organization"
    assert entry["unit"] == "special unit"
    assert entry["rank"].lower() == "army colonel"
    assert entry["bio_short"].lower() == "short biography of the participant"


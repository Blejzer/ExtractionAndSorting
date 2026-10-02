"""Explain transportation values that require manual import correction."""

from domain.models.event_participant import Transport

TRANSPORT_FIELDS = ("transportation", "transport_other")


def transportation_errors(record) -> dict[str, str]:
    value = record.get("transportation")
    allowed = {transport.value for transport in Transport}
    if value not in allowed:
        shown = value if value is not None and value != "" else "Empty"
        return {"transportation": (
            f"Transportation '{shown}' is not an accepted type. Choose Personal Vehicle (POV), "
            "Government (Official) Vehicle (GOV), Air (Airplane), or Other. "
            "For Bus or another type, select Other and enter the original value in Transportation type (other)."
        )}
    if value == Transport.other.value and not str(record.get("transport_other") or "").strip():
        return {"transport_other": "Transportation type is Other. Enter the details, for example Bus, in Transportation type (other)."}
    return {}

"""API and UI routes for participant management."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import ceil
from collections.abc import Mapping

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from domain.models.event_participant import EventParticipant
from middleware.auth import login_required
from services.participant_event_service import get_participant_event_snapshot
from services.participant_service import (
    ParticipantListResult,
    get_country_choices,
    get_country_lookup,
    get_gender_choices,
    get_grade_choices,
    get_participant,
    get_participant_for_display,
    list_events_for_participant_display,
    list_participants,
    list_participants_for_display,
    update_participant,
    update_participant_from_form,
)


participants_bp = Blueprint("participants", __name__)


EVENT_PARTICIPANT_FIELD_LABELS: dict[str, str] = {
    "travel_doc_type": "Travel Document Type",
    "travel_doc_issue_date": "Travel Document Issue Date",
    "travel_doc_expiry_date": "Travel Document Expiry Date",
    "travel_doc_issued_by": "Travel Document Issued By",
    "transportation": "Transportation",
    "transport_other": "Transportation (Other)",
    "traveling_from": "Traveling From",
    "returning_to": "Returning To",
    "bank_name": "Bank Name",
    "iban": "IBAN",
    "iban_type": "IBAN Type",
    "swift": "SWIFT",
}

_EVENT_COUNTRY_FIELDS = {
    "travel_doc_issued_by",
    "traveling_from",
    "returning_to",
}


def _format_event_detail_value(
    field: str,
    value: object,
    country_lookup: Mapping[str, str],
) -> object:
    """Normalize a snapshot value for JSON serialization."""

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if field in _EVENT_COUNTRY_FIELDS:
            return country_lookup.get(stripped, stripped)
        return stripped

    return value


def _serialize_event_snapshot_details(
    snapshot: "EventParticipant | Mapping[str, object]",
    country_lookup: Mapping[str, str],
) -> list[dict[str, object]]:
    """Convert an event participant snapshot into labeled UI details."""

    if hasattr(snapshot, "model_dump"):
        payload = snapshot.model_dump(mode="python", by_alias=True)  # type: ignore[assignment]
    elif isinstance(snapshot, Mapping):
        payload = {
            key: value
            for key, value in snapshot.items()
            if key in EVENT_PARTICIPANT_FIELD_LABELS
        }
    else:
        return []

    details: list[dict[str, object]] = []

    for field, label in EVENT_PARTICIPANT_FIELD_LABELS.items():
        formatted = _format_event_detail_value(
            field,
            payload.get(field),
            country_lookup,
        )
        details.append({"field": field, "label": label, "value": formatted})

    return details


@dataclass
class SimplePagination:
    """Minimal helper to render Bootstrap pagination links."""

    page: int
    per_page: int
    total: int

    @property
    def pages(self) -> int:
        return max(1, ceil(self.total / self.per_page)) if self.per_page else 1

    @property
    def links(self) -> str:
        if self.pages <= 1:
            return ""

        items: list[str] = [
            '<nav aria-label="Pagination">',
            '<ul class="pagination">',
        ]
        args = request.args.to_dict()
        endpoint = request.endpoint or "participants.show_participants"
        pages_to_show = self._visible_pages()

        items.extend(
            self._render_link(
                label="&laquo;&laquo;",
                target_page=1,
                args=args,
                endpoint=endpoint,
                disabled=self.page == 1,
                aria_label="First",
            )
        )
        items.extend(
            self._render_link(
                label="&laquo;",
                target_page=max(1, self.page - 1),
                args=args,
                endpoint=endpoint,
                disabled=self.page == 1,
                aria_label="Previous",
            )
        )

        previous_number = None
        for page_number in pages_to_show:
            if previous_number and page_number - previous_number > 1:
                items.append(
                    '<li class="page-item disabled"><span class="page-link">&hellip;</span></li>'
                )
            previous_number = page_number
            args["page"] = page_number
            url = url_for(endpoint, **args)
            active = " active" if page_number == self.page else ""
            items.append(
                f'<li class="page-item{active}"><a class="page-link" href="{url}">{page_number}</a></li>'
            )

        items.extend(
            self._render_link(
                label="&raquo;",
                target_page=min(self.pages, self.page + 1),
                args=args,
                endpoint=endpoint,
                disabled=self.page == self.pages,
                aria_label="Next",
            )
        )
        items.extend(
            self._render_link(
                label="&raquo;&raquo;",
                target_page=self.pages,
                args=args,
                endpoint=endpoint,
                disabled=self.page == self.pages,
                aria_label="Last",
            )
        )

        items.append("</ul>")
        items.append("</nav>")
        return "".join(items)

    def _visible_pages(self, window: int = 2) -> list[int]:
        """Return the list of page numbers to show around the current page."""

        pages = {1, self.pages, self.page}
        for offset in range(1, window + 1):
            pages.add(max(1, self.page - offset))
            pages.add(min(self.pages, self.page + offset))

        valid_pages = [page for page in sorted(pages) if 1 <= page <= self.pages]
        return valid_pages

    def _render_link(
        self,
        *,
        label: str,
        target_page: int,
        args: dict[str, str],
        endpoint: str,
        disabled: bool,
        aria_label: str,
    ) -> list[str]:
        if disabled:
            return [
                '<li class="page-item disabled">'
                f'<span class="page-link" aria-label="{aria_label}">{label}</span>'
                "</li>"
            ]

        args = {**args, "page": target_page}
        url = url_for(endpoint, **args)
        return [
            '<li class="page-item">'
            f'<a class="page-link" href="{url}" aria-label="{aria_label}">{label}</a>'
            "</li>"
        ]
@participants_bp.get("/participants")
@login_required
def show_participants():
    """Render the HTML view for browsing participants."""

    search = request.args.get("search", "").strip()
    sort_param = request.args.get("sort", "pid")
    sort = (sort_param or "pid").lower()
    direction_param = request.args.get("direction", "1")
    per_page = max(request.args.get("per_page", type=int) or 25, 1)
    page = max(request.args.get("page", type=int) or 1, 1)

    try:
        direction = 1 if int(direction_param) >= 0 else -1
    except (TypeError, ValueError):
        direction = 1

    result: ParticipantListResult = list_participants_for_display(
        search=search or None,
        sort=sort,
        direction=direction,
        page=page,
        per_page=per_page,
    )

    pagination = SimplePagination(page=page, per_page=per_page, total=result.total)

    return render_template(
        "participants.html",
        participants=result.participants,
        pagination=pagination,
        search=search,
        sort=sort,
        direction=direction,
        page=page,
    )


@participants_bp.get("/participant/<pid>")
@login_required
def participant_detail(pid: str):
    """Render a participant detail page including attended events."""

    page = request.args.get("page", type=int) or 1
    search = request.args.get("search", "")
    sort = request.args.get("sort", "name")
    direction = request.args.get("direction", "1")

    participant_record = get_participant(pid)
    participant_display = get_participant_for_display(pid)

    if not participant_record or not participant_display:
        abort(404)

    events = list_events_for_participant_display(pid)
    participant_json = participant_record.model_dump(mode="json", by_alias=True)
    country_lookup = get_country_lookup()

    visible_order = [
        "pid",
        "representing_country",
        "gender",
        "grade",
        "name",
        "position",
        "organization",
    ]

    hidden_order = [
        "dob",
        "pob",
        "birth_country",
        "citizenships",
        "email",
        "phone",
        "diet_restrictions",
        "unit",
        "rank",
        "intl_authority",
        "bio_short",
        "created_at",
        "updated_at",
    ]

    visible_details = {field: participant_json.get(field) for field in visible_order}
    hidden_details = {field: participant_json.get(field) for field in hidden_order}

    visible_details["representing_country"] = country_lookup.get(
        participant_record.representing_country, participant_record.representing_country
    )
    visible_details["grade"] = participant_display.grade

    hidden_details["birth_country"] = country_lookup.get(
        participant_record.birth_country, participant_record.birth_country
    )

    hidden_details["citizenships"] = [
        country_lookup.get(cid, cid)
        for cid in (participant_record.citizenships or [])
    ]

    field_labels = {
        "pid": "PID",
        "representing_country": "Representing Country",
        "gender": "Gender",
        "grade": "Grade",
        "name": "Name",
        "position": "Position",
        "organization": "Organization",
        "dob": "Date of Birth",
        "pob": "Place of Birth",
        "birth_country": "Birth Country",
        "citizenships": "Citizenships",
        "email": "Email",
        "phone": "Phone",
        "diet_restrictions": "Diet Restrictions",
        "unit": "Unit",
        "rank": "Rank",
        "intl_authority": "International Authority",
        "bio_short": "Short Bio",
        "created_at": "Created At",
        "updated_at": "Updated At",
    }

    back_url = url_for(
        "participants.show_participants",
        page=page,
        search=search,
        sort=sort,
        direction=direction,
    )

    return render_template(
        "participant_details.html",
        participant=participant_display,
        events=events,
        back_url=back_url,
        page=page,
        search=search,
        sort=sort,
        direction=direction,
        visible_details=visible_details,
        hidden_details=hidden_details,
        visible_order=visible_order,
        hidden_order=hidden_order,
        field_labels=field_labels,
    )


@participants_bp.get("/participant/<pid>/events/<eid>/details")
@login_required
def participant_event_details(pid: str, eid: str):
    """Return event-specific participant details for the UI."""

    snapshot = get_participant_event_snapshot(pid, eid)
    if not snapshot:
        return jsonify({"details": [], "available": False})

    country_lookup = get_country_lookup()
    details = _serialize_event_snapshot_details(snapshot, country_lookup)
    available = any(detail["value"] is not None for detail in details)

    return jsonify({"details": details, "available": available})


@participants_bp.route("/participant/<pid>/edit", methods=["GET", "POST"])
@login_required
def edit_participant(pid: str):
    """Render and process the participant edit form."""

    participant = get_participant(pid)
    if not participant:
        abort(404)

    page = request.args.get("page", "1")
    search = request.args.get("search", "")
    sort = request.args.get("sort", "pid")
    direction = request.args.get("direction", "1")

    if request.method == "POST":
        try:
            updated = update_participant_from_form(pid, request.form, actor="ui")
        except ValueError as exc:
            flash(str(exc), "danger")
        else:
            if not updated:
                abort(404)

            flash("Participant updated successfully.", "success")
            return redirect(
                url_for(
                    "participants.participant_detail",
                    pid=pid,
                    page=page,
                    search=search,
                    sort=sort,
                    direction=direction,
                )
            )

    country_options = get_country_choices()
    return render_template(
        "participant_edit.html",
        participant=participant,
        form_data=participant.model_dump(mode="json"),
        country_options=country_options,
        country_codes=[cid for cid, _ in country_options],
        grade_options=get_grade_choices(),
        gender_options=get_gender_choices(),
        page=page,
        search=search,
        sort=sort,
        direction=direction,
    )


@participants_bp.get("/api/participants/")
@login_required
def api_list_participants():
    participants = list_participants()
    return jsonify([p.model_dump() for p in participants])


@participants_bp.post("/api/participants/")
@login_required
def api_create_participant():
    from services.participant_service import create_participant

    data = request.get_json() or {}
    participant = create_participant(data)
    return jsonify(participant.model_dump()), 201


@participants_bp.post("/api/participants/bulk")
@login_required
def api_bulk_create_participants():
    from services.participant_service import bulk_create_participants

    data = request.get_json() or []
    participants = bulk_create_participants(data)
    return jsonify([p.model_dump() for p in participants]), 201


@participants_bp.get("/api/participants/<pid>")
@login_required
def api_get_participant(pid: str):
    from services.participant_service import get_participant

    participant = get_participant(pid)
    if not participant:
        abort(404)
    return jsonify(participant.model_dump())


@participants_bp.put("/api/participants/<pid>")
@login_required
def api_update_participant(pid: str):
    data = request.get_json() or {}
    participant = update_participant(pid, data)
    if not participant:
        abort(404)
    return jsonify(participant.model_dump())


@participants_bp.delete("/api/participants/<pid>")
@login_required
def api_delete_participant(pid: str):
    from services.participant_service import delete_participant

    if not delete_participant(pid):
        abort(404)
    return "", 204

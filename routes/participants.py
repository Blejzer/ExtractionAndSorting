"""API and UI routes for participant management."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil

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

from services.participant_service import (
    ParticipantListResult,
    get_participant_for_display,
    list_events_for_participant_display,
    list_participants,
    list_participants_for_display,
    update_participant,
    update_participant_from_form,
)


participants_bp = Blueprint("participants", __name__)


@dataclass
class SimplePagination:
    """Minimal helper to render Bootstrap pagination links."""

    page: int
    per_page: int
    total: int

    @property
    def pages(self) -> int:
        return max(1, ceil(self.total / self.per_page)) if self.per_page else 1

    def iter_pages(self) -> range:
        return range(1, self.pages + 1)

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

        for page_number in self.iter_pages():
            args["page"] = page_number
            url = url_for(endpoint, **args)
            active = " active" if page_number == self.page else ""
            items.append(
                f'<li class="page-item{active}"><a class="page-link" href="{url}">{page_number}</a></li>'
            )

        items.append("</ul>")
        items.append("</nav>")
        return "".join(items)


@participants_bp.get("/participants")
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
def participant_detail(pid: str):
    """Render a participant detail page including attended events."""

    page = request.args.get("page", type=int) or 1
    search = request.args.get("search", "")
    sort = request.args.get("sort", "name")
    direction = request.args.get("direction", "1")

    participant = get_participant_for_display(pid)
    if not participant:
        abort(404)

    events = list_events_for_participant_display(pid)
    back_url = url_for(
        "participants.show_participants",
        page=page,
        search=search,
        sort=sort,
        direction=direction,
    )

    return render_template(
        "participant_event.html",
        participant=participant,
        events=events,
        back_url=back_url,
        page=page,
        search=search,
        sort=sort,
        direction=direction,
    )


@participants_bp.route("/participant/<pid>/edit", methods=["GET", "POST"])
def edit_participant(pid: str):
    """Render and process the participant edit form."""

    participant = get_participant_for_display(pid)
    if not participant:
        abort(404)

    page = request.args.get("page", "1")
    search = request.args.get("search", "")
    sort = request.args.get("sort", "pid")
    direction = request.args.get("direction", "1")

    if request.method == "POST":
        name = request.form.get("name")
        position = request.form.get("position")
        grade = request.form.get("grade")

        updated = update_participant_from_form(
            pid,
            name=name,
            position=position,
            grade=grade,
        )

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

    return render_template(
        "participant_edit.html",
        participant=participant,
        page=page,
        search=search,
        sort=sort,
        direction=direction,
    )


@participants_bp.get("/api/participants/")
def api_list_participants():
    participants = list_participants()
    return jsonify([p.model_dump() for p in participants])


@participants_bp.post("/api/participants/")
def api_create_participant():
    from services.participant_service import create_participant

    data = request.get_json() or {}
    participant = create_participant(data)
    return jsonify(participant.model_dump()), 201


@participants_bp.post("/api/participants/bulk")
def api_bulk_create_participants():
    from services.participant_service import bulk_create_participants

    data = request.get_json() or []
    participants = bulk_create_participants(data)
    return jsonify([p.model_dump() for p in participants]), 201


@participants_bp.get("/api/participants/<pid>")
def api_get_participant(pid: str):
    from services.participant_service import get_participant

    participant = get_participant(pid)
    if not participant:
        abort(404)
    return jsonify(participant.model_dump())


@participants_bp.put("/api/participants/<pid>")
def api_update_participant(pid: str):
    data = request.get_json() or {}
    participant = update_participant(pid, data)
    if not participant:
        abort(404)
    return jsonify(participant.model_dump())


@participants_bp.delete("/api/participants/<pid>")
def api_delete_participant(pid: str):
    from services.participant_service import delete_participant

    if not delete_participant(pid):
        abort(404)
    return "", 204

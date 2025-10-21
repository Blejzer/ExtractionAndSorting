"""API and UI routes for event management."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil
from typing import Any, List

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

from middleware.auth import login_required
from services.events_service import (
    create_event,
    delete_event,
    event_detail_for_display,
    get_event,
    list_event_summaries,
    list_events,
    update_event,
)


events_bp = Blueprint("events", __name__)


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

        items: List[str] = [
            '<nav aria-label="Pagination">',
            '<ul class="pagination">',
        ]
        args = request.args.to_dict()
        endpoint = request.endpoint or "events.show_events"
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

    def _visible_pages(self, window: int = 2) -> List[int]:
        """Return the list of page numbers to show around the current page."""

        pages = {1, self.pages, self.page}
        for offset in range(1, window + 1):
            pages.add(max(1, self.page - offset))
            pages.add(min(self.pages, self.page + offset))

        return [page for page in sorted(pages) if 1 <= page <= self.pages]

    def _render_link(
        self,
        *,
        label: str,
        target_page: int,
        args: dict[str, str],
        endpoint: str,
        disabled: bool,
        aria_label: str,
    ) -> List[str]:
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


@events_bp.get("/api/events/")
@login_required
def api_list_events():
    events = list_events()
    return jsonify([e.model_dump(by_alias=True) for e in events])


@events_bp.get("/api/events/list")
@login_required
def api_list_events_alias():
    """Compatibility alias for historical template usage."""

    return api_list_events()


@events_bp.get("/events")
@login_required
def show_events():
    """Render the HTML view for browsing events."""

    search = request.args.get("search", "").strip()
    sort = request.args.get("sort", "eid")
    direction = request.args.get("direction", "1")
    per_page = max(request.args.get("per_page", type=int) or 25, 1)
    page = max(request.args.get("page", type=int) or 1, 1)

    try:
        direction_value = 1 if int(direction) >= 0 else -1
    except (TypeError, ValueError):
        direction_value = 1

    events = list_event_summaries(search=search, sort=sort, direction=direction_value)

    total = len(events)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_events = events[start:end]
    pagination = SimplePagination(page=page, per_page=per_page, total=total)

    return render_template(
        "events.html",
        events=paginated_events,
        pagination=pagination,
        search=search,
        sort=sort,
        direction=direction_value,
        page=page,
    )


@events_bp.post("/api/events/")
@login_required
def api_create_event():
    data = request.get_json() or {}
    event = create_event(data)
    return jsonify(event.model_dump(by_alias=True)), 201


@events_bp.get("/api/events/<eid>")
@login_required
def api_get_event(eid: str):
    event = get_event(eid)
    if not event:
        abort(404)
    return jsonify(event.model_dump(by_alias=True))


@events_bp.get("/events/<eid>")
@login_required
def event_detail(eid: str):
    """Render the HTML detail view for a single event."""

    participant_sort = request.args.get("participant_sort") or request.args.get("sort", "name")
    participant_direction_raw = request.args.get("participant_direction") or request.args.get("direction", "1")

    try:
        participant_direction = 1 if int(participant_direction_raw) >= 0 else -1
    except (TypeError, ValueError):
        participant_direction = 1

    list_page = request.args.get("list_page", type=int)
    list_sort = request.args.get("list_sort")
    list_direction = request.args.get("list_direction")
    list_search = request.args.get("list_search")

    detail = event_detail_for_display(
        eid, sort=participant_sort, direction=participant_direction
    )
    if not detail:
        abort(404)

    back_params: dict[str, Any] = {
        key: value
        for key, value in {
            "page": list_page,
            "sort": list_sort,
            "direction": list_direction,
            "search": list_search,
        }.items()
        if value not in (None, "")
    }
    back_url = url_for("events.show_events", **back_params)

    return render_template(
        "event_detail.html",
        event=detail.event,
        participants=detail.participants,
        participant_sort=participant_sort,
        participant_direction=participant_direction,
        list_page=list_page,
        list_sort=list_sort,
        list_direction=list_direction,
        list_search=list_search,
        back_url=back_url,
    )


def _parse_event_date(value: str, *, field: str, errors: dict[str, str]) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        errors[field] = "Enter a valid date in YYYY-MM-DD format."
        return None
    return parsed.replace(tzinfo=timezone.utc)


@events_bp.route("/events/<eid>/edit", methods=["GET", "POST"])
@login_required
def edit_event(eid: str):
    """Render and process the HTML form for editing an event."""

    detail = event_detail_for_display(eid)
    if not detail:
        abort(404)

    event = detail.event
    errors: dict[str, str] = {}
    form_errors: list[str] = []

    default_form = {
        "title": event.title or "",
        "type": event.type or "",
        "place": event.place or "",
        "country": event.country_code or "",
        "start_date": event.start_date.strftime("%Y-%m-%d") if event.start_date else "",
        "end_date": event.end_date.strftime("%Y-%m-%d") if event.end_date else "",
    }

    form_data = {key: request.form.get(key, default_form[key]).strip() for key in default_form}

    if request.method == "POST":
        title = form_data["title"]
        if not title:
            errors["title"] = "Title is required."

        start_date = _parse_event_date(form_data["start_date"], field="start_date", errors=errors)
        end_date = _parse_event_date(form_data["end_date"], field="end_date", errors=errors)

        if start_date and end_date and start_date > end_date:
            errors["end_date"] = "End date must be on or after the start date."

        if not errors:
            updates = {
                "title": title,
                "type": form_data["type"] or None,
                "place": form_data["place"],
                "country": form_data["country"] or None,
                "start_date": start_date,
                "end_date": end_date,
            }

            try:
                updated = update_event(eid, updates)
            except ValueError as exc:
                form_errors.append(str(exc))
            else:
                if not updated:
                    abort(404)
                flash("Event updated successfully.", "success")
                return redirect(url_for("events.event_detail", eid=eid))

    return render_template(
        "event_edit.html",
        event=event,
        form_data=form_data,
        errors=errors,
        form_errors=form_errors,
    )


@events_bp.put("/api/events/<eid>")
@login_required
def api_update_event(eid: str):
    data = request.get_json() or {}
    event = update_event(eid, data)
    if not event:
        abort(404)
    return jsonify(event.model_dump(by_alias=True))


@events_bp.delete("/api/events/<eid>")
@login_required
def api_delete_event(eid: str):
    if not delete_event(eid):
        abort(404)
    return "", 204

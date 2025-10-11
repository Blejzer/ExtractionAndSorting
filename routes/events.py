"""API and UI routes for event management."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from math import ceil
from typing import List

from flask import Blueprint, abort, jsonify, render_template, request, url_for

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
def api_list_events():
    events = list_events()
    return jsonify([e.model_dump(by_alias=True) for e in events])


@events_bp.get("/api/events/list")
def api_list_events_alias():
    """Compatibility alias for historical template usage."""

    return api_list_events()


@events_bp.get("/events")
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
def api_create_event():
    data = request.get_json() or {}
    event = create_event(data)
    return jsonify(event.model_dump(by_alias=True)), 201


@events_bp.get("/api/events/<eid>")
def api_get_event(eid: str):
    event = get_event(eid)
    if not event:
        abort(404)
    return jsonify(event.model_dump(by_alias=True))


@events_bp.get("/events/<eid>")
def event_detail(eid: str):
    """Render the HTML detail view for a single event."""

    search = request.args.get("search", "")
    sort = request.args.get("sort", "name")
    direction = request.args.get("direction", "1")
    page = request.args.get("page", type=int) or 1

    try:
        direction_value = 1 if int(direction) >= 0 else -1
    except (TypeError, ValueError):
        direction_value = 1

    detail = event_detail_for_display(eid, sort=sort, direction=direction_value)
    if not detail:
        abort(404)

    return render_template(
        "event_detail.html",
        event=detail.event,
        participants=[asdict(p) for p in detail.participants],
        sort=sort,
        direction=direction_value,
        page=page,
        search=search,
    )


@events_bp.put("/api/events/<eid>")
def api_update_event(eid: str):
    data = request.get_json() or {}
    event = update_event(eid, data)
    if not event:
        abort(404)
    return jsonify(event.model_dump(by_alias=True))


@events_bp.delete("/api/events/<eid>")
def api_delete_event(eid: str):
    if not delete_event(eid):
        abort(404)
    return "", 204

# routes/participant_dtos.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_paginate import Pagination
# from services.participant_service import (
#     get_participants, get_countries_map, attach_country_names, paginate_list,
#     get_participant_by_pid, update_participant, get_participant_country_name,
#     get_events_for_participant
# )
from middleware.auth import login_required



participants_bp = Blueprint("participants", __name__)

@participants_bp.route("/participants")
@login_required
def show_participants():
    # Query params
    search = request.args.get("search", "")
    sort_field = request.args.get("sort", "pid")
    sort_direction = int(request.args.get("direction", 1))

    # TODO: Fetch rows
    # cursor = get_participants(search, sort_field, sort_direction)
    # participants = list(cursor)

    # TODO: Enrich with country names efficiently
    # countries_map = get_countries_map()
    # participants = attach_country_names(participants, countries_map)

    # TODO: Pagination
    # paginated, page, per_page, total = paginate_list(participants)
    # pagination = Pagination(page=page, per_page=per_page, total=total, css_framework="bootstrap5")

    return render_template(
        "participants.html",
        # TODO: Pass these to the template
        # participants=paginated,
        # search=search,
        # sort=sort_field,
        # direction=sort_direction,
        # pagination=pagination,
        # page=page
    )

@participants_bp.route("/participant/<pid>")
@login_required
def participant_detail(pid):
    # Persist navigation context
    page = request.args.get("page", default=1, type=int)
    search = request.args.get("search", default="", type=str)
    sort = request.args.get("sort", default="name", type=str)
    back_url = request.referrer or url_for("main.show_home")

    # TODO: Fetch participant and events
    # participant = get_participant_by_pid(pid)
    # if not participant:
    #     return "Participant not found", 404
    #
    # participant["country"] = get_participant_country_name(participant)
    # events = get_events_for_participant(pid)

    return render_template(
        "participant_event.html",
        # TODO: Pass these to the template
        # participant=participant,
        # events=events,
        # back_url=back_url,
        # page=page,
        # search=search,
        # sort=sort
    )

@participants_bp.route("/participant/<pid>/edit", methods=["GET", "POST"])
@login_required
def edit_participant(pid):
    # TODO: Fetch participant
    # participant = get_participant_by_pid(pid)
    # if not participant:
    #     return "Participant not found", 404
    #
    # # Preserve nav context for back/redirect
    # page = request.args.get("page", "1")
    # search = request.args.get("search", "")
    # sort = request.args.get("sort", "")
    #
    # if request.method == "POST":
    #     name = (request.form.get("name") or "").strip()
    #     position = (request.form.get("position") or "").strip()
    #     grade = (request.form.get("grade") or "").strip()
    #
    #     # Keep your LASTNAME-in-caps normalization here if desired:
    #     def normalize_lastname_caps(full_name: str) -> str:
    #         if full_name.isupper():
    #             return full_name
    #         parts = full_name.split()
    #         if len(parts) <= 1:
    #             return full_name
    #         return " ".join(parts[:-1]) + " " + parts[-1].upper()
    #
    #     name = normalize_lastname_caps(name)
    #
    #     count = update_participant(pid, name, position, grade)
    #     if count == 0:
    #         flash("No changes saved (record not found or identical values).", "warning")
    #     else:
    #         flash("Participant updated successfully.", "success")
    #
    #     return redirect(url_for('participants.participant_detail',
    #                             pid=pid, page=page, search=search, sort=sort))

    # GET → render the edit form (use your participant_edit.html)
    return render_template(
        "participant_edit.html",
        # TODO: Pass these to the template
        # participant=participant,
        # page=page,
        # search=search,
        # sort=sort
    )
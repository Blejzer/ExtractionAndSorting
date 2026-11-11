# Event evaluation summaries

Open an event and select **Evaluations**. Paste one event-wide summary, preview it and select **Save evaluation**. To correct a saved summary, edit its source text, preview again and select **Replace saved evaluation**.

Supported input: rating averages and a stated scale; Yes/No/Maybe counts such as `12Y 9N`; overall average, satisfaction, pretest/posttest percentages and reported knowledge increase; recommendations, future training topics and other comments. Decimal commas and label/value lines or tab/pipe table rows are supported.

The summary is aggregate data. Choice totals do not establish sample sizes for rating averages. Comment paragraphs do not identify respondents. Repeated comments and requested topics are retained. Original percentages and averages are stored as reported; the pre/post difference is separately calculated in percentage points.

## Analysis and storage

CSV exports contain event ID, record type, stable question key, wording, value, unit, response option, comment category/text and applicable scale limits. A derived pre/post-difference row uses the explicit unit `percentage_points`. This allows combining exports across events without confusing source percentages, counts or scores. New questions receive a deterministic key from their wording.

Reports use the existing database connection and the `event_evaluations` collection. Event ID is both `_id` and `event_id`, so there is one current summary per event. Documents contain original `raw_text`, schema-versioned `report`, optimistic-lock `version`, and created/updated timestamps and actors. Event metadata and rosters are not replaced. The existing database user needs read/write access to this collection.

Authentication is required for pages, saves and exports. Saves require CSRF protection and a session-bound preview of the exact text and event, expiring after 30 minutes. Stale/repeated saves cannot overwrite a newer revision. Invalid scores/percentages, conflicting scales and duplicate questions require correction. Unrecognized text stays in the original paste and parsing warnings are shown for review.

## Validation

```sh
python -m pytest -q tests/test_event_evaluation_service.py tests/test_event_evaluations.py tests/test_event_detail_route.py
```

Tests use fictional comments and an in-memory database substitute; they do not connect to Atlas.

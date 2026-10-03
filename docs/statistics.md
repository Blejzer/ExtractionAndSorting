# Attendance, training areas, and participant statistics

The authenticated **Statistics** navigation entry opens `/statistics`. The same
filtered report is available as JSON at `/api/statistics`. Reports use the app's
existing MongoDB connection and never modify records or biographies.

## Invitation policies

No setup is required. Reports automatically apply the programme owner's rules:
seven countries with three expected attendees per country in recent years, and
six countries (excluding Croatia) with four expected attendees each earlier.
The approximate transition defaults to **2021-01-01**, based on the owner's
October 2026 description of "the past five or so years." This date remains fixed
as time passes. If Croatian attendance is recorded earlier, the estimate moves
back to that earliest event. Later first Croatian attendance never postpones the
transition or conceals intervening no-shows. Inference uses the full dataset
before year/area filtering and is clearly labeled as estimated in HTML and JSON.

For an exact known transition, optionally set
`STATISTICS_POLICY_CHANGE_DATE=YYYY-MM-DD` or use the optional date correction in
the form. A supplied date takes precedence; clearing it restores automatic mode.
The cutoff is inclusive. Actual attendance totals never depend on this date.

For exceptions, edit an event's **Statistics settings**, choose an event-specific
policy, select its invited countries, and enter the expected allocation. These
settings override the historical policy. An empty invited-country selection
means none of the seven regional countries were invited, not all countries.

- **No-show:** zero attendees from an invited country.
- **Attended below allocation:** one or more attendees, fewer than expected.
- **Met / above allocation:** exactly / more than expected.
- **Unfilled places** and **additional attendees** are calculated separately for
  each country. Extra attendees never cancel another country's shortfall.
- **Places filled** caps each country's numerator at its allocation, then divides
  by its expected places across resolved events with a known allocation.
- Events with unresolved attendee countries are not assessed for shortfalls or
  no-shows, because an unidentified attendee could belong to the absent country.
- Empty rosters are also unassessed: no attendance uploaded is not evidence that
  every invited country failed to attend.

Reports combine the stored event roster with participant-event links and
deduplicate `(event, participant)` pairs. **Attendances** count those pairs;
**unique people** count distinct PIDs. Programme IDs, Mongo IDs, and embedded
roster references resolve to the same person/event where both IDs are stored.
Country catalog ISO codes supplement names and CID references. Future events
without uploaded attendance are excluded. Those with uploaded attendance are
included and flagged, consistent with the attendance-only upload rule; future
dates are not used for age or police-service estimates. Undated events appear
only under All years. Dangling links to nonexistent events are counted in data
coverage warnings. Missing/duplicate event identifiers are also reported.
All uploaded attendee records are treated as actual attendance, per the existing
business process. Repeat attendance is not classified as a negative outcome.
Unavailable invitation comparisons display a dash instead of an apparent zero.
If no attendance matches the selected events, the report explains this and shows
the stored profile/link counts instead of implying that nobody participated.

## Training areas

Automatic multi-area detection uses English and common Bosnian/Croatian/Serbian
event titles: cybercrime,
cryptocurrency, dark web, financial crime/money laundering, narcotics, human
trafficking/migrant smuggling, firearms, corruption, environmental crime, and
cross-cutting organized-crime investigations. Generic organized-crime wording
does not add an umbrella category to an already specific event. Unrecognized
titles remain **Unclassified**; inference is never written to the database.

Use reviewed selections in the event edit form to override suggestions, including
an explicitly empty selection. An event may contribute to several areas, so
category totals can exceed overall totals. People are deduplicated within each
area. The year and area filters apply to all report sections.
The country-by-area table shows attendances and unique people separately.

## Diversity and police experience

Diversity counts unique attendees in the selected events. Age uses date of birth
at each person's latest dated event in that selection. Other profile breakdowns
use current stored values, not reconstructed historical affiliations. Unknown
fields and missing profiles remain visible in coverage figures.

Police-experience extraction is local and deterministic; it does not call an AI
service. It recognizes numeric total-service statements and police joining years
in English and common Bosnian/Croatian/Serbian wording. It preserves supporting
bio text and distinguishes:

- **Stated duration:** used as stated, without increasing it based on import or
  profile-update timestamps, which are not biography reference dates.
- **Years since joining (estimate):** event year minus joining year; this does not
  verify continuous service and is deliberately an estimate.
- **Needs review:** approximate, conflicting, interrupted, implausible, or
  role-specific evidence, or a joining year with only undated attendance.
- **Not found:** no recognized clear statement, including unsupported wording.

The displayed median and bands include both stated durations and joining-year
estimates; extraction coverage and methods are shown alongside them. They are
not verified employment histories. Bios and extraction results are not persisted.

The adapter makes four projected collection reads; contact information, travel
documents, and banking details are not requested. Costs, test outcomes, and
training-level progression are outside this change.

## Validation

Run `python -m pytest`. Calculation tests cover historical invitation rules,
duplicate/partial rosters, missing profiles, multi-area counting, filtering,
person-weighted diversity, and biography uncertainty. Route tests cover login,
HTML escaping, invalid filters, DB-unavailability handling, and metadata editing.
Live figures must be checked in an environment with working Atlas connectivity.

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
Country catalog ISO codes supplement names and CID references.
Country labels containing recognized regional suffixes such as
`Bosnia and Herzegovina, Europe & Eurasia` and `Serbia, Europe & Eurasia` resolve
to the underlying country. Stored catalog labels are unchanged; unknown suffixes
and lists of multiple countries are not treated as a single country.
Future events without uploaded attendance are excluded. Those with uploaded attendance are
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

## Diversity and professional experience

Diversity counts unique attendees in the selected events. Age uses date of birth
at each person's latest dated event in that selection. Organization is grouped
into **Police** and **Prosecutor**, using the stored employer or position and,
when these do not establish a sector, explicit current employment in the bio.
Specific agency names are not reported as organization categories. Rank is not
used to classify this organization split. Ambiguous employers, other sectors,
and missing profiles contribute to a separate unclassified-people count.
Shares use all unique selected people, so the two categories need not sum to 100%.
Police employer aliases include ASP, MUP, FMUP, MVR/МВР, SIPA, FUP, UKP and
SBPOK, alongside full agency names and local-language police/prosecution names.
Explicit current employment in local-language biographies (for example,
“Zaposlen u Upravi policije”) can supply a category when employer and position
are missing. Past employment and training mentions alone do not establish one.
**Review unclassified organizations** under Diversity lists each remaining
attendee's PID, name, country, organization, position and stored short bio.
The list follows the event filters and counts each PID once. It reads the
existing report data and makes no profile changes. Missing employment details
remain unclassified rather than automatically being assigned a sector.
The diversity view contains gender, age, and this organization split, with
**gender by country** retained. Personal-role detection remains internal to
experience extraction to distinguish service in different professions.

Regional acronym references: [SIPA](https://www.sipa.gov.ba/en/about-us/general-info),
[FUP/FMUP](https://www.fmup.gov.ba/v2/stranica.php?idstranica=8),
[MVR](https://mvr.gov.mk/en-GB/ministerstvo), and
[UKP/SBPOK](https://www.mup.gov.rs/wps/wcm/connect/4bcf62ca-2eef-476c-9dec-d371ddc19ac7/lat-program%2Bza%2Bborbu%2Bprotiv%2Btrgovine%2Bl%D1%98udima%2B2024-2029.pdf?CVID=oW96pNm&MOD=AJPERES).

Country evidence rows prefer the stored profile affiliation, then a consistent
stored attendance affiliation in the selection. Countries are resolved exclusively
from these represented-country references through the country catalog. Missing or
unresolved references remain Unknown. Position, organization, biography, name,
citizenship, and travel information are not used to assign represented country.
Unknown fields and missing profiles remain visible in coverage figures.

Professional-experience extraction is local and deterministic; it does not call an
AI service. It recognizes numeric service statements in police, prosecution,
judiciary, legal practice, and customs, plus existing police joining-year and BCS
total-service wording and dated employment timelines. It records the scope,
preserves supporting text, and distinguishes:

- **Stated duration:** used as stated, without increasing it based on import or
  profile-update timestamps, which are not biography reference dates.
- **Years since joining (estimate):** event year minus joining year; this does not
  verify continuous service and is deliberately an estimate.
- **Career timeline (estimate):** earliest relevant employment year to the selected
  event year (or the last stated employment endpoint). Dated employment bullets
  must have continuous calendar-year coverage; gaps, conflicting ranges, and
  unknown endpoints require review. Graduation and bar-exam dates do not establish
  employment. Support roles contribute to a professional career and are labeled;
  the first appointment in the person's professional role is calculated separately.
- **Stated lower bound:** "over 20 years" displays **20+**, with
  `min_years=20`, `qualifier=more_than`, and `years=null`. "At least 20" and "20+"
  preserve an inclusive lower-bound qualifier instead.
- **Stated range / Approximate duration:** range endpoints or the original
  approximation are retained; they are not converted to an exact value.
- **Needs review:** conflicting, negated, interrupted, implausible, or unit-specific
  evidence, or a joining year with no usable past attendance date.
- **Not found:** no recognized clear statement, including unsupported wording.

Experience coverage includes recognized bounds, ranges, and approximations.
The median and bands include only exact stated durations and dated career
estimates. Excluded bounds/ranges/approximations have separate counts, and the
point-value denominator is displayed. Different career durations are never added:
the detected personal role selects its scope when multiple professions are
mentioned; otherwise conflicting or multiple-profession evidence needs review.
These are not verified employment histories. Bios and inferred fields are not
persisted or written back to participant profiles.

For the supplied Danica Arapović Kovačević example, the report detects
**20+ years in prosecution**, grouped under **Prosecutor**, with the
sentence "I have been Cantonal prosecutor for over 20 years" as evidence. Her
stored `C027` represented-country reference resolves to BiH through the catalog.
The supplied Miroslav Filipović timeline establishes a prosecution-office career
starting in 1996 as an expert assistant, and personal appointment as a prosecutor
from 1999. At the 2026 event these yield **30 career years** and **27 years as a
prosecutor**, both calendar-year estimates. He is grouped under **Prosecutor**.
His stored `C194` represented-country
reference resolves to Serbia through the catalog.

### Diagnosing missing participant fields

Run the read-only calls in [statistics-diagnostics.js](statistics-diagnostics.js)
against the application's database in `mongosh`. They return only the two
requested participant profiles' reporting fields, country catalog, and their
stored country affiliations in attendance links. No credentials, contact data,
travel documents, or financial details are requested. These fields distinguish
an unresolved country reference from a missing biography or unsupported wording.

The adapter makes four projected collection reads; contact information, travel
documents, and banking details are not requested. Costs, test outcomes, and
training-level progression are outside this change.

## Validation

Run `python -m pytest`. Calculation tests cover historical invitation rules,
duplicate/partial rosters, missing profiles, multi-area counting, filtering,
person-weighted diversity, and biography uncertainty. Route tests cover login,
HTML escaping, invalid filters, DB-unavailability handling, and metadata editing.
Live figures must be checked in an environment with working Atlas connectivity.

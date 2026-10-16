// Run in mongosh with the application's database already selected.
// Read-only diagnostics for the two rows reported as Unknown.
const statisticsParticipantIds = ["P0104", "P0304"];
printjson({participants: db.participants.find(
  {pid: {$in: statisticsParticipantIds}},
  {_id: 0, pid: 1, representing_country: 1, position: 1, organization: 1, rank: 1, bio_short: 1}
).toArray()});
printjson({countries: db.countries.find(
  {}, {_id: 1, cid: 1, country: 1, iso: 1}
).toArray()});
printjson({attendanceCountries: db.participant_events.find(
  {participant_id: {$in: statisticsParticipantIds}},
  {_id: 0, participant_id: 1, event_id: 1, representing_country: 1}
).toArray()});

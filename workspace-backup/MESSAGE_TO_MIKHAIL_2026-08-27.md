# Slack message to Mikhail — 2026-08-27

Attach: `stage_events_examples.csv` (25 rows)

---

Hey Mikhail, quick question on stage events.

I'm seeing some candidates where a whole run of stages gets written at the exact same millisecond.
Recruiter Screen, Moved to ATS, Onsite, Offer, Hired all at once, when the candidate is actually
still sitting in Sequence.

It's not every candidate, and I know this has been around a while. I'm trying to work out from a
user perspective what someone is actually doing when it happens. Any idea what triggers it?

Example: `1787796628310x620175981918488200` at `2026-08-27 02:10:31.632`, plus 25 more in the file.

---

## Only if he asks. Do not volunteer.

**How often:** share of all stage events of that type landing in a same-timestamp batch.
Offer 38% (3,879/10,303), Hired 36% (3,532/9,903), Final Interview 33% (4,310/12,928),
Offsite 10% (5,073/53,022), Recruiter Screen 4%, Contacted 0.2%.

**How long:** back to Jan 2024. Worst month on record is Apr 2024. Not caused by the new funnel.

**Why we care now:** existing reports read the candidate's current stage, not the event log, so this
has never affected anyone's numbers. Stage-to-stage conversion has to come from events, which is
where it starts to matter.

**Useful follow-up if he confirms it's expected:** is there a field that separates a real stage move
from one of these? The duplicate flag doesn't work, it groups on event type rather than target
stage, so it also flags legitimate same-day moves.

## Separate message, another day
Of the 8 "Sequence" stages created 24 Aug 07:55-08:00, seven are typed `Contacted` and one
`Positive Response`. Point_of_process values are 2, 2.1, 3.6 and 4. Intentional?

# Gotchas — the dumping ground

Anything you know about the data / dashboards / pipeline that isn't captured in the per-page homework files. No structure required. Bullet points, paragraphs, whatever. Future-you and future-Blake will thank you.

## What belongs here

Roughly: "things I know that would take a stranger 3 days to figure out."

Examples of the kind of thing we'd love to have:
- Stage name quirks ("`Actual Screen` is not the same as `Screen` in the ATS — the actual screen needs an evaluation event + note")
- Date-column weirdness ("`date_contacted` is the *first* contact, `date_contacted_latest` is the most recent — most measures use the first")
- Source system idiosyncrasies ("Eucalyptus sends duplicate events when a recruiter edits a note; we dedupe on (candidate_id, event_type, date_created)")
- Test data and how to spot it ("anything with `job_title` starting with `TEST` or `SAMPLE` should be excluded")
- Client system differences ("Wolt uses our API directly; DoorDash uses a middleware that delays events by ~15min; Recruitee is a totally separate system")
- Things that are counterintuitive in the data ("`is_archived` goes TRUE when the job closes OR when it's deleted — they're different but we treat them the same")
- Workarounds you've put in for past bugs ("there was a 2-week window in Dec 2025 where `who_event_created_for` was null on ~20% of events — I backfilled those from `who_created_event`")
- Deprecated stuff that might still appear in data ("the `talent_status` column is still populated but nobody should filter on it, it hasn't been updated since 2024")
- Weird date-table behaviors ("Calendar WBR is different from Calendar because WBR weeks run Sunday-Saturday, not Monday-Sunday")

---

## Your dump starts here

(format however you want — headings, bullets, stream of consciousness, all fine)



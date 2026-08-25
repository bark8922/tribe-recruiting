# Backend role alert for Sashka

Design spec. Nothing built yet. Blocked on Sashka confirming the filter.

Source: Silver Medalists call, 25 Aug 2026 (Martin, Sashka, Blake).
Fireflies: https://app.fireflies.ai/view/01M0WJNCTKP8REBT6HW26TDF80?t=876

---

## What she asked for

Sashka at 14:36:

> "I talked to Mikhail about me getting notifications if a new backend role opens up in our dashboard. But he told me to reach out to you. So is there an option we can do that? A Slack notification or an email, anything could work."

Blake: "I'll just have it send a direct message to you."
Sashka: "Perfect."

Martin at 15:18 set the constraint:

> "Back end engineering role, we would be open to full stack as well. What if it's gonna be Python engineer? Blake doesn't know that's also backend. Node JS Software Engineer, Python Software Engineer. So now he would just build it for backend engineering and you'd be missing out probably on 50% of the roles."

Blake at 16:36 agreed to work from categories and subcategories rather than title strings, and to send Sashka a sample of recent jobs first.

## Decisions since the call

| Decision | Value |
|---|---|
| Cadence | Once daily digest, one Slack DM |
| Build location | n8n |
| Filter | Confirm with Sashka from the sample before building |

## Why this is not a `subcategory = 'Backend'` filter

2026 client jobs, from `dash_jobs`. Both error directions are real.

False positives inside Backend and Full Stack:

| Job title | Tagged as |
|---|---|
| Salesforce Developer (AVIV, Albania) | Engineering / Full Stack |
| Senior Salesforce Developer (Contract) (AVIV, Zagreb) | Engineering / Android |
| Salesforce developer (internal) | Engineering / Backend |
| software developer (internal, no location) | Engineering / Backend |

False negatives sitting outside Backend:

| Job title | Tagged as |
|---|---|
| Platform Engineer, Event Streaming (Wolt) | Engineering / Not apply |
| Senior Software Engineer (Upvest) | Engineering / Not apply |
| Sr SWE (Platform & Infra) (Aiven) | Engineering / Backend, caught by luck |
| Junior Analytics Engineer (Glovo) | tagged Data & Analytics / Analytics Engineer in Aug, Engineering / Data in Jun |

Same role, two taxonomies, two months apart. The tag is a hint, not a rule.

So the filter needs two passes: a subcategory allowlist from Sashka's answers, plus a title-token rescue for the `Not apply` rows, plus a title-token exclusion for Salesforce and similar.

## 2026 subcategory volumes

Client roles opened 1 Jan to 25 Aug 2026, test/internal/BD excluded.

| Category | Subcategory | Count |
|---|---|---|
| Engineering | Backend | 54 |
| Engineering | Mobile | 23 |
| Engineering | Full Stack | 19 |
| Engineering | Frontend | 19 |
| Engineering (Eng Management) | Not apply | 19 |
| Engineering | Not apply | 12 |
| Engineering | Machine Learning | 9 + 2 (ML / AI) |
| Engineering | Engineering Management | 5 |
| Engineering | Security | 4 |
| Engineering | Data | 4 |
| Engineering | Automation QA | 4 |
| Engineering | Android | 4 |
| Engineering | iOS | 2 |
| Data & Analytics | Data Scientist | 17 |
| Data & Analytics | Data Analyst | 6 |
| Product Management | Product Manager | 19 |

Backend alone is 54 roles a year, about 1 a week. Backend plus Full Stack plus the two `Not apply` buckets is roughly 104, about 2 a week. Either volume is fine for a daily digest.

## Build plan

Clone the structure of the existing n8n workflow **Intake Eligibility - Daily Feed** (`h6zd1GWVqz6caAKh`). It already runs a weekday cron against the same Supabase project and carries the test/BD/internal exclusions.

One change from that workflow: read `dash_jobs`, not `jobs`.

`jobs` has `job_category` but every row is NULL, and it has no `job_subcategory` at all. `dash_jobs` has `job_category`, `job_subcategory` and `job_location` populated, 1958 of 1985 rows carry a subcategory, and it is current to 2026-08-23.

Nodes:

1. **Schedule trigger.** Weekdays 08:00 CET, matching the intake feed.
2. **Get Jobs.** Supabase `dash_jobs`, credential `dNdTbItf3GRYrfvX`.
3. **Filter and dedupe.** Code node:
   - `date_created` within the last 24 hours (48 on Monday, to cover the weekend)
   - `is_job_archived` false
   - client not test / BD / internal, reusing the `excluded()` function from the intake feed
   - subcategory in Sashka's allowlist, OR title matches a rescue token
   - title does not match an exclusion token (salesforce, sap, and whatever else she rejects)
   - dedupe on `job_id` against a ledger, so a re-run or a retag never DMs twice
4. **Slack DM.** One message to Sashka listing the day's matches. Skip the send when the list is empty.

Dedupe ledger: a new Supabase table `sashka_role_alerts (job_id text primary key, sent_at timestamptz)` in the same project, matching how `role_briefings` works for the TA briefing bot. Workflow static data would also work but does not survive a workflow re-import.

## Message format

Send nothing on a day with no matches. Silence is the normal state.

```
2 new roles you might want (25 Aug)

• Senior Backend Engineer — AVIV, Berlin
  Engineering / Backend · opened today · TA: Kristina
• Member of Technical Staff (Fullstack) — Enam, Berlin
  Engineering / Full Stack · opened today · TA: Philip

Not right? Tell Blake which ones to drop.
```

Naming the TA is worth including. Her next step after seeing a role is asking that recruiter for an intro, so the message should already tell her who to ask.

## Open questions for Sashka

1. Which subcategories does she want. The sample sheet answers this.
2. Frontend, Mobile, iOS, Android: in or out. She said "any backend role" but the silver medalist pool is wider than backend.
3. Product Manager and Data Scientist: in or out. Not backend, but 36 roles a year and plausibly matchable.
4. Does she want a weekend digest, or weekdays only.

## Related

- Stage work: `silver-medalist-decisions.md`. The Silver Medalists prospect stage still needs Mikhail, and 15 of 21 active clients have no Prospects stage at all.
- The `new-role-briefing` edge function in tribe-job-intel already DMs TAs when a role opens. If this alert grows past a simple filter, fold it in there instead of maintaining two detectors.

## Files

- `Backend_Role_Alert_Filter_Check.xlsx` — the sample sheet to send Sashka.

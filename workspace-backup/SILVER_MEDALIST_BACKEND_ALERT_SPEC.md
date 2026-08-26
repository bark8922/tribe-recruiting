# Backend role alert for Sashka

**LIVE since 2026-08-26.** DMs Sashka as Tribe Bot every weekday at 09:00 Prague. n8n workflow `UDTat0pq3EueGdxb`, "Sashka Role Alert - Daily Digest".
https://blakebarkley.app.n8n.cloud/workflow/UDTat0pq3EueGdxb

Design spec below. Filter confirmed by Sashka 26 Aug.

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

From the 347 client roles opened 1 April to 23 August 2026 in `dash_jobs`. Both error directions are real.

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

## Source choice

Keboola, GitHub and Supabase are not three sources. They are one chain, each a copy of the one before:

```
Bubble → Keboola / Snowflake → GitHub gz → Supabase
                                            ├─ jobs      (pipeline-cache-refresh edge fn)
                                            └─ dash_*    (dash-load.service on the droplet)
```

`pipeline-cache-refresh` fetches `bark8922/tribe-recruiting/main/recruiting-dashboard/public/dashboard_data_snowflake.json.gz` and upserts into `jobs`. So reading Supabase means reading a copy of a copy of Keboola.

| Source | Reaching it from n8n | Payload for a 2-row lookup | Verdict |
|---|---|---|---|
| Keboola / Snowflake | No Snowflake credential exists in n8n. Storage API table export is async: create export job, poll, download from S3 | small once you have it | Most work, closest to origin |
| GitHub gz | One HTTP GET, but the repo is private so it needs the PAT | ~28 MB uncompressed, ungzip and parse the entire dashboard export in a Code node, daily | Simple call, heavy payload |
| Supabase `dash_jobs` | Native Supabase node, credential `dNdTbItf3GRYrfvX` already in use by the sibling workflow | Postgres filters server-side, returns the 2 matching rows | **Use this** |

Going to Keboola direct buys freshness the digest cannot use. The upstream pipeline is the freshness ceiling either way, and a daily digest does not need sub-day latency. It costs a new Snowflake credential plus a polling loop, to save a few hours of lag on a message that fires once a day.

The gz is a single fetch, but you would download and parse the whole dashboard export every morning to find one or two jobs.

**Use `dash_jobs`, not `jobs`.** `jobs` has a `job_category` column but every row is NULL, because `pipeline-cache-refresh` never maps it, and it has no `job_subcategory` at all. `dash_jobs` has `job_category`, `job_subcategory` and `job_location` populated, 1958 of 1985 rows carry a subcategory, it is current to 2026-08-23, and it holds open roles (28 unarchived vs 17 archived opened since 1 Aug).

### The cost of that choice, and the guard

Two copies deep means two extra things that can break, and `dash_*` is loaded by `dash-load.service` on the droplet, the loader that already has a failure-alert workflow (`MjA9K549jEmYcraq`) because it has failed silently before.

If that loader stops, the digest goes quiet and Sashka reads silence as "no roles opened" rather than "the pipe broke."

So add a staleness guard to the Code node: if `max(date_created)` in `dash_jobs` is more than 3 days old, DM Blake instead of staying silent. Cheap, and it turns a silent failure into a visible one.

## Build plan

Clone the structure of the existing n8n workflow **Intake Eligibility - Daily Feed** (`h6zd1GWVqz6caAKh`). It already runs a weekday cron against the same Supabase project and carries the test/BD/internal exclusions.

Nodes:

1. **Schedule trigger.** Weekdays. Time depends on when the loader runs, see below.
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

## When to fire it

**Weekdays 08:00 CET** (`0 6 * * 1-5` UTC), matching the Intake Eligibility feed.

### Why the loader schedule does not block this

`dash_jobs` is loaded by `dash-load.service` on the droplet and that timer is not visible from inside Supabase. `track_commit_timestamp` is off, so the last write time cannot be read from Postgres either.

It does not matter, because the workflow keys on a **dedupe ledger of job ids**, not on a "last 24 hours" window. A role is sent once, the first digest after it appears in `dash_jobs`. If the loader is late, the role lands in the next morning's digest instead. Latency moves, nothing is ever dropped.

A time window would have had the opposite property: miss the window, lose the role silently.

For reference, the sibling path is known: there is no cron on `pipeline-cache-refresh`; the Supabase `jobs` table refreshes when `roles-roundup-daily-ping` runs at `45 14 * * *`, and `jobs.last_synced_at` read 2026-08-25 14:45:04 UTC, which matches. If `dash-load.service` turns out to run mid-afternoon too, moving the digest to ~17:30 CET would get roles to Sashka the same day instead of the next morning. That is a one-line change to the cron.

### First-run guard

On an empty ledger the workflow would DM all 84 historical matches. Two guards, both needed:

1. Only consider roles with `date_created` within the last 7 days.
2. Seed the ledger with every current `dash_jobs` job id before the first live run.

## Sashka's answers, returned 26 Aug 2026

Google Sheet: https://docs.google.com/spreadsheets/d/1uWUt20yNKEMI4QHQOXjyDb5YGz40kWLLtW-XIYIfg3g

She answered all 87 subcategory buckets. Blake marked 128 rows on tab 2. **Zero conflicts** between the two: every row Blake marked No sits in a bucket she also marked No.

### The 14 buckets she wants

| Category | Subcategory | 2026 roles |
|---|---|---|
| Engineering | Backend | 43 |
| Engineering | Mobile | 23 |
| Engineering | Full Stack | 19 |
| Engineering | Frontend | 13 |
| Engineering | Not apply | 12 |
| Engineering (Engineering Management) | Not apply | 12 |
| Engineering | Engineering Management | 5 |
| Engineering | Data | 4 |
| Engineering | Security | 4 |
| Engineering | Android | 4 |
| Engineering (Engineering Management) | Mobile | 2 |
| Engineering (Engineering Management) | Applications | 2 |
| Engineering | iOS | 1 |
| IT & Technical Support | Salesforce Developer | 1 |

Everything else is No, including Product Manager (19), Data Scientist (8), Machine Learning (6), all QA and DevOps buckets, `Other / Tech`, and `(no category)`.

### Volume this produces

89 of the 347 roles over 21 weeks. 4.2 a week on average, but lumpy: 3 weeks would have been silent, the busiest week had 11. A daily digest handles that shape fine.

### Two corrections to this spec

**Salesforce is wanted, not excluded.** She marked `IT & Technical Support / Salesforce Developer` as Yes. The planned title exclusion for salesforce and sap was my assumption and it was wrong. Drop it.

**The `Not apply` buckets are a grab-bag and she probably did not see what is in them.** Saying Yes to both correctly catches the two roles that motivated this whole exercise, "Platform Engineer, Event Streaming" and "Senior Software Engineer". It also catches 5 roles that contradict her own No answers elsewhere:

| Role caught via `Not apply` | But she said No to |
|---|---|
| Cloud Security Engineer | Engineering / Security |
| Technical Support Engineer - Australia / India | IT & TS / Technical Support Engineer |
| Applied Scientist | Engineering / Machine Learning |
| GTM Engineer | not an engineering role |
| Director IT | IT leadership |

This is not her being careless. It is the tag being empty, which is the whole reason we asked. These are the rows that become the title rules.

## THE FILTER (final, no further input needed)

Two steps. Bucket first, then a title blocklist for the `Not apply` grab-bag.

### Step 1: the 14 buckets above

### Step 2: drop these titles even when the bucket says yes

| Pattern (case-insensitive) | Evidence from her own answers |
|---|---|
| `\bsap\b` | She marked SAP Integration Architect US **and** EMEA as No. Same work, same client. |
| `tech(nical)?\s+support` | She marked Technical Support Engineer Auckland, Technical Support Engineer P1 India, and L1 Tech Support as No. Unanimous, and one of them is the same client. |
| `\bscientist\b` | She marked all 5 Data Scientist roles No, plus both Machine Learning buckets. |
| `\bgtm\b` or `go.to.market` | Go-to-market, not product engineering. Every Sales bucket and `Other / Tech` are No. |
| `^director\s+it\b` or `\bit director\b` | She marked every IT & Technical Support bucket No except Salesforce Developer, including IT System Admin/Engineer. |

Applied to the 347: removes exactly 5 roles, **all of them from `Not apply`**. It removes nothing from any bucket she explicitly chose. That was the safety check.

**Final volume: 84 roles over 21 weeks, 4.0 a week.** 3 silent weeks, busiest week 11.

### The 20 questionable rows, resolved

**Keep, 15.** Six engineering managers (Platform Berlin, T&S Kristina, France Kristina, Recontacting EM France, Head of Engineering, Engineering Director Platforms). She said Yes to six other engineering-manager roles by name, and the only EM she rejected was rejected for being Machine Learning, not for being an EM. Two of these have near-identical twins in her Yes list: "Engineering Manager, Platform - Berlin" and "Engineering Manager Trust & Safety, Berlin (Wlad)".

Three platform and generic software engineers (Platform Engineer Event Streaming, Sr Platform Engineer Contractor, Senior Software Engineer). Precedent is direct: "Sr SWE (Platform & Infra)" in Helsinki and Israel are both Yes via Backend, and "Senior Software Engineer (Finland)" and "(Israel)" are both Yes via Full Stack.

Cloud Security Engineer. She marked `Engineering / Security` Yes, which contains Cyber Defense & Incident Response Engineer.

Five Salesforce roles. Two independent signals point the same way: `Engineering / Full Stack` is Yes and `IT & Technical Support / Salesforce Developer` is Yes.

**Drop, 5.** SAP Integration Engineer, Technical Support Engineer, Applied Scientist, GTM Engineer, Director IT. Each has a direct precedent she rejected, listed in the table above.

### Two judgement calls worth knowing about

**Security is incoherent in her answers, and I biased toward including.** Four security roles, split by which bucket they landed in: Cyber Defense (Yes, Security bucket) and Cloud Security Engineer (Yes, via Not apply) get through; Security Engineer (No, DevOps/SRE bucket) and Security Platform Engineer (No, Eng Mgmt/DevOps bucket) do not. That split is the tag talking, not her. If she complains about security roles, the fix is one line.

**Senior Salesforce Adminstrator is the weakest keep.** It is an administrator, not a developer, and she rejected every other IT & Technical Support admin bucket. But it is the only role in the bucket she marked Yes, so dropping it would override an explicit answer with an inference. Kept. One role in five months either way.

## What was actually built, 26 Aug 2026

Workflow `UDTat0pq3EueGdxb`, 9 nodes, **inactive**. Timezone pinned to `Europe/Prague`, errors routed to the shared `Error Handler` (`8PW5GEV1I4Jn80rQ`).

| Node | Does |
|---|---|
| Every Weekday 9am Prague | `0 0 9 * * 1-5`, timezone pinned so it stays 9am across DST |
| Get Recent Jobs | Supabase `dash_jobs`, server-side `date_created=gte.` last 30 days |
| Get Sent Ledger | Supabase `sashka_role_alerts`, `executeOnce` |
| Build Digest | staleness guard, 7-day window, 14 buckets, title blocklist, junk rows, dedupe, message |
| Loader Stale? | routes to the alert or the digest |
| DM Stale Alert | Blake, when the newest role is >3 days old |
| DM Digest to Sashka | sends as **Tribe Bot** to Sashka Sarafinovska, `U04M6HZDY8M` |
| Split Job Ids | one item per matched role |
| Record Sent | writes job_id, title, client, sent_at, dry_run to the ledger |

### Deviations from the plan above

**30-day query window, not 7.** The spec's staleness guard needs `max(date_created)`, which a 7-day filter cannot produce once the loader stops: zero rows returned looks identical to zero roles opened. Querying 30 days (about 50 rows) makes the guard readable. The 7-day candidate window still applies, in the Code node.

**Salesforce/SAP exclusion.** Salesforce is kept, per her answer. SAP stays blocked, as the spec's own correction says.

**Blocklist is token matching, not regex.** Same five rules, implemented by normalising the title to space-delimited tokens and doing `indexOf(" sap ")` and so on. Avoids escaping bugs and handles `go-to-market` for free.

### Two bugs found by running it

**Ledger write was silently skipped.** `Split Job Ids` read `$json.job_ids`, but the Slack node replaces item JSON, so it got `undefined`, emitted zero items, and `Record Sent` never ran. The execution still reported success. Left alone, the same roles would have been DM'd every single day forever. Now reads `$('Build Digest')` directly.

**Ledger columns were null.** `Record Sent` mapped only `job_id` and `sent_at`. Dedupe was fine, the table was unreadable. Now writes all four.

### Verification, three runs

1. Sent a 2-role digest: Parloa Senior Engineer/Staff/Principal (Backend, Filip) and Enam Member of Technical Staff Fullstack (Full Stack, Aleksandra). `Record Sent` did not run, which is how the first bug surfaced.
2. After the fix, both rows written to the ledger.
3. Third run produced zero matches, no DM, no write. Dedupe confirmed closed.

The ledger was already seeded (`seeded: true`, 10:16 on 26 Aug), so the first-run guard is satisfied.

### Message format and sender, decided 26 Aug

Sends as **Tribe Bot** (n8n credential `Tribe Bot Bot`, id `Yn3CT08H4JCJIQld`, Slack app `A0AESHNF9AS`, bot `B0AED6DGKJB`). Confirmed by test DM: it displays as "Tribe Bot" with a custom avatar, not as anything referencing Blake. A bot DM always shows the owning Slack app as sender, never the person who built the workflow.

Header is `Silver Medalist watch: N new roles`. Each role renders as:

```
• *<link|Job Title>*
   Client · Location
   Category / Subcategory · opened YYYY-MM-DD · TA: name
```

**Job links.** Title links to `https://overview.tribe.xyz/board_view/{job_id}` using the bare Bubble unique id. Blake confirmed 26 Aug that this opens the correct role.

The links people paste in Slack look like `board_view/senior-ta-high-volume---dutch-1779368342428x874602515472056300`, a slug plus the id. **That slug cannot be rebuilt from `job_title`**: "Senior Talent Acquisition Partner (High Volume)" becomes `senior-ta-high-volume---dutch`, which is a separate Bubble field, not a slugified title. It is absent from `out.c-reporting-v2.job` and from `dash_jobs`. Carrying real slugs would mean adding the field to the Bubble extract and threading it through Keboola into `dash_jobs`. Not worth it, since the bare id resolves.

Closing line is `Are these roles relevant? If not, please let Blake know.` The earlier "Not right? Tell Blake which ones to drop" assumed she already knew what the digest was and what dropping meant.

### Sashka's identity, verified

Sashka Sarafinovska, `U04M6HZDY8M`, Talent Coordinator. Confirmed against both the Slack directory and Blake's own screenshot before anything was sent. Worth stating because **Aleksandra Vistac (`U03BFLEC43F`) is a different person** who appears inside the digest as the TA on the Enam role. "Sashka" is a diminutive of Aleksandra, so the two are easy to confuse.

### Test to Sashka, 26 Aug

Sent successfully. Tribe Bot opened its own DM channel with her, `D0BSN25NMV1`, which proves the app is permitted to message her. A dry run to Blake could never have proven that, since Slack apps can be restricted per user.

She received it cold, with no heads-up, listing the Parloa and Enam roles.

**No duplicate risk.** Both test roles were already written to the ledger by the earlier real run, so a run immediately after the test produced zero matches. The forced test send re-fired them by blinding the lookup, not by removing the rows.

### Activated 2026-08-26

Published, `activeVersionId` `3d0dc5f5-bd84-4ffc-8e4d-7fb37b2fe9c0`. First scheduled run is the next weekday at 09:00 Prague.

**Expect silence at first.** Parloa leaves the 7-day window on 27 Aug and Enam on 30 Aug, and both are already in the ledger. Unless a new Engineering role opens, the first live runs send nothing. That is the design working, not a failure, so do not go hunting for a break.

**`dry_run` is still hardcoded `true`** on new ledger rows in `Record Sent`. Harmless, it is only a label on the ledger, but it now says something untrue. Flip it if the column ever gets used for anything.

### How to tell it is working

- n8n executions list for `UDTat0pq3EueGdxb`. A silent day is a successful execution ending at `Build Digest` with zero items, not an error.
- `select * from sashka_role_alerts order by sent_at desc` shows what has actually been sent.
- If `dash-load.service` stops, Blake gets the staleness DM instead, and Sashka gets nothing. Silence on Sashka's side alone never means the loader died.

### Do not repeat this mistake

To force a test digest when the ledger already blocks every candidate role, do **not** delete ledger rows. Instead set `Get Sent Ledger` `filterType` to `string` with a filter matching nothing, and disable `Record Sent`. Then revert both, and clear `filterString`. A leftover test filter would silently empty the ledger and re-send every role.

## Remaining minor items

1. Weekdays only, or a weekend digest too.
2. `(no category)` is a No, so a role that loses its tag is invisible to the alert. 4 roles since April.
3. The digest message should carry a "wrong? tell Blake" line so her corrections replace these inferences over time.

## Related

- Stage work: `silver-medalist-decisions.md`. The Silver Medalists prospect stage still needs Mikhail, and 15 of 21 active clients have no Prospects stage at all.
- The `new-role-briefing` edge function in tribe-job-intel already DMs TAs when a role opens. If this alert grows past a simple filter, fold it in there instead of maintaining two detectors.

## Files

- `Role_Alert_Filter_Check.xlsx` — the sheet to send Sashka. **347 roles, every function, 1 April to 23 August 2026.** One Yes/No column, plus a tab of all 87 category/subcategory pairs used in 2026.
- `Backend_Role_Alert_Filter_Check.xlsx` — superseded, delete it. It covered only 161 roles and only some categories.

### Why the sheet is all functions, not just engineering

A first version filtered to engineering-ish categories. That was the same mistake Martin warned about, made one level higher: instead of hand-picking which titles count, it hand-picked which categories count. It also came out inconsistent, including 22 `Other / Non-Tech` store managers while excluding all 92 Sales and 44 Operations roles, which from Sashka's side looks arbitrary.

The sheet now carries every role and lets her draw the line.

| Function | Roles |
|---|---|
| Sales | 92 |
| Engineering (all 4 category spellings) | 104 |
| Operations | 42 |
| Marketing | 29 |
| Other | 24 |
| Product Management | 14 |
| Data & Analytics | 12 |
| People & Talent (HR) | 8 |
| IT & Technical Support | 7 |
| Design | 6 |
| Finance & Accounting | 4 |
| No category set | 4 |
| Customer Support, Legal | 2 |

347 rows is fewer decisions than it looks. She filters by Category and answers a whole block at a time.

## Junk rows the live filter must exclude

Three rows dated 2026-06-04 have the job title literally `"title"`, subcategory Operations Associate, and are still marked Open. Two have no client name at all, one is attached to Aiven.

Whatever filter gets built, exclude rows with no `client_name` and rows whose title is a placeholder. Otherwise the digest will one day DM Sashka about a role called "title".

## A note on volumes

`dash_jobs` gives 584 client roles for 2026 after exclusions. An earlier count off Keboola's `out.c-reporting-v2.job` gave higher subcategory numbers because it only filtered `test = FALSE` and still included Tribe internal, Tribe IR and BD pipelines. The sheet's row list and its subcategory tab are now both computed from the same filtered set, so they agree with each other.

## Security note, unrelated to this build

`dash_jobs` and `jobs` both have row level security disabled, so the project's publishable anon key can read every row of both. Worth deciding whether that is intended.

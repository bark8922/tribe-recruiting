# Andy — Power BI handover homework

Hey Andy — this is a structured "homework packet" to capture the stuff that's in your head about the WBR/MBR Power BI dashboards before you leave. We already have the DAX (we extracted it from the PBIP), so we're NOT asking you to re-explain any formula. We're asking for the *stuff the DAX doesn't tell us* — why you picked this relationship over that one, which clients chronically look weird and why, which measures on each page are actually load-bearing vs decorative.

No calls, no screenshares. Just text boxes to fill in, one page at a time, in any order.

---

## Time budget

- **8 pages**, ~20-30 min each = **~4 hours**
- **+ 3 global files**, ~15-30 min each = **~1 hour**
- **+ one CSV export per page** (Power BI "Export data" → dump in `snapshots/`), ~5-10 min each
- **Total: ~5-6 hours**, spread over however many days you want

You can do this in 30-min chunks. You can skip questions you can't answer — a short "n/a" is fine. Finishing all 8 pages at 80% is more useful than perfecting one.

---

## What's in this packet

```
andy-homework/
├── README.md                             ← you are here
├── pages/                                ← one file per dashboard page
│   ├── 01-time-to-hire.md
│   ├── 02-kpi-ta-summary.md
│   ├── 03-kpi-ts-summary.md
│   ├── 04-internal-recruitment.md
│   ├── 05-new-role-estimate.md
│   ├── 06-pipelines-health.md
│   ├── 07-sourcing-stats.md
│   └── 08-ta-actual-screens-target.md
├── global/                               ← cross-page knowledge
│   ├── client-mappings.md
│   ├── relationships.md
│   └── gotchas.md
└── snapshots/                            ← drop CSV exports here
    └── README.md
```

---

## How each page file is structured

Each `pages/*.md` file has two halves:

1. **Reference section (prefilled, don't edit)** — lists every visual on the page, every measure used with its full DAX, every calculated column, every table touched. This is so you don't have to open Power BI to remember what's on the page.
2. **Homework section (you fill in)** — 9 structured questions. Each has a `_Your answer:_` line or check-boxes. Just type underneath.

The 9 questions are the same on every page:

1. Who uses this page and how often?
2. In plain English, what question does this page answer?
3. Which measures are the "real answer" on this page? (check up to 3)
4. For each load-bearing measure — why THIS `USERELATIONSHIP` vs alternatives?
5. Default filter rules (what's applied when someone opens the page cold?)
6. Known outliers (TAs / clients / roles that chronically look weird, and why)
7. Known bugs or workarounds
8. If a stranger had to rebuild this in SQL tomorrow, what are the top 3 gotchas?
9. CSV snapshot — export the main table, drop it in `snapshots/`

**Question 4 is the single highest-value field.** The DAX says `USERELATIONSHIP('Calendar'[Date], candidate_stage[date_contacted])` — but doesn't say *why* you used `date_contacted` vs `date_screen` vs `date_hired`. That's the knowledge we need from you.

**Question 8 is the second-most-valuable.** It's a dumping ground for gotchas — "don't forget to exclude test clients," "stages get renamed in the Wolt ATS," etc. No formatting rules, just dump what's in your head.

---

## Recommended order (but do them in any order that works for you)

1. `07-sourcing-stats.md` — easiest warmup, you know this one cold
2. `02-kpi-ta-summary.md` and `03-kpi-ts-summary.md` — sequential, you built these
3. `08-ta-actual-screens-target.md` — closely related to the KPI pages
4. `01-time-to-hire.md` — we're actively hitting drift on this in our rebuild, so this one is hot
5. `06-pipelines-health.md` — operational
6. `05-new-role-estimate.md` — unique predictive logic, most valuable to capture
7. `04-internal-recruitment.md` — save for last, different domain

---

## Global files (do these once, they cover all pages)

- **`global/client-mappings.md`** — the canonical list of client-name merges and splits. Wolt HQ vs Wolt NBB, DoorDash vs 7Rooms, anything that's ever been renamed. We've pre-listed what we know; you extend and correct.
- **`global/relationships.md`** — one line per **inactive** relationship in the model: in which measure do you activate this, and why? We pre-listed all 45 relationships (32 active, 13 inactive); you only need to annotate the 13 inactive ones. The active ones get used by default — no prompt needed unless there's something weird about one.
- **`global/gotchas.md`** — free-form dumping ground. Anything not covered by the per-page files. Stage renaming quirks, event-type weirdness, test-client lists, date-column tricks, Recruitee-vs-standard distinctions, anything.

---

## Snapshots

For each page, export the main table visual(s) — the ones with the actual numbers, not the slicers/headers/icons — as CSV for the **last 4 full weeks** (or whatever window makes sense). Drop them in `snapshots/`. Whatever Power BI spits out is fine; we'll format-wrangle later.

These snapshots are **ground truth**. Future you (or future me, or whoever picks this up in 2027) uses them to verify a rebuild is producing the right numbers. Without these, we have logic but no way to know we got it right.

---

## If you get stuck

- Can't answer a question → write "n/a" or a one-liner and move on. Finishing is better than perfect.
- Question doesn't fit the page → skip it, note at bottom.
- Want to add a section → go for it, any format.
- Dashboard isn't loading or you can't export → ping Blake, we'll find a workaround.

**Ping Blake directly (Slack or email) with any questions.** No need to go through a thread.

Thanks Andy — this makes a huge difference for whoever picks this up after you. 🙏

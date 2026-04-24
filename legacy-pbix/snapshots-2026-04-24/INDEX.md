# Power BI Data Snapshots — 2026-04-24

Andy exported the data underlying every table visual on two of his Power BI dashboards — **KPI Dashboard (Leadership)** and **Project Performance** — before leaving. These CSVs/XLSXs are our **ground truth** for future rebuilds, metric reconciliation, and logic verification.

## Contents

- **115 xlsx files** across 19 page-folders, in 2 dashboards
- **60 files have real data** (~5,600 total rows)
- **55 files are empty placeholders** — these correspond to chart/card/slicer visuals that don't export to xlsx (Power BI renders them, but "Export data" produces a 0-row file or a "Data connected to Power BI" placeholder). Not a bug — expected behavior. Listed at the bottom for completeness.

## Why this matters

When Power BI goes dark end of August 2026, every number that was on a dashboard becomes unverifiable unless we captured it. Andy's export is the last running-Power-BI snapshot we'll ever have. Use it to:

- **Diff a rebuild.** If someone rewrites a page in SQL, run it against the same week and diff row-by-row against these files.
- **Resolve "what should this number be?" disputes.** The values here are authoritative for 2026-04-24.
- **Understand column lineage.** Column headers on each export tell you the DAX measure name that produced them — we've cross-referenced against `../dax_index.json` below.
- **Reconstruct join grain.** The row-level tables (Time to Hire > Candidate Details, Problem Pipelines by jobs, TA Actual Screens Target > Weekly Details) show you the exact join keys Andy used.

## Related files

- `../dax_index.json` — machine-readable DAX library (200 measures, 290 columns)
- `../pages/*.md` — per-page deep-dives with visual + DAX lineage
- `../andy-homework/` — Andy's narrative answers (gotchas, client mappings, relationship intent)
- `../metric-ownership.md` — who owns the non-derivable thresholds/formulas

---

## Key ground-truth tables (ranked by future-rebuild value)

These 10 tables are the high-value ground truth. If you only ever use a subset of this snapshot collection, start here.

| Rank | File | Rows | What it gives you |
|:-:|:---|:-:|:---|
| 1 | `Project Performance - TIme to Hire/Candidate Details.xlsx` | 957 | Every hired candidate with job category, subcategory, created date, contacted date, hired date, name. Candidate-level source of truth for every Time-to-Hire/Fill/Find calculation. |
| 2 | `Project Performance - KPI - TA Summary/data (4).xlsx` | 1653 | Per-TA × Client × Job Title full funnel: Viewed, Screens, Actual Screens, ATS, Offers, Hires + Tech Role flag. 1,653 rows. The master KPI grain. |
| 3 | `KPI Dashboard (Leadership) - TA Actual Screens Target/Weekly Details.xlsx` | 519 | Every (Manager, TA, Client, Week) actual-screens and target pairing, with Exclude-OKR flag. 519 rows. Authoritative for WBR/MBR TA target diffs. |
| 4 | `KPI Dashboard (Leadership) - Pipelines Health/Problem Pipelines jobs that either do not have a first hire with at least 25 actual screens, or required more than a 321 actual screens-to-hire ratio.xlsx` | 279 | Full 279-row list of every flagged problem pipeline with client/hiring manager/TA/job details. Directly encodes the 25-screen / 32:1-ratio rule from Martin. |
| 5 | `Project Performance - Overview/Client and Job Performance (based on Action Date. TS & TA filters won't work if the job is shared).xlsx` | 135 | 135-row per-(Client × Job Category × Job Title) full funnel + ALL conversion percentages in one table. Best single-file snapshot of the entire funnel. |
| 6 | `KPI Dashboard (Leadership) - WBR II/data (1).xlsx` | 127 | 127 rows, every job flagged with an 'early warning sign' — Job Days Opened plus actual screens / ATS / offers to date. |
| 7 | `Project Performance - Weekly Progress/Weekly Performance.xlsx` | 15 | 15 weeks of full-funnel totals + all 6 conversion rates, all metrics in one row per week. Good for trend verification. |
| 8 | `KPI Dashboard (Leadership) - MBR/TA's Target (Dolphins and Whales).xlsx` | 25 | 25-row MBR TA-level rollup for the Dolphins/Whales BU with 12w history + 4w metrics + Jobs Opened > 60d + comment. Same grain as our rebuilt MBR. |
| 9 | `KPI Dashboard (Leadership) - WBR/TA's Weekly Target (Dolphins and Whales).xlsx` | ? | WBR TA view, 22 rows, 15 columns — includes Last 12w Hires/ATS/Screens, % Actual Screens to Hires, Time to Fill. Companion to our rebuilt WBR. |
| 10 | `Project Performance - Overview/Hired Candidate details.xlsx` | 16 | 16 named hires with Sourcer, TA, Source Type, all stage dates, LinkedIn URL, External Recruiter flag. Audit-grade. |

---

## Per-page snapshot inventory

Each section corresponds to one PBI page. Columns are shown with inferred DAX measure lineage where detectable.

### KPI Dashboard (Leadership) → Data Cleanliness/Hygiene

Folder: `KPI Dashboard (Leadership) - Data CleanlinessHygiene/`  ·  2 file(s) with data

#### `Data Hygiene – Issue Count Aggregated by TA's Manager.xlsx` — 7 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Manager` | calc-col `WBR TS Actual.Manager` |
| 2 | `# Data Cleanliness/Hygiene` | measure `# Data Cleanliness/Hygiene` |
| 3 | `% Data Cleanliness/Hygiene` | measure `% Data Cleanliness/Hygiene` |

Sample: `Salem Mansuri` · `10` · `0.06896551724137931`

#### `Data Hygiene – Issue Count per TA.xlsx` — 13 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `# Data Cleanliness/Hygiene` | measure `# Data Cleanliness/Hygiene` |
| 3 | `% Data Cleanliness/Hygiene` | measure `% Data Cleanliness/Hygiene` |

Sample: `Iulian Hodivoianu` · `8` · `0.32`

### KPI Dashboard (Leadership) → KPI - TA Summary (Color) / KPI - TA Summary

Folder: `KPI Dashboard (Leadership) - KPI - TA Summary (Color)/`  ·  1 file(s) with data  ·  5 empty

#### `data (3).xlsx` — 366 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `Client` | raw column `WBR TA Target.Client` |
| 3 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 4 | `Tech Role` | calc-col `job.Tech Role` |
| 5 | `Viewed by TA` | measure `# events - LinkedIn visited (date created)` |
| 6 | `Screens` | raw column `WBR TS Actual.Screens` |
| 7 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 8 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 9 | `Offers` | measure `# candidates - offer (offered date)` |
| 10 | `Hires` | measure `# candidates - hired (hired date)` |

Sample: `Adelya Khakimova` · `SevenRooms` · `Implementation Specialist - 7R (Dubai)` · `No` · `297` · `18` · `18` · `13`

### KPI Dashboard (Leadership) → KPI - TS Summary

Folder: `KPI Dashboard (Leadership) - KPI - TS Summary/`  ·  4 file(s) with data  ·  6 empty

#### `# of Candidates Disqualified at Each Stage.xlsx` — 88 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Current Stage` | raw column `candidate_stage.Current Stage` |
| 2 | _(blank)_ | — |
| 3 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 4 | `Recruiter Screen` | dimension / grouping column |
| 5 | `Actual Screen` | raw column `OKR TA.Actual Screen` |
| 6 | `Move to ATS` | dimension / grouping column |
| 7 | `Onsite` | measure `Onsite` |
| 8 | `Offer` | dimension / grouping column |
| 9 | `Total` | dimension / grouping column |

Sample: `Client` · `Job Title` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates`

#### `Pipelines without Hires by Official Sourcers.xlsx` — 13 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Official Sourcer` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Official Sourcer` |
| 2 | `Jobs` | dimension / grouping column |
| 3 | `0-30 days` | measure `0-30 days` |
| 4 | `30-60 days` | measure `30-60 days` |
| 5 | `60-90 days` | measure `60-90 days` |
| 6 | `>90 days` | measure `>90 days` |

Sample: `Valeriia Yurykova` · `8` · `3` · `1` · `1` · `3`

#### `Pipelines without Hires.xlsx` — 84 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 2 | `Client` | raw column `WBR TA Target.Client` |
| 3 | `Days Opened` | dimension / grouping column |

Sample: `CPG Partnerships Manager - Wolt Ads, Nor` · `Wolt` · `172`

#### `data (2).xlsx` — 14 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Sourcer` | raw column `WBR TA Actual.Sourcer` |
| 2 | `% Contacted to Positive Response` | measure `% Contacted to Positive Response` |
| 3 | `% Screens to Actual Screen` | measure `% Screens to Actual Screen` |
| 4 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 5 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 6 | `Positive Response` | measure `# candidates - positive response` |
| 7 | `Screens` | raw column `WBR TS Actual.Screens` |
| 8 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 9 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 10 | `Offers` | measure `# candidates - offer (offered date)` |
| 11 | `Hires` | measure `# candidates - hired (hired date)` |
| 12 | `Jobs` | dimension / grouping column |

Sample: `Andrea Akovic` · `0.12533875338753386` · `0.42567567567567566` · `0.4603174603174603` · `1476` · `185` · `148` · `63` · `29` · `16`

### KPI Dashboard (Leadership) → MBR

Folder: `KPI Dashboard (Leadership) - MBR/`  ·  4 file(s) with data

#### `Client's Target.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `Last 12w Hires` | measure `Last 12w Hires` |
| 3 | `Hires` | measure `# candidates - hired (hired date)` |
| 4 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 5 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 6 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 7 | `Offers` | measure `# candidates - offer (offered date)` |

Sample: `Aiven` · `1` · `217` · `45` · `19`

#### `TA's Target  (Ponies and Unicorns).xlsx` — 15 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Last 12w Hires` | measure `Last 12w Hires` |
| 4 | `Last 12w ATS` | measure `Last 12w ATS` |
| 5 | `Last 12w Screens` | dimension / grouping column |
| 6 | `Hires` | measure `# candidates - hired (hired date)` |
| 7 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 8 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 9 | `ATS` | raw column `WBR TS Actual.ATS` |
| 10 | `Jobs Opened > 60 days` | dimension / grouping column |
| 11 | `Latest Comment` | dimension / grouping column |

Sample: `Enam` · `Aleksandra Vistac` · `1` · `61` · `152` · `248` · `31` · `9` · `3` · `things are picking up but still issues w`

#### `TA's Target (Dolphins and Whales).xlsx` — 25 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Last 12w Hires` | measure `Last 12w Hires` |
| 4 | `Last 12w ATS` | measure `Last 12w ATS` |
| 5 | `Last 12w Screens` | dimension / grouping column |
| 6 | `Hires` | measure `# candidates - hired (hired date)` |
| 7 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 8 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 9 | `ATS` | raw column `WBR TS Actual.ATS` |
| 10 | `Jobs Opened > 60 days` | dimension / grouping column |
| 11 | `Latest Comment` | dimension / grouping column |

Sample: `Aiven` · `Fuad Safarov` · `1` · `12` · `23` · `112` · `18` · `9` · `Building the pipeline for his newly assi`

#### `TS's Target Last 4 Weeks.xlsx` — 13 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TS` | raw column `WBR TS Comment.TS` |
| 2 | `Last 12w Hires` | measure `Last 12w Hires` |
| 3 | `Last 12w % Actual Screens to ATS` | measure `Last 12w % Actual Screens to ATS` |
| 4 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 5 | `Target` | calc-col `WBR TS Actual.Target` |
| 6 | `Recruiter Screens` | measure `# candidates - screen (screened date)` |
| 7 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 8 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 9 | `Latest Comment` | dimension / grouping column |

Sample: `Andrea Akovic` · `0.36470588235294116` · `654` · `590` · `72` · `53` · `22` · `MBR: Lower Moved to ATS and Actual scree`

### KPI Dashboard (Leadership) → Missing Comment

Folder: `KPI Dashboard (Leadership) - Missing Comment/`  ·  3 file(s) with data  ·  2 empty

#### `Missing WBR Comment by Manager.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Manager` | calc-col `WBR TS Actual.Manager` |
| 2 | `Missing TA` | dimension / grouping column |
| 3 | `Missing TS` | dimension / grouping column |
| 4 | `Total Missing` | dimension / grouping column |

Sample: `Chené Elliot` · `24` · `15` · `39`

#### `Missing WBR TA Comment Details.xlsx` — 115 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Manager` | calc-col `WBR TS Actual.Manager` |
| 2 | `Week` | calc-col `analytic_usage.Week` |
| 3 | `TA` | raw column `WBR TS Actual.TA` |

Sample: `Niki Vokalkova` · `2025W53 (29/12-4/1)` · `Fuad Safarov`

#### `Missing WBR TS Comment Details.xlsx` — 51 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Manager` | calc-col `WBR TS Actual.Manager` |
| 2 | `Week` | calc-col `analytic_usage.Week` |
| 3 | `TS` | raw column `WBR TS Comment.TS` |

Sample: `Vladimir Stankovic` · `2025W52 (22/12-28/12)` · `Ejla Suljcic`

### KPI Dashboard (Leadership) → Pipelines Health

Folder: `KPI Dashboard (Leadership) - Pipelines Health/`  ·  4 file(s) with data  ·  2 empty

#### `% Actual Screens to ATS is below 50% (Active Pipelines).xlsx` — 53 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TS` | raw column `WBR TS Comment.TS` |
| 2 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 3 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 4 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 5 | `Screen` | measure `# candidates - screen (screened date)` |
| 6 | `Actual Screen` | raw column `OKR TA.Actual Screen` |
| 7 | `ATS` | raw column `WBR TS Actual.ATS` |
| 8 | `Offers` | measure `# candidates - offer (offered date)` |
| 9 | `Hires` | measure `# candidates - hired (hired date)` |

Sample: `Adelya Khakimova` · `8` · `0.3783783783783784` · `254` · `123` · `111` · `42` · `6` · `6`

#### `Active Problem Pipelines by TA.xlsx` — 30 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `Problem Pipelines %` | measure `Problem Pipelines %` |
| 3 | `Total Active Pipelines` | measure `Total Active Pipelines` |
| 4 | `0-30 days` | measure `0-30 days` |
| 5 | `30-60 days` | measure `30-60 days` |
| 6 | `> 60 days` | measure `> 60 days` |

Sample: `Georgia Vasilaki` · `1` · `1` · `1`

#### `Problem Pipelines by Client.xlsx` — 34 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `% Problem Jobs` | measure `% Problem Jobs` |
| 3 | `# Problem jobs` | measure `# Problem jobs` |
| 4 | `# Total Jobs` | dimension / grouping column |

Sample: `Sirius` · `1` · `3` · `3`

#### `Problem Pipelines jobs that either do not have a first hire with at least 25 actual screens, or required more than a 321 actual screens-to-hire ratio.xlsx` — 279 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `Hiring Manager` | dimension / grouping column |
| 3 | `TA` | raw column `WBR TS Actual.TA` |
| 4 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 5 | `Archived` | dimension / grouping column |
| 6 | `Job Created Date` | dimension / grouping column |
| 7 | `First Hires` | dimension / grouping column |
| 8 | `Job Opened (days) w/o hires` | dimension / grouping column |
| 9 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 10 | `Screen` | measure `# candidates - screen (screened date)` |
| 11 | `Actual Screen` | raw column `OKR TA.Actual Screen` |
| 12 | `ATS` | raw column `WBR TS Actual.ATS` |
| 13 | `Offers` | measure `# candidates - offer (offered date)` |
| 14 | `Hires` | measure `# candidates - hired (hired date)` |

Sample: `Circula` · `Riaan` · `Marko Pavicevic` · `Product Lead` · `True` · `2023-10-19 00:00:00` · `918` · `717` · `97` · `68` · `36` · `1`

### KPI Dashboard (Leadership) → Sourcing Stats

Folder: `KPI Dashboard (Leadership) - Sourcing Stats/`  ·  2 file(s) with data  ·  4 empty

#### `Linkedin Profile Viewed (7am-8pm only).xlsx` — 49 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Hour` | dimension / grouping column |
| 2 | `7` | dimension / grouping column |
| 3 | `8` | dimension / grouping column |
| 4 | `9` | dimension / grouping column |
| 5 | `10` | dimension / grouping column |
| 6 | `11` | dimension / grouping column |
| 7 | `12` | dimension / grouping column |
| 8 | `13` | dimension / grouping column |
| 9 | `14` | dimension / grouping column |
| 10 | `15` | dimension / grouping column |
| 11 | `16` | dimension / grouping column |
| 12 | `17` | dimension / grouping column |
| 13 | `18` | dimension / grouping column |
| 14 | `19` | dimension / grouping column |
| 15 | `20` | dimension / grouping column |
| 16 | `Total` | dimension / grouping column |

Sample: `Date` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create` · `# events - LinkedIn visited (date create`

#### `Sourcer Time Distribution Across Clients (calculated from candidate sourcing volume).xlsx` — 15 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `client_name` | raw column `client.client_name` |
| 2 | `Aiven` | dimension / grouping column |
| 3 | `AVIV` | dimension / grouping column |
| 4 | `Doordash` | dimension / grouping column |
| 5 | `Enam` | dimension / grouping column |
| 6 | `FTAPI` | dimension / grouping column |
| 7 | `Glovo` | dimension / grouping column |
| 8 | `Grover` | dimension / grouping column |
| 9 | `Nexi` | dimension / grouping column |
| 10 | `Parloa` | dimension / grouping column |
| 11 | `SevenRooms` | dimension / grouping column |
| 12 | `Taxfix` | dimension / grouping column |
| 13 | `Tribe.xyz (IR)` | dimension / grouping column |
| 14 | `Wolt` | dimension / grouping column |

Sample: `Sourcer` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client` · `Finance sourcer allocation per client`

### KPI Dashboard (Leadership) → TA Actual Screens Target

Folder: `KPI Dashboard (Leadership) - TA Actual Screens Target/`  ·  3 file(s) with data

#### `By Manager.xlsx` — 19 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Manager` | calc-col `WBR TS Actual.Manager` |
| 2 | `Total TA` | dimension / grouping column |
| 3 | `# TA Reach Target` | dimension / grouping column |
| 4 | `%` | dimension / grouping column |

Sample: `Tijana Lazovic` · `26` · `16` · `0.6153846153846154`

#### `By TA.xlsx` — 44 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 3 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 4 | `Hires` | measure `# candidates - hired (hired date)` |
| 5 | `% Actual Screens to Hired` | measure `% Actual Screens to Hired` |

Sample: `Nenad Skoko` · `34` · `278` · `22` · `0.07913669064748201`

#### `Weekly Details.xlsx` — 519 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Manager` | calc-col `WBR TS Actual.Manager` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Client` | raw column `WBR TA Target.Client` |
| 4 | `Week` | calc-col `analytic_usage.Week` |
| 5 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 6 | `Target` | calc-col `WBR TS Actual.Target` |
| 7 | `Exclude OKR` | raw column `WBR TA Comment.Exclude OKR` |

Sample: `Chené Elliot` · `Adelya Khakimova` · `Wolt North, Baltics & Benelux` · `2025W51 (15/12-21/12)` · `9` · `15`

### KPI Dashboard (Leadership) → WBR

Folder: `KPI Dashboard (Leadership) - WBR/`  ·  6 file(s) with data  ·  2 empty

#### `Client's Target.xlsx` — 18 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `Last 12w Hires` | measure `Last 12w Hires` |
| 3 | `Hires` | measure `# candidates - hired (hired date)` |
| 4 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 5 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 6 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 7 | `Offers` | measure `# candidates - offer (offered date)` |
| 8 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |

Sample: `Aiven` · `1` · `114` · `29` · `8` · `6`

#### `TA's  Weekly Target (Dolphins and Whales).xlsx` — 22 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Last 12w Hires` | measure `Last 12w Hires` |
| 4 | `Last 12w ATS` | measure `Last 12w ATS` |
| 5 | `Last 12w Screens` | dimension / grouping column |
| 6 | `Last 12w % Actual Screens to Hires` | measure `Last 12w % Actual Screens to Hires` |
| 7 | `Last 12w Time to Fill` | measure `Last 12w Time to Fill` |
| 8 | `Hires` | measure `# candidates - hired (hired date)` |
| 9 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 10 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 11 | `ATS` | raw column `WBR TS Actual.ATS` |
| 12 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 13 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 14 | `Jobs Opened > 60 days` | dimension / grouping column |
| 15 | `Comment` | raw column `WBR TS Comment.Comment` |

Sample: `Aiven` · `Fuad Safarov` · `1` · `12` · `23` · `0.043478260869565216` · `14` · `60` · `9` · `1` · `0.1111111111111111` · `4` · `Building the pipeline for his newly assi`

#### `TA's Weekly Target  (Ponies and Unicorns).xlsx` — 14 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Last 12w Hires` | measure `Last 12w Hires` |
| 4 | `Last 12w ATS` | measure `Last 12w ATS` |
| 5 | `Last 12w Screens` | dimension / grouping column |
| 6 | `Last 12w % Actual Screens to Hires` | measure `Last 12w % Actual Screens to Hires` |
| 7 | `Last 12w Time to Fill` | measure `Last 12w Time to Fill` |
| 8 | `Hires` | measure `# candidates - hired (hired date)` |
| 9 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 10 | `Actual screens` | measure `# candidates - actual screen (actual screen date)` |
| 11 | `ATS` | raw column `WBR TS Actual.ATS` |
| 12 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 13 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 14 | `Jobs Opened > 60 days` | dimension / grouping column |
| 15 | `Comment` | raw column `WBR TS Comment.Comment` |

Sample: `Enam` · `Aleksandra Vistac` · `1` · `61` · `152` · `0.006578947368421052` · `411` · `102` · `9` · `1` · `0.1111111111111111` · `5` · `3` · `things are picking up but still issues w`

#### `TS Overall Conversion Rate with Officially Assigned Active Pipelines (1).xlsx` — 14 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TS` | raw column `WBR TS Comment.TS` |
| 2 | `Active Jobs` | dimension / grouping column |
| 3 | `% Contacted to Positive Response` | measure `% Contacted to Positive Response` |
| 4 | `Positive Response` | measure `# candidates - positive response` |
| 5 | `% Screens to Actual Screen` | measure `% Screens to Actual Screen` |
| 6 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 7 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 8 | `ATS` | raw column `WBR TS Actual.ATS` |

Sample: `Andrea Akovic` · `7` · `0.12051282051282051` · `141` · `0.5` · `57` · `0.43859649122807015` · `25`

#### `TS's Weekly Target.xlsx` — 13 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TS` | raw column `WBR TS Comment.TS` |
| 2 | `Last 12w Hires` | measure `Last 12w Hires` |
| 3 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 4 | `Target` | calc-col `WBR TS Actual.Target` |
| 5 | `Recruiter Screens` | measure `# candidates - screen (screened date)` |
| 6 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 7 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 8 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 9 | `# TA` | calc-col `WBR TS Actual.# TA` |
| 10 | `TA` | raw column `WBR TS Actual.TA` |
| 11 | `Comment` | raw column `WBR TS Comment.Comment` |

Sample: `Andrea Akovic` · `212` · `200` · `25` · `15` · `5` · `7` · `4` · `Chené Elliot, Kristina Colovic, Samantha`

#### `data (3).xlsx` — 3 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Selecting Week:` | dimension / grouping column |

Sample: `2026W16 (13/4-19/4)`

### KPI Dashboard (Leadership) → WBR II

Folder: `KPI Dashboard (Leadership) - WBR II/`  ·  1 file(s) with data  ·  1 empty

#### `data (1).xlsx` — 127 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 4 | `Job Created` | dimension / grouping column |
| 5 | `Job Days Opened` | calc-col `job.Job Days Opened` |
| 6 | `# Actual Screens` | calc-col `job.# Actual Screens` |
| 7 | `# ATS` | calc-col `job.# ATS` |
| 8 | `# Offers` | calc-col `job.# Offers` |

Sample: `SevenRooms` · `Abigail Caliwag` · `Customer Success Specialist` · `2025-09-05 00:00:00` · `231`

### Project Performance → Candidate Response Rate

Folder: `Project Performance - Candidate Response Rate/`  ·  3 file(s) with data  ·  1 empty

#### `Response Rate by Client.xlsx` — 22 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `# Candidates Contacted` | dimension / grouping column |
| 3 | `# Candidate Responded` | dimension / grouping column |
| 4 | `% Response Rate` | measure `% Response Rate` |
| 5 | `% Positive Response Rate` | measure `% Positive Response Rate` |
| 6 | `% Linkedin Connect Accepted` | measure `% Linkedin connect accepted` |

Sample: `Wemolo` · `6` · `6` · `1` · `0.3333333333333333`

#### `Response Rate by Job CategoryJob Title.xlsx` — 17 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job Category` | dimension / grouping column |
| 2 | `# Candidates Contacted` | dimension / grouping column |
| 3 | `# Candidate Responded` | dimension / grouping column |
| 4 | `% Response Rate` | measure `% Response Rate` |
| 5 | `% Positive Response Rate` | measure `% Positive Response Rate` |
| 6 | `% Linkedn Connect Accepted` | dimension / grouping column |

Sample: `Product Manager` · `85` · `56` · `0.6588235294117647` · `0.35294117647058826` · `0.581081081081081`

#### `Response Rate by TA.xlsx` — 44 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 3 | `# Candidates Contacted` | dimension / grouping column |
| 4 | `# Candidate Responded` | dimension / grouping column |
| 5 | `% Response Rate` | measure `% Response Rate` |
| 6 | `% Positive Response Rate` | measure `% Positive Response Rate` |
| 7 | `% Linkedin Connect Accepted` | measure `% Linkedin connect accepted` |

Sample: `Georgia Vasilaki` · `3` · `12` · `12` · `1` · `1`

### Project Performance → Internal Recruitment

Folder: `Project Performance - Internal Recruitment/`  ·  6 file(s) with data  ·  2 empty

#### `# of Candidates Disqualified at Each Stage.xlsx` — 9 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Current Stage` | raw column `candidate_stage.Current Stage` |
| 2 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 3 | `Recruiter Screen` | dimension / grouping column |
| 4 | `Actual Screen` | raw column `OKR TA.Actual Screen` |
| 5 | `Move to ATS` | dimension / grouping column |
| 6 | `Onsite` | measure `Onsite` |
| 7 | `Total` | dimension / grouping column |

Sample: `Job Title` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates`

#### `Interviewed By.xlsx` — 11 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |

Sample: `18`

#### `Missed Opportunities.xlsx` — 23 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Week` | calc-col `analytic_usage.Week` |
| 2 | `Role` | raw column `IR Comment.Role` |
| 3 | `Client` | raw column `WBR TA Target.Client` |
| 4 | `Headcount` | raw column `IR Comment.Headcount` |
| 5 | `Status` | raw column `IR Comment.Status` |
| 6 | `Outcome` | raw column `IR Comment.Outcome` |

Sample: `2026W10 (2/3-8/3)` · `DH - Executive Search Talent Partner (AP` · `DeliveryHero` · `1` · `Ongoing` · `Pending client feedback`

#### `Sourced By.xlsx` — 18 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Sourced By` | dimension / grouping column |
| 2 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 3 | `Positive Response` | measure `# candidates - positive response` |
| 4 | `Hired` | measure `# candidates - hired (hired date)` |

Sample: `Andrea Akovic` · `52` · `22`

#### `Weekly Performance.xlsx` — 14 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Week` | calc-col `analytic_usage.Week` |
| 2 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 3 | `Positive Response` | measure `# candidates - positive response` |
| 4 | `Recruiter Screens` | measure `# candidates - screen (screened date)` |
| 5 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 6 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 7 | `Onsite` | measure `Onsite` |
| 8 | `Culture Interview` | measure `Culture Interview` |
| 9 | `Call with Client` | dimension / grouping column |
| 10 | `Offered` | dimension / grouping column |
| 11 | `Hired` | measure `# candidates - hired (hired date)` |

Sample: `2026W17 (20/4-26/4)` · `173` · `75` · `49` · `13` · `3`

#### `data (1).xlsx` — 8 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job` | dimension / grouping column |
| 2 | `Opened` | dimension / grouping column |
| 3 | `Hired` | measure `# candidates - hired (hired date)` |

Sample: `(Generalist) Talent Acquisition Partner ` · `30`

### Project Performance → KPI - TA Summary

Folder: `Project Performance - KPI - TA Summary/`  ·  2 file(s) with data  ·  5 empty

#### `data (2).xlsx` — 37 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `TA Screening Time (H)` | measure `TA linkedin candidate screening time` |
| 3 | `% Screen to ATS` | measure `% Screen to ATS` |
| 4 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 5 | `% Offer to Hire` | measure `% Offer to Hire` |
| 6 | `Actual Screen` | raw column `OKR TA.Actual Screen` |
| 7 | `Hires` | measure `# candidates - hired (hired date)` |

Sample: `Adelya Khakimova` · `143.11666666666667` · `0.4448979591836735` · `0.4801762114537445` · `0.875` · `227` · `14`

#### `data (4).xlsx` — 1653 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `TA` | raw column `WBR TS Actual.TA` |
| 2 | `Client` | raw column `WBR TA Target.Client` |
| 3 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 4 | `Tech Role` | calc-col `job.Tech Role` |
| 5 | `Viewed by TA` | measure `# events - LinkedIn visited (date created)` |
| 6 | `Screens` | raw column `WBR TS Actual.Screens` |
| 7 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 8 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 9 | `Offers` | measure `# candidates - offer (offered date)` |
| 10 | `Hires` | measure `# candidates - hired (hired date)` |

Sample: `Adelya Khakimova` · `SevenRooms` · `Implementation Specialist - 7R (Dubai)` · `No` · `297` · `18` · `18` · `13`

### Project Performance → KPI - TS Summary

Folder: `Project Performance - KPI - TS Summary/`  ·  4 file(s) with data  ·  6 empty

#### `# of Candidates Disqualified at Each Stage.xlsx` — 101 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Current Stage` | raw column `candidate_stage.Current Stage` |
| 2 | _(blank)_ | — |
| 3 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 4 | `Recruiter Screen` | dimension / grouping column |
| 5 | `Actual Screen` | raw column `OKR TA.Actual Screen` |
| 6 | `Move to ATS` | dimension / grouping column |
| 7 | `Onsite` | measure `Onsite` |
| 8 | `Offer` | dimension / grouping column |
| 9 | `Total` | dimension / grouping column |

Sample: `Client` · `Job Title` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates` · `# Candidates`

#### `Pipelines without Hires by Official Sourcers.xlsx` — 14 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Official Sourcer` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Official Sourcer` |
| 2 | `Jobs` | dimension / grouping column |
| 3 | `0-30 days` | measure `0-30 days` |
| 4 | `30-60 days` | measure `30-60 days` |
| 5 | `60-90 days` | measure `60-90 days` |
| 6 | `>90 days` | measure `>90 days` |

Sample: `Mia Gjorgievska` · `4` · `4`

#### `Pipelines without Hires.xlsx` — 89 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 2 | `Client` | raw column `WBR TA Target.Client` |
| 3 | `Days Opened` | dimension / grouping column |

Sample: `(Mia) Senior PM - Public Sector` · `Aleph Alpha` · `554`

#### `data (2).xlsx` — 15 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Sourcer` | raw column `WBR TA Actual.Sourcer` |
| 2 | `% Contacted to Positive Response` | measure `% Contacted to Positive Response` |
| 3 | `% Screens to Actual Screen` | measure `% Screens to Actual Screen` |
| 4 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 5 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 6 | `Positive Response` | measure `# candidates - positive response` |
| 7 | `Screens` | raw column `WBR TS Actual.Screens` |
| 8 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 9 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 10 | `Offers` | measure `# candidates - offer (offered date)` |
| 11 | `Hires` | measure `# candidates - hired (hired date)` |
| 12 | `Jobs` | dimension / grouping column |

Sample: `Andrea Akovic` · `0.1150278293135436` · `0.3987341772151899` · `0.4603174603174603` · `1617` · `186` · `158` · `63` · `29` · `21`

### Project Performance → New Role Estimate

Folder: `Project Performance - New Role Estimate/`  ·  1 file(s) with data  ·  4 empty

#### `# for 1 Hires per Country.xlsx` — 63 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job Country` | raw column `job.Job Country` |
| 2 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 3 | `Time to Hire` | measure `Job - Time to Hire` |
| 4 | `# Viewed for 1 Hire` | dimension / grouping column |
| 5 | `# Contacted for 1 Hire` | dimension / grouping column |
| 6 | `# Screens for 1 Hire` | dimension / grouping column |
| 7 | `# Actual screens for 1 Hire` | dimension / grouping column |
| 8 | `# ATS for 1 Hire` | dimension / grouping column |
| 9 | `# Offers for 1 Hire` | dimension / grouping column |

Sample: `Australia` · `1` · `21` · `1304` · `332` · `45` · `32` · `8` · `1`

### Project Performance → Overview (Project Performance PBIX)

Folder: `Project Performance - Overview/`  ·  4 file(s) with data  ·  8 empty

#### `Client and Job Performance (based on Action Date. TS & TA filters won't work if the job is shared).xlsx` — 135 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `Job Category` | dimension / grouping column |
| 3 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 4 | `Viewed` | raw column `WBR TS Actual.Viewed` |
| 5 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 6 | `Positive Response` | measure `# candidates - positive response` |
| 7 | `Screens` | raw column `WBR TS Actual.Screens` |
| 8 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 9 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 10 | `Offers` | measure `# candidates - offer (offered date)` |
| 11 | `Hires` | measure `# candidates - hired (hired date)` |
| 12 | `% Viewed to Contacted` | measure `% Viewed to Contacted` |
| 13 | `% Contacted to Positive Response` | measure `% Contacted to Positive Response` |
| 14 | `% Screens to Actual Screen` | measure `% Screens to Actual Screen` |
| 15 | `% Actual Screens to ATS` | measure `% Actual Screens to ATS` |
| 16 | `% ATS to Offers` | measure `% ATS to Offers` |
| 17 | `% Offer to Hire` | measure `% Offer to Hire` |

Sample: `Aiven` · `HR` · `Senior Talent Partner - Cork (Ireland)` · `4` · `3` · `2` · `0`

#### `Hired Candidate details.xlsx` — 16 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `#` | dimension / grouping column |
| 2 | `Name` | raw column `Sourcing Team List.Name` |
| 3 | `Client` | raw column `WBR TA Target.Client` |
| 4 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 5 | `Sourcer` | raw column `WBR TA Actual.Sourcer` |
| 6 | `TA` | raw column `WBR TS Actual.TA` |
| 7 | `Source Type` | dimension / grouping column |
| 8 | `Contacted Date` | dimension / grouping column |
| 9 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 10 | `Offered Date` | dimension / grouping column |
| 11 | `Hired Date` | dimension / grouping column |
| 12 | `LinkedIn` | dimension / grouping column |
| 13 | `External Recruiter` | dimension / grouping column |

Sample: `1` · `Amir Kehonjic` · `Nexi` · `Field Sales Manager - Zurich` · `Adis Prepoljac` · `Adis Prepoljac` · `Applicant` · `2026-04-24 00:00:00` · `2026-04-24 00:00:00` · `2026-04-24 00:00:00` · `2026-04-24 00:00:00` · `https://linkedin.com/in/amir-kehonjic` · `No`

#### `TA Overview.xlsx` — 41 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TA` | raw column `WBR TS Actual.TA` |
| 3 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 4 | `Candidate Response` | measure `Candidate Response` |
| 5 | `% Response Rate` | measure `% Response Rate` |
| 6 | `Rejected` | dimension / grouping column |
| 7 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 8 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 9 | `Offer` | dimension / grouping column |
| 10 | `Hires` | measure `# candidates - hired (hired date)` |
| 11 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |

Sample: `Aiven` · `Fuad Safarov` · `46` · `13` · `0.2826086956521739` · `16` · `5` · `3` · `1` · `1` · `3`

#### `TS Overview.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `TS` | raw column `WBR TS Comment.TS` |
| 3 | `Hires` | measure `# candidates - hired (hired date)` |
| 4 | `Sourced` | raw column `WBR TS Actual.Sourced` |
| 5 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 6 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |

Sample: `Aiven` · `Rodrigo Gomes` · `31` · `2`

### Project Performance → Recruiting Trends & Conversion Rate

Folder: `Project Performance - Recruiting Trends & conversions/`  ·  5 file(s) with data  ·  5 empty

#### `# for 1 Hires per Job CatJob SubcategoryJob Title (Based on Contacted Date).xlsx` — 35 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job Category` | dimension / grouping column |
| 2 | `Job Subcategory` | dimension / grouping column |
| 3 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 4 | `# Viewed for 1 Hire` | dimension / grouping column |
| 5 | `# Contacted for 1 Hire` | dimension / grouping column |
| 6 | `# Screens for 1 Hire` | dimension / grouping column |
| 7 | `# Actual screens for 1 Hire` | dimension / grouping column |
| 8 | `# ATS for 1 Hire` | dimension / grouping column |
| 9 | `# Offers for 1 Hire` | dimension / grouping column |

Sample: `Engineering Management` · `QA` · `1` · `399` · `259` · `32` · `19` · `10` · `2`

#### `% Hired by Source and Client.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `source` | raw column `candidate.source` |
| 2 | _(blank)_ | — |
| 3 | `Agency` | dimension / grouping column |
| 4 | `Applicant` | dimension / grouping column |
| 5 | `Referral` | dimension / grouping column |
| 6 | `Sourced` | raw column `WBR TS Actual.Sourced` |

Sample: `Client` · `hired date` · `hired date` · `hired date` · `hired date` · `hired date`

#### `% Hired by Source and Job Category.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `source` | raw column `candidate.source` |
| 2 | _(blank)_ | — |
| 3 | `Agency` | dimension / grouping column |
| 4 | `Applicant` | dimension / grouping column |
| 5 | `Referral` | dimension / grouping column |
| 6 | `Sourced` | raw column `WBR TS Actual.Sourced` |

Sample: `Job Category` · `hired date` · `hired date` · `hired date` · `hired date` · `hired date`

#### `Hired by Source and Client.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `source` | raw column `candidate.source` |
| 2 | _(blank)_ | — |
| 3 | `Agency` | dimension / grouping column |
| 4 | `Applicant` | dimension / grouping column |
| 5 | `Referral` | dimension / grouping column |
| 6 | `Sourced` | raw column `WBR TS Actual.Sourced` |
| 7 | `Total` | dimension / grouping column |

Sample: `Client` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)`

#### `Hired by Source and Job Category.xlsx` — 20 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `source` | raw column `candidate.source` |
| 2 | _(blank)_ | — |
| 3 | `Agency` | dimension / grouping column |
| 4 | `Applicant` | dimension / grouping column |
| 5 | `Referral` | dimension / grouping column |
| 6 | `Sourced` | raw column `WBR TS Actual.Sourced` |
| 7 | `Total` | dimension / grouping column |

Sample: `Job Category` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)` · `# candidates - hired (hired date)`

### Project Performance → Time to Hire

Folder: `Project Performance - TIme to Hire/`  ·  3 file(s) with data  ·  1 empty

#### `Candidate Details.xlsx` — 957 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `#` | dimension / grouping column |
| 2 | `Client` | raw column `WBR TA Target.Client` |
| 3 | `Job Title` | raw column `Temp_Inactive_Jobs_Sourcers_WBR.Job Title` |
| 4 | `Job Category` | dimension / grouping column |
| 5 | `Job Subcategory` | dimension / grouping column |
| 6 | `Job Created Date` | dimension / grouping column |
| 7 | `Contacted Date` | dimension / grouping column |
| 8 | `Hired Date` | dimension / grouping column |
| 9 | `Name` | raw column `Sourcing Team List.Name` |

Sample: `1` · `Aiven` · `Inside Sales Solution Architect - Banglo` · `Information Technology` · `Solutions Architect` · `2026-04-08 00:00:00` · `2026-04-08 00:00:00` · `2026-04-22 00:00:00` · `Tejal Saharkar`

#### `First Hired per Job by ClientJob Title (1).xlsx` — 18 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Client` | raw column `WBR TA Target.Client` |
| 2 | `# Jobs` | raw column `WBR TS Actual.# Jobs` |
| 3 | `Time to Hire` | measure `Job - Time to Hire` |
| 4 | `Time to Find a Hire` | measure `Job - Time to Find a Hire` |
| 5 | `Time to Fill` | measure `Job - Time to Fill` |

Sample: `Aiven` · `1` · `14` · `14`

#### `First Hired per Job by Job CategorySubcategory.xlsx` — 16 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Job Category` | dimension / grouping column |
| 2 | `# Job` | dimension / grouping column |
| 3 | `Time to Hire` | measure `Job - Time to Hire` |
| 4 | `Time to Find a Hire` | measure `Job - Time to Find a Hire` |
| 5 | `Time to Fill` | measure `Job - Time to Fill` |

Sample: `Data Analytics` · `3` · `25` · `49` · `54`

### Project Performance → Weekly Progress

Folder: `Project Performance - Weekly Progress/`  ·  2 file(s) with data  ·  1 empty

#### `Monthly Performance.xlsx` — 7 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Year/Month` | calc-col `analytic_usage.Year/Month` |
| 2 | `Linkedin Viewed` | measure `# events - LinkedIn visited (date created)` |
| 3 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 4 | `Reacted` | measure `# candidates - reacted (contacted date)` |
| 5 | `Positive Response` | measure `# candidates - positive response` |
| 6 | `Recruiter Screens` | measure `# candidates - screen (screened date)` |
| 7 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 8 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 9 | `Offered` | dimension / grouping column |
| 10 | `Hired` | measure `# candidates - hired (hired date)` |
| 11 | `% Viewed to Contacted` | measure `% Viewed to Contacted` |
| 12 | `% Contacted to Reacted` | measure `% Contacted to Reacted` |
| 13 | `% Contacted to Positive Response` | measure `% Contacted to Positive Response` |
| 14 | `% Positive Response to Screen` | measure `% Positive Response to Screen` |
| 15 | `% Screen to ATS` | measure `% Screen to ATS` |
| 16 | `% Screens Actual to ATS` | measure `% Screens Actual to ATS` |

Sample: `2026/04` · `25343` · `9293` · `2189` · `1804` · `1442` · `1108` · `626` · `46` · `40` · `0.3666890265556564` · `0.21349402776283224` · `0.19412460992144626` · `0.7993348115299335` · `0.4860248447204969` · `0.5649819494584838`

#### `Weekly Performance.xlsx` — 15 rows

| # | Column | Likely DAX measure / source |
|:-:|:---|:---|
| 1 | `Week` | calc-col `analytic_usage.Week` |
| 2 | `Linkedin Viewed` | measure `# events - LinkedIn visited (date created)` |
| 3 | `Contacted` | measure `# candidates - contacted (contacted date)` |
| 4 | `Reacted` | measure `# candidates - reacted (contacted date)` |
| 5 | `Positive Response` | measure `# candidates - positive response` |
| 6 | `Recruiter Screens` | measure `# candidates - screen (screened date)` |
| 7 | `Actual Screens` | measure `# candidates - actual screen (actual screen date)` |
| 8 | `Moved to ATS` | measure `# candidates - move to ATS (moved date)` |
| 9 | `Offered` | dimension / grouping column |
| 10 | `Hired` | measure `# candidates - hired (hired date)` |
| 11 | `% Viewed to Contacted` | measure `% Viewed to Contacted` |
| 12 | `% Contacted to Reacted` | measure `% Contacted to Reacted` |
| 13 | `% Contacted to Positive Response` | measure `% Contacted to Positive Response` |
| 14 | `% Positive Response to Screen` | measure `% Positive Response to Screen` |
| 15 | `% Screen to ATS` | measure `% Screen to ATS` |
| 16 | `% Screens Actual to ATS` | measure `% Screens Actual to ATS` |

Sample: `2026W17 (20/4-26/4)` · `7828` · `2392` · `333` · `496` · `371` · `277` · `158` · `14` · `14` · `0.30556974961676037` · `0.11454849498327759` · `0.20735785953177258` · `0.7479838709677419` · `0.7939698492462312` · `0.5703971119133574`

---



---

## Cross-snapshot join map

How the snapshots relate to each other — useful when you're trying to reconcile a number or verify a rebuild. Every snapshot is a projection of the same underlying Bubble data through different DAX filters.

### Common keys

| Key | Appears in | Notes |
|:---|:---|:---|
| `TA` (recruiter name) | Every page | Canonical casing/spelling: the WBR/MBR target files. Some names have double-spaces or diacritics (see memory `reference_verified_query_logic.md`). |
| `Client` | Every page | Canonical mapping: Wolt sub-BUs merged per `../andy-homework/global/client-mappings.md`. DoorDash + SevenRooms → Wolt for target aggregation. |
| `Week` | WBR, MBR, Missing Comment, TA Actual Screens Target Weekly Details | Week format: `"2026W17 (21/4-27/4)"` or `"Week start end"` date. WBR weeks are Sunday-Saturday. |
| `Job Title` + `Client` | KPI TA Summary detail, Time to Hire, Problem Pipelines, WBR II, Overview | Together act as job-identifier since job_id isn't in the exports. |
| `Candidate name` | Time to Hire > Candidate Details, Overview > Hired Candidate details | Candidate-grain; join by (Client, Job Title, Name) |
| `Manager` | Missing Comment, TA Actual Screens Target (By Manager), WBR TA Target roster | From BambooHR historical report-to (see `reference_recruiting_data_semantics.md`). |

### Hierarchy — which table is the source, which is the rollup

```
CANDIDATE grain
  ├── Time to Hire > Candidate Details.xlsx (957 rows)
  │     └── aggregated up to:
  │         ├── Time to Hire > First Hired per Job by Client/Job Title (18 rows)
  │         ├── Time to Hire > First Hired per Job by Job Category/Subcategory (16 rows)
  │         └── Time to Hire > Month Trends (chart, no export)
  └── Overview > Hired Candidate details (16 rows — named hires only, subset)

JOB grain
  ├── Pipelines Health > Problem Pipelines (jobs ... 25 screens, 32:1 ratio) (279 rows)
  │     └── aggregated up to:
  │         ├── Pipelines Health > Active Problem Pipelines by TA (30 rows)
  │         ├── Pipelines Health > Problem Pipelines by Client (34 rows)
  │         ├── Pipelines Health > Problem Pipelines by Hiring Manager (empty)
  │         └── Pipelines Health > Problem Pipelines by TA (empty)
  ├── WBR II > data (1) (127 rows — early warning jobs)
  ├── KPI - TS Summary > Pipelines without Hires (84 rows)
  └── KPI - TS Summary > Pipelines without Hires by Official Sourcers (13 rows)

TA x CLIENT x JOB TITLE grain
  ├── KPI - TA Summary > data (3/4) (366 / 1,653 rows)
  │     └── aggregated up to:
  │         ├── KPI - TA Summary > data (2) (per-TA rollup, 37 rows)
  │         ├── Overview > TA Overview (Client × TA summary, 41 rows)
  │         └── MBR / WBR target files

TA x CLIENT x WEEK grain
  ├── TA Actual Screens Target > Weekly Details (519 rows)
  │     └── aggregated up to:
  │         ├── TA Actual Screens Target > By TA (44 rows)
  │         ├── TA Actual Screens Target > By Manager (19 rows)
  │         ├── WBR TA's Weekly Target (Dolphins/Whales + Ponies/Unicorns)
  │         └── MBR TA's Target (Dolphins/Whales + Ponies/Unicorns)

WEEK grain
  ├── Weekly Progress > Weekly Performance (15 weeks, all funnel metrics + conversions)
  └── Weekly Progress > Monthly Performance (rolled up to month)

SOURCE grain (for Recruiting Trends)
  ├── Recruiting Trends > Hired by Source x Client (20 rows, pivoted)
  └── Recruiting Trends > Hired by Source x Job Category (20 rows, pivoted)
```

### Target vs actuals pairing

To reconcile "actuals vs target" for any TA/Client/Week, these are the paired files:

| Slice | Actuals file | Target file |
|:---|:---|:---|
| WBR TA (weekly) | `... - WBR/TA's Weekly Target (Dolphins and Whales).xlsx` | target columns embedded (Last 12w columns are historical baselines) |
| MBR TA (monthly) | `... - MBR/TA's Target (Dolphins and Whales).xlsx` | target columns embedded (same columns as WBR but 4w cadence) |
| WBR Client (weekly) | `... - WBR/Client's Target.xlsx` | target columns embedded |
| MBR Client (monthly) | `... - MBR/Client's Target.xlsx` | target columns embedded |
| TA Actual Screens OKR | `... - TA Actual Screens Target/By TA.xlsx` | `... - TA Actual Screens Target/Weekly Details.xlsx` has both columns |
| WBR TS | `... - WBR/TS's Weekly Target.xlsx` | target column embedded |

### What's in these snapshots that we can verify in OUR new dashboard

Our rebuilt dashboard (`bark8922/tribe-recruiting`) has equivalents for:

- **WBR TA table** ↔ `WBR/TA's Weekly Target (Dolphins and Whales).xlsx` + `(Ponies and Unicorns).xlsx`
- **MBR TA table** ↔ `MBR/TA's Target (Dolphins and Whales).xlsx` + `(Ponies and Unicorns).xlsx`
- **WBR Client summary** ↔ `WBR/Client's Target.xlsx`
- **MBR Client summary** ↔ `MBR/Client's Target.xlsx`
- **WBR TS** ↔ `WBR/TS's Weekly Target.xlsx`
- **Project Dashboard KPIs** ↔ `Project Performance - Overview/*`
- **Time to Hire tab** ↔ `Project Performance - TIme to Hire/*`

For these, a future rebuild-verification session should diff the rebuilt dashboard's numbers directly against these files for week 17 (2026-04-21 to 2026-04-27) — or whatever week Andy exported (check modifiedTime).

For the **Pipelines Health**, **Recruiting Trends**, **Sourcing Stats**, **Data Cleanliness/Hygiene**, and **WBR II** snapshots — we don't have rebuilt equivalents yet. If/when we rebuild any of those, these files are the ground-truth target to match against.


## Empty exports (placeholders)

55 files contained no data. These correspond to chart / card / slicer visuals that Power BI's "Export data" can't serialize as a table. Not missing data — just visuals that aren't table-shaped. They're preserved here for completeness in case someone wants to cross-reference visual names.

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - KPI - TA Summary (Color)**

- `% Actual Screens to ATS by QuarterMonth.xlsx`
- `Hired by QuarterMonth.xlsx`
- `Screening Time (H) by QuarterMonth.xlsx`
- `data (1).xlsx`
- `data (2).xlsx`

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - KPI - TS Summary**

- `% Actual Screens to ATS by QuarterMonth.xlsx`
- `% Contacted to Positive Response by QuarterMonth.xlsx`
- `% Screens to Actual Screens by QuarterMonth.xlsx`
- `Top 5 Reasons.xlsx`
- `data (1).xlsx`
- `data (3).xlsx`

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - Missing Comment**

- `By Business Unit.xlsx`
- `data (1).xlsx`

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - Pipelines Health**

- `Problem Pipelines by Hiring Manager.xlsx`
- `Problem Pipelines by TA.xlsx`

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - Sourcing Stats**

- `# Days with viewed (Last 6 weeks, wo weekend). Can be only filtered by TS.xlsx`
- `Linkedin Profile Viewed & Contacted by Day (1).xlsx`
- `Linkedin Profile Viewed & Contacted by Day.xlsx`
- `Linkedin Profile Viewed & Contacted by Hour.xlsx`

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - WBR**

- `data (1).xlsx`
- `data (2).xlsx`

**KPI Dashboard (Leadership)/KPI Dashboard (Leadership) - WBR II**

- `# Pipelines with Early Warning Sign by TA.xlsx`

**Project Performance Page_Data Downloads/Project Performance - Candidate Response Rate**

- `Response Rate by YearMonthWeek.xlsx`

**Project Performance Page_Data Downloads/Project Performance - Internal Recruitment**

- `Disqualified Reasons.xlsx`
- `Total Progress.xlsx`

**Project Performance Page_Data Downloads/Project Performance - KPI - TA Summary**

- `% Actual Screens to ATS by QuarterMonth.xlsx`
- `Hired by QuarterMonth.xlsx`
- `Screening Time (H) by QuarterMonth.xlsx`
- `data (1).xlsx`
- `data (3).xlsx`

**Project Performance Page_Data Downloads/Project Performance - KPI - TS Summary**

- `% Actual Screens to ATS by QuarterMonth.xlsx`
- `% Contacted to Positive Response by QuarterMonth.xlsx`
- `% Screens to Actual Screens by QuarterMonth.xlsx`
- `Top 5 Reasons.xlsx`
- `data (1).xlsx`
- `data (3).xlsx`

**Project Performance Page_Data Downloads/Project Performance - New Role Estimate**

- `Reason Candidates Declined.xlsx`
- `The result you are selecting is based on the following historical data.xlsx`
- `To get 1 Hire.xlsx`
- `data (1).xlsx`

**Project Performance Page_Data Downloads/Project Performance - Overview**

- `Actual Screens.xlsx`
- `Candidates Contacted.xlsx`
- `Disqualified Reason.xlsx`
- `Hires.xlsx`
- `Moved to ATS.xlsx`
- `Offers.xlsx`
- `Recruiter Screens.xlsx`
- `data (1).xlsx`

**Project Performance Page_Data Downloads/Project Performance - Recruiting Trends & conversions**

- `Actual Screens and ATS by MonthWeek.xlsx`
- `Disqualified Reason (All time data, date filter cannot be applied)).xlsx`
- `Funnel based on Contacted Date.xlsx`
- `Linkedin Profile Viewed and Contacted by MonthWeek.xlsx`
- `Offers and Hires by MonthWeek.xlsx`

**Project Performance Page_Data Downloads/Project Performance - TIme to Hire**

- `Month Trends.xlsx`

**Project Performance Page_Data Downloads/Project Performance - Weekly Progress**

- `Total Progress.xlsx`

# CSV Export — Scope (planning only, nothing built)

**Decision needed:** which tables get the button in Phase 1, or all 25 at once.

## What it is
A small "⬇ CSV" button on the top-right of each table card. Clicking it downloads
exactly what's on screen — current filters, current date preset, current drill-down.
Filename like `tribe_wbr_client-summary_last-4w_2026-06-11.csv`.

## Why it's cheap
The dashboard is client-side React; the full dataset (`dashboard_data.json`, ~5.7MB)
already ships to every user's browser. Every table renders from an in-memory array,
so export = serialize that same array. No pipeline, Keboola, or data changes. No new
hosting. Works offline once the page loads.

## Build shape
- One shared helper (`downloadCSV(filename, headers, rows)`) + one `<ExportBtn>` component (~40 lines total).
- Wire it to each table's existing memoized row array — roughly 1–3 lines per table.
- 25 tables across 8 tabs (Project Dashboard, Weekly Summary, WBR, MBR, Profitability, TTH, TS Summary, IR).

## Effort
- Phase 1 (5–6 highest-traffic tables: PD job table, Weekly Summary, WBR client + per-TA, TTH jobs, TS Summary): ~half a day incl. build-verify + deploy.
- Phase 2 (remaining tables incl. drill-down modals): another half day.

## Risks
1. **App.jsx edit truncation** — known gotcha, 4,542 lines. Mitigation: Python search/replace, `npx vite build` before every push (twice bitten).
2. **Local folder lags GitHub** — clone bark8922/tribe-recruiting fresh, diff first, never cp local→repo blind.
3. **Export leaks past role gates** — WBR/MBR are leadership-only; a downloaded CSV can be forwarded anywhere. Same is true of screenshots today, but worth a conscious OK.
4. **Excel encoding** — recruiter/client names have diacritics; need UTF-8 BOM or Excel mangles them. Trivial, just must not forget.
5. **Pivot tables** — weekly-column tables export as rendered (one column per week), not raw rows. Fine for the stated use case (Excel/AI tools); flagging so nobody expects raw data.

## Explicitly out of scope
Rodrigo's PBI "Data Download" tab (candidate lists + LinkedIn URLs). Different data —
candidate-level detail isn't in dashboard_data.json. Separate 1–2 day build if/when approved.

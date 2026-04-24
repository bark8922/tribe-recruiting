# Snapshots

Drop CSV exports from Power BI here. One file per page (more if a page has multiple primary tables worth capturing).

## Naming

`<page-slug>-<what>-<week-range>.csv`

Examples:
- `time-to-hire-job-detail-w13-16.csv`
- `sourcing-stats-per-sourcer-w13-16.csv`
- `kpi-ta-summary-w16.csv`
- `pipelines-health-client-rollup-w13-16.csv`

## How to export

1. Open the page in Power BI
2. Apply the **default filters only** (the view someone sees when opening the page cold)
3. For each main table visual: click the `...` menu → **Export data** → **Summarized data** (CSV)
4. Save as above, drop it here

If a page has charts but no tables, you can skip it OR export the chart's underlying data the same way.

## Why these matter

After you leave, these are **ground truth**. When someone rebuilds Time to Hire in SQL six months from now, they diff their output against `time-to-hire-*.csv` to know if they got it right. Without these, there's no way to verify a rebuild.

## If the export is huge

If Power BI caps at 150k rows, that's fine — just export what you can. We don't need every row ever, just a representative recent slice.

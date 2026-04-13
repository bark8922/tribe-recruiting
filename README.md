# Tribe.xyz Recruiting Dashboard

Internal recruiting analytics dashboard — replaces Power BI + Keboola/Snowflake for Tribe.xyz's staffing pipeline (~€96K/year savings).

**Live:** https://tribe-recruiting.pages.dev

## Stack

- React 18 + Vite 5 + Recharts
- Tailwind CSS (via CDN)
- Hosted on Cloudflare Pages (auto-deploys from `main`)
- Data source: pre-computed `dashboard_data.json` generated from Keboola/Snowflake queries

## Project Structure

```
tribe-recruiting/
├── .gitignore
├── README.md
└── recruiting-dashboard/
    ├── package.json
    ├── vite.config.js
    ├── index.html
    └── src/
        ├── main.jsx
        ├── App.jsx             # Main dashboard UI
        ├── index.css
        └── dashboard_data.json # Pre-computed KPIs / time series
```

## Local Development

```bash
cd recruiting-dashboard
npm install
npm run dev      # http://localhost:5173
```

## Deployment

Pushes to `main` trigger a Cloudflare Pages build automatically.

**Cloudflare Pages settings** (Settings → Builds & deployments):
- Build command: `cd recruiting-dashboard && npm install && npm run build`
- Build output directory: `recruiting-dashboard/dist`
- Root directory: `/`
- Node version: `18` (or later)

## Iteration Workflow

1. Edit `recruiting-dashboard/src/App.jsx` (UI) or `recruiting-dashboard/src/dashboard_data.json` (data).
2. `cd recruiting-dashboard && npm run dev` to preview locally.
3. Commit and push — Cloudflare rebuilds and deploys automatically.

## Data Refresh

`dashboard_data.json` is generated from Keboola/Snowflake queries. See internal docs for the refresh pipeline.

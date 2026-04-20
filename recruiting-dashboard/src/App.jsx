import React, { useState, useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid, Legend
} from 'recharts';
import { Search } from 'lucide-react';
import dashboardDataPbi from './dashboard_data.json';
import dashboardDataSnowflake from './dashboard_data_snowflake.json';

// WEEKS is now derived per-render from data.wbr_ta_weekly_roster keys so that
// newly-added weeks (e.g. w16, w17) appear automatically once the weekly roster
// syncs from Andy's Google Sheet. See derivation inside WBRTab.
// WBR TA Target sheet values are WEEKLY targets (the "Month" column is the
// period the target applies to, NOT the cadence). PBI compares weekly actual
// directly to the target value — so no /4.33 divide. Verified against w16
// Client's Target PBI screenshot 2026-04-20: Rule-B (no divide) matches 28/36
// colored cells vs Rule-A (with /4.33) which only matched 11/36.
const WEEKLY_DIVISOR = 1;

// Client name normalization — matches Power BI's Replace Value steps exactly
// PBI merges DoorDash → "Wolt HQ" and SevenRooms → "Wolt HQ" in target + notes tables
const normalizeClient = (client) => {
  if (!client) return client;
  const trimmed = client.trim();
  if (trimmed.toUpperCase() === 'AVIV') return 'Aviv';
  if (trimmed.toLowerCase() === 'doordash') return 'Wolt HQ';     // PBI: DoorDash → Wolt HQ
  if (trimmed.toLowerCase() === 'sevenrooms') return 'Wolt HQ';   // PBI: SevenRooms → Wolt HQ
  if (trimmed.toLowerCase() === 'nexi') return 'Nexi';
  return trimmed;
};

// Check if a raw Keboola client key matches a display client for 12w hires / roles lookup
// Keboola uses "Wolt" for all Wolt divisions, "AVIV " for Aviv, "Doordash" for DoorDash, etc.
// PBI merges SevenRooms + DoorDash into the Wolt group, so any Wolt division should also match those
const kebolaClientMatches = (rawKeboolaClient, displayClient) => {
  const norm = normalizeClient(rawKeboolaClient);
  if (norm === displayClient) return true;
  const raw = rawKeboolaClient.trim();
  // Keboola "Wolt", "Doordash", "SevenRooms" all match any Wolt division
  if (displayClient.startsWith('Wolt') && (raw === 'Wolt' || raw.toLowerCase() === 'doordash' || raw.toLowerCase() === 'sevenrooms')) return true;
  return false;
};

// Normalize TA name — Keboola has double spaces for some names
const normalizeTa = (name) => (name || '').replace(/\s+/g, ' ').trim();

// Business unit group — PBI groups by CLIENT name, not the team_group field in targets
// Aviv + all Wolt divisions (incl DoorDash/SevenRooms → Wolt HQ) = Dolphins & Whales
// Everything else = Ponies & Unicorns
const getBuGroup = (displayClient) => {
  if (!displayClient) return 'Ponies/Unicorns';
  if (displayClient === 'Aviv' || displayClient.startsWith('Wolt')) return 'Dolphins/Whales';
  return 'Ponies/Unicorns';
};

// Color based on % of target (5-tier heatmap) — matches Power BI exactly
// Below 50%: Red | 50-75%: Orange | 75-100%: Yellow | 100-120%: Light green | Above 120%: Green
const getCellStyle = (actual, weeklyTarget) => {
  if (!weeklyTarget || weeklyTarget === 0) return {};
  const pct = (actual / weeklyTarget) * 100;
  if (pct > 120) return { backgroundColor: '#166534', color: '#bbf7d0' };    // green
  if (pct >= 100) return { backgroundColor: '#2f6846', color: '#d1fae5' };   // light green
  if (pct >= 75) return { backgroundColor: '#854d0e', color: '#fef9c3' };    // yellow
  if (pct >= 50) return { backgroundColor: '#9a3412', color: '#fed7aa' };    // orange
  return { backgroundColor: '#991b1b', color: '#fecaca' };                   // red
};

// WBR Tab
const WBRTab = ({ data }) => {
  // Derive week list from the weekly roster (keys like "w15", "w16"). This
  // auto-advances as Andy adds new weeks to the TA Weekly Note sheet.
  const WEEKS = useMemo(() => {
    const rosterKeys = Object.keys(data?.wbr_ta_weekly_roster || {});
    const tsKeys = Object.keys(data?.wbr_ts_weekly_roster || {});
    const allKeys = new Set([...rosterKeys, ...tsKeys]);
    const nums = [...allKeys]
      .filter((k) => /^w\d+$/.test(k))
      .map((k) => parseInt(k.slice(1), 10))
      .filter(Number.isFinite);
    return nums.length ? nums.sort((a, b) => a - b) : [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15];
  }, [data]);

  // Default to the latest available week so "opening the dashboard" always
  // lands on the most recent completed week.
  const [selectedWeek, setSelectedWeek] = useState(() => {
    const rosterKeys = Object.keys(data?.wbr_ta_weekly_roster || {});
    const tsKeys = Object.keys(data?.wbr_ts_weekly_roster || {});
    const nums = [...new Set([...rosterKeys, ...tsKeys])]
      .filter((k) => /^w\d+$/.test(k))
      .map((k) => parseInt(k.slice(1), 10))
      .filter(Number.isFinite);
    return nums.length ? Math.max(...nums) : 15;
  });

  // Build client summary for selected week
  const clientSummary = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const summary = {};

    // Per-week roster from TA Weekly Note — determines which clients are active this week
    const weeklyRoster = data.wbr_ta_weekly_roster?.[weekKey] || [];
    const activeClientsThisWeek = new Set(
      weeklyRoster.map(pair => normalizeClient(pair.split('|')[0]))
    );
    const hasWeeklyRoster = weeklyRoster.length > 0;

    // Initialize from targets, filtered to only clients active in this week
    data.targets.forEach((t) => {
      const display = normalizeClient(t.client);
      // Skip clients not in the weekly note for this week
      if (hasWeeklyRoster && !activeClientsThisWeek.has(display)) return;
      if (!summary[display]) {
        summary[display] = {
          client: display,
          contacted: 0, screened: 0, ats: 0, offers: 0, hires: 0,
          contacted_target: 0, screened_target: 0, ats_target: 0, hires_target: 0,
          roles: 0, hires_12w: 0,
        };
      }
      summary[display].contacted_target += (t.contacted || 0) / WEEKLY_DIVISOR;
      summary[display].screened_target += (t.actual_screens || 0) / WEEKLY_DIVISOR;
      summary[display].ats_target += (t.moved_to_ats || 0) / WEEKLY_DIVISOR;
      summary[display].hires_target += (t.hires || 0) / WEEKLY_DIVISOR;
    });

    // Add actuals — skip "roster-only" target rows (empty team_group + all-zero targets).
    // These are TAs added to the Weekly Note but not in the official TA Target sheet;
    // PBI excludes them from the Client Summary. Example (w16): Iryna Dyda showed up
    // under Aviv with 101 contacted, inflating Aviv from 549 (PBI) to 652. Filtering
    // team_group='' brings Aviv to 551 (+2 vs PBI), screens 95 exact, ATS 47 exact.
    data.targets.forEach((t) => {
      if (!t.team_group) return;  // skip roster-only placeholder TAs
      const display = normalizeClient(t.client);
      // Find matching actuals for this TA across all matching raw client keys
      Object.keys(data.wbr_actuals).forEach((key) => {
        const [rawClient, rawTa] = key.split('|');
        if (kebolaClientMatches(rawClient, display) && normalizeTa(rawTa) === normalizeTa(t.ta)) {
          const wk = data.wbr_actuals[key]?.[weekKey];
          if (wk && summary[display]) {
            summary[display].contacted += wk.contacted || 0;
            summary[display].screened += wk.actual_screens || wk.screened || 0;
            summary[display].ats += wk.ats || 0;
            summary[display].offers += wk.offers || 0;
            summary[display].hires += wk.hires || 0;
          }
        }
      });

    });

    // --- # Jobs (roles) column ---
    // data.roles is keyed by (raw_client|TA) and DOUBLE-COUNTS jobs that have
    // multiple TAs assigned, so Aviv summed to 23 TA-assignments across 6 TAs
    // while only having ~26 distinct jobs. Use data.jobs (one row per job)
    // for a clean distinct count.
    //
    // Filters: is_job_archived=false AND is_external_recruiter=false
    // (PBI excludes external-recruiter jobs from the WBR client summary.)
    //
    // Keboola uses a single catch-all 'Wolt' client_name for every Wolt
    // sub-BU. To split those across Wolt HQ/Tech/Market/etc. we look up each
    // job's job_recruiter in data.targets to find the TA's canonical client
    // (sub-BU). Unmatched Wolt jobs stay unallocated rather than dumping into
    // a default sub-BU.
    const recruiterToWoltSubBu = new Map();
    data.targets.forEach((t) => {
      const ta = normalizeTa(t.ta);
      const cl = normalizeClient(t.client);
      if (ta && cl && cl.startsWith('Wolt') && !recruiterToWoltSubBu.has(ta)) {
        recruiterToWoltSubBu.set(ta, cl);
      }
    });

    const seenJobIds = new Set();
    (data.jobs || []).forEach((job) => {
      if (String(job.is_job_archived).toLowerCase() !== 'false') return;
      if (String(job.is_external_recruiter).toLowerCase() !== 'false') return;
      if (seenJobIds.has(job.job_id)) return;
      seenJobIds.add(job.job_id);

      const raw = (job.client_name || '').trim();
      let client = normalizeClient(raw);
      if (client === 'Wolt') {
        // Split Wolt catch-all via recruiter's canonical sub-BU
        client = recruiterToWoltSubBu.get(normalizeTa(job.job_recruiter)) || null;
      }
      if (client && summary[client]) summary[client].roles += 1;
    });

    // --- Last 12w Hires column ---
    // Normalize raw client key so 'AVIV ' → 'Aviv', 'Doordash' → 'Wolt HQ'.
    // For raw 'Wolt' catch-all, split via the TA's canonical Wolt sub-BU.
    Object.entries(data.hires_12w || {}).forEach(([key, val]) => {
      const [rawClient, rawTa] = key.split('|');
      let client = normalizeClient(rawClient);
      if (client === 'Wolt') {
        client = recruiterToWoltSubBu.get(normalizeTa(rawTa)) || null;
      }
      if (client && summary[client]) summary[client].hires_12w += val;
    });

    return Object.values(summary).sort((a, b) => a.client.localeCompare(b.client));
  }, [data, selectedWeek]);

  // TA detail table
  const taDetail = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const details = [];

    // Per-week roster — filter to only TAs active this week
    const weeklyRoster = data.wbr_ta_weekly_roster?.[weekKey] || [];
    const activePairsThisWeek = new Set(
      weeklyRoster.map(pair => {
        const [c, ta] = pair.split('|');
        return `${normalizeClient(c)}|${normalizeTa(ta)}`;
      })
    );
    const hasWeeklyRoster = weeklyRoster.length > 0;

    data.targets.forEach((t) => {
      const display = normalizeClient(t.client);
      // Skip (client, TA) pairs not in the weekly note for this week
      if (hasWeeklyRoster && !activePairsThisWeek.has(`${display}|${normalizeTa(t.ta)}`)) return;
      let actual = { contacted: 0, screened: 0, ats: 0, offers: 0, hires: 0 };

      Object.keys(data.wbr_actuals).forEach((key) => {
        const [rawClient, rawTa] = key.split('|');
        if (kebolaClientMatches(rawClient, display) && normalizeTa(rawTa) === normalizeTa(t.ta)) {
          const wk = data.wbr_actuals[key]?.[weekKey];
          if (wk) {
            actual.contacted += wk.contacted || 0;
            actual.screened += wk.actual_screens || wk.screened || 0;
            actual.ats += wk.ats || 0;
            actual.offers += wk.offers || 0;
            actual.hires += wk.hires || 0;
          }
        }
      });

      // Roles for this TA
      let roles = 0;
      Object.keys(data.roles || {}).forEach((key) => {
        const [rClient, rTa] = key.split('|');
        if (kebolaClientMatches(rClient, display) && normalizeTa(rTa) === normalizeTa(t.ta)) {
          roles += data.roles[key] || 0;
        }
      });

      // 12w hires for this TA
      let hires12w = 0;
      Object.keys(data.hires_12w || {}).forEach((key) => {
        const [hClient, hTa] = key.split('|');
        if (kebolaClientMatches(hClient, display) && normalizeTa(hTa) === normalizeTa(t.ta)) {
          hires12w += data.hires_12w[key] || 0;
        }
      });

      // 12w ATS for this TA
      let ats12w = 0;
      Object.keys(data.ta_ats_12w || {}).forEach((key) => {
        const [c12, t12] = key.split('|');
        if (kebolaClientMatches(c12, display) && normalizeTa(t12) === normalizeTa(t.ta)) {
          ats12w += data.ta_ats_12w[key] || 0;
        }
      });

      // 12w Screens for this TA
      let screens12w = 0;
      Object.keys(data.ta_screens_12w || {}).forEach((key) => {
        const [c12, t12] = key.split('|');
        if (kebolaClientMatches(c12, display) && normalizeTa(t12) === normalizeTa(t.ta)) {
          screens12w += data.ta_screens_12w[key] || 0;
        }
      });

      // 12w Time to Fill (weighted avg across matching entries)
      let ttfSum = 0, ttfCount = 0;
      Object.keys(data.ta_ttf_12w || {}).forEach((key) => {
        const [cTtf, tTtf] = key.split('|');
        if (kebolaClientMatches(cTtf, display) && normalizeTa(tTtf) === normalizeTa(t.ta)) {
          ttfSum += data.ta_ttf_12w[key] || 0;
          ttfCount += 1;
        }
      });
      const ttf12w = ttfCount > 0 ? Math.round(ttfSum / ttfCount) : null;

      // Jobs opened > 60 days
      let jobs60d = 0;
      Object.keys(data.ta_jobs_60d || {}).forEach((key) => {
        const [c60, t60] = key.split('|');
        if (kebolaClientMatches(c60, display) && normalizeTa(t60) === normalizeTa(t.ta)) {
          jobs60d += data.ta_jobs_60d[key] || 0;
        }
      });

      // Find TA note for this week — match using same normalization as actuals
      const note = (data.ta_weekly_notes || []).find(
        (n) => n.ta === t.ta && n.week === selectedWeek &&
          kebolaClientMatches(n.client || '', display)
      );

      // Computed ratios
      const pctScreensToHires = screens12w > 0 ? Math.round((hires12w / screens12w) * 100) : null;
      const pctScreensToAts = actual.screened > 0 ? Math.round((actual.ats / actual.screened) * 100) : null;

      details.push({
        client: display,
        ta: t.ta,
        team_group: getBuGroup(display),
        contacted: actual.contacted,
        screened: actual.screened,
        ats: actual.ats,
        offers: actual.offers,
        hires: actual.hires,
        roles,
        hires_12w: hires12w,
        ats_12w: ats12w,
        screens_12w: screens12w,
        ttf_12w: ttf12w,
        jobs_60d: jobs60d,
        pct_screens_to_hires: pctScreensToHires,
        pct_screens_to_ats: pctScreensToAts,
        contacted_target: (t.contacted || 0) / WEEKLY_DIVISOR,
        screened_target: (t.actual_screens || 0) / WEEKLY_DIVISOR,
        ats_target: (t.moved_to_ats || 0) / WEEKLY_DIVISOR,
        hires_target: (t.hires || 0) / WEEKLY_DIVISOR,
        comment: note?.comment || '',
        reasoning: note?.reasoning || '',
      });
    });

    // Sort: group first, then client, then TA
    const groupOrder = { 'Dolphins/Whales': 0, 'Ponies/Unicorns': 1, 'Other': 2 };
    return details.sort((a, b) => {
      const ga = groupOrder[a.team_group] ?? 2;
      const gb = groupOrder[b.team_group] ?? 2;
      if (ga !== gb) return ga - gb;
      if (a.client !== b.client) return a.client.localeCompare(b.client);
      return a.ta.localeCompare(b.ta);
    });
  }, [data, selectedWeek]);

  // TS weekly data for selected week - enriched with actuals
  const tsData = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const tsMap = {};

    // Start from ts_weekly targets for this week
    (data.ts_weekly || [])
      .filter((t) => t.week === selectedWeek)
      .forEach((t) => {
        tsMap[t.ts] = { ...t, contacted_target: Number(t.contacted_target) || null };
      });

    // Only show sourcers who have a ts_weekly entry for this week
    // (actuals-only sourcers without a weekly entry are excluded)

    // Enrich all with actuals and job data
    return Object.values(tsMap).map((t) => {
      const tsName = t.ts;
      const actuals = data.ts_actuals?.[tsName]?.[weekKey] || {};
      const jobs = data.ts_jobs?.[tsName] || {};
      const hires12w = data.ts_hires_12w?.[tsName] || 0;

      return {
        ...t,
        contacted: actuals.contacted || 0,
        recruiter_screens: actuals.recruiter_screens || actuals.screened || 0,
        actual_screens: actuals.actual_screens || 0,
        ats: actuals.ats || 0,
        offers: actuals.offers || 0,
        hires: actuals.hires || 0,
        num_jobs: jobs.num_jobs || 0,
        num_tas: jobs.num_tas || 0,
        ta_names: jobs.ta_names || '',
        hires_12w: hires12w,
      };
    }).sort((a, b) => a.ts.localeCompare(b.ts));
  }, [data, selectedWeek]);

  // TS Overall Conversion Rate with Officially Assigned Active Pipelines
  // ─────────────────────────────────────────────────────────────────────
  // All numerators + denominators come from Andy's `ts_conversion` export
  // (ts_queries_v4.sql — validated vs PBI wk15).
  // PBI's view scopes BOTH contacted and recruiter_screens to the active
  // pipelines, which is why an unscoped ts_actuals aggregate produces the
  // wrong %s. Using the scoped values from ts_conversion yields an exact
  // match to PBI.
  // Strict filter: job.job_sourcer = TS AND credited event from TS exists on the job.
  // Roster filter: ts_weekly entries for the selected week (same source of truth
  // as the TS Weekly table above — Andy's WBR Target Google Sheet). Scopes to
  // who was an active TS at the point in time of the selected week.
  const tsConversion = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const activeRoster = new Set(
      (data.ts_weekly || [])
        .filter((t) => t.week === selectedWeek)
        .map((t) => t.ts)
    );
    // Use per-week cumulative conversion data when available; fall back to
    // static snapshot for backward compat.
    const weeklyConv = data.ts_conversion_weekly?.[weekKey];
    const source = weeklyConv
      ? weeklyConv.filter((row) => activeRoster.has(row.ts))
      : (data.ts_conversion || []).filter((row) => activeRoster.has(row.ts));
    return source.map((row) => {
      const contacted = row.contacted || 0;
      const recruiterScreens = row.recruiter_screens || 0;
      const actualScreens = row.actual_screens || 0;
      const positiveResponse = row.positive_response || 0;
      const ats = row.ats || 0;
      return {
        ts: row.ts,
        active_jobs: row.active_pipelines || 0,
        contacted,
        positive_responses: positiveResponse,
        recruiter_screens: recruiterScreens,
        actual_screens: actualScreens,
        ats,
        pct_contacted_to_pr:  contacted        > 0 ? Math.round(positiveResponse / contacted        * 1000) / 10 : null,
        pct_screen_to_actual: recruiterScreens > 0 ? Math.round(actualScreens    / recruiterScreens * 1000) / 10 : null,
        pct_actual_to_ats:    actualScreens    > 0 ? Math.round(ats              / actualScreens    * 1000) / 10 : null,
      };
    }).sort((a, b) => a.ts.localeCompare(b.ts));
  }, [data, selectedWeek]);

  return (
    <div className="space-y-6">
      {/* Week Selector */}
      <div className="flex items-center gap-3">
        <label className="text-sm font-medium text-gray-300">Select Week:</label>
        <select
          value={selectedWeek}
          onChange={(e) => setSelectedWeek(parseInt(e.target.value))}
          className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm"
        >
          {WEEKS.map((w) => (
            <option key={w} value={w}>Week {w}</option>
          ))}
        </select>
      </div>

      {/* Client Summary */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Client Summary — Week {selectedWeek}</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2">Client</th>
                <th className="text-center px-2 py-2">Roles</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Screens</th>
                <th className="text-center px-2 py-2">ATS</th>
                <th className="text-center px-2 py-2">Offers</th>
                <th className="text-center px-2 py-2">Hires</th>
                <th className="text-center px-2 py-2">12w Hires</th>
              </tr>
            </thead>
            <tbody>
              {clientSummary.map((row, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-2 text-white font-medium">{row.client}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.roles}</td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.contacted, row.contacted_target)}>
                    {row.contacted}
                  </td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.screened, row.screened_target)}>
                    {row.screened}
                  </td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.ats, row.ats_target)}>
                    {row.ats}
                  </td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.offers}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.roles, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.contacted, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.screened, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.ats, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.offers, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.hires, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.hires_12w, 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* TA Detail */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TA Weekly Detail — Week {selectedWeek}</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm" style={{ minWidth: '1400px' }}>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2 sticky left-0 bg-gray-800 z-10">Client</th>
                <th className="text-left px-2 py-2">TA</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12 Weeks Hires">12w H</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12 Weeks ATS">12w ATS</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12 Weeks Screens">12w Scr</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12w % Actual Screens to Hires">12w %S→H</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12w Time to Fill (days)">12w TTF</th>
                <th className="text-center px-1 py-2 text-xs" title="Weekly Hires">Hires</th>
                <th className="text-center px-1 py-2 text-xs" title="Weekly Contacted">Cntd</th>
                <th className="text-center px-1 py-2 text-xs" title="Weekly Actual Screens">Scrn</th>
                <th className="text-center px-1 py-2 text-xs" title="Weekly ATS">ATS</th>
                <th className="text-center px-1 py-2 text-xs" title="% Actual Screens to ATS">%S→A</th>
                <th className="text-center px-1 py-2 text-xs" title="# Active Roles"># Jobs</th>
                <th className="text-center px-1 py-2 text-xs" title="Jobs Opened > 60 days">{'>'}60d</th>
                <th className="text-left px-2 py-2 text-xs min-w-[120px]">Comment</th>
              </tr>
            </thead>
            <tbody>
              {['Dolphins/Whales', 'Ponies/Unicorns'].map((group) => {
                const groupRows = taDetail.filter(r => r.team_group === group);
                if (groupRows.length === 0) return null;
                return (
                  <React.Fragment key={group}>
                    <tr className="bg-gray-900">
                      <td colSpan={15} className="text-left px-2 py-3 text-white font-bold text-base" style={{ borderTop: '3px solid #4B5563' }}>
                        {group === 'Dolphins/Whales' ? '🐬 Dolphins & Whales' : '🦄 Ponies & Unicorns'}
                      </td>
                    </tr>
                    {groupRows.map((row, idx) => {
                      const prevRow = idx > 0 ? groupRows[idx - 1] : null;
                      const isClientChange = !prevRow || prevRow.client !== row.client;
                      return (
                        <tr key={`${group}-${idx}`} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} ${isClientChange ? 'border-t border-gray-600' : ''}`}>
                          <td className="text-left px-2 py-2 text-white font-medium sticky left-0 bg-inherit z-10">{isClientChange ? row.client : ''}</td>
                          <td className="text-left px-2 py-2 text-gray-300 whitespace-nowrap">{row.ta}</td>
                          <td className="text-center px-1 py-2 text-gray-300">{row.hires_12w || '—'}</td>
                          <td className="text-center px-1 py-2 text-gray-300">{row.ats_12w || '—'}</td>
                          <td className="text-center px-1 py-2 text-gray-300">{row.screens_12w || '—'}</td>
                          <td className="text-center px-1 py-2 text-gray-400">{row.pct_screens_to_hires != null ? `${row.pct_screens_to_hires}%` : '—'}</td>
                          <td className="text-center px-1 py-2 text-gray-400">{row.ttf_12w != null ? row.ttf_12w : '—'}</td>
                          <td className="text-center px-1 py-2 text-gray-300">{row.hires || ''}</td>
                          <td className="text-center px-1 py-2" style={getCellStyle(row.contacted, row.contacted_target)}>
                            {row.contacted || ''}
                          </td>
                          <td className="text-center px-1 py-2" style={getCellStyle(row.screened, row.screened_target)}>
                            {row.screened || ''}
                          </td>
                          <td className="text-center px-1 py-2" style={getCellStyle(row.ats, row.ats_target)}>
                            {row.ats || ''}
                          </td>
                          <td className="text-center px-1 py-2 text-gray-400">{row.pct_screens_to_ats != null ? `${row.pct_screens_to_ats}%` : '—'}</td>
                          <td className="text-center px-1 py-2 text-gray-300">{row.roles || ''}</td>
                          <td className="text-center px-1 py-2 text-gray-300">{row.jobs_60d || ''}</td>
                          <td className="text-left px-2 py-2 text-gray-400 text-xs max-w-xs truncate" title={row.comment}>
                            {row.comment || '—'}
                          </td>
                        </tr>
                      );
                    })}
                    <tr className="bg-gray-700 font-semibold" style={{ borderTop: '2px solid #6B7280' }}>
                      <td className="text-left px-2 py-2 text-white sticky left-0 bg-gray-700 z-10">{group} Total</td>
                      <td className="text-left px-2 py-2 text-gray-300">—</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.hires_12w, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.ats_12w, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.screens_12w, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">—</td>
                      <td className="text-center px-1 py-2 text-white">—</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.hires, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.contacted, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.screened, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.ats, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">—</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.roles, 0)}</td>
                      <td className="text-center px-1 py-2 text-white">{groupRows.reduce((s, r) => s + r.jobs_60d, 0)}</td>
                      <td className="text-left px-2 py-2 text-gray-400">—</td>
                    </tr>
                  </React.Fragment>
                );
              })}
              <tr className="bg-gray-600 border-t-2 border-gray-500 font-bold">
                <td className="text-left px-2 py-2 text-white sticky left-0 bg-gray-600 z-10">Grand Total</td>
                <td className="text-left px-2 py-2 text-gray-300">—</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.hires_12w, 0)}</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.ats_12w, 0)}</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.screens_12w, 0)}</td>
                <td className="text-center px-1 py-2 text-white">—</td>
                <td className="text-center px-1 py-2 text-white">—</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.hires, 0)}</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.contacted, 0)}</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.screened, 0)}</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.ats, 0)}</td>
                <td className="text-center px-1 py-2 text-white">—</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.roles, 0)}</td>
                <td className="text-center px-1 py-2 text-white">{taDetail.reduce((s, r) => s + r.jobs_60d, 0)}</td>
                <td className="text-left px-2 py-2 text-gray-400">—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* TS Weekly */}
      <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-lg font-semibold text-white mb-4">TS (Sourcer) Weekly — Week {selectedWeek}</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-300 border-b border-gray-600">
                  <th className="text-left px-2 py-2">Sourcer</th>
                  <th className="text-center px-2 py-2">12w Hires</th>
                  <th className="text-center px-2 py-2">Contacted</th>
                  <th className="text-center px-2 py-2">Target</th>
                  <th className="text-center px-2 py-2">Recruiter Screens</th>
                  <th className="text-center px-2 py-2">Actual Screens</th>
                  <th className="text-center px-2 py-2">Moved to ATS</th>
                  <th className="text-center px-2 py-2"># Jobs</th>
                  <th className="text-center px-2 py-2"># TA</th>
                  <th className="text-left px-2 py-2">TA Names</th>
                  <th className="text-left px-2 py-2">Comment</th>
                </tr>
              </thead>
              <tbody>
                {tsData.map((row, idx) => (
                  <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                    <td className="text-left px-2 py-2 text-white font-medium">{row.ts}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w}</td>
                    <td className="text-center px-2 py-2" style={getCellStyle(row.contacted, row.contacted_target)}>
                      {row.contacted}
                    </td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.contacted_target || '—'}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.recruiter_screens}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.actual_screens}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.ats}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.num_jobs}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.num_tas}</td>
                    <td className="text-left px-2 py-2 text-gray-400 text-xs">{row.ta_names || '—'}</td>
                    <td className="text-left px-2 py-2 text-gray-400 text-xs max-w-xs truncate" title={row.comment || ''}>{row.comment || '—'}</td>
                  </tr>
                ))}
                <tr className="bg-gray-750 border-t border-gray-600 font-semibold">
                  <td className="text-left px-2 py-2 text-white">Total</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.hires_12w, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.contacted, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{Number(tsData.reduce((sum, r) => sum + (Number(r.contacted_target) || 0), 0)).toFixed(1)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.recruiter_screens, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.actual_screens, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.ats, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.num_jobs, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.num_tas, 0)}</td>
                  <td className="text-left px-2 py-2 text-gray-400">—</td>
                  <td className="text-left px-2 py-2 text-gray-400">—</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

      {/* TS Overall Conversion Rate with Officially Assigned Active Pipelines */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-1">TS Overall Conversion Rate with Officially Assigned Active Pipelines</h3>
        <p className="text-xs text-gray-500 mb-4">
          Pipelines where the sourcer is officially assigned <em>and</em> actively working the pipeline.
          Funnel metrics count candidates on those pipelines only.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2">TS</th>
                <th className="text-center px-2 py-2">Active Jobs</th>
                <th className="text-center px-2 py-2">% Contacted to Positive Response</th>
                <th className="text-center px-2 py-2">Positive Response</th>
                <th className="text-center px-2 py-2">% Screens to Actual Screen</th>
                <th className="text-center px-2 py-2">Actual Screens</th>
                <th className="text-center px-2 py-2">% Actual Screens to ATS</th>
                <th className="text-center px-2 py-2">ATS</th>
              </tr>
            </thead>
            <tbody>
              {tsConversion.map((row, idx) => {
                const fmt = (v) => v == null ? '—' : `${v}%`;
                // PBI-style conditional formatting thresholds (approximated from PBI screenshot)
                const cell = (v, greenAt) => {
                  if (v == null) return 'text-gray-500';
                  return v >= greenAt ? 'bg-green-700/40 text-white' : 'bg-red-700/40 text-white';
                };
                return (
                  <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                    <td className="text-left px-2 py-2 text-white font-medium">{row.ts}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.active_jobs}</td>
                    <td className={`text-center px-2 py-2 ${cell(row.pct_contacted_to_pr, 20)}`}>{fmt(row.pct_contacted_to_pr)}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.positive_responses}</td>
                    <td className={`text-center px-2 py-2 ${cell(row.pct_screen_to_actual, 75)}`}>{fmt(row.pct_screen_to_actual)}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.actual_screens}</td>
                    <td className={`text-center px-2 py-2 ${cell(row.pct_actual_to_ats, 55)}`}>{fmt(row.pct_actual_to_ats)}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.ats}</td>
                  </tr>
                );
              })}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.active_jobs, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.positive_responses, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.actual_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.ats, 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="text-xs text-gray-500 mt-3">
          Source: <code className="text-gray-400">ts_queries_v4.sql</code> · Andy Hsu logic (2026-04-14) · Validated vs PBI week 15 at 99.4%
        </p>
      </div>

    </div>
  );
};

// MBR Tab — Monthly Business Review (last 4 weeks, w12-w15 = 2026-03-16 to 2026-04-12)
const MBRTab = ({ data }) => {
  const MBR_WEEKS = (data.mbr_window?.weeks || ['w12','w13','w14','w15']);
  const windowLabel = `${data.mbr_window?.start || '2026-03-16'} → ${data.mbr_window?.end || '2026-04-12'}`;

  // Build client-level rows from mbr_client_totals (Keboola-sourced, includes Wolt subdivisions)
  const clientRows = useMemo(() => {
    const rows = Object.entries(data.mbr_client_totals || {}).map(([client, t]) => ({
      client,
      contacted: t.contacted || 0,
      actual_screens: t.actual_screens || 0,
      ats: t.ats || 0,
      offers: t.offers || 0,
      hires: t.hires || 0,
      hires_12w: t.hires_12w || 0,
    }));
    // hide all-zero rows
    return rows.filter(r => r.contacted + r.actual_screens + r.ats + r.offers + r.hires + r.hires_12w > 0)
               .sort((a, b) => a.client.localeCompare(b.client));
  }, [data]);

  // Build TA-level rows (with targets and comments)
  const taRows = useMemo(() => {
    const targets = data.mbr_ta_targets || [];
    // Map of latest comment per normalized TA key (from ta_weekly_notes, picking latest week available)
    const latestNote = {};
    (data.ta_weekly_notes || []).forEach(n => {
      const key = `${normalizeTa(n.ta)}`;
      if (!latestNote[key] || n.week > latestNote[key].week) {
        latestNote[key] = n;
      }
    });

    const result = [];
    targets.forEach(t => {
      const key = `${t.client}|${normalizeTa(t.ta)}`;
      const a = data.mbr_ta_actuals?.[key] || {};
      const note = latestNote[normalizeTa(t.ta)];
      result.push({
        client: t.client,
        ta: t.ta,
        team_group: t.team_group,
        contacted: a.contacted || 0,
        actual_screens: a.actual_screens || 0,
        ats: a.ats || 0,
        offers: a.offers || 0,
        hires: a.hires || 0,
        hires_12w: a.hires_12w || 0,
        ats_12w: a.ats_12w || 0,
        screens_12w: a.screens_12w || 0,
        jobs_60d: a.jobs_60d || 0,
        contacted_target: t.contacted || 0,
        actual_screens_target: t.actual_screens || 0,
        ats_target: t.moved_to_ats || 0,
        hires_target: t.hires || 0,
        pct_screens_to_hires: a.screens_12w > 0 ? Math.round((a.hires_12w || 0) / a.screens_12w * 100) : null,
        comment: note?.comment || '',
      });
    });
    const groupOrder = { 'Dolphins/Whales': 0, 'Ponies/Unicorns': 1 };
    return result.sort((a, b) => {
      const ga = groupOrder[a.team_group] ?? 2;
      const gb = groupOrder[b.team_group] ?? 2;
      if (ga !== gb) return ga - gb;
      if (a.client !== b.client) return a.client.localeCompare(b.client);
      return a.ta.localeCompare(b.ta);
    });
  }, [data]);

  // TS rows from mbr_ts_actuals — only include sourcers with a ts_weekly target row
  const tsRows = useMemo(() => {
    const targets = {};
    (data.ts_weekly || []).forEach(t => {
      // Sum monthly target = sum of weekly targets across the 4 MBR weeks
      const wNum = parseInt(String(t.week));
      if (wNum >= 12 && wNum <= 15) {
        targets[t.ts] = (targets[t.ts] || 0) + (Number(t.contacted_target) || 0);
      }
    });
    // Latest comment per TS: pick the most recent (year, week) ts_weekly row with a non-empty comment
    const latestComment = {};
    (data.ts_weekly || []).forEach(t => {
      if (!t.comment) return;
      const key = t.ts;
      const prev = latestComment[key];
      const curRank = (Number(t.year) || 0) * 100 + (Number(t.week) || 0);
      const prevRank = prev ? (Number(prev.year) || 0) * 100 + (Number(prev.week) || 0) : -1;
      if (curRank > prevRank) latestComment[key] = t;
    });
    const rows = [];
    Object.keys(targets).forEach(ts => {
      const a = data.mbr_ts_actuals?.[ts] || {};
      rows.push({
        ts,
        contacted: a.contacted_4w || 0,
        contacted_target: targets[ts],
        recruiter_screens: a.recruiter_screens_4w || 0,
        actual_screens: a.actual_screens_4w || 0,
        ats: a.ats_4w || 0,
        hires_12w: a.hires_12w || 0,
        screens_12w: a.screens_12w || 0,
        ats_12w: a.ats_12w || 0,
        pct_actual_to_ats_12w: a.screens_12w > 0 ? Math.round((a.ats_12w || 0) / a.screens_12w * 100) : null,
        comment: latestComment[ts]?.comment || '',
      });
    });
    return rows.sort((a, b) => a.ts.localeCompare(b.ts));
  }, [data]);

  const clientTotals = clientRows.reduce((acc, r) => ({
    contacted: acc.contacted + r.contacted,
    actual_screens: acc.actual_screens + r.actual_screens,
    ats: acc.ats + r.ats,
    offers: acc.offers + r.offers,
    hires: acc.hires + r.hires,
    hires_12w: acc.hires_12w + r.hires_12w,
  }), { contacted:0, actual_screens:0, ats:0, offers:0, hires:0, hires_12w:0 });

  const renderTaGroup = (group) => {
    const rows = taRows.filter(r => r.team_group === group);
    if (rows.length === 0) return null;
    const label = group === 'Dolphins/Whales' ? '🐬 Dolphins & Whales' : '🦄 Ponies & Unicorns';
    const totals = rows.reduce((a, r) => ({
      contacted: a.contacted + r.contacted,
      actual_screens: a.actual_screens + r.actual_screens,
      ats: a.ats + r.ats,
      hires: a.hires + r.hires,
      hires_12w: a.hires_12w + r.hires_12w,
      ats_12w: a.ats_12w + r.ats_12w,
      screens_12w: a.screens_12w + r.screens_12w,
      jobs_60d: a.jobs_60d + r.jobs_60d,
      contacted_target: a.contacted_target + r.contacted_target,
      actual_screens_target: a.actual_screens_target + r.actual_screens_target,
      ats_target: a.ats_target + r.ats_target,
      hires_target: a.hires_target + r.hires_target,
    }), { contacted:0, actual_screens:0, ats:0, hires:0, hires_12w:0, ats_12w:0, screens_12w:0, jobs_60d:0, contacted_target:0, actual_screens_target:0, ats_target:0, hires_target:0 });

    return (
      <div className="bg-gray-800 rounded-lg p-4" key={group}>
        <h3 className="text-lg font-semibold text-white mb-4">{label} — TAs (Last 4 Weeks)</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm" style={{ minWidth: '1400px' }}>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2 sticky left-0 bg-gray-800 z-10">Client</th>
                <th className="text-left px-2 py-2">TA</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12w Hires">12w H</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12w ATS">12w ATS</th>
                <th className="text-center px-1 py-2 text-xs" title="Last 12w Screens">12w Scr</th>
                <th className="text-center px-1 py-2 text-xs" title="12w % Screens → Hires">12w %S→H</th>
                <th className="text-center px-1 py-2 text-xs" title="4w Hires">Hires</th>
                <th className="text-center px-1 py-2 text-xs">Tgt</th>
                <th className="text-center px-1 py-2 text-xs" title="4w Contacted">Cntd</th>
                <th className="text-center px-1 py-2 text-xs">Tgt</th>
                <th className="text-center px-1 py-2 text-xs" title="4w Actual Screens">Scrn</th>
                <th className="text-center px-1 py-2 text-xs">Tgt</th>
                <th className="text-center px-1 py-2 text-xs" title="4w Moved to ATS">ATS</th>
                <th className="text-center px-1 py-2 text-xs">Tgt</th>
                <th className="text-center px-1 py-2 text-xs" title="Jobs Opened &gt; 60 days">{'>'}60d</th>
                <th className="text-left px-2 py-2 text-xs min-w-[140px]">Latest Comment</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, idx) => {
                const prev = idx > 0 ? rows[idx - 1] : null;
                const clientChange = !prev || prev.client !== r.client;
                return (
                  <tr key={idx} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} ${clientChange ? 'border-t border-gray-600' : ''}`}>
                    <td className="text-left px-2 py-2 text-white font-medium sticky left-0 bg-inherit z-10">{clientChange ? r.client : ''}</td>
                    <td className="text-left px-2 py-2 text-gray-300 whitespace-nowrap">{r.ta}</td>
                    <td className="text-center px-1 py-2 text-gray-300">{r.hires_12w || '—'}</td>
                    <td className="text-center px-1 py-2 text-gray-300">{r.ats_12w || '—'}</td>
                    <td className="text-center px-1 py-2 text-gray-300">{r.screens_12w || '—'}</td>
                    <td className="text-center px-1 py-2 text-gray-400">{r.pct_screens_to_hires != null ? `${r.pct_screens_to_hires}%` : '—'}</td>
                    <td className="text-center px-1 py-2 text-gray-300">{r.hires || ''}</td>
                    <td className="text-center px-1 py-2 text-gray-500">{r.hires_target ? r.hires_target.toFixed(1) : '—'}</td>
                    <td className="text-center px-1 py-2" style={getCellStyle(r.contacted, r.contacted_target)}>{r.contacted || ''}</td>
                    <td className="text-center px-1 py-2 text-gray-500">{r.contacted_target || '—'}</td>
                    <td className="text-center px-1 py-2" style={getCellStyle(r.actual_screens, r.actual_screens_target)}>{r.actual_screens || ''}</td>
                    <td className="text-center px-1 py-2 text-gray-500">{r.actual_screens_target || '—'}</td>
                    <td className="text-center px-1 py-2" style={getCellStyle(r.ats, r.ats_target)}>{r.ats || ''}</td>
                    <td className="text-center px-1 py-2 text-gray-500">{r.ats_target || '—'}</td>
                    <td className="text-center px-1 py-2 text-gray-300">{r.jobs_60d || ''}</td>
                    <td className="text-left px-2 py-2 text-gray-400 text-xs max-w-xs truncate" title={r.comment}>{r.comment || '—'}</td>
                  </tr>
                );
              })}
              <tr className="bg-gray-700 font-semibold" style={{ borderTop: '2px solid #6B7280' }}>
                <td className="text-left px-2 py-2 text-white sticky left-0 bg-gray-700 z-10">{group} Total</td>
                <td className="text-left px-2 py-2 text-gray-300">—</td>
                <td className="text-center px-1 py-2 text-white">{totals.hires_12w}</td>
                <td className="text-center px-1 py-2 text-white">{totals.ats_12w}</td>
                <td className="text-center px-1 py-2 text-white">{totals.screens_12w}</td>
                <td className="text-center px-1 py-2 text-white">—</td>
                <td className="text-center px-1 py-2 text-white">{totals.hires}</td>
                <td className="text-center px-1 py-2 text-white">{totals.hires_target.toFixed(1)}</td>
                <td className="text-center px-1 py-2 text-white">{totals.contacted}</td>
                <td className="text-center px-1 py-2 text-white">{totals.contacted_target}</td>
                <td className="text-center px-1 py-2 text-white">{totals.actual_screens}</td>
                <td className="text-center px-1 py-2 text-white">{totals.actual_screens_target}</td>
                <td className="text-center px-1 py-2 text-white">{totals.ats}</td>
                <td className="text-center px-1 py-2 text-white">{totals.ats_target}</td>
                <td className="text-center px-1 py-2 text-white">{totals.jobs_60d}</td>
                <td className="text-left px-2 py-2 text-gray-400">—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <div className="text-sm text-gray-400">Monthly Business Review — last 4 weeks</div>
        <div className="text-xl font-semibold text-white mt-1">Window: {windowLabel} (weeks {MBR_WEEKS.join(', ')})</div>
      </div>

      {/* 1. Client's Target */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Client's Target — Last 4 Weeks</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2">Client</th>
                <th className="text-center px-2 py-2">Last 12w Hires</th>
                <th className="text-center px-2 py-2">Hires</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Actual Screens</th>
                <th className="text-center px-2 py-2">Moved to ATS</th>
                <th className="text-center px-2 py-2">Offers</th>
              </tr>
            </thead>
            <tbody>
              {clientRows.map((row, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-2 text-white font-medium">{row.client}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.contacted}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.actual_screens}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.ats}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.offers}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{clientTotals.hires_12w}</td>
                <td className="text-center px-2 py-2 text-white">{clientTotals.hires}</td>
                <td className="text-center px-2 py-2 text-white">{clientTotals.contacted}</td>
                <td className="text-center px-2 py-2 text-white">{clientTotals.actual_screens}</td>
                <td className="text-center px-2 py-2 text-white">{clientTotals.ats}</td>
                <td className="text-center px-2 py-2 text-white">{clientTotals.offers}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* 2. Ponies & Unicorns */}
      {renderTaGroup('Ponies/Unicorns')}

      {/* 3. Dolphins & Whales */}
      {renderTaGroup('Dolphins/Whales')}

      {/* 4. TS Target Last 4 Weeks */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TS's Target — Last 4 Weeks</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2">Sourcer</th>
                <th className="text-center px-2 py-2">12w Hires</th>
                <th className="text-center px-2 py-2" title="Last 12w % Actual Screens → ATS">12w %S→A</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Target</th>
                <th className="text-center px-2 py-2">Recruiter Screens</th>
                <th className="text-center px-2 py-2">Actual Screens</th>
                <th className="text-center px-2 py-2">Moved to ATS</th>
                <th className="text-left px-2 py-2 text-xs min-w-[180px]">Latest Comment</th>
              </tr>
            </thead>
            <tbody>
              {tsRows.map((r, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-2 text-white font-medium">{r.ts}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{r.hires_12w || '—'}</td>
                  <td className="text-center px-2 py-2 text-gray-400">{r.pct_actual_to_ats_12w != null ? `${r.pct_actual_to_ats_12w}%` : '—'}</td>
                  <td className="text-center px-2 py-2" style={getCellStyle(r.contacted, r.contacted_target)}>{r.contacted}</td>
                  <td className="text-center px-2 py-2 text-gray-500">{r.contacted_target || '—'}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{r.recruiter_screens}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{r.actual_screens}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{r.ats}</td>
                  <td className="text-left px-2 py-2 text-gray-400 text-xs max-w-xs truncate" title={r.comment}>{r.comment || '—'}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.hires_12w, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.contacted, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.contacted_target, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.recruiter_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.actual_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.ats, 0)}</td>
                <td className="text-left px-2 py-2 text-gray-400">—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

// Project Dashboard Tab
const ProjectDashboardTab = ({ data }) => {
  const [selectedClient, setSelectedClient] = useState('');
  const [searchTitle, setSearchTitle] = useState('');
  const [sortField, setSortField] = useState('client');

  const activeJobs = useMemo(() => data.jobs.filter((j) => !j.is_ext), [data]);

  const uniqueClients = useMemo(() => {
    const s = new Set();
    activeJobs.forEach((j) => s.add(normalizeClient(j.client)));
    return Array.from(s).sort();
  }, [activeJobs]);

  const kpis = useMemo(() => {
    return activeJobs.reduce(
      (acc, j) => ({
        openRoles: acc.openRoles + 1,
        contacted: acc.contacted + (j.contacted || 0),
        screened: acc.screened + (j.screened || 0),
        hires: acc.hires + (j.hires || 0),
      }),
      { openRoles: 0, contacted: 0, screened: 0, hires: 0 }
    );
  }, [activeJobs]);

  const getDaysOpen = (d) => Math.floor((new Date('2026-04-10') - new Date(d)) / 86400000);

  const filteredJobs = useMemo(() => {
    let jobs = activeJobs;
    if (selectedClient) jobs = jobs.filter((j) => normalizeClient(j.client) === selectedClient);
    if (searchTitle) jobs = jobs.filter((j) => j.title.toLowerCase().includes(searchTitle.toLowerCase()));
    return jobs.sort((a, b) => {
      let av, bv;
      switch (sortField) {
        case 'client': av = normalizeClient(a.client); bv = normalizeClient(b.client); break;
        case 'title': av = a.title; bv = b.title; break;
        case 'ta': av = a.ta || ''; bv = b.ta || ''; break;
        case 'contacted': av = a.contacted || 0; bv = b.contacted || 0; break;
        case 'screened': av = a.screened || 0; bv = b.screened || 0; break;
        case 'hires': av = a.hires || 0; bv = b.hires || 0; break;
        case 'days_open': av = getDaysOpen(a.date_created); bv = getDaysOpen(b.date_created); break;
        default: return 0;
      }
      return av < bv ? -1 : av > bv ? 1 : 0;
    });
  }, [activeJobs, selectedClient, searchTitle, sortField]);

  const clientHiresChart = useMemo(() => {
    const s = {};
    activeJobs.forEach((j) => {
      const c = normalizeClient(j.client);
      s[c] = (s[c] || 0) + (j.hires || 0);
    });
    return Object.entries(s).map(([c, h]) => ({ client: c, hires: h })).sort((a, b) => b.hires - a.hires).slice(0, 10);
  }, [activeJobs]);

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-4 gap-4">
        {[
          ['Open Roles', kpis.openRoles],
          ['Total Contacted', kpis.contacted],
          ['Total Screened', kpis.screened],
          ['Total Hires', kpis.hires],
        ].map(([label, val]) => (
          <div key={label} className="bg-gray-800 rounded-lg p-4 border border-gray-700">
            <div className="text-gray-400 text-sm">{label}</div>
            <div className="text-3xl font-bold text-white mt-2">{val}</div>
          </div>
        ))}
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Weekly Contacted Trend</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data.weekly_trend} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#444" />
            <XAxis dataKey="week" stroke="#999" label={{ value: 'Week', position: 'insideBottomRight', offset: -5, fill: '#999' }} />
            <YAxis stroke="#999" />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #444' }} labelStyle={{ color: '#fff' }} />
            <Legend />
            <Line type="monotone" dataKey="contacted" stroke="#60a5fa" dot={{ fill: '#60a5fa', r: 4 }} activeDot={{ r: 6 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Job Performance</h3>
        <div className="flex gap-3 mb-4">
          <div className="flex-1 relative">
            <Search className="absolute left-2 top-2.5 w-4 h-4 text-gray-500" />
            <input type="text" placeholder="Search job title..." value={searchTitle} onChange={(e) => setSearchTitle(e.target.value)}
              className="w-full pl-8 pr-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm placeholder-gray-500" />
          </div>
          <select value={selectedClient} onChange={(e) => setSelectedClient(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            <option value="">All Clients</option>
            {uniqueClients.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700">
                {[['client','Client'],['title','Job Title'],['ta','TA'],['','Category'],['contacted','Contacted'],['screened','Screened'],['','ATS'],['','Offers'],['hires','Hires'],['days_open','Days Open']].map(([f,l]) => (
                  <th key={l} className={`${f ? 'cursor-pointer hover:text-white' : ''} ${['Category','Contacted','Screened','ATS','Offers','Hires','Days Open'].includes(l) ? 'text-center' : 'text-left'} px-3 py-2`}
                    onClick={() => f && setSortField(f)}>{l}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filteredJobs.map((j, idx) => (
                <tr key={idx} className={j.screened > 25 && j.hires === 0 ? 'bg-red-900 bg-opacity-20' : idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-3 py-2 text-white font-medium">{normalizeClient(j.client)}</td>
                  <td className="text-left px-3 py-2 text-gray-300">{j.title}</td>
                  <td className="text-left px-3 py-2 text-gray-300">{j.ta || '—'}</td>
                  <td className="text-left px-3 py-2 text-gray-300">{j.category}</td>
                  <td className="text-center px-3 py-2 text-gray-300">{j.contacted || 0}</td>
                  <td className="text-center px-3 py-2 text-gray-300">{j.screened || 0}</td>
                  <td className="text-center px-3 py-2 text-gray-300">{j.ats || 0}</td>
                  <td className="text-center px-3 py-2 text-gray-300">{j.offers || 0}</td>
                  <td className="text-center px-3 py-2 text-gray-300">{j.hires || 0}</td>
                  <td className="text-center px-3 py-2 text-gray-300">{getDaysOpen(j.date_created)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Hires by Client (Top 10)</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={clientHiresChart} margin={{ top: 5, right: 30, left: 0, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#444" />
            <XAxis dataKey="client" stroke="#999" angle={-45} textAnchor="end" height={80} />
            <YAxis stroke="#999" />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #444' }} labelStyle={{ color: '#fff' }} />
            <Bar dataKey="hires" fill="#10b981" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};


// Main Dashboard
const RecruitingDashboard = () => {
  const [activeTab, setActiveTab] = useState('wbr');
  // Data source toggle: 'pbi' = current Power BI / Bubble pipeline (source of truth)
  //                     'snowflake' = new Keboola-Snowflake pipeline (parallel-run for validation)
  const [dataSource, setDataSource] = useState('pbi');
  const dashboardData = dataSource === 'pbi' ? dashboardDataPbi : dashboardDataSnowflake;
  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="bg-gray-800 border-b border-gray-700 px-6 py-6 flex items-start justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">Tribe.xyz Recruiting Dashboard</h1>
          <p className="text-sm text-gray-400 mt-1">
            {dataSource === 'pbi' ? 'Power BI / Bubble pipeline' : 'Snowflake pipeline (parallel-run)'}
          </p>
        </div>
        <div className="flex items-center gap-2 bg-gray-900 border border-gray-700 rounded-lg p-1">
          {[
            ['pbi', 'Power BI'],
            ['snowflake', 'Snowflake'],
          ].map(([val, label]) => (
            <button
              key={val}
              onClick={() => setDataSource(val)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                dataSource === val
                  ? 'bg-white text-gray-900'
                  : 'text-gray-400 hover:text-gray-200'
              }`}
              title={val === 'pbi'
                ? 'Current source of truth: Bubble → n8n → data.json'
                : 'New: Keboola MCP → render_json → dashboard_data_snowflake.json'}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      <div className="bg-gray-800 border-b border-gray-700 px-6">
        <div className="flex gap-8">
          {['wbr', 'mbr', 'project'].map((tab) => (
            <button key={tab} onClick={() => setActiveTab(tab)}
              className={`py-4 px-2 font-medium border-b-2 transition-colors ${
                activeTab === tab ? 'text-white border-white' : 'text-gray-400 border-transparent hover:text-gray-300'
              }`}>
              {tab === 'wbr' ? 'WBR' : tab === 'mbr' ? 'MBR' : 'Project Dashboard'}
            </button>
          ))}
        </div>
      </div>
      <div className="px-6 py-6">
        {activeTab === 'wbr' && <WBRTab data={dashboardData} />}
        {activeTab === 'mbr' && <MBRTab data={dashboardData} />}
        {activeTab === 'project' && <ProjectDashboardTab data={dashboardData} />}
      </div>
    </div>
  );
};

export default RecruitingDashboard;

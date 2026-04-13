import React, { useState, useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid, Legend
} from 'recharts';
import { Search } from 'lucide-react';
import dashboardData from './dashboard_data.json';

const WEEKS = Array.from({ length: 15 }, (_, i) => i + 1);
const WEEKLY_DIVISOR = 4.33;

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
  const [selectedWeek, setSelectedWeek] = useState(15);

  // Build client summary for selected week
  const clientSummary = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const summary = {};

    // Initialize from targets (April 2026)
    data.targets.forEach((t) => {
      const display = normalizeClient(t.client);
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

    // Add actuals
    data.targets.forEach((t) => {
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

    // Add roles and 12w hires — data is now keyed by PBI-aligned client names
    // Aggregate ALL entries per client (includes non-target TAs, matching PBI behavior)
    Object.entries(data.roles || {}).forEach(([key, val]) => {
      const client = key.split('|')[0];
      if (summary[client]) summary[client].roles += val;
    });
    Object.entries(data.hires_12w || {}).forEach(([key, val]) => {
      const client = key.split('|')[0];
      if (summary[client]) summary[client].hires_12w += val;
    });

    return Object.values(summary).sort((a, b) => a.client.localeCompare(b.client));
  }, [data, selectedWeek]);

  // TA detail table
  const taDetail = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const details = [];

    data.targets.forEach((t) => {
      const display = normalizeClient(t.client);
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

  // TS Overall Conversion Rate (cumulative across all weeks)
  const tsConversion = useMemo(() => {
    const result = [];
    const allSourcers = new Set();
    // Collect all sourcers from ts_weekly (roster)
    (data.ts_weekly || []).forEach((t) => allSourcers.add(t.ts));

    allSourcers.forEach((tsName) => {
      const weeklyData = data.ts_actuals?.[tsName] || {};
      let contacted = 0, recruiterScreens = 0, actualScreens = 0, ats = 0, offers = 0, hires = 0;
      Object.values(weeklyData).forEach((wk) => {
        contacted += wk.contacted || 0;
        recruiterScreens += wk.recruiter_screens || wk.screened || 0;
        actualScreens += wk.actual_screens || 0;
        ats += wk.ats || 0;
        offers += wk.offers || 0;
        hires += wk.hires || 0;
      });
      const jobs = data.ts_jobs?.[tsName] || {};
      const positiveResponses = data.ts_positive_responses?.[tsName] || 0;

      result.push({
        ts: tsName,
        active_jobs: jobs.num_jobs || 0,
        contacted,
        positive_responses: positiveResponses,
        pct_contacted_to_pr: contacted > 0 ? Math.round(positiveResponses / contacted * 1000) / 10 : 0,
        recruiter_screens: recruiterScreens,
        actual_screens: actualScreens,
        pct_screen_to_actual: recruiterScreens > 0 ? Math.round(actualScreens / recruiterScreens * 1000) / 10 : 0,
        ats,
        pct_actual_to_ats: actualScreens > 0 ? Math.round(ats / actualScreens * 1000) / 10 : 0,
      });
    });
    return result.sort((a, b) => a.ts.localeCompare(b.ts));
  }, [data]);

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

      {/* TS Overall Conversion Rate */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TS Overall Conversion Rate — 2026 YTD</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2">Sourcer</th>
                <th className="text-center px-2 py-2">Active Jobs</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">% → PR</th>
                <th className="text-center px-2 py-2">Pos. Response</th>
                <th className="text-center px-2 py-2">Recruiter Screens</th>
                <th className="text-center px-2 py-2">% → Actual</th>
                <th className="text-center px-2 py-2">Actual Screens</th>
                <th className="text-center px-2 py-2">% → ATS</th>
                <th className="text-center px-2 py-2">ATS</th>
              </tr>
            </thead>
            <tbody>
              {tsConversion.map((row, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-2 text-white font-medium">{row.ts}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.active_jobs}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.contacted}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.pct_contacted_to_pr}%</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.positive_responses}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.recruiter_screens}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.pct_screen_to_actual}%</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.actual_screens}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.pct_actual_to_ats}%</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.ats}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.active_jobs, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.contacted, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.positive_responses, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.recruiter_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.actual_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsConversion.reduce((s, r) => s + r.ats, 0)}</td>
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
  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="bg-gray-800 border-b border-gray-700 px-6 py-6">
        <h1 className="text-3xl font-bold text-white">Tribe.xyz Recruiting Dashboard</h1>
        <p className="text-sm text-gray-400 mt-1">Replacing Power BI</p>
      </div>
      <div className="bg-gray-800 border-b border-gray-700 px-6">
        <div className="flex gap-8">
          {['wbr', 'project'].map((tab) => (
            <button key={tab} onClick={() => setActiveTab(tab)}
              className={`py-4 px-2 font-medium border-b-2 transition-colors ${
                activeTab === tab ? 'text-white border-white' : 'text-gray-400 border-transparent hover:text-gray-300'
              }`}>
              {tab === 'wbr' ? 'WBR' : 'Project Dashboard'}
            </button>
          ))}
        </div>
      </div>
      <div className="px-6 py-6">
        {activeTab === 'wbr' && <WBRTab data={dashboardData} />}
        {activeTab === 'project' && <ProjectDashboardTab data={dashboardData} />}
      </div>
    </div>
  );
};

export default RecruitingDashboard;

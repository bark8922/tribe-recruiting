import React, { useState, useMemo, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, AreaChart, Area, FunnelChart, Funnel, LabelList,
  CartesianGrid, Legend, PieChart, Pie, Cell
} from 'recharts';
import { Search } from 'lucide-react';
// dashboard_data_snowflake.json is loaded at runtime (gzipped) by RecruitingDashboard
// instead of being imported here. Inlining the ~50MB JSON pushed the JS bundle past
// Cloudflare Pages' 25 MiB per-file limit and broke every deploy from 2026-06-15.
import clientProfitabilityData from './client_profitability.json';
import teamLeadsData from './team_leads.json';

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

// Business unit group — driven ENTIRELY by the display client name, not per-TA
// team_group. Some Aviv TAs are tagged 'Ponies/Unicorns' in the target sheet
// (internal team-management labels) even though Aviv the client belongs to
// Dolphins & Whales. Using the client name keeps the dashboard's grouping
// consistent with how the business reports by client.
// Dolphins & Whales: Aviv, Aiven, all Wolt divisions (incl DoorDash/SevenRooms
// → Wolt HQ via normalizeClient). Everything else → Ponies & Unicorns.
const DOLPHINS_WHALES_CLIENTS = new Set(['Aviv', 'Aiven']);

// MBR target rows come from Andy's sheet in long form ("Wolt North, Baltics & Benelux")
// but mbr_ta_actuals is keyed in ABBREV form ("Wolt NBB"). Without this map,
// every NBB/C&S TA shows zeros (Adelya, Jelena, Tina, etc.).
const MBR_WOLT_ABBREV = {
  'Wolt Central & South': 'Wolt C&S',
  'Wolt North, Baltics & Benelux': 'Wolt NBB',
};
const mbrAbbrevClient = (c) => MBR_WOLT_ABBREV[(c || '').trim()] || c;
const getBuGroup = (displayClient) => {
  if (!displayClient) return 'Ponies/Unicorns';
  if (DOLPHINS_WHALES_CLIENTS.has(displayClient) || displayClient.startsWith('Wolt')) {
    return 'Dolphins/Whales';
  }
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

// True when every tracked weekly target on the row is met (actual >= target).
// Cells with no target (null / 0) are skipped — they aren't being measured.
// Returns false if no targets are set at all (so we don't accidentally flag a
// row with no targets as "all green"). Used to suppress the
// "Missing comment" warning when a TA / TS is fully green for the week —
// per Blake 2026-04-29: comments are only required when something is below
// target.
const allTargetsMet = (pairs) => {
  let hasAnyTarget = false;
  for (const [actual, target] of pairs) {
    if (!target || target === 0) continue;
    hasAnyTarget = true;
    if ((actual || 0) < target) return false;
  }
  return hasAnyTarget;
};

// WBR Tab

// --- CSV export (DOM-based: serializes the rendered table, so it cannot diverge from screen) ---
const csvCell = (s) => {
  const v = String(s ?? '').replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').trim();
  return /[",\n;]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
};
const CsvBtn = ({ fname }) => {
  const ref = React.useRef(null);
  const onClick = () => {
    const sib = ref.current?.nextElementSibling;
    const table = sib && sib.tagName === 'TABLE' ? sib : ref.current?.parentElement?.querySelector('table');
    if (!table) return;
    const lines = Array.from(table.querySelectorAll('tr')).map((tr) =>
      Array.from(tr.querySelectorAll('th,td')).map((c) => csvCell(c.textContent)).join(',')
    );
    const blob = new Blob(['\uFEFF' + lines.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fname + '_' + new Date().toISOString().slice(0, 10) + '.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };
  return (
    <div ref={ref} className="flex justify-end mb-1">
      <button type="button" onClick={onClick} title="Download this table as CSV"
        className="text-xs px-2 py-1 rounded border border-gray-600 bg-gray-700 text-gray-300 hover:bg-gray-600">
        ⤓ CSV
      </button>
    </div>
  );
};

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

  // Drill-down modal — click a Client, TA, or TS row to see last 6 weeks.
  // { kind: 'client'|'ta'|'ts', title, displayClient?, taName?, tsName? }
  const [drillDown, setDrillDown] = useState(null);

  // Team-lead filter (Bamboo-derived, see refresh_team_leads.py).
  // Single-select dropdown; when set, filters TA Weekly Detail + TS Weekly +
  // TS Overall Conversion Rate to that lead's direct reports only (no transitive).
  // Client Summary stays unfiltered. Ex-employees / unmatched names never
  // appear under any team filter (only in 'All teams' view).
  const [selectedTeamLead, setSelectedTeamLead] = useState('');
  const teamLeadOptions = useMemo(
    () => (teamLeadsData.leads || []).map(l => l.name).sort((a, b) => a.localeCompare(b)),
    []
  );
  const selectedLeadReports = useMemo(() => {
    if (!selectedTeamLead) return null;
    const lead = (teamLeadsData.leads || []).find(l => l.name === selectedTeamLead);
    return lead ? new Set(lead.reports) : new Set();
  }, [selectedTeamLead]);

  // 6 weeks ending at the currently selected week (w12-w17 if selectedWeek=17).
  const drillWeeks = useMemo(
    () => [5,4,3,2,1,0].map(i => `w${selectedWeek - i}`),
    [selectedWeek]
  );

  // Aggregate wbr_actuals for a given TA across all of their clients.
  const drillTaWeekly = (taName) => {
    const normalized = normalizeTa(taName);
    const out = {};
    for (const [key, byWeek] of Object.entries(data.wbr_actuals || {})) {
      const [, keyTa] = key.split('|');
      if (normalizeTa(keyTa) !== normalized) continue;
      for (const [wk, v] of Object.entries(byWeek || {})) {
        const b = out[wk] || { contacted: 0, screened: 0, actual_screens: 0, ats: 0, offers: 0, hires: 0 };
        b.contacted      += v.contacted || 0;
        b.screened       += v.screened || 0;
        b.actual_screens += v.actual_screens || 0;
        b.ats            += v.ats || 0;
        b.offers         += v.offers || 0;
        b.hires          += v.hires || 0;
        out[wk] = b;
      }
    }
    return out;
  };

  // TS weekly is already indexed: ts_actuals[ts][wk] = metrics.
  const drillTsWeekly = (tsName) => data.ts_actuals?.[tsName] || {};

  // Aggregate wbr_actuals for a DISPLAY client across all TAs (matching the
  // Client Summary rollup logic with Wolt sub-BU routing via kebolaClientMatches).
  const drillClientWeekly = (displayClient) => {
    const out = {};
    const targetsByTa = (data.targets || []).filter(t => t.team_group && normalizeClient(t.client) === displayClient);
    for (const t of targetsByTa) {
      const targetTaNorm = normalizeTa(t.ta);
      for (const [key, byWeek] of Object.entries(data.wbr_actuals || {})) {
        const [rawClient, rawTa] = key.split('|');
        if (!kebolaClientMatches(rawClient, displayClient)) continue;
        if (normalizeTa(rawTa) !== targetTaNorm) continue;
        for (const [wk, v] of Object.entries(byWeek || {})) {
          const b = out[wk] || { contacted: 0, screened: 0, actual_screens: 0, ats: 0, offers: 0, hires: 0 };
          b.contacted      += v.contacted || 0;
          b.screened       += v.actual_screens || v.screened || 0;
          b.actual_screens += v.actual_screens || 0;
          b.ats            += v.ats || 0;
          b.offers         += v.offers || 0;
          b.hires          += v.hires || 0;
          out[wk] = b;
        }
      }
    }
    return out;
  };

  // Weekly targets for a TA (sum across their client assignments).
  const drillTaTargets = (taName) => {
    const normalized = normalizeTa(taName);
    return (data.targets || [])
      .filter(t => t.team_group && normalizeTa(t.ta) === normalized)
      .reduce((a, r) => ({
        contacted:      a.contacted      + (Number(r.contacted) || 0),
        actual_screens: a.actual_screens + (Number(r.actual_screens) || 0),
        moved_to_ats:   a.moved_to_ats   + (Number(r.moved_to_ats) || 0),
        hires:          a.hires          + (Number(r.hires) || 0),
      }), { contacted: 0, actual_screens: 0, moved_to_ats: 0, hires: 0 });
  };

  // Weekly targets for a DISPLAY client (sum of rostered TAs' targets).
  const drillClientTargets = (displayClient) =>
    (data.targets || [])
      .filter(t => t.team_group && normalizeClient(t.client) === displayClient)
      .reduce((a, r) => ({
        contacted:      a.contacted      + (Number(r.contacted) || 0),
        actual_screens: a.actual_screens + (Number(r.actual_screens) || 0),
        moved_to_ats:   a.moved_to_ats   + (Number(r.moved_to_ats) || 0),
        hires:          a.hires          + (Number(r.hires) || 0),
      }), { contacted: 0, actual_screens: 0, moved_to_ats: 0, hires: 0 });

  // Per-week TS contacted target from Andy's sheet; falls back to 100 if blank.
  const drillTsContactedTarget = (tsName, wkKey) => {
    const wNum = parseInt(wkKey.replace(/^w/, ''));
    const rec = (data.ts_weekly || []).find(t => t.ts === tsName && t.week === wNum);
    return Number(rec?.contacted_target) || 100;
  };

  // Past comments per week for the drill-down popup.
  // TA: pull from data.ta_weekly_notes (joined across clients for the same TA+week).
  const drillTaComments = (taName) => {
    const norm = normalizeTa(taName);
    const acc = {};
    (data.ta_weekly_notes || []).forEach((n) => {
      if (normalizeTa(n.ta) !== norm) return;
      const wk = `w${n.week}`;
      const txt = (n.comment || n.reasoning || '').trim();
      if (!txt) return;
      if (!acc[wk]) acc[wk] = [];
      if (!acc[wk].includes(txt)) acc[wk].push(txt);
    });
    const out = {};
    Object.entries(acc).forEach(([wk, parts]) => { out[wk] = parts.join(' · '); });
    return out;
  };
  // TS: pull from data.ts_weekly (one row per TS+week).
  const drillTsComments = (tsName) => {
    const out = {};
    (data.ts_weekly || []).forEach((t) => {
      if (t.ts !== tsName) return;
      const wk = `w${t.week}`;
      const txt = (t.comment || t.reasoning || '').trim();
      if (txt) out[wk] = txt;
    });
    return out;
  };

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
    // Wolt catch-all recruiter → sub-BU map. Only include (client, TA) pairs
    // where team_group is set — roster-only placeholders (e.g. Simon Siew and
    // Vladimir Stankovic listed under Wolt Tech with team_group='') would
    // otherwise inflate Wolt Tech hires from 1 (PBI actual) to 7 by absorbing
    // hires from raw-'Wolt' rows where those TAs are job_recruiter.
    const recruiterToWoltSubBu = new Map();
    data.targets.forEach((t) => {
      if (!t.team_group) return;
      const ta = normalizeTa(t.ta);
      const cl = normalizeClient(t.client);
      if (ta && cl && cl.startsWith('Wolt') && !recruiterToWoltSubBu.has(ta)) {
        recruiterToWoltSubBu.set(ta, cl);
      }
    });

    // Prefer the pipeline-computed ta_jobs_weekly (PBI DAX replica:
    // DISTINCTCOUNT(event.job_id) per (client, who_event_created_for, week))
    // validated 2026-04-20 vs PBI w16 at 129/130 = 99.2% (14/15 exact).
    // Filter to (display_client, TA) pairs present in data.targets with
    // non-empty team_group — mirrors PBI's implicit WBR TA Target↔Actual
    // relationship filter.
    const targetRosterPairs = new Set();
    data.targets.forEach((t) => {
      if (!t.team_group) return;
      targetRosterPairs.add(`${normalizeClient(t.client)}|${normalizeTa(t.ta)}`);
    });

    if (data.ta_jobs_weekly && data.ta_jobs_weekly[weekKey]) {
      Object.entries(data.ta_jobs_weekly[weekKey]).forEach(([key, val]) => {
        const [rawClient, rawTa] = key.split('|');
        const rec = normalizeTa(rawTa);
        let client = normalizeClient(rawClient);
        if (client === 'Wolt') {
          client = recruiterToWoltSubBu.get(rec) || null;
        }
        if (!client || !summary[client]) return;
        if (!targetRosterPairs.has(`${client}|${rec}`)) return;
        summary[client].roles += val;
      });
    } else {
      // Fallback for older snapshots without ta_jobs_weekly: distinct-jobs
      // approximation from data.jobs filtered by weekly roster. Yields ~110%
      // of PBI vs 99.2% for the pipeline metric.
      const rosterPairs = new Set();
      (data.wbr_ta_weekly_roster?.[weekKey] || []).forEach((pair) => {
        const [c, ta] = pair.split('|');
        rosterPairs.add(`${normalizeClient(c)}|${normalizeTa(ta)}`);
      });
      const seenJobIds = new Set();
      (data.jobs || []).forEach((job) => {
        if (String(job.is_job_archived).toLowerCase() !== 'false') return;
        if (String(job.is_external_recruiter).toLowerCase() !== 'false') return;
        if (seenJobIds.has(job.job_id)) return;
        const rec = normalizeTa(job.job_recruiter);
        let client = normalizeClient((job.client_name || '').trim());
        if (client === 'Wolt') client = recruiterToWoltSubBu.get(rec) || null;
        if (!client || !summary[client]) return;
        if (rosterPairs.size && !rosterPairs.has(`${client}|${rec}`)) return;
        seenJobIds.add(job.job_id);
        summary[client].roles += 1;
      });
    }

    // --- Last 12w Hires column ---
    // Use mbr_client_totals directly — it's the per-display-client rollup
    // of aux_12w hires with Wolt sub-BU correctly split via the MBR roster.
    // data.hires_12w (keyed by raw 'Wolt|TA') would credit Ketevan Khorava
    // with 193 Wolt hires via event.who_event_created_for attribution,
    // inflating Wolt total to 709 (vs PBI ~82). mbr_client_totals sums
    // to 148 vs PBI 141 (105% accuracy, 8/15 clients exact).
    // MBR abbreviations → display names for the lookup.
    const MBR_TO_DISPLAY = {
      'Wolt C&S': 'Wolt Central & South',
      'Wolt NBB': 'Wolt North, Baltics & Benelux',
    };
    Object.entries(data.mbr_client_totals || {}).forEach(([mbrClient, totals]) => {
      const displayClient = MBR_TO_DISPLAY[mbrClient] || mbrClient;
      if (summary[displayClient]) {
        summary[displayClient].hires_12w = totals.hires_12w || 0;
      }
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

      // Skip TAs who have zero weekly activity AND no comment/reasoning for
      // the selected week — even if they're on the WBR target roster. Lets
      // the table stay focused on people with something to review each week.
      const hasWeeklyActivity =
        (actual.contacted || 0) +
        (actual.screened || 0) +
        (actual.ats || 0) +
        (actual.offers || 0) +
        (actual.hires || 0) > 0;
      const hasNote = !!((note?.comment && note.comment.trim()) ||
                        (note?.reasoning && note.reasoning.trim()));
      if (!hasWeeklyActivity && !hasNote) return;

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
        comment: note?.comment || note?.reasoning || '',
        reasoning: note?.reasoning || '',
      });
    });

    // Dedupe by (display_client, TA) — a single TA with multiple target rows
    // that normalize to the same display client (e.g. Zelimir Stajcic has
    // target rows under both SevenRooms and Wolt HQ, both of which normalize
    // to 'Wolt HQ') would otherwise render twice. Keep the entry with
    // non-empty activity if any; otherwise the first one.
    const deduped = new Map();
    details.forEach((row) => {
      const key = `${row.client}|${normalizeTa(row.ta)}`;
      const prev = deduped.get(key);
      if (!prev) { deduped.set(key, row); return; }
      const activityOf = (r) => (r.contacted||0)+(r.screened||0)+(r.ats||0)+(r.offers||0)+(r.hires||0);
      if (activityOf(row) > activityOf(prev)) deduped.set(key, row);
    });

    // Sort: group first, then client, then TA
    const groupOrder = { 'Dolphins/Whales': 0, 'Ponies/Unicorns': 1, 'Other': 2 };
    const _detail = Array.from(deduped.values()).sort((a, b) => {
      const ga = groupOrder[a.team_group] ?? 2;
      const gb = groupOrder[b.team_group] ?? 2;
      if (ga !== gb) return ga - gb;
      if (a.client !== b.client) return a.client.localeCompare(b.client);
      return a.ta.localeCompare(b.ta);
    });
    return selectedLeadReports
      ? _detail.filter(r => selectedLeadReports.has(r.ta))
      : _detail;
  }, [data, selectedWeek, selectedLeadReports]);

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

    // Enrich all with actuals and job data, then drop rows with no activity
    // AND no comment for the week (same rule we apply to TA Detail). Catches
    // weekly-note placeholders like Mia Gjorgievska who show up on the roster
    // but have nothing to review.
    const _tsBase = Object.values(tsMap).map((t) => {
      const tsName = t.ts;
      const actuals = data.ts_actuals?.[tsName]?.[weekKey] || {};
      // Prefer the per-week ts_jobs_weekly (new pipeline metric) when present;
      // fall back to the static ts_jobs dict for older snapshots.
      const weeklyJobs = data.ts_jobs_weekly?.[weekKey]?.[tsName];
      const jobs = weeklyJobs || data.ts_jobs?.[tsName] || {};
      const hires12w = data.ts_hires_12w?.[tsName] || 0;

      // Derived targets: the WBR TS Weekly Note sheet only has a `Contacted Target`
      // column. Recruiter Screens / Actual Screens / Moved-to-ATS use FLAT
      // weekly targets per Blake (2026-04-29): 10 / 7 / 4 for every sourcer,
      // independent of contacted_target. (Previous funnel-ratio derivation
      // produced too-many red cells against PBI.)
      // TSes without an explicit contacted target get a 100 default so the
      // contacted cell still receives a color.
      const contactedTarget = Number(t.contacted_target) || 100;
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
        contacted_target: t.contacted_target, // keep raw for display (blank if null)
        recruiter_screens_target: 10,
        actual_screens_target:    7,
        ats_target:               4,
        _contacted_color_target:  contactedTarget, // always non-null for getCellStyle
      };
    }).filter((r) => {
      const anyActivity = (r.contacted||0) + (r.recruiter_screens||0) + (r.actual_screens||0)
        + (r.ats||0) + (r.offers||0) + (r.hires||0) + (r.num_jobs||0) > 0;
      const hasNote = !!((r.comment && r.comment.trim()) || (r.reasoning && r.reasoning.trim()));
      return anyActivity || hasNote;
    });
    const _tsDataSorted = _tsBase.sort((a, b) => a.ts.localeCompare(b.ts));
    return selectedLeadReports
      ? _tsDataSorted.filter(r => selectedLeadReports.has(r.ts))
      : _tsDataSorted;
  }, [data, selectedWeek, selectedLeadReports]);

  // TS Overall Conversion Rate with Officially Assigned Active Pipelines
  // ─────────────────────────────────────────────────────────────────────
  // Source: data.ts_conversion (built from ts_conversion.sql → snowflake_ts_conversion.csv).
  // Calibrated 2026-04-20 vs PBI w16 — 12/12 Active Pipelines exact, 12/12 colour
  // triplets exact, 98.99% aggregate volume accuracy.
  //
  // Active Pipelines = job.job_sourcer = TS  AND  ≥1 event with credit_sourcer = TS
  // on a candidate of that job (Andy Hsu rule, 2026-04-14).
  //
  // We deliberately do NOT fall back to data.ts_conversion_weekly — that legacy
  // field was built from a cumulative ts_actuals aggregate and inflates AS/ATS
  // wildly (Marina AS 61 → 245, Mia PR/Contacted → 188.9%). The scoped
  // snapshot is the only correct source here.
  //
  // Roster filter: ts_weekly entries for the selected week (same source of truth
  // as the TS Weekly table above — Andy's WBR Target Google Sheet).
  const tsConversion = useMemo(() => {
    const activeRoster = new Set(
      (data.ts_weekly || [])
        .filter((t) => t.week === selectedWeek)
        .map((t) => t.ts)
    );
    const source = (data.ts_conversion || []).filter((row) => activeRoster.has(row.ts));
    const _convBase = source.map((row) => {
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
        // PBI treats 0-numerator rows as blank (no red colour), e.g. Valeriia w16 has PR=0 → blank cell.
        pct_contacted_to_pr:  (contacted        > 0 && positiveResponse > 0) ? Math.round(positiveResponse / contacted        * 1000) / 10 : null,
        pct_screen_to_actual: (recruiterScreens > 0 && actualScreens    > 0) ? Math.round(actualScreens    / recruiterScreens * 1000) / 10 : null,
        pct_actual_to_ats:    (actualScreens    > 0 && ats              > 0) ? Math.round(ats              / actualScreens    * 1000) / 10 : null,
      };
    });
    const _convSorted = _convBase.sort((a, b) => a.ts.localeCompare(b.ts));
    return selectedLeadReports
      ? _convSorted.filter(r => selectedLeadReports.has(r.ts))
      : _convSorted;
  }, [data, selectedWeek, selectedLeadReports]);

  return (
    <div className="space-y-6">
      {drillDown && (
        <div
          onClick={(e) => { if (e.target === e.currentTarget) setDrillDown(null); }}
          style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.75)', zIndex: 50, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '2rem' }}
        >
          <div className="bg-gray-800 rounded-lg" style={{ maxWidth: '1300px', width: '100%', maxHeight: '90vh', overflowY: 'auto', padding: '1.5rem' }}>
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-lg font-semibold text-white">{drillDown.title}</h3>
              <button onClick={() => setDrillDown(null)} className="text-gray-400 hover:text-white px-2 py-1">✕</button>
            </div>
            <p className="text-xs text-gray-500 mb-4">Last 6 weeks ending at the currently selected week ({drillWeeks[0]} → {drillWeeks[drillWeeks.length-1]}). Colors use this person's weekly target from Andy's sheet.</p>
            {(() => {
              const showComment = (drillDown.kind === 'ta' || drillDown.kind === 'ts');
              const commentsByWeek = drillDown.kind === 'ta'
                ? drillTaComments(drillDown.taName)
                : drillDown.kind === 'ts' ? drillTsComments(drillDown.tsName)
                : {};
              return (<>
            <CsvBtn fname="wbr_drilldown_weekly" />
            <table className="text-sm" style={{ width: '100%', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
              <colgroup>
                <col style={{ width: '70px' }} />
                <col style={{ width: '100px' }} />
                {drillDown.kind === 'ts' && <col style={{ width: '100px' }} />}
                <col style={{ width: '100px' }} />
                <col style={{ width: '80px' }} />
                <col style={{ width: '80px' }} />
                <col style={{ width: '80px' }} />
                {showComment && <col />}
              </colgroup>
              <thead>
                <tr className="text-gray-300 border-b border-gray-600">
                  <th className="text-left px-3 py-2">Week</th>
                  <th className="text-center px-2 py-2">Contacted</th>
                  {drillDown.kind === 'ts' && <th className="text-center px-2 py-2">Rec Scrn</th>}
                  <th className="text-center px-2 py-2">Act Scrn</th>
                  <th className="text-center px-2 py-2">ATS</th>
                  <th className="text-center px-2 py-2">Offers</th>
                  <th className="text-center px-2 py-2">Hires</th>
                  {showComment && <th className="text-left px-3 py-2">Comment</th>}
                </tr>
              </thead>
              <tbody>
                {drillWeeks.map((wk, idx) => {
                  const byWeek = drillDown.kind === 'ta' ? drillTaWeekly(drillDown.taName)
                              : drillDown.kind === 'ts' ? drillTsWeekly(drillDown.tsName)
                              : drillClientWeekly(drillDown.displayClient);
                  const v = byWeek[wk] || {};
                  let contactedTgt, asTgt, atsTgt, rsTgt = null, hiresTgt;
                  if (drillDown.kind === 'ta') {
                    const t = drillTaTargets(drillDown.taName);
                    contactedTgt = t.contacted; asTgt = t.actual_screens; atsTgt = t.moved_to_ats; hiresTgt = t.hires;
                  } else if (drillDown.kind === 'ts') {
                    contactedTgt = drillTsContactedTarget(drillDown.tsName, wk);
                    rsTgt = 10;   // flat weekly target (Blake 2026-04-29)
                    asTgt = 7;
                    atsTgt = 4;
                    hiresTgt = 0;
                  } else {
                    const t = drillClientTargets(drillDown.displayClient);
                    contactedTgt = t.contacted; asTgt = t.actual_screens; atsTgt = t.moved_to_ats; hiresTgt = t.hires;
                  }
                  const c = showComment ? commentsByWeek[wk] : null;
                  return (
                    <tr key={wk} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                      <td className="text-left px-3 py-2 text-white font-medium align-top">{wk}</td>
                      <td className="text-center px-2 py-2 align-top" style={getCellStyle(v.contacted || 0, contactedTgt)}>{v.contacted || 0}</td>
                      {drillDown.kind === 'ts' && (
                        <td className="text-center px-2 py-2 align-top" style={getCellStyle(v.recruiter_screens || v.screened || 0, rsTgt)}>{v.recruiter_screens || v.screened || 0}</td>
                      )}
                      <td className="text-center px-2 py-2 align-top" style={getCellStyle(v.actual_screens || 0, asTgt)}>{v.actual_screens || 0}</td>
                      <td className="text-center px-2 py-2 align-top" style={getCellStyle(v.ats || 0, atsTgt)}>{v.ats || 0}</td>
                      <td className="text-center px-2 py-2 text-gray-300 align-top">{v.offers || 0}</td>
                      <td className="text-center px-2 py-2 align-top" style={hiresTgt > 0 ? getCellStyle(v.hires || 0, hiresTgt) : undefined}>{v.hires || 0}</td>
                      {showComment && (
                        <td className="text-left px-3 py-2 align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.5' }}>
                          {c ? <span className="text-gray-200">{c}</span> : <span className="text-gray-600">—</span>}
                        </td>
                      )}
                    </tr>
                  );
                })}
                <tr className="bg-gray-700 font-bold text-base border-t border-gray-600">
                  <td className="text-left px-3 py-2 text-white">6w Total</td>
                  {(() => {
                    const byWeek = drillDown.kind === 'ta' ? drillTaWeekly(drillDown.taName)
                                : drillDown.kind === 'ts' ? drillTsWeekly(drillDown.tsName)
                                : drillClientWeekly(drillDown.displayClient);
                    const sumKey = (k) => drillWeeks.reduce((s, wk) => s + ((byWeek[wk] || {})[k] || 0), 0);
                    const cells = [
                      sumKey('contacted'),
                      ...(drillDown.kind === 'ts' ? [drillWeeks.reduce((s, wk) => s + (((byWeek[wk] || {}).recruiter_screens) || ((byWeek[wk] || {}).screened) || 0), 0)] : []),
                      sumKey('actual_screens'),
                      sumKey('ats'),
                      sumKey('offers'),
                      sumKey('hires'),
                    ];
                    return cells.map((v, i) => <td key={i} className="text-center px-2 py-2 text-white">{v}</td>);
                  })()}
                  {showComment && <td className="text-left px-3 py-2 text-gray-400">—</td>}
                </tr>
              </tbody>
            </table></>
              );
            })()}
            <p className="text-xs text-gray-500 mt-4">Click outside this panel or press ✕ to close.</p>
          </div>
        </div>
      )}

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
        <span className="text-xs text-gray-500 ml-2">Tip: click any Client, TA, or TS row below for a 6-week drill-down</span>
      </div>

      {/* Team-lead filter (Bamboo-derived). Client Summary always shows full;
          TA Detail / TS Weekly / TS Conversion filter to lead's direct reports. */}
      <div className="flex items-center gap-3 flex-wrap">
        <label className="text-sm font-medium text-gray-300">Filter by lead:</label>
        <select
          value={selectedTeamLead}
          onChange={(e) => setSelectedTeamLead(e.target.value)}
          className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm"
          style={{ minWidth: '220px' }}
        >
          <option value="">All teams</option>
          {teamLeadOptions.map((name) => (
            <option key={name} value={name}>{name}</option>
          ))}
        </select>
        {selectedTeamLead && (
          <span className="inline-flex items-center gap-2 px-3 py-1 rounded text-sm font-semibold"
                style={{ backgroundColor: '#1e40af', color: '#dbeafe' }}>
            Showing: {selectedTeamLead}'s team
            <button
              onClick={() => setSelectedTeamLead('')}
              className="text-blue-200 hover:text-white"
              style={{ marginLeft: '4px', fontWeight: 'normal' }}
              title="Clear filter"
            >
              ✕
            </button>
          </span>
        )}
      </div>

      {/* Client Summary — compact 600px centered */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Client Summary — Week {selectedWeek}</h3>
        <div style={{ overflowX: 'auto' }}>
          <CsvBtn fname="wbr_client_summary" />
          <table className="text-sm" style={{ width: '780px', maxWidth: '100%', margin: '0 auto', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '180px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '90px' }} />
              <col style={{ width: '80px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '70px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '70px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-3 py-2">Client</th>
                <th className="text-center px-2 py-2">Roles</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Screens</th>
                <th className="text-center px-2 py-2">ATS</th>
                <th className="text-center px-2 py-2">Offers</th>
                <th className="text-center px-2 py-2">Hires</th>
                <th className="text-center px-2 py-2" title="Last 12w Hires">12w H</th>
              </tr>
            </thead>
            <tbody>
              {clientSummary.map((row, idx) => (
                <tr
                  key={idx}
                  onClick={() => setDrillDown({ kind: 'client', displayClient: row.client, title: `${row.client} · Last 6 weeks` })}
                  className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} hover:bg-gray-700 cursor-pointer`}
                  title="Click for 6-week drill-down"
                >
                  <td className="text-left px-3 py-2 text-white font-medium whitespace-normal align-top">{row.client}</td>
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
              <tr className="bg-gray-700 border-t border-gray-600 font-bold text-base">
                <td className="text-left px-3 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.roles, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.contacted, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.screened, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.ats, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.offers, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{clientSummary.reduce((sum, r) => sum + r.hires, 0)}</td>
                <td className="text-center px-2 py-2 text-white" title="Sum of 12w hires across ALL active clients (includes Wolt Volume + other hidden rows), to match PBI's Total behaviour">{Object.values(data.mbr_client_totals || {}).reduce((s, v) => s + (v.hires_12w || 0), 0) || clientSummary.reduce((sum, r) => sum + r.hires_12w, 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* TA Detail */}
      {(!selectedTeamLead || taDetail.length > 0) && (
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TA Weekly Detail — Week {selectedWeek}</h3>
        <div>
          <CsvBtn fname="wbr_ta_weekly_detail" />
          <table className="text-sm" style={{ width: '100%', tableLayout: 'fixed', borderCollapse: 'separate', borderSpacing: 0 }}>
            <colgroup>
              <col style={{ width: '120px' }} />
              <col style={{ width: '136px' }} />
              <col style={{ width: '64px' }} />
              <col style={{ width: '64px' }} />
              <col style={{ width: '70px' }} />
              <col style={{ width: '64px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '58px' }} />
              <col style={{ width: '78px' }} />
              <col style={{ width: '70px' }} />
              <col style={{ width: '54px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '54px' }} />
              <col />
            </colgroup>
            <thead className="sticky top-0 z-20">
              <tr className="text-gray-300 bg-gray-800">
                <th className="text-left px-3 py-2 sticky left-0 bg-gray-800 z-30 border-b border-gray-600">Client</th>
                <th className="text-left px-3 py-2 bg-gray-800 border-b border-gray-600">TA</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Last 12 Weeks Hires">12w Hires</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Last 12 Weeks ATS">12w ATS</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Last 12 Weeks Screens">12w Scrns</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Last 12w % Actual Screens to Hires">12w S→H</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Last 12w Time to Fill (days)">12w TTF</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Weekly Hires">Hires</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Weekly Contacted">Contacted</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Weekly Actual Screens">Screens</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Weekly ATS">ATS</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="% Actual Screens to ATS">% S→A</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="# Active Roles"># Jobs</th>
                <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Jobs Opened > 60 days">60d+</th>
                <th className="text-left px-3 py-2 bg-gray-800 border-b border-gray-600">Comment</th>
              </tr>
            </thead>
            <tbody>
              {['Dolphins/Whales', 'Ponies/Unicorns'].map((group, groupIdx) => {
                const groupRows = taDetail.filter(r => r.team_group === group);
                if (groupRows.length === 0) return null;
                // Repeat the header at the top of each group (after the first)
                // so it stays visible when the user is scrolled past the first
                // group's header. Requested 2026-04-20.
                const repeatHeader = groupIdx > 0 ? (
                  <tr className="text-gray-300 bg-gray-800 border-t border-gray-600">
                    <th className="text-left px-3 py-2 sticky left-0 bg-gray-800">Client</th>
                    <th className="text-left px-3 py-2">TA</th>
                    <th className="text-center px-2 py-2" title="Last 12 Weeks Hires">12w Hires</th>
                    <th className="text-center px-2 py-2" title="Last 12 Weeks ATS">12w ATS</th>
                    <th className="text-center px-2 py-2" title="Last 12 Weeks Screens">12w Scrns</th>
                    <th className="text-center px-2 py-2" title="Last 12w % Actual Screens to Hires">12w S→H</th>
                    <th className="text-center px-2 py-2" title="Last 12w Time to Fill (days)">12w TTF</th>
                    <th className="text-center px-2 py-2" title="Weekly Hires">Hires</th>
                    <th className="text-center px-2 py-2" title="Weekly Contacted">Contacted</th>
                    <th className="text-center px-2 py-2" title="Weekly Actual Screens">Screens</th>
                    <th className="text-center px-2 py-2" title="Weekly ATS">ATS</th>
                    <th className="text-center px-2 py-2" title="% Actual Screens to ATS">% S→A</th>
                    <th className="text-center px-2 py-2" title="# Active Roles"># Jobs</th>
                    <th className="text-center px-2 py-2" title="Jobs Opened > 60 days">60d+</th>
                    <th className="text-left px-3 py-2">Comment</th>
                  </tr>
                ) : null;
                return (
                  <React.Fragment key={group}>
                    <tr className="bg-gray-900">
                      <td colSpan={15} className="text-left px-2 py-3 text-white font-bold text-base" style={{ borderTop: '3px solid #4B5563' }}>
                        {group === 'Dolphins/Whales' ? '🐬 Dolphins & Whales' : '🦄 Ponies & Unicorns'}
                      </td>
                    </tr>
                    {repeatHeader}
                    {groupRows.map((row, idx) => {
                      const prevRow = idx > 0 ? groupRows[idx - 1] : null;
                      const isClientChange = !prevRow || prevRow.client !== row.client;
                      return (
                        <tr
                          key={`${group}-${idx}`}
                          onClick={() => setDrillDown({ kind: 'ta', taName: row.ta, title: `${row.ta} · Last 6 weeks` })}
                          className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} ${isClientChange ? 'border-t border-gray-600' : ''} hover:bg-gray-700 cursor-pointer`}
                          title="Click for 6-week drill-down"
                        >
                          <td className="text-left px-3 py-2 text-white font-medium sticky left-0 bg-inherit z-10 whitespace-normal align-top">{isClientChange ? row.client : ''}</td>
                          <td className="text-left px-3 py-2 text-gray-300 whitespace-normal align-top">{row.ta}</td>
                          <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w || '—'}</td>
                          <td className="text-center px-2 py-2 text-gray-300">{row.ats_12w || '—'}</td>
                          <td className="text-center px-2 py-2 text-gray-300">{row.screens_12w || '—'}</td>
                          <td className="text-center px-2 py-2 text-gray-400">{row.pct_screens_to_hires != null ? `${row.pct_screens_to_hires}%` : '—'}</td>
                          <td className="text-center px-2 py-2 text-gray-400">{row.ttf_12w != null ? row.ttf_12w : '—'}</td>
                          <td className="text-center px-2 py-2 text-gray-300">{row.hires || ''}</td>
                          <td className="text-center px-2 py-2" style={getCellStyle(row.contacted, row.contacted_target)}>
                            {row.contacted || ''}
                          </td>
                          <td className="text-center px-2 py-2" style={getCellStyle(row.screened, row.screened_target)}>
                            {row.screened || ''}
                          </td>
                          <td className="text-center px-2 py-2" style={getCellStyle(row.ats, row.ats_target)}>
                            {row.ats || ''}
                          </td>
                          <td className="text-center px-2 py-2 text-gray-400">{row.pct_screens_to_ats != null ? `${row.pct_screens_to_ats}%` : '—'}</td>
                          <td className="text-center px-2 py-2 text-gray-300">{row.roles || ''}</td>
                          <td className="text-center px-2 py-2 text-gray-300">{row.jobs_60d || ''}</td>
                          <td className="text-left px-3 py-3 text-gray-300 align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.55' }}>
                            {row.comment
                              ? row.comment
                              : (allTargetsMet([[row.contacted, row.contacted_target], [row.screened, row.screened_target], [row.ats, row.ats_target]])
                                  ? <span className="inline-block px-2 py-0.5 rounded text-xs font-semibold" style={{ backgroundColor: '#166534', color: '#bbf7d0' }}>✓ All targets met</span>
                                  : <span className="inline-block px-2 py-0.5 rounded text-xs font-semibold" style={{ backgroundColor: '#991b1b', color: '#fecaca' }}>⚠ Missing comment</span>)}
                          </td>
                        </tr>
                      );
                    })}
                    <tr className="bg-gray-700 font-bold text-base" style={{ borderTop: '2px solid #6B7280' }}>
                      <td className="text-left px-3 py-2 text-white sticky left-0 bg-gray-700 z-10">{group} Total</td>
                      <td className="text-left px-3 py-2 text-gray-300">—</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.hires_12w, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.ats_12w, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.screens_12w, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">—</td>
                      <td className="text-center px-2 py-2 text-white">—</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.hires, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.contacted, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.screened, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.ats, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">—</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.roles, 0)}</td>
                      <td className="text-center px-2 py-2 text-white">{groupRows.reduce((s, r) => s + r.jobs_60d, 0)}</td>
                      <td className="text-left px-3 py-2 text-gray-400">—</td>
                    </tr>
                  </React.Fragment>
                );
              })}
              <tr className="bg-gray-600 border-t-2 border-gray-500 font-bold text-base">
                <td className="text-left px-3 py-2 text-white sticky left-0 bg-gray-600 z-10">Grand Total</td>
                <td className="text-left px-3 py-2 text-gray-300">—</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.hires_12w, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.ats_12w, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.screens_12w, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.hires, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.contacted, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.screened, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.ats, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.roles, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{taDetail.reduce((s, r) => s + r.jobs_60d, 0)}</td>
                <td className="text-left px-3 py-2 text-gray-400">—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      )}

      {/* TS Weekly — compact with wide comment + TA Names columns */}
      {(!selectedTeamLead || tsData.length > 0) && (
      <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-lg font-semibold text-white mb-4">TS (Sourcer) Weekly — Week {selectedWeek}</h3>
          <div>
            <CsvBtn fname="wbr_ts_weekly" />
            <table className="text-sm" style={{ width: '100%', tableLayout: 'fixed', borderCollapse: 'separate', borderSpacing: 0 }}>
              <colgroup>
                <col style={{ width: '150px' }} />
                <col style={{ width: '64px' }} />
                <col style={{ width: '88px' }} />
                <col style={{ width: '64px' }} />
                <col style={{ width: '76px' }} />
                <col style={{ width: '76px' }} />
                <col style={{ width: '54px' }} />
                <col style={{ width: '60px' }} />
                <col style={{ width: '54px' }} />
                <col style={{ width: '210px' }} />
                <col />
              </colgroup>
              <thead className="sticky top-0 z-20">
                <tr className="text-gray-300 bg-gray-800">
                  <th className="text-left px-3 py-2 bg-gray-800 border-b border-gray-600">Sourcer</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Last 12 Weeks Hires">12w Hires</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Weekly Contacted">Contacted</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Weekly Contacted Target">Target</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Recruiter Screens">Rec Scrns</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Actual Screens">Act Scrns</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="Moved to ATS">ATS</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="# Active Jobs"># Jobs</th>
                  <th className="text-center px-2 py-2 bg-gray-800 border-b border-gray-600" title="# TAs supported"># TAs</th>
                  <th className="text-left px-3 py-2 bg-gray-800 border-b border-gray-600">TA Names</th>
                  <th className="text-left px-3 py-2 bg-gray-800 border-b border-gray-600">Comment</th>
                </tr>
              </thead>
              <tbody>
                {tsData.map((row, idx) => (
                  <tr
                    key={idx}
                    onClick={() => setDrillDown({ kind: 'ts', tsName: row.ts, title: `${row.ts} · Last 6 weeks` })}
                    className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} hover:bg-gray-700 cursor-pointer`}
                    title="Click for 6-week drill-down"
                  >
                    <td className="text-left px-3 py-2 text-white font-medium whitespace-normal align-top">{row.ts}</td>
                    <td className="text-center px-2 py-2 text-gray-300 align-top">{row.hires_12w}</td>
                    <td className="text-center px-2 py-2 align-top" style={getCellStyle(row.contacted, row._contacted_color_target)}>
                      {row.contacted}
                    </td>
                    <td className="text-center px-2 py-2 text-gray-300 align-top">{row.contacted_target || '—'}</td>
                    <td className="text-center px-2 py-2 align-top" style={getCellStyle(row.recruiter_screens, row.recruiter_screens_target)}>
                      {row.recruiter_screens}
                    </td>
                    <td className="text-center px-2 py-2 align-top" style={getCellStyle(row.actual_screens, row.actual_screens_target)}>
                      {row.actual_screens}
                    </td>
                    <td className="text-center px-2 py-2 align-top" style={getCellStyle(row.ats, row.ats_target)}>
                      {row.ats}
                    </td>
                    <td className="text-center px-2 py-2 text-gray-300 align-top">{row.num_jobs}</td>
                    <td className="text-center px-2 py-2 text-gray-300 align-top">{row.num_tas}</td>
                    <td className="text-left px-3 py-3 text-gray-300 align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.55' }}>{row.ta_names || '—'}</td>
                    <td className="text-left px-3 py-3 text-gray-300 align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.55' }}>{row.comment ? row.comment : (allTargetsMet([[row.contacted, row._contacted_color_target], [row.recruiter_screens, row.recruiter_screens_target], [row.actual_screens, row.actual_screens_target], [row.ats, row.ats_target]]) ? <span className="inline-block px-2 py-0.5 rounded text-xs font-semibold" style={{ backgroundColor: '#166534', color: '#bbf7d0' }}>✓ All targets met</span> : <span className="inline-block px-2 py-0.5 rounded text-xs font-semibold" style={{ backgroundColor: '#991b1b', color: '#fecaca' }}>⚠ Missing comment</span>)}</td>
                  </tr>
                ))}
                <tr className="bg-gray-750 border-t border-gray-600 font-bold text-base">
                  <td className="text-left px-3 py-2 text-white">Total</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.hires_12w, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.contacted, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{Number(tsData.reduce((sum, r) => sum + (Number(r.contacted_target) || 0), 0)).toFixed(1)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.recruiter_screens, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.actual_screens, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.ats, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.num_jobs, 0)}</td>
                  <td className="text-center px-2 py-2 text-white">{tsData.reduce((sum, r) => sum + r.num_tas, 0)}</td>
                  <td className="text-left px-3 py-2 text-gray-400">—</td>
                  <td className="text-left px-3 py-2 text-gray-400">—</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* TS Overall Conversion Rate with Officially Assigned Active Pipelines */}
      {(!selectedTeamLead || tsConversion.length > 0) && (
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-1">TS Overall Conversion Rate with Officially Assigned Active Pipelines</h3>
        <p className="text-xs text-gray-500 mb-4">
          Pipelines where the sourcer is officially assigned <em>and</em> actively working the pipeline.
          Funnel metrics count candidates on those pipelines only.
        </p>
        <div style={{ overflowX: 'auto' }}>
          <CsvBtn fname="wbr_ts_conversion" />
          <table className="text-sm" style={{ width: '900px', maxWidth: '100%', margin: '0 auto', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '150px' }} />
              <col style={{ width: '90px' }} />
              <col style={{ width: '120px' }} />
              <col style={{ width: '90px' }} />
              <col style={{ width: '120px' }} />
              <col style={{ width: '90px' }} />
              <col style={{ width: '120px' }} />
              <col style={{ width: '70px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-3 py-2">TS</th>
                <th className="text-center px-2 py-2">Active Jobs</th>
                <th className="text-center px-2 py-2" title="% Contacted to Positive Response">% C→PR</th>
                <th className="text-center px-2 py-2">Positive Resp</th>
                <th className="text-center px-2 py-2" title="% Recruiter Screens to Actual Screens">% S→AS</th>
                <th className="text-center px-2 py-2">Actual Scrn</th>
                <th className="text-center px-2 py-2" title="% Actual Screens to ATS">% AS→ATS</th>
                <th className="text-center px-2 py-2">ATS</th>
              </tr>
            </thead>
            <tbody>
              {tsConversion.map((row, idx) => {
                const fmt = (v) => v == null ? '—' : `${v}%`;
                // PBI conditional-formatting thresholds (calibrated vs PBI w16 screenshot 2026-04-20):
                //   - % Contacted to Positive Response: green ≥20% (Gustavo 22%, Nare 23%, Jovana 39% green; Marina 18% red)
                //   - % Screens to Actual Screen      : green ≥60% (Nare 61%, Zelimir 89% green; Andrea 55% red)
                //   - % Actual Screens to ATS         : green ≥50% (Milica 50%, Valeriia 51% green; Naledi 43%, Mia 45% red)
                // Use the same hex colours as getCellStyle (#166534 green / #991b1b red) so the
                // TS Conversion table matches the rest of the dashboard's palette. Inline style
                // instead of Tailwind classes — the dashboard ships Tailwind v2 via CDN and the
                // opacity syntax isn't available.
                const binaryCell = (v, greenAt) => {
                  if (v == null) return {};
                  return v >= greenAt
                    ? { backgroundColor: '#166534', color: '#bbf7d0' }
                    : { backgroundColor: '#991b1b', color: '#fecaca' };
                };
                return (
                  <tr key={idx} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} hover:bg-gray-700`}>
                    <td className="text-left px-3 py-2 text-white font-medium">{row.ts}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.active_jobs}</td>
                    <td className="text-center px-2 py-2" style={binaryCell(row.pct_contacted_to_pr, 20)}>{fmt(row.pct_contacted_to_pr)}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.positive_responses}</td>
                    <td className="text-center px-2 py-2" style={binaryCell(row.pct_screen_to_actual, 60)}>{fmt(row.pct_screen_to_actual)}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.actual_screens}</td>
                    <td className="text-center px-2 py-2" style={binaryCell(row.pct_actual_to_ats, 50)}>{fmt(row.pct_actual_to_ats)}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.ats}</td>
                  </tr>
                );
              })}
              <tr className="bg-gray-700 border-t border-gray-600 font-bold text-base">
                <td className="text-left px-3 py-2 text-white">Total</td>
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
          Source: <code className="text-gray-400">ts_conversion.sql</code> · Andy Hsu logic · Calibrated vs PBI w16 2026-04-20 — 12/12 Active Pipelines exact, 12/12 colour triplets exact, 98.99% aggregate volume accuracy
        </p>
      </div>
      )}

    </div>
  );
};

// MBR Tab — Monthly Business Review (last 4 weeks, w12-w15 = 2026-03-16 to 2026-04-12)
const MBRTab = ({ data }) => {
  const MBR_WEEKS = (data.mbr_window?.weeks || ['w12','w13','w14','w15']);
  const windowLabel = `${data.mbr_window?.start || '2026-03-16'} → ${data.mbr_window?.end || '2026-04-12'}`;

  // Build client-level rows from mbr_client_totals + per-client targets
  // (sum mbr_ta_targets per display client, x weekCount for the 4w window).
  const clientRows = useMemo(() => {
    const weekCount = MBR_WEEKS.length || 4;
    const clientTargets = {};
    (data.mbr_ta_targets || []).forEach((t) => {
      const disp = mbrAbbrevClient(t.client);
      if (!disp) return;
      const bucket = clientTargets[disp] || { contacted: 0, actual_screens: 0, moved_to_ats: 0, hires: 0 };
      bucket.contacted      += (Number(t.contacted) || 0) * weekCount;
      bucket.actual_screens += (Number(t.actual_screens) || 0) * weekCount;
      bucket.moved_to_ats   += (Number(t.moved_to_ats) || 0) * weekCount;
      bucket.hires          += (Number(t.hires) || 0) * weekCount;
      clientTargets[disp] = bucket;
    });
    const rows = Object.entries(data.mbr_client_totals || {}).map(([client, t]) => {
      const tg = clientTargets[client] || { contacted: 0, actual_screens: 0, moved_to_ats: 0, hires: 0 };
      return {
        client,
        contacted: t.contacted || 0,
        actual_screens: t.actual_screens || 0,
        ats: t.ats || 0,
        offers: t.offers || 0,
        hires: t.hires || 0,
        hires_12w: t.hires_12w || 0,
        contacted_target:      tg.contacted,
        actual_screens_target: tg.actual_screens,
        ats_target:            tg.moved_to_ats,
        hires_target:          tg.hires,
      };
    });
    return rows.filter(r => r.contacted + r.actual_screens + r.ats + r.offers + r.hires + r.hires_12w > 0)
               .sort((a, b) => a.client.localeCompare(b.client));
  }, [data]);

  // Build TA-level rows (with targets and comments)
  const taRows = useMemo(() => {
    const targets = data.mbr_ta_targets || [];
    // mbr_ta_targets rows hold *weekly* targets per (client, TA) — see
    // WBR Target sheet "TA target" tab. The MBR view shows the last 4
    // complete weeks of actuals, so per-TA targets must be scaled by
    // weekCount (matches the client-level rollup above and the TS rollup
    // which already sums 4 weekly targets).
    const weekCount = MBR_WEEKS.length || 4;
    // Map of latest comment per normalized TA key (from ta_weekly_notes, picking latest week available)
    const latestNote = {};
    (data.ta_weekly_notes || []).forEach(n => {
      // Skip rows with no content (Andy sometimes adds placeholder weeks with
      // empty comment AND reasoning — they shouldn't overwrite an earlier
      // week's real comment).
      if (!n.comment && !n.reasoning) return;
      const key = `${normalizeTa(n.ta)}`;
      if (!latestNote[key] || n.week > latestNote[key].week) {
        latestNote[key] = n;
      }
    });

    // Most-recent week in the MBR window — used for the "previous-week" activity
    // check (mirrors WBR's hide-if-no-activity-in-selected-week rule).
    const lastMbrWeek = MBR_WEEKS[MBR_WEEKS.length - 1];
    const lastMbrWeekNum = parseInt(String(lastMbrWeek).replace(/^w/, ''), 10);

    const result = [];
    targets.forEach(t => {
      const displayClient = mbrAbbrevClient(t.client);
      const key = `${displayClient}|${normalizeTa(t.ta)}`;
      const a = data.mbr_ta_actuals?.[key] || {};
      const note = latestNote[normalizeTa(t.ta)];

      // Per-week (last MBR week) actuals — pulled from data.wbr_actuals so we can
      // apply the same "no activity in the most recent week → hide" rule that WBR
      // uses. Sums across any wbr_actuals keys whose client+TA normalize to this
      // (displayClient, t.ta) pair.
      let lastWeekActivity = 0;
      Object.keys(data.wbr_actuals || {}).forEach((wkey) => {
        const [rawClient, rawTa] = wkey.split('|');
        if (
          (kebolaClientMatches(rawClient, displayClient) || mbrAbbrevClient(normalizeClient(rawClient)) === displayClient) &&
          normalizeTa(rawTa) === normalizeTa(t.ta)
        ) {
          const wk = data.wbr_actuals[wkey]?.[lastMbrWeek];
          if (wk) {
            lastWeekActivity +=
              (wk.contacted || 0) +
              (wk.actual_screens || wk.screened || 0) +
              (wk.ats || 0) +
              (wk.offers || 0) +
              (wk.hires || 0);
          }
        }
      });

      // WBR-parity visibility: a TA also shows if there is a comment/reasoning
      // for the LAST MBR week even with zero activity that week (e.g. someone on
      // leave whose lead still left a note). Mirrors WBRTab's hasNote rule.
      const lwNote = (data.ta_weekly_notes || []).find((n) =>
        normalizeTa(n.ta) === normalizeTa(t.ta) && n.week === lastMbrWeekNum &&
        (kebolaClientMatches(n.client || '', displayClient) ||
         mbrAbbrevClient(normalizeClient(n.client || '')) === displayClient)
      );
      const hasLastWeekNote = !!((lwNote?.comment && lwNote.comment.trim()) ||
                                 (lwNote?.reasoning && lwNote.reasoning.trim()));

      result.push({
        client: displayClient,
        ta: t.ta,
        // Derive BU group from the client (matches WBR). Overrides per-TA
        // team_group set in Andy's target sheet so e.g. Aiven TAs tagged
        // Ponies/Unicorns still roll up to Dolphins/Whales at the BU level.
        team_group: getBuGroup(displayClient),
        contacted: a.contacted || 0,
        actual_screens: a.actual_screens || 0,
        ats: a.ats || 0,
        offers: a.offers || 0,
        hires: a.hires || 0,
        hires_12w: a.hires_12w || 0,
        ats_12w: a.ats_12w || 0,
        screens_12w: a.screens_12w || 0,
        jobs_60d: a.jobs_60d || 0,
        contacted_target:      (Number(t.contacted)      || 0) * weekCount,
        actual_screens_target: (Number(t.actual_screens) || 0) * weekCount,
        ats_target:            (Number(t.moved_to_ats)   || 0) * weekCount,
        hires_target:          (Number(t.hires)          || 0) * weekCount,
        pct_screens_to_hires: a.screens_12w > 0 ? Math.round((a.hires_12w || 0) / a.screens_12w * 100) : null,
        comment: note?.comment || note?.reasoning || '',
        _last_week_activity: lastWeekActivity,
        _last_week_note: hasLastWeekNote,
      });
    });

    const groupOrder = { 'Dolphins/Whales': 0, 'Ponies/Unicorns': 1 };
    // Visibility filter (Blake 2026-07-06): mirror the WBR rule exactly — show a
    // TA if they had activity in the last MBR week OR have a comment/reasoning
    // for that week. The comment path keeps people like Milica Mladzic (on leave
    // with a lead note but zero week-27 activity) visible, matching WBR. TAs with
    // neither activity nor a note for the last week drop off.
    const filtered = result.filter((r) =>
      (r._last_week_activity > 0) || r._last_week_note
    );
    return filtered.sort((a, b) => {
      const ga = groupOrder[a.team_group] ?? 2;
      const gb = groupOrder[b.team_group] ?? 2;
      if (ga !== gb) return ga - gb;
      if (a.client !== b.client) return a.client.localeCompare(b.client);
      return a.ta.localeCompare(b.ta);
    });
  }, [data]);

  // TS rows from mbr_ts_actuals — only include sourcers with a ts_weekly target row
  const tsRows = useMemo(() => {
    // Use dynamic MBR window (w13-w16 as of 2026-04-21), not a hardcoded range.
    // data.mbr_window.weeks is set by render_json.py to the last 4 complete Mon-Sun weeks.
    const mbrWeekNums = new Set(
      (data.mbr_window?.weeks || []).map(w => parseInt(String(w).replace(/^w/, '')))
    );
    const targets = {};
    (data.ts_weekly || []).forEach(t => {
      const wNum = parseInt(String(t.week));
      if (mbrWeekNums.has(wNum)) {
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
    const weekCount = mbrWeekNums.size || 4;
    Object.keys(targets).forEach(ts => {
      const a = data.mbr_ts_actuals?.[ts] || {};
      const contactedTarget = targets[ts] || (100 * weekCount);
      rows.push({
        ts,
        contacted: a.contacted_4w || 0,
        contacted_target: targets[ts],
        _contacted_color_target: contactedTarget,
        recruiter_screens: a.recruiter_screens_4w || 0,
        recruiter_screens_target: 10 * weekCount,
        actual_screens: a.actual_screens_4w || 0,
        actual_screens_target: 7 * weekCount,
        ats: a.ats_4w || 0,
        ats_target: 4 * weekCount,
        hires_12w: a.hires_12w || 0,
        screens_12w: a.screens_12w || 0,
        ats_12w: a.ats_12w || 0,
        pct_actual_to_ats_12w: a.screens_12w > 0 ? Math.round((a.ats_12w || 0) / a.screens_12w * 100) : null,
        comment: latestComment[ts]?.comment || '',
      });
    });
    // Mia filter: drop TSes with no actuals in the MBR window AND no comment —
    // they shouldn't clutter the list just because a target exists.
    const tsRowsFiltered = rows.filter((r) =>
      (r.contacted > 0) || (r.actual_screens > 0) || (r.ats > 0) ||
      (r.hires_12w > 0) || (r.screens_12w > 0) || (r.ats_12w > 0) ||
      (r.recruiter_screens > 0) || !!r.comment
    );
    return tsRowsFiltered.sort((a, b) => a.ts.localeCompare(b.ts));
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
          <CsvBtn fname={'mbr_tas_' + group} />
          <table className="text-sm" style={{ width: '1500px', maxWidth: '100%', margin: '0 auto', tableLayout: 'fixed' }}>
            <colgroup>
              <col style={{ width: '100px' }} />
              <col style={{ width: '140px' }} />
              <col style={{ width: '55px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '70px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ width: '55px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ width: '55px' }} />
              <col style={{ width: '395px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-3 py-2 sticky left-0 bg-gray-800 z-10">Client</th>
                <th className="text-left px-3 py-2">TA</th>
                <th className="text-center px-2 py-2" title="Last 12w Hires">12w H</th>
                <th className="text-center px-2 py-2" title="Last 12w ATS">12w ATS</th>
                <th className="text-center px-2 py-2" title="Last 12w Screens">12w Scr</th>
                <th className="text-center px-2 py-2" title="12w % Screens → Hires">12w %S→H</th>
                <th className="text-center px-2 py-2" title="4w Hires">Hires</th>
                <th className="text-center px-2 py-2">Tgt</th>
                <th className="text-center px-2 py-2" title="4w Contacted">Cntd</th>
                <th className="text-center px-2 py-2">Tgt</th>
                <th className="text-center px-2 py-2" title="4w Actual Screens">Scrn</th>
                <th className="text-center px-2 py-2">Tgt</th>
                <th className="text-center px-2 py-2" title="4w Moved to ATS">ATS</th>
                <th className="text-center px-2 py-2">Tgt</th>
                <th className="text-center px-2 py-2" title="Jobs Opened &gt; 60 days">{'>'}60d</th>
                <th className="text-left px-3 py-2">Latest Comment</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, idx) => {
                const prev = idx > 0 ? rows[idx - 1] : null;
                const clientChange = !prev || prev.client !== r.client;
                return (
                  <tr key={idx} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} ${clientChange ? 'border-t border-gray-600' : ''} hover:bg-gray-700`}>
                    <td className="text-left px-3 py-2 text-white font-medium sticky left-0 bg-inherit z-10 whitespace-normal align-top">{clientChange ? r.client : ''}</td>
                    <td className="text-left px-3 py-2 text-gray-300 whitespace-normal align-top">{r.ta}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{r.hires_12w || '—'}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{r.ats_12w || '—'}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{r.screens_12w || '—'}</td>
                    <td className="text-center px-2 py-2 text-gray-400">{r.pct_screens_to_hires != null ? `${r.pct_screens_to_hires}%` : '—'}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{r.hires || ''}</td>
                    <td className="text-center px-2 py-2 text-gray-500">{r.hires_target ? r.hires_target.toFixed(1) : '—'}</td>
                    <td className="text-center px-2 py-2" style={getCellStyle(r.contacted, r.contacted_target)}>{r.contacted || ''}</td>
                    <td className="text-center px-2 py-2 text-gray-500">{r.contacted_target || '—'}</td>
                    <td className="text-center px-2 py-2" style={getCellStyle(r.actual_screens, r.actual_screens_target)}>{r.actual_screens || ''}</td>
                    <td className="text-center px-2 py-2 text-gray-500">{r.actual_screens_target || '—'}</td>
                    <td className="text-center px-2 py-2" style={getCellStyle(r.ats, r.ats_target)}>{r.ats || ''}</td>
                    <td className="text-center px-2 py-2 text-gray-500">{r.ats_target || '—'}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{r.jobs_60d || ''}</td>
                    <td className="text-left px-3 py-3 text-gray-300 align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.55' }}>{r.comment || '—'}</td>
                  </tr>
                );
              })}
              <tr className="bg-gray-700 font-bold text-base" style={{ borderTop: '2px solid #6B7280' }}>
                <td className="text-left px-3 py-2 text-white sticky left-0 bg-gray-700 z-10">{group} Total</td>
                <td className="text-left px-3 py-2 text-gray-300">—</td>
                <td className="text-center px-2 py-2 text-white">{totals.hires_12w}</td>
                <td className="text-center px-2 py-2 text-white">{totals.ats_12w}</td>
                <td className="text-center px-2 py-2 text-white">{totals.screens_12w}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{totals.hires}</td>
                <td className="text-center px-2 py-2 text-white">{totals.hires_target.toFixed(1)}</td>
                <td className="text-center px-2 py-2 text-white">{totals.contacted}</td>
                <td className="text-center px-2 py-2 text-white">{totals.contacted_target}</td>
                <td className="text-center px-2 py-2 text-white">{totals.actual_screens}</td>
                <td className="text-center px-2 py-2 text-white">{totals.actual_screens_target}</td>
                <td className="text-center px-2 py-2 text-white">{totals.ats}</td>
                <td className="text-center px-2 py-2 text-white">{totals.ats_target}</td>
                <td className="text-center px-2 py-2 text-white">{totals.jobs_60d}</td>
                <td className="text-left px-3 py-2 text-gray-400">—</td>
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

      {/* 1. Client's Target — compact 540px, colgroup widths, color coding */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Client's Target — Last 4 Weeks</h3>
        <div style={{ overflowX: 'auto' }}>
          <CsvBtn fname="mbr_client_targets_last4w" />
          <table className="text-sm" style={{ width: '780px', maxWidth: '100%', margin: '0 auto', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '130px' }} />
              <col style={{ width: '75px' }} />
              <col style={{ width: '70px' }} />
              <col style={{ width: '100px' }} />
              <col style={{ width: '95px' }} />
              <col style={{ width: '70px' }} />
              <col style={{ width: '80px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-3 py-2">Client</th>
                <th className="text-center px-2 py-2" title="Last 12w Hires">12w H</th>
                <th className="text-center px-2 py-2">Hires</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Act Scrn</th>
                <th className="text-center px-2 py-2">ATS</th>
                <th className="text-center px-2 py-2">Offers</th>
              </tr>
            </thead>
            <tbody>
              {clientRows.map((row, idx) => (
                <tr key={idx} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} hover:bg-gray-700`}>
                  <td className="text-left px-3 py-2 text-white font-medium whitespace-normal align-top">{row.client}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w}</td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.hires, row.hires_target)}>{row.hires}</td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.contacted, row.contacted_target)}>{row.contacted}</td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.actual_screens, row.actual_screens_target)}>{row.actual_screens}</td>
                  <td className="text-center px-2 py-2" style={getCellStyle(row.ats, row.ats_target)}>{row.ats}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.offers}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-bold text-base">
                <td className="text-left px-3 py-2 text-white">Total</td>
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

      {/* 4. TS Target — colgroup with narrow numeric cols, wide comment */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TS's Target — Last 4 Weeks</h3>
        <div style={{ overflowX: 'auto' }}>
          <CsvBtn fname="mbr_ts_targets_last4w" />
          <table className="text-sm" style={{ width: '1300px', maxWidth: '100%', margin: '0 auto', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '150px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '85px' }} />
              <col style={{ width: '90px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '80px' }} />
              <col style={{ width: '80px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '425px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-3 py-2">Sourcer</th>
                <th className="text-center px-2 py-2" title="Last 12w Hires">12w H</th>
                <th className="text-center px-2 py-2" title="Last 12w % Actual Screens → ATS">12w %S→A</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Tgt</th>
                <th className="text-center px-2 py-2">Rec Scrn</th>
                <th className="text-center px-2 py-2">Act Scrn</th>
                <th className="text-center px-2 py-2">ATS</th>
                <th className="text-left px-3 py-2">Latest Comment</th>
              </tr>
            </thead>
            <tbody>
              {tsRows.map((r, idx) => (
                <tr key={idx} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} hover:bg-gray-700`}>
                  <td className="text-left px-3 py-2 text-white font-medium whitespace-normal align-top">{r.ts}</td>
                  <td className="text-center px-2 py-2 text-gray-300 align-top">{r.hires_12w || '—'}</td>
                  <td className="text-center px-2 py-2 text-gray-400 align-top">{r.pct_actual_to_ats_12w != null ? `${r.pct_actual_to_ats_12w}%` : '—'}</td>
                  <td className="text-center px-2 py-2 align-top" style={getCellStyle(r.contacted, r._contacted_color_target)}>{r.contacted}</td>
                  <td className="text-center px-2 py-2 text-gray-500 align-top">{r.contacted_target || '—'}</td>
                  <td className="text-center px-2 py-2 align-top" style={getCellStyle(r.recruiter_screens, r.recruiter_screens_target)}>{r.recruiter_screens}</td>
                  <td className="text-center px-2 py-2 align-top" style={getCellStyle(r.actual_screens, r.actual_screens_target)}>{r.actual_screens}</td>
                  <td className="text-center px-2 py-2 align-top" style={getCellStyle(r.ats, r.ats_target)}>{r.ats}</td>
                  <td className="text-left px-3 py-3 text-gray-300 align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.55' }}>{r.comment || '—'}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-bold text-base">
                <td className="text-left px-3 py-2 text-white">Total</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.hires_12w, 0)}</td>
                <td className="text-center px-2 py-2 text-white">—</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.contacted, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.contacted_target, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.recruiter_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.actual_screens, 0)}</td>
                <td className="text-center px-2 py-2 text-white">{tsRows.reduce((s, r) => s + r.ats, 0)}</td>
                <td className="text-left px-3 py-2 text-gray-400">—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

// Project Dashboard Tab — mirrors PBIX Overview page with client-grouped collapsible sections
// Data surfaces (produced by refresh_staging/project_dashboard{,_hires}.sql):
//   data.project_dashboard.rows — per-(client, job, TA, TS, source, ext, iso week) funnel
//   data.project_dashboard_hires — one row per hired candidate since 2025-01-01
// Validated 2026-04-20 vs PBIX Overview page Apr 13-19 (24/24 per-client within 1-3 units).

const PD_PERIODS = [
  ['this_week',  'This week (Mon-today)'],
  ['last_week',  'Last full week (Mon-Sun)'],
  ['last_4w',    'Last 4 weeks'],
  ['last_month', 'Last calendar month'],
  ['last_12w',   'Last 12 weeks'],
  ['qtd',        'Quarter to date'],
  ['ytd',        'Year to date'],
  ['all',        'All time (2026+)'],
];

const pdPeriodWindow = (period) => {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const dow = today.getDay();
  const daysToMonday = dow === 0 ? 6 : dow - 1;
  const thisMonday = new Date(today.getTime() - daysToMonday * 86400000);
  const addDays = (d, n) => new Date(d.getTime() + n * 86400000);
  switch (period) {
    case 'this_week':  return [thisMonday, today];
    case 'last_week':  return [addDays(thisMonday, -7),  addDays(thisMonday, -1)];
    case 'last_4w':    return [addDays(thisMonday, -28), addDays(thisMonday, -1)];
    case 'last_month': {
      const first = new Date(today.getFullYear(), today.getMonth(), 1);
      const endPrev = addDays(first, -1);
      return [new Date(endPrev.getFullYear(), endPrev.getMonth(), 1), endPrev];
    }
    case 'last_12w':   return [addDays(thisMonday, -84), addDays(thisMonday, -1)];
    case 'qtd':        return [new Date(today.getFullYear(), Math.floor(today.getMonth() / 3) * 3, 1), today];
    case 'ytd':        return [new Date(today.getFullYear(), 0, 1), today];
    case 'all':
    default:           return [new Date('2025-01-01'), today];
  }
};

const pdIsoDate = (d) => {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
};

const pdIsoKey = (d) => {
  const t = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
  const wk = Math.ceil((((t - yearStart) / 86400000) + 1) / 7);
  return `${t.getUTCFullYear()}-W${String(wk).padStart(2, '0')}`;
};

const pdWeekSetFor = (start, end) => {
  const set = new Set();
  const cur = new Date(start.getTime());
  while (cur <= end) { set.add(pdIsoKey(cur)); cur.setDate(cur.getDate() + 1); }
  return set;
};

// Project Dashboard client normalization — unlike normalizeClient (which merges
// Doordash/SevenRooms into "Wolt HQ" for MBR/WBR), PD keeps them as distinct
// clients to match how the external PBIX Project Dashboard displays them.
const normalizeClientPD = (client) => {
  if (!client) return client;
  const trimmed = client.trim();
  if (trimmed.toUpperCase() === 'AVIV') return 'Aviv';
  if (trimmed.toLowerCase() === 'doordash') return 'DoorDash';
  if (trimmed.toLowerCase() === 'sevenrooms') return 'SevenRooms';
  if (trimmed.toLowerCase() === 'nexi') return 'Nexi';
  return trimmed;
};

// Non-real PD clients: always excluded from Project Dashboard (Tribe.xyz (IR) is kept).
const PD_EXCLUDED_CLIENTS = new Set(['Tribe.xyz', 'Tribe: Talent Pools']);

const pdPct = (v) => v == null ? '—' : `${(v * 100).toFixed(0)}%`;

// ---------- PD Disqualified Reasons section ----------
// Filterable port of the PBI Overview "Disqualified Reason" pie. Data comes
// from public/dq_reasons.json (lazy-fetched on first expand), dictionary-
// encoded rows: [clientIdx, jobIdx, taIdx, weekIdx, reasonIdx, n], weekIdx -1
// = candidate has no Disqualified event (included only in All time).
const DQ_COLORS = ['#60a5fa', '#34d399', '#fbbf24', '#a78bfa', '#f472b6', '#fb923c', '#93c5fd', '#a3e635', '#fca5a5', '#5eead4', '#c4b5fd', '#fdba74', '#86efac', '#f9a8d4', '#cbd5e1'];
const DQ_PERIODS = [['all', 'All time'], ['ytd', 'This year'], ['12w', 'Last 12 weeks'], ['4w', 'Last 4 weeks']];

const DQReasonsSection = () => {
  const [open, setOpen] = useState(true);
  const [dq, setDq] = useState(null);
  const [err, setErr] = useState(null);
  const [client, setClient] = useState('');
  const [job, setJob] = useState('');
  const [ta, setTa] = useState('');
  const [period, setPeriod] = useState('all');
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const [showAll, setShowAll] = useState(false);

  React.useEffect(() => {
    if (!open || dq || err) return;
    fetch('dq_reasons.json')
      .then((r) => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(setDq)
      .catch((e) => setErr(String(e && e.message ? e.message : e)));
  }, [open, dq, err]);

  const usingCustom = customStart !== '' || customEnd !== '';
  const range = useMemo(() => {
    if (usingCustom) return [customStart || '0000-01-01', customEnd || '9999-12-31'];
    if (period === 'all') return null;
    const now = new Date();
    if (period === 'ytd') return [now.getFullYear() + '-01-01', '9999-12-31'];
    const weeks = period === '4w' ? 4 : 12;
    const d = new Date(now); d.setDate(d.getDate() - weeks * 7);
    return [d.toISOString().slice(0, 10), '9999-12-31'];
  }, [period, usingCustom, customStart, customEnd]);

  // Shared filter predicates. Each dropdown's option list applies every OTHER
  // active filter plus the time range, so picking a TA narrows jobs/clients,
  // picking "Last 4 weeks" hides clients/jobs/TAs with no DQs in the window, etc.
  const filt = useMemo(() => {
    if (!dq) return null;
    const ci = client === '' ? -1 : dq.clients.indexOf(client);
    const ji = job === '' ? -1 : dq.jobs.indexOf(job);
    const ti = ta === '' ? -1 : dq.tas.indexOf(ta);
    const inRange = (r) => {
      if (!range) return true;
      if (r[3] < 0) return false;
      const w = dq.weeks[r[3]];
      return w >= range[0] && w <= range[1];
    };
    return { ci, ji, ti, inRange };
  }, [dq, client, job, ta, range]);
  const clientOptions = useMemo(() => {
    if (!dq) return [];
    const { ji, ti, inRange } = filt;
    return [...new Set(dq.rows.filter((r) => (ji < 0 || r[1] === ji) && (ti < 0 || r[2] === ti) && inRange(r)).map((r) => dq.clients[r[0]]))].sort();
  }, [dq, filt]);
  const jobOptions = useMemo(() => {
    if (!dq) return [];
    const { ci, ti, inRange } = filt;
    return [...new Set(dq.rows.filter((r) => (ci < 0 || r[0] === ci) && (ti < 0 || r[2] === ti) && inRange(r)).map((r) => dq.jobs[r[1]]))].sort();
  }, [dq, filt]);
  const taOptions = useMemo(() => {
    if (!dq) return [];
    const { ci, ji, inRange } = filt;
    return [...new Set(dq.rows.filter((r) => (ci < 0 || r[0] === ci) && (ji < 0 || r[1] === ji) && inRange(r)).map((r) => dq.tas[r[2]]))].sort();
  }, [dq, filt]);
  // Auto-clear a selection that fell out of its (now narrower) option list.
  React.useEffect(() => { if (dq && client && !clientOptions.includes(client)) setClient(''); }, [dq, client, clientOptions]);
  React.useEffect(() => { if (dq && job && !jobOptions.includes(job)) setJob(''); }, [dq, job, jobOptions]);
  React.useEffect(() => { if (dq && ta && !taOptions.includes(ta)) setTa(''); }, [dq, ta, taOptions]);

  const byReason = useMemo(() => {
    if (!dq) return [];
    const { ci, ji, ti, inRange } = filt;
    const m = new Map();
    for (const r of dq.rows) {
      if (ci >= 0 && r[0] !== ci) continue;
      if (ji >= 0 && r[1] !== ji) continue;
      if (ti >= 0 && r[2] !== ti) continue;
      if (!inRange(r)) continue;
      m.set(r[4], (m.get(r[4]) || 0) + r[5]);
    }
    return [...m.entries()].map(([ri, n]) => ({ reason: dq.reasons[ri], n })).sort((a, b) => b.n - a.n);
  }, [dq, filt]);

  const total = byReason.reduce((s, r) => s + r.n, 0);
  const shown = showAll ? byReason : byReason.slice(0, 10);
  const maxN = byReason.length ? byReason[0].n : 1;
  const donutData = useMemo(() => {
    const top = byReason.slice(0, 10);
    const rest = byReason.slice(10).reduce((s, r) => s + r.n, 0);
    return rest > 0 ? [...top, { reason: 'Rest', n: rest }] : top;
  }, [byReason]);

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <button type="button" onClick={() => setOpen(!open)} className="w-full flex items-center justify-between text-left">
        <h3 className="text-lg font-semibold text-white">
          Disqualified Reasons{open && total > 0 ? ` · ${total.toLocaleString()}` : ''}
        </h3>
        <span className="text-gray-400 text-sm">{open ? '▾ hide' : '▸ show'}</span>
      </button>
      {open && err && (
        <div className="text-sm text-gray-400 mt-3">
          Data not available yet — dq_reasons.json populates on the next scheduled refresh. ({err})
        </div>
      )}
      {open && !err && !dq && <div className="text-sm text-gray-400 mt-3">Loading…</div>}
      {open && dq && (
        <div className="mt-3">
          <div className="flex flex-wrap gap-2 items-center mb-3">
            <select value={client} onChange={(e) => setClient(e.target.value)}
              className="px-2 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" style={{ maxWidth: 180 }}>
              <option value="">All clients</option>
              {clientOptions.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
            <select value={job} onChange={(e) => setJob(e.target.value)}
              className="px-2 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" style={{ maxWidth: 220 }}>
              <option value="">All jobs</option>
              {jobOptions.map((j) => <option key={j} value={j}>{j}</option>)}
            </select>
            <select value={ta} onChange={(e) => setTa(e.target.value)}
              className="px-2 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" style={{ maxWidth: 160 }}>
              <option value="">All TAs</option>
              {taOptions.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
            <select value={period} disabled={usingCustom}
              onChange={(e) => setPeriod(e.target.value)}
              className="px-2 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs">
              {DQ_PERIODS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </select>
            <div className="flex items-center gap-1 text-xs text-gray-400">
              <span>or custom:</span>
              <input type="date" value={customStart} onChange={(e) => setCustomStart(e.target.value)}
                className="px-1 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" />
              <span>→</span>
              <input type="date" value={customEnd} onChange={(e) => setCustomEnd(e.target.value)}
                className="px-1 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" />
              {usingCustom && (
                <button onClick={() => { setCustomStart(''); setCustomEnd(''); }}
                  className="ml-1 px-1 text-xs text-gray-300 hover:text-white">clear</button>
              )}
            </div>
            <span className="ml-auto text-xs text-gray-400">{total.toLocaleString()} disqualified</span>
          </div>
          {byReason.length === 0 ? (
            <div className="text-sm text-gray-400">No disqualifications match the current filters.</div>
          ) : (
            <div className="flex flex-wrap gap-6 items-start">
              <div style={{ width: 230, height: 230 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={donutData} dataKey="n" nameKey="reason" innerRadius={58} outerRadius={92} stroke="none" isAnimationActive={false}>
                      {donutData.map((e, i) => (
                        <Cell key={i} fill={e.reason === 'Rest' ? '#4b5563' : DQ_COLORS[i % DQ_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      formatter={(v, name) => [v.toLocaleString() + ' (' + (total ? (100 * v / total).toFixed(1) : 0) + '%)', name]}
                      contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #4b5563', borderRadius: 6, fontSize: 12 }}
                      itemStyle={{ color: '#e5e7eb' }} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="flex-1" style={{ minWidth: 320 }}>
                <CsvBtn fname="pd_dq_reasons" />
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-gray-300 border-b border-gray-700">
                      <th className="text-left px-2 py-1">Reason</th>
                      <th className="text-right px-2 py-1">#</th>
                      <th className="text-right px-2 py-1">%</th>
                      <th className="px-2 py-1" style={{ width: '30%' }}></th>
                    </tr>
                  </thead>
                  <tbody>
                    {shown.map((r, i) => (
                      <tr key={r.reason} className="border-b border-gray-700">
                        <td className="px-2 py-1 text-gray-200">
                          <span className="inline-block rounded-sm mr-2" style={{ width: 8, height: 8, backgroundColor: i < 10 ? DQ_COLORS[i % DQ_COLORS.length] : '#4b5563' }}></span>
                          {r.reason}
                        </td>
                        <td className="px-2 py-1 text-right text-white">{r.n.toLocaleString()}</td>
                        <td className="px-2 py-1 text-right text-gray-400">{total ? (100 * r.n / total).toFixed(1) : '0.0'}%</td>
                        <td className="px-2 py-1">
                          <div className="bg-gray-700 rounded-sm" style={{ height: 6 }}>
                            <div className="rounded-sm" style={{ height: 6, width: Math.max(2, Math.round(100 * r.n / maxN)) + '%', backgroundColor: i < 10 ? DQ_COLORS[i % DQ_COLORS.length] : '#4b5563' }}></div>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {byReason.length > 10 && (
                  <button type="button" onClick={() => setShowAll(!showAll)}
                    className="mt-2 text-xs text-gray-400 hover:text-white">
                    {showAll ? '▴ show top 10' : `▾ show all ${byReason.length} reasons`}
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};


const ProjectDashboardTab = ({ data }) => {
  const [period, setPeriod] = useState('last_week');
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const [searchText, setSearchText] = useState('');
  const [filterClient, setFilterClient] = useState('');
  const [filterTa, setFilterTa] = useState('');
  const [filterTs, setFilterTs] = useState('');
  const [filterCategory, setFilterCategory] = useState('');
  const [filterSource, setFilterSource] = useState('');
  const [showExternal, setShowExternal] = useState(true);
  const [showInternal, setShowInternal] = useState(true);
  const [hiresOpen, setHiresOpen] = useState(false);
  const [expandedJobs, setExpandedJobs] = useState(new Set());
  const [expandedTas, setExpandedTas] = useState(new Set());
  const [expandedTses, setExpandedTses] = useState(new Set());
  // Attribution mode: 'job' = TA from job.job_recruiter (default, original),
  // 'event' = TA from event.who_event_created_for (PBI-parity, picks up
  // cross-team work + handovers). Both data surfaces ship side by side.
  const [attrMode, setAttrMode] = useState('job');

  const eventAttrRows = (data.project_dashboard_eventattr && data.project_dashboard_eventattr.rows) || [];
  const eventAttrAvailable = eventAttrRows.length > 0;
  const pdRows = (attrMode === 'event' && eventAttrAvailable)
    ? eventAttrRows
    : ((data.project_dashboard && data.project_dashboard.rows) || []);
  const pdHires = data.project_dashboard_hires || [];

  const usingCustom = !!(customStart && customEnd);
  const [startDate, endDate] = useMemo(() => {
    if (usingCustom) return [new Date(customStart), new Date(customEnd)];
    return pdPeriodWindow(period);
  }, [period, customStart, customEnd, usingCustom]);
  const startStr = pdIsoDate(startDate);
  const endStr = pdIsoDate(endDate);
  const weekSet = useMemo(() => pdWeekSetFor(startDate, endDate), [startDate, endDate]);

  const filtered = useMemo(() => {
    const s = searchText.trim().toLowerCase();
    return pdRows.filter((r) => {
      const wk = `${r.iso_year}-W${String(r.iso_week).padStart(2, '0')}`;
      if (!weekSet.has(wk)) return false;
      if (!showExternal && r.is_external_recruiter) return false;
      if (PD_EXCLUDED_CLIENTS.has(r.client)) return false;
      if (filterClient && normalizeClientPD(r.client) !== filterClient) return false;
      if (filterTa && r.ta !== filterTa) return false;
      if (filterTs && r.ts !== filterTs) return false;
      if (filterCategory && r.job_category !== filterCategory) return false;
      if (filterSource && r.candidate_source !== filterSource) return false;
      if (s) {
        const hay = `${r.client} ${r.job_title} ${r.ta} ${r.ts} ${r.job_category}`.toLowerCase();
        if (!hay.includes(s)) return false;
      }
      return true;
    });
  }, [pdRows, weekSet, searchText, filterClient, filterTa, filterTs, filterCategory, filterSource, showExternal, showInternal]);

  const kpis = useMemo(() => filtered.reduce((a, r) => ({
    contacted: a.contacted + r.contacted,
    positive_response: a.positive_response + r.positive_response,
    actual_screens: a.actual_screens + r.actual_screens,
    ats: a.ats + r.ats,
    offered: a.offered + r.offered,
    hired: a.hired + r.hired,
  }), { contacted: 0, positive_response: 0, actual_screens: 0, ats: 0, offered: 0, hired: 0 }), [filtered]);

  // ── Client summary (for section headers) ──
  const byClient = useMemo(() => {
    const m = new Map();
    for (const r of filtered) {
      const c = normalizeClientPD(r.client);
      if (!m.has(c)) m.set(c, { client: c, jobIds: new Set(), tas: new Set(), tses: new Set(),
        viewed: 0, contacted: 0, positive_response: 0, screens: 0, actual_screens: 0, ats: 0, offered: 0, hired: 0 });
      const row = m.get(c);
      row.jobIds.add(r.job_id);
      if (r.ta) row.tas.add(r.ta);
      if (r.ts) row.tses.add(r.ts);
      row.viewed += (r.viewed || 0); row.contacted += r.contacted; row.positive_response += r.positive_response;
      row.screens += (r.screens || 0); row.actual_screens += r.actual_screens; row.ats += r.ats;
      row.offered += r.offered; row.hired += r.hired;
    }
    // 2026-06-04: when filtered by sourcer, PD's viewed CTE has ts='' (job-level
    // TA attribution only). Surface client-level viewed totals from ts_summary_by_client.
    // Includes view-only clients (sourcer viewed candidates but hasn't contacted/screened
    // anyone there yet) — those become new rows in the per-client rollup.
    if (filterTs) {
      const tsByClient = (data.ts_summary_by_client || []).filter(r => {
        if (r.ts !== filterTs) return false;
        const wk = `${r.iso_year}-W${String(r.iso_week).padStart(2, '0')}`;
        return weekSet.has(wk);
      });
      const viewedByClient = {};
      tsByClient.forEach(r => {
        const c = normalizeClientPD(r.client);
        viewedByClient[c] = (viewedByClient[c] || 0) + (r.viewed || 0);
      });
      // Overwrite viewed on existing rows + add new rows for view-only clients.
      for (const row of m.values()) {
        if (viewedByClient[row.client] != null) row.viewed = viewedByClient[row.client];
      }
      Object.entries(viewedByClient).forEach(([c, v]) => {
        if (!m.has(c) && v > 0) {
          m.set(c, {
            client: c, jobIds: new Set(), tas: new Set(), tses: new Set([filterTs]),
            viewed: v, contacted: 0, positive_response: 0,
            screens: 0, actual_screens: 0, ats: 0, offered: 0, hired: 0,
          });
        }
      });
    }
    return Array.from(m.values()).sort((a, b) => a.client.localeCompare(b.client));
  }, [filtered, filterTs, weekSet, data]);

  // ── Per-client job rollup ──
  const jobsByClient = useMemo(() => {
    const m = new Map();
    for (const r of filtered) {
      const c = normalizeClientPD(r.client);
      const key = `${c}|${r.job_id}`;
      if (!m.has(key)) m.set(key, {
        client: c, job_id: r.job_id, job_title: r.job_title, job_category: r.job_category,
        is_external_recruiter: r.is_external_recruiter, tas: new Set(), tses: new Set(),
        viewed: 0, contacted: 0, positive_response: 0, screens: 0, actual_screens: 0, ats: 0, offered: 0, hired: 0,
      });
      const row = m.get(key);
      if (r.ta) row.tas.add(r.ta);
      if (r.ts) row.tses.add(r.ts);
      row.viewed += (r.viewed || 0); row.contacted += r.contacted; row.positive_response += r.positive_response;
      row.screens += (r.screens || 0); row.actual_screens += r.actual_screens; row.ats += r.ats;
      row.offered += r.offered; row.hired += r.hired;
    }
    const by = {};
    for (const row of m.values()) {
      const ex = row.is_external_recruiter;
      row.ta_display = Array.from(row.tas).sort().join(', ');
      row.ts_display = Array.from(row.tses).sort().join(', ');
      row.pct_v_c = ex ? null : (row.viewed ? row.contacted / row.viewed : null);
      row.pct_c_pr = ex ? null : (row.contacted ? row.positive_response / row.contacted : null);
      row.pct_s_as = ex ? null : (row.screens ? row.actual_screens / row.screens : null);
      row.pct_as_ats = ex ? null : (row.actual_screens ? row.ats / row.actual_screens : null);
      row.pct_ats_off = ex ? null : (row.ats ? row.offered / row.ats : null);
      row.pct_c_hire = ex ? null : (row.contacted ? row.hired / row.contacted : null);
      (by[row.client] ||= []).push(row);
    }
    // Jobs sorted A-Z by title within each client (Blake's preference)
    for (const c in by) by[c].sort((a, b) => (a.job_title || '').localeCompare(b.job_title || ''));
    return by;
  }, [filtered]);

  // ── Per-client TA rollup ──
  const tasByClient = useMemo(() => {
    const m = new Map();
    for (const r of filtered) {
      if (!r.ta) continue;
      const c = normalizeClientPD(r.client);
      const key = `${c}|${r.ta}`;
      if (!m.has(key)) m.set(key, {
        client: c, ta: r.ta, jobIds: new Set(),
        viewed: 0, contacted: 0, positive_response: 0, screens: 0, actual_screens: 0, ats: 0, offered: 0, hired: 0,
      });
      const row = m.get(key);
      row.jobIds.add(r.job_id);
      row.viewed += (r.viewed || 0); row.contacted += r.contacted; row.positive_response += r.positive_response;
      row.screens += (r.screens || 0); row.actual_screens += r.actual_screens; row.ats += r.ats;
      row.offered += r.offered; row.hired += r.hired;
    }
    const by = {};
    for (const row of m.values()) {
      row.num_jobs = row.jobIds.size;
      row.pct_response = row.contacted ? row.positive_response / row.contacted : null;
      (by[row.client] ||= []).push(row);
    }
    for (const c in by) by[c].sort((a, b) => (a.ta || '').localeCompare(b.ta || ''));
    return by;
  }, [filtered]);

  // ── Per-client TS rollup ──
  const tsesByClient = useMemo(() => {
    const m = new Map();
    for (const r of filtered) {
      if (!r.ts) continue;
      const c = normalizeClientPD(r.client);
      const key = `${c}|${r.ts}`;
      if (!m.has(key)) m.set(key, {
        client: c, ts: r.ts, jobIds: new Set(),
        viewed: 0, contacted: 0, positive_response: 0, screens: 0, actual_screens: 0, ats: 0, offered: 0, hired: 0,
      });
      const row = m.get(key);
      row.jobIds.add(r.job_id);
      row.viewed += (r.viewed || 0); row.contacted += r.contacted; row.positive_response += r.positive_response;
      row.screens += (r.screens || 0); row.actual_screens += r.actual_screens; row.ats += r.ats;
      row.offered += r.offered; row.hired += r.hired;
    }
    const by = {};
    for (const row of m.values()) {
      row.num_jobs = row.jobIds.size;
      (by[row.client] ||= []).push(row);
    }
    for (const c in by) by[c].sort((a, b) => (a.ts || '').localeCompare(b.ts || ''));
    return by;
  }, [filtered]);

  // Filter dropdown options
  const uniqueClients    = useMemo(() => Array.from(new Set(pdRows.filter((r) => !PD_EXCLUDED_CLIENTS.has(r.client)).map((r) => normalizeClientPD(r.client)))).sort(), [pdRows]);
  const uniqueTas        = useMemo(() => Array.from(new Set(pdRows.map((r) => r.ta).filter(Boolean))).sort(), [pdRows]);
  const uniqueTses       = useMemo(() => Array.from(new Set(pdRows.map((r) => r.ts).filter(Boolean))).sort(), [pdRows]);
  const uniqueCategories = useMemo(() => Array.from(new Set(pdRows.map((r) => r.job_category).filter(Boolean))).sort(), [pdRows]);
  const uniqueSources    = useMemo(() => Array.from(new Set(pdRows.map((r) => r.candidate_source).filter(Boolean))).sort(), [pdRows]);

  const filteredHires = useMemo(() => {
    const s = searchText.trim().toLowerCase();
    return pdHires.filter((h) => {
      if (!h.date_hired) return false;
      if (h.date_hired < startStr || h.date_hired > endStr) return false;
      if (!showExternal && h.is_external_recruiter) return false;
      if (PD_EXCLUDED_CLIENTS.has(h.client)) return false;
      if (filterClient && normalizeClientPD(h.client) !== filterClient) return false;
      if (filterTa && h.ta !== filterTa) return false;
      if (filterTs && h.ts !== filterTs) return false;
      if (filterSource && h.candidate_source !== filterSource) return false;
      if (s) {
        const hay = `${h.client} ${h.job_title} ${h.ta} ${h.ts}`.toLowerCase();
        if (!hay.includes(s)) return false;
      }
      return true;
    });
  }, [pdHires, startStr, endStr, searchText, filterClient, filterTa, filterTs, filterSource, showExternal, showInternal]);

  const toggle = (set, setSet, client) => {
    const n = new Set(set);
    n.has(client) ? n.delete(client) : n.add(client);
    setSet(n);
  };
  const expandAll = (section) => {
    const all = new Set(byClient.map((c) => c.client));
    if (section === 'jobs') setExpandedJobs(all);
    if (section === 'tas') setExpandedTas(all);
    if (section === 'tses') setExpandedTses(all);
  };
  const collapseAll = (section) => {
    if (section === 'jobs') setExpandedJobs(new Set());
    if (section === 'tas') setExpandedTas(new Set());
    if (section === 'tses') setExpandedTses(new Set());
  };

  if (pdRows.length === 0) {
    return (
      <div className="bg-gray-800 rounded-lg p-8 text-center">
        <div className="text-lg text-white mb-3">Project Dashboard data not available yet</div>
        <div className="text-sm text-gray-400" style={{ maxWidth: 640, margin: '0 auto' }}>
          Keboola transformation output hasn't landed in <span className="font-mono bg-gray-700 px-1 rounded">snowflake_project_dashboard.csv</span> yet.
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Filter bar */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex flex-wrap gap-3 items-center">
          <select value={period} onChange={(e) => { setPeriod(e.target.value); setCustomStart(''); setCustomEnd(''); }}
            disabled={usingCustom}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            {PD_PERIODS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </select>
          <div className="flex items-center gap-1 text-xs text-gray-400">
            <span>or custom:</span>
            <input type="date" value={customStart} onChange={(e) => setCustomStart(e.target.value)}
              className="px-2 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" />
            <span>→</span>
            <input type="date" value={customEnd} onChange={(e) => setCustomEnd(e.target.value)}
              className="px-2 py-1 bg-gray-700 text-white rounded border border-gray-600 text-xs" />
            {usingCustom && (
              <button onClick={() => { setCustomStart(''); setCustomEnd(''); }}
                className="ml-1 px-2 py-1 text-xs text-gray-300 hover:text-white">clear</button>
            )}
          </div>
          <div className="flex items-center gap-2 ml-auto">
            <span className="text-xs text-gray-400">Attribution:</span>
            <div className="flex rounded border border-gray-600 overflow-hidden text-xs">
              <button
                onClick={() => setAttrMode('job')}
                className={attrMode === 'job' ? 'px-2 py-1 bg-blue-700 text-white' : 'px-2 py-1 bg-gray-700 text-gray-300 hover:text-white'}
                title="TA = job's assigned recruiter (original method, default)">
                Job-assigned TA
              </button>
              <button
                onClick={() => { if (eventAttrAvailable) setAttrMode('event'); }}
                disabled={!eventAttrAvailable}
                className={(attrMode === 'event' ? 'px-2 py-1 bg-blue-700 text-white' : 'px-2 py-1 bg-gray-700 text-gray-300 hover:text-white') + (eventAttrAvailable ? '' : ' opacity-40 cursor-not-allowed')}
                title={eventAttrAvailable ? 'TA = who logged the event (PBI parity, captures cross-team work + handovers)' : 'Event-based data not loaded yet'}>
                Event-based{eventAttrAvailable ? '' : ' (n/a)'}
              </button>
            </div>
          </div>
          <span className="text-xs text-gray-400">window: {startStr} → {endStr}</span>
        </div>
        <div className="flex flex-wrap gap-3 items-center mt-3">
          <div className="flex-1 relative" style={{ minWidth: 220 }}>
            <Search className="absolute left-2 top-2.5 w-4 h-4 text-gray-500" />
            <input type="text" placeholder="Search client / title / TA / TS / category..."
              value={searchText} onChange={(e) => setSearchText(e.target.value)}
              className="w-full pl-8 pr-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm placeholder-gray-500" />
          </div>
          <select value={filterClient} onChange={(e) => setFilterClient(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            <option value="">All Clients</option>
            {uniqueClients.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={filterTa} onChange={(e) => setFilterTa(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            <option value="">All TAs</option>
            {uniqueTas.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={filterTs} onChange={(e) => setFilterTs(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            <option value="">All TSes</option>
            {uniqueTses.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={filterCategory} onChange={(e) => setFilterCategory(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            <option value="">All Categories</option>
            {uniqueCategories.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={filterSource} onChange={(e) => setFilterSource(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm">
            <option value="">All Sources</option>
            {uniqueSources.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <label className="flex items-center gap-2 text-sm text-gray-300">
            <input type="checkbox" checked={showExternal} onChange={(e) => setShowExternal(e.target.checked)} />
            Include external recruiters
          </label>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-6 gap-4">
        {[
          ['Contacted', kpis.contacted],
          ['Positive Response', kpis.positive_response],
          ['Actual Screens', kpis.actual_screens],
          ['ATS', kpis.ats],
          ['Offered', kpis.offered],
          ['Hired', kpis.hired],
        ].map(([label, val]) => (
          <div key={label} className="bg-gray-800 rounded-lg p-4 border border-gray-700">
            <div className="text-gray-400 text-xs uppercase tracking-wide">{label}</div>
            <div className="text-3xl font-bold text-white mt-2">{val.toLocaleString()}</div>
          </div>
        ))}
      </div>

      {/* Section 1: Job Performance (grouped by client, collapsible) */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-white">Job Performance by Client ({byClient.length} clients, {Object.values(jobsByClient).reduce((n, a) => n + a.length, 0)} jobs)</h3>
          <div className="flex gap-2 text-xs">
            <button onClick={() => expandAll('jobs')} className="px-2 py-1 text-gray-300 hover:text-white">Expand all</button>
            <button onClick={() => collapseAll('jobs')} className="px-2 py-1 text-gray-300 hover:text-white">Collapse all</button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <CsvBtn fname="pd_job_performance_by_client" />
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700 text-xs">
                <th className="text-left px-2 py-1" style={{ width: 22 }}></th>
                <th className="text-left px-2 py-1">Client / Job</th>
                <th className="text-center px-1 py-1"># Jobs</th>
                <th className="text-center px-1 py-1">Viewed</th>
                <th className="text-center px-1 py-1">Contacted</th>
                <th className="text-center px-1 py-1">Pos Resp</th>
                <th className="text-center px-1 py-1">Screens</th>
                <th className="text-center px-1 py-1">Actual Screens</th>
                <th className="text-center px-1 py-1">ATS</th>
                <th className="text-center px-1 py-1">Offered</th>
                <th className="text-center px-1 py-1">Hired</th>
                <th className="text-center px-1 py-1">% V→C</th>
                <th className="text-center px-1 py-1">% C→PR</th>
                <th className="text-center px-1 py-1">% S→AS</th>
                <th className="text-center px-1 py-1">% AS→ATS</th>
                <th className="text-center px-1 py-1">% ATS→Off</th>
                <th className="text-center px-1 py-1">% C→H</th>
              </tr>
            </thead>
            <tbody>
              {byClient.map((c) => {
                const open = expandedJobs.has(c.client);
                const jobRows = jobsByClient[c.client] || [];
                const pctVC = c.viewed ? c.contacted / c.viewed : null;
                const pctCPR = c.contacted ? c.positive_response / c.contacted : null;
                const pctSAS = c.screens ? c.actual_screens / c.screens : null;
                const pctASATS = c.actual_screens ? c.ats / c.actual_screens : null;
                const pctATSOFF = c.ats ? c.offered / c.ats : null;
                const pctCH = c.contacted ? c.hired / c.contacted : null;
                return (
                  <React.Fragment key={c.client}>
                    <tr className="cursor-pointer hover:bg-gray-700 border-t border-gray-700"
                        style={{ backgroundColor: '#374151' }}
                        onClick={() => toggle(expandedJobs, setExpandedJobs, c.client)}>
                      <td className="px-2 py-1 text-gray-400">{open ? '▼' : '▶'}</td>
                      <td className="px-2 py-1 text-white font-semibold">{c.client}</td>
                      <td className="text-center px-1 py-1 text-gray-300">{jobRows.length}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.viewed}</td>
                      <td className="text-center px-1 py-1 text-gray-200 font-medium">{c.contacted}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.positive_response}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.screens}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.actual_screens}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.ats}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.offered}</td>
                      <td className="text-center px-1 py-1 text-gray-200 font-medium">{c.hired}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctVC)}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctCPR)}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctSAS)}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctASATS)}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctATSOFF)}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctCH)}</td>
                    </tr>
                    {open && jobRows.map((r, i) => (
                      <tr key={`${c.client}|${r.job_id}`}
                          className={r.actual_screens > 25 && r.hired === 0 ? 'bg-red-900 bg-opacity-20' : ''}
                          style={r.actual_screens > 25 && r.hired === 0 ? {} : { backgroundColor: i % 2 === 0 ? '#1F2937' : '#232B3A' }}>
                        <td></td>
                        <td className="px-4 py-0.5 text-gray-300 text-xs leading-tight">
                          ↳ {r.job_title}{r.is_external_recruiter && <span className="ml-1 text-yellow-400">EXT</span>}
                          <span className="text-gray-500"> · {r.ta_display || '—'}{r.ts_display ? ` · TS: ${r.ts_display}` : ''}</span>
                        </td>
                        <td></td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.viewed}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.contacted}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.positive_response}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.screens}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.actual_screens}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.ats}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.offered}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.hired}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_v_c)}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_c_pr)}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_s_as)}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_as_ats)}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_ats_off)}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_c_hire)}</td>
                      </tr>
                    ))}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="mt-2 text-xs text-gray-500">% columns exclude jobs flagged EXT. Click a client row to expand.</div>
      </div>

      {/* Section 2: TA Overview */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-white">TA Overview by Client ({byClient.length} clients)</h3>
          <div className="flex gap-2 text-xs">
            <button onClick={() => expandAll('tas')} className="px-2 py-1 text-gray-300 hover:text-white">Expand all</button>
            <button onClick={() => collapseAll('tas')} className="px-2 py-1 text-gray-300 hover:text-white">Collapse all</button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <CsvBtn fname="pd_ta_overview_by_client" />
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700 text-xs">
                <th className="text-left px-2 py-1" style={{ width: 22 }}></th>
                <th className="text-left px-2 py-1">Client / TA</th>
                <th className="text-center px-1 py-1"># TAs</th>
                <th className="text-center px-1 py-1">Contacted</th>
                <th className="text-center px-1 py-1">Pos Resp</th>
                <th className="text-center px-1 py-1">% Response</th>
                <th className="text-center px-1 py-1">Actual Screens</th>
                <th className="text-center px-1 py-1">ATS</th>
                <th className="text-center px-1 py-1">Offered</th>
                <th className="text-center px-1 py-1">Hires</th>
                <th className="text-center px-1 py-1"># Jobs</th>
              </tr>
            </thead>
            <tbody>
              {byClient.map((c) => {
                const open = expandedTas.has(c.client);
                const taRows = tasByClient[c.client] || [];
                const pctResp = c.contacted ? c.positive_response / c.contacted : null;
                return (
                  <React.Fragment key={c.client}>
                    <tr className="cursor-pointer hover:bg-gray-700 border-t border-gray-700"
                        style={{ backgroundColor: '#374151' }}
                        onClick={() => toggle(expandedTas, setExpandedTas, c.client)}>
                      <td className="px-2 py-1 text-gray-400">{open ? '▼' : '▶'}</td>
                      <td className="px-2 py-1 text-white font-semibold">{c.client}</td>
                      <td className="text-center px-1 py-1 text-gray-300">{taRows.length}</td>
                      <td className="text-center px-1 py-1 text-gray-200 font-medium">{c.contacted}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.positive_response}</td>
                      <td className="text-center px-1 py-1 text-gray-400">{pdPct(pctResp)}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.actual_screens}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.ats}</td>
                      <td className="text-center px-1 py-1 text-gray-200">{c.offered}</td>
                      <td className="text-center px-1 py-1 text-gray-200 font-medium">{c.hired}</td>
                      <td className="text-center px-1 py-1 text-gray-300">{c.jobIds.size}</td>
                    </tr>
                    {open && taRows.map((r, i) => (
                      <tr key={`${c.client}|${r.ta}`}
                          style={{ backgroundColor: i % 2 === 0 ? '#1F2937' : '#232B3A' }}>
                        <td></td>
                        <td className="px-4 py-0.5 text-gray-300 text-xs leading-tight">↳ {r.ta}</td>
                        <td></td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.contacted}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.positive_response}</td>
                        <td className="text-center px-1 py-0.5 text-gray-500 text-xs">{pdPct(r.pct_response)}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.actual_screens}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.ats}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.offered}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.hired}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.num_jobs}</td>
                      </tr>
                    ))}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Section 3: TS Overview */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-white">TS Overview by Client ({Object.keys(tsesByClient).length} clients with TS activity)</h3>
          <div className="flex gap-2 text-xs">
            <button onClick={() => expandAll('tses')} className="px-2 py-1 text-gray-300 hover:text-white">Expand all</button>
            <button onClick={() => collapseAll('tses')} className="px-2 py-1 text-gray-300 hover:text-white">Collapse all</button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <CsvBtn fname="pd_ts_overview_by_client" />
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700 text-xs">
                <th className="text-left px-2 py-1" style={{ width: 22 }}></th>
                <th className="text-left px-2 py-1">Client / TS</th>
                <th className="text-center px-1 py-1"># TSes</th>
                <th className="text-center px-1 py-1">Contacted</th>
                <th className="text-center px-1 py-1">Hired</th>
                <th className="text-center px-1 py-1"># Jobs</th>
              </tr>
            </thead>
            <tbody>
              {byClient.filter(c => tsesByClient[c.client]?.length).map((c) => {
                const open = expandedTses.has(c.client);
                const tsRows = tsesByClient[c.client] || [];
                return (
                  <React.Fragment key={c.client}>
                    <tr className="cursor-pointer hover:bg-gray-700 border-t border-gray-700"
                        style={{ backgroundColor: '#374151' }}
                        onClick={() => toggle(expandedTses, setExpandedTses, c.client)}>
                      <td className="px-2 py-1 text-gray-400">{open ? '▼' : '▶'}</td>
                      <td className="px-2 py-1 text-white font-semibold">{c.client}</td>
                      <td className="text-center px-1 py-1 text-gray-300">{tsRows.length}</td>
                      <td className="text-center px-1 py-1 text-gray-200 font-medium">{c.contacted}</td>
                      <td className="text-center px-1 py-1 text-gray-200 font-medium">{c.hired}</td>
                      <td className="text-center px-1 py-1 text-gray-300">{c.jobIds.size}</td>
                    </tr>
                    {open && tsRows.map((r, i) => (
                      <tr key={`${c.client}|${r.ts}`}
                          style={{ backgroundColor: i % 2 === 0 ? '#1F2937' : '#232B3A' }}>
                        <td></td>
                        <td className="px-4 py-0.5 text-gray-300 text-xs leading-tight">↳ {r.ts}</td>
                        <td></td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.contacted}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.hired}</td>
                        <td className="text-center px-1 py-0.5 text-gray-300 text-xs">{r.num_jobs}</td>
                      </tr>
                    ))}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Hires drill-down */}
      <div className="bg-gray-800 rounded-lg p-4">
        <button onClick={() => setHiresOpen(!hiresOpen)} className="text-lg font-semibold text-white w-full text-left">
          {hiresOpen ? '▼' : '▶'} Hires in period ({filteredHires.length})
        </button>
        {hiresOpen && (
          <div className="overflow-x-auto mt-3">
            <CsvBtn fname="pd_hires_in_period" />
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-300 border-b border-gray-700">
                  {['Hire Date','Client','Job Title','TA','TS','Source','Contacted','Actual Screen','Offer','Ext?'].map((l) => (
                    <th key={l} className="text-left px-3 py-2">{l}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredHires.map((h) => (
                  <tr key={h.candidate_id} className="border-t border-gray-700">
                    <td className="px-3 py-2 text-white">{h.date_hired}</td>
                    <td className="px-3 py-2 text-gray-300">{normalizeClientPD(h.client)}</td>
                    <td className="px-3 py-2 text-gray-300">{h.job_title}</td>
                    <td className="px-3 py-2 text-gray-300">{h.ta || '—'}</td>
                    <td className="px-3 py-2 text-gray-300">{h.ts || '—'}</td>
                    <td className="px-3 py-2 text-gray-300">{h.candidate_source || '—'}</td>
                    <td className="px-3 py-2 text-gray-400 text-xs">{h.date_contacted || '—'}</td>
                    <td className="px-3 py-2 text-gray-400 text-xs">{h.date_screen_actual || '—'}</td>
                    <td className="px-3 py-2 text-gray-400 text-xs">{h.date_offer || '—'}</td>
                    <td className="px-3 py-2 text-gray-400">{h.is_external_recruiter ? 'Yes' : ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <DQReasonsSection />
    </div>
  );
};


// ---------- Time to Hire Tab ----------
// Metric definitions (from Andy's homework doc + PBI DAX):
//   Time to Hire    = date_first_hired - date_first_hired_contacted  (> 0 only)
//   Time to Find    = date_first_hired_contacted - date_created      (> 0 only)
//   Time to Fill    = date_first_hired - date_created                (> 0 only)
// Filters match PBI: not test, not archived, not external recruiter,
//   client not in (Tribe.xyz, Kamila AI - TEST), job_title not blank.
// Uses FIRST HIRED CANDIDATE per job (Andy Q8).
const avgPositive = (arr, key) => {
  const vs = arr.map(r => r[key]).filter(v => typeof v === 'number' && v > 0);
  if (!vs.length) return null;
  return Math.round(vs.reduce((s, v) => s + v, 0) / vs.length);
};
// Avg using per-job include flag (e.g., has_t2f=1 means include, 0 means exclude).
// Matches PBI DAX: candidate-level filter determines inclusion, not the job value > 0.
const avgWithFlag = (arr, key, flagKey) => {
  const vs = arr.filter(r => Number(r[flagKey]) === 1).map(r => r[key]).filter(v => typeof v === 'number');
  if (!vs.length) return null;
  return Math.round(vs.reduce((s, v) => s + v, 0) / vs.length);
};
// Avg using per-job month-list inclusion (PBI DAX semantics).
// Row included if any entry in row[monthsKey] is in periodMonths Set.
// Falls back to row[fallbackFlagKey] === 1 when the row lacks a month-list
// (legacy data produced before the TTH transformation shipped).
const avgByMonths = (arr, valueKey, monthsKey, periodMonths, fallbackFlagKey) => {
  const vs = arr.filter(r => {
    const ms = r[monthsKey];
    if (Array.isArray(ms) && ms.length > 0) {
      return ms.some(m => periodMonths.has(m));
    }
    return fallbackFlagKey ? Number(r[fallbackFlagKey]) === 1 : false;
  }).map(r => r[valueKey]).filter(v => typeof v === 'number');
  if (!vs.length) return null;
  return Math.round(vs.reduce((s, v) => s + v, 0) / vs.length);
};
const countPositive = (arr, key) => arr.filter(r => r[key] > 0).length;

const TTHTab = ({ data }) => {
  const jobs = data.tth_jobs || [];
  const monthly = data.tth_monthly || [];

  // Filters — period (Year/Quarter/Month), client, TA, tech role
  const [year, setYear] = useState('2026');
  const [quarter, setQuarter] = useState('All');
  const [month, setMonth] = useState('All');
  const [client, setClient] = useState('All');
  const [ta, setTa] = useState('All');
  const [techRole, setTechRole] = useState('All');
  const [drillClient, setDrillClient] = useState(null);      // expanded row in table 1
  const [drillCategory, setDrillCategory] = useState(null);  // expanded row in table 2

  // Available filter options — derive from hire_months (ANY hire, matching PBI)
  const years = useMemo(() => {
    const ys = new Set();
    jobs.forEach(j => (j.hire_months || []).forEach(m => { if (m) ys.add(m.slice(0,4)); }));
    return ['All', ...Array.from(ys).sort().reverse()];
  }, [jobs]);
  const months = useMemo(() => {
    if (year === 'All') return ['All'];
    const ms = new Set();
    jobs.forEach(j => (j.hire_months || []).forEach(m => {
      if (m && m.slice(0,4) === year) ms.add(m);
    }));
    return ['All', ...Array.from(ms).sort()];
  }, [jobs, year]);
  const clients = useMemo(() => {
    const cs = new Set(jobs.map(j => j.client).filter(Boolean));
    return ['All', ...Array.from(cs).sort()];
  }, [jobs]);
  const tas = useMemo(() => {
    const ts = new Set(jobs.map(j => j.ta).filter(Boolean));
    return ['All', ...Array.from(ts).sort()];
  }, [jobs]);

  // A period-match predicate: does this job have any hire falling in the selected year/quarter/month?
  const jobMatchesPeriod = (j) => {
    if (year === 'All') return true;
    const hms = j.hire_months || [];
    if (!hms.length) return false;
    if (month !== 'All') return hms.includes(month);
    if (quarter !== 'All') {
      // quarter format: "Q1".."Q4", months Q1=01-03, Q2=04-06, Q3=07-09, Q4=10-12
      const qNum = Number(quarter.replace('Q',''));
      const qMonths = [qNum*3-2, qNum*3-1, qNum*3].map(n => String(n).padStart(2,'0'));
      return hms.some(m => m.slice(0,4) === year && qMonths.includes(m.slice(5,7)));
    }
    return hms.some(m => m.slice(0,4) === year);
  };

  // Year-specific flag keys — fallback for legacy data without t2f_months.
  const t2fFlag = year === 'All' ? 'has_t2f' : `has_t2f_${year}`;
  const t2fiFlag = year === 'All' ? 'has_t2fi' : `has_t2fi_${year}`;
  // Set of YYYY-MM strings matching the current year/quarter/month filter.
  // Used with avgByMonths for PBI-accurate month-level T2F/T2Fi filtering.
  const periodMonths = useMemo(() => {
    const set = new Set();
    if (year === 'All') {
      jobs.forEach(j => (j.hire_months || []).forEach(m => { if (m) set.add(m); }));
      return set;
    }
    if (month !== 'All') { set.add(month); return set; }
    if (quarter !== 'All') {
      const qn = Number(quarter.replace('Q', ''));
      for (let i = 0; i < 3; i++) {
        const mm = String(qn * 3 - 2 + i).padStart(2, '0');
        set.add(`${year}-${mm}`);
      }
      return set;
    }
    for (let i = 1; i <= 12; i++) set.add(`${year}-${String(i).padStart(2, '0')}`);
    return set;
  }, [jobs, year, quarter, month]);

  // Apply filters to get working set
  const filtered = useMemo(() => {
    return jobs.filter(j => {
      if (!jobMatchesPeriod(j)) return false;
      if (client !== 'All' && j.client !== client) return false;
      if (ta !== 'All' && j.ta !== ta) return false;
      if (techRole !== 'All' && j.tech_role !== techRole) return false;
      return true;
    });
  }, [jobs, year, quarter, month, client, ta, techRole]);

  // KPI totals
  const kpis = useMemo(() => ({
    jobs: filtered.length,
    tth: avgPositive(filtered, 'tth'),
    t2find: avgByMonths(filtered, 't2find', 't2f_months', periodMonths, t2fFlag),
    t2fill: avgByMonths(filtered, 't2fill', 't2fi_months', periodMonths, t2fiFlag),
  }), [filtered, t2fFlag, t2fiFlag]);

  // Per-client aggregation (alphabetical by client; drill-down job titles also alphabetical)
  const byClient = useMemo(() => {
    const groups = {};
    filtered.forEach(j => { (groups[j.client] = groups[j.client] || []).push(j); });
    return Object.entries(groups).map(([name, js]) => ({
      client: name,
      jobs: js.length,
      tth: avgPositive(js, 'tth'),
      t2find: avgByMonths(js, 't2find', 't2f_months', periodMonths, t2fFlag),
      t2fill: avgByMonths(js, 't2fill', 't2fi_months', periodMonths, t2fiFlag),
      items: [...js].sort((a, b) => (a.job_title || '').localeCompare(b.job_title || '', undefined, { sensitivity: 'base' })),
    })).sort((a, b) => (a.client || '').localeCompare(b.client || '', undefined, { sensitivity: 'base' }));
  }, [filtered, periodMonths, t2fFlag, t2fiFlag]);

  // Per-category aggregation (alphabetical by category; subcategory drill-down also alphabetical)
  const byCategory = useMemo(() => {
    const groups = {};
    filtered.forEach(j => { (groups[j.job_category] = groups[j.job_category] || []).push(j); });
    return Object.entries(groups).map(([cat, js]) => {
      const subGroups = {};
      js.forEach(j => { (subGroups[j.job_subcategory || '-'] = subGroups[j.job_subcategory || '-'] || []).push(j); });
      const subs = Object.entries(subGroups).map(([s, ss]) => ({
        subcategory: s, jobs: ss.length,
        tth: avgPositive(ss, 'tth'),
        t2find: avgByMonths(ss, 't2find', 't2f_months', periodMonths, t2fFlag),
        t2fill: avgByMonths(ss, 't2fill', 't2fi_months', periodMonths, t2fiFlag),
      })).sort((a, b) => (a.subcategory || '').localeCompare(b.subcategory || '', undefined, { sensitivity: 'base' }));
      return {
        category: cat, jobs: js.length,
        tth: avgPositive(js, 'tth'),
        t2find: avgByMonths(js, 't2find', 't2f_months', periodMonths, t2fFlag),
        t2fill: avgByMonths(js, 't2fill', 't2fi_months', periodMonths, t2fiFlag),
        subs,
      };
    }).sort((a, b) => (a.category || '').localeCompare(b.category || '', undefined, { sensitivity: 'base' }));
  }, [filtered, periodMonths, t2fFlag, t2fiFlag]);

  // Monthly trend — iterate each job's hire_months and bucket (each job counts once per month with a hire).
  // For trend, per-month flag is based on the month's year (e.g., 2025-04 uses has_t2f_2025).
  const trend = useMemo(() => {
    const groups = {};
    filtered.forEach(j => {
      (j.hire_months || []).forEach(mo => {
        if (!mo) return;
        if (year !== 'All' && mo.slice(0,4) !== year) return;
        (groups[mo] = groups[mo] || []).push(j);
      });
    });
    return Object.entries(groups).map(([mo, js]) => {
      const yr = mo.slice(0,4);
      const moSet = new Set([mo]);
      return {
        month: mo,
        monthLabel: new Date(mo + '-01').toLocaleString('en-US', { month: 'short', year: 'numeric' }),
        tth: avgPositive(js, 'tth'),
        t2find: avgByMonths(js, 't2find', 't2f_months', moSet, `has_t2f_${yr}`),
        t2fill: avgByMonths(js, 't2fill', 't2fi_months', moSet, `has_t2fi_${yr}`),
      };
    }).sort((a, b) => a.month.localeCompare(b.month));
  }, [filtered, year]);

  const Kpi = ({ label, value }) => (
    <div className="bg-blue-100 text-blue-900 rounded-lg px-4 py-3 min-w-[120px] flex-1">
      <div className="text-2xl font-bold">{value == null ? '-' : value}</div>
      <div className="text-xs text-blue-800 mt-1">{label}</div>
    </div>
  );

  const Select = ({ label, value, onChange, options }) => (
    <div>
      <div className="text-xs text-gray-400 mb-1">{label}</div>
      <select value={value} onChange={e => onChange(e.target.value)}
        className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1">
        {options.map(o => <option key={o} value={o}>{o}</option>)}
      </select>
    </div>
  );

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Time to Hire</h2>
          <div className="text-sm text-gray-400 mt-1 max-w-3xl leading-relaxed">
            <div><span className="font-semibold">Time to Hire:</span> days between contacted date and hired date of the first hire</div>
            <div><span className="font-semibold">Time to Find a Hire:</span> days between job creation date and contacted date of the first hire</div>
            <div><span className="font-semibold">Time to Fill:</span> days between job creation date and first hired date</div>
          </div>
        </div>
        <div className="text-xs text-gray-500">
          {jobs.length} jobs rostered &middot; source: Snowflake job table
        </div>
      </div>

      <div className="bg-gray-800 border border-gray-700 rounded-lg p-4 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        <Select label="Year" value={year} onChange={v => { setYear(v); setQuarter('All'); setMonth('All'); }} options={years} />
        <Select label="Quarter" value={quarter} onChange={v => { setQuarter(v); setMonth('All'); }} options={year === 'All' ? ['All'] : ['All', 'Q1', 'Q2', 'Q3', 'Q4']} />
        <Select label="Month" value={month} onChange={setMonth} options={months} />
        <Select label="Client" value={client} onChange={setClient} options={clients} />
        <Select label="TA" value={ta} onChange={setTa} options={tas} />
        <Select label="Tech Role" value={techRole} onChange={setTechRole} options={['All', 'Yes', 'No']} />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Kpi label="# Jobs" value={kpis.jobs} />
        <Kpi label="Time to Hire" value={kpis.tth} />
        <Kpi label="Time to Find a Hire" value={kpis.t2find} />
        <Kpi label="Time to Fill" value={kpis.t2fill} />
      </div>
      <div className="bg-gray-800 border border-gray-700 rounded-lg p-4">
        <div className="text-sm font-semibold text-white mb-2">Month Trends</div>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={trend} margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="monthLabel" stroke="#9CA3AF" fontSize={11} />
              <YAxis stroke="#9CA3AF" fontSize={11} />
              <Tooltip contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: '6px' }} labelStyle={{ color: '#F3F4F6' }} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Line type="monotone" dataKey="tth" name="Time to Hire" stroke="#22C55E" strokeWidth={2} dot={{ r: 3 }} />
              <Line type="monotone" dataKey="t2find" name="Time to Find a Hire" stroke="#F59E0B" strokeWidth={2} dot={{ r: 3 }} />
              <Line type="monotone" dataKey="t2fill" name="Time to Fill" stroke="#3B82F6" strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
      <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden">
        <div className="px-4 py-3 bg-gray-700 text-sm font-semibold text-white">
          First Hired per Job by Client / Job Title
        </div>
        <div className="overflow-x-auto">
          <CsvBtn fname="tth_first_hired_by_client_job" />
          <table className="min-w-full text-sm">
            <thead className="bg-gray-900 text-gray-300">
              <tr>
                <th className="text-left px-4 py-2 font-medium">Client</th>
                <th className="text-right px-3 py-2 font-medium"># Jobs</th>
                <th className="text-right px-3 py-2 font-medium">Time to Hire</th>
                <th className="text-right px-3 py-2 font-medium">Time to Find a Hire</th>
                <th className="text-right px-3 py-2 font-medium">Time to Fill</th>
              </tr>
            </thead>
            <tbody>
              {byClient.map(row => (
                <React.Fragment key={row.client}>
                  <tr onClick={() => setDrillClient(drillClient === row.client ? null : row.client)}
                    className="border-t border-gray-700 hover:bg-gray-700 cursor-pointer">
                    <td className="px-4 py-2 text-white">
                      <span className="inline-block w-3 text-gray-400">{drillClient === row.client ? '\u25BE' : '\u25B8'}</span> {row.client}
                    </td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.jobs}</td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.tth ?? '-'}</td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.t2find ?? '-'}</td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.t2fill ?? '-'}</td>
                  </tr>
                  {drillClient === row.client && row.items.map(j => (
                    <tr key={j.job_id} className="border-t border-gray-700/50 bg-gray-900/40 text-xs">
                      <td className="px-4 py-1.5 pl-10 text-gray-300 max-w-[420px] truncate" title={j.job_title}>
                        {j.job_title}
                      </td>
                      <td className="px-3 py-1.5 text-right text-gray-500">1</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{j.tth || '-'}</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{j.t2find || '-'}</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{j.t2fill || '-'}</td>
                    </tr>
                  ))}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden">
        <div className="px-4 py-3 bg-gray-700 text-sm font-semibold text-white">
          First Hired per Job by Job Category / Subcategory
        </div>
        <div className="overflow-x-auto">
          <CsvBtn fname="tth_first_hired_by_category" />
          <table className="min-w-full text-sm">
            <thead className="bg-gray-900 text-gray-300">
              <tr>
                <th className="text-left px-4 py-2 font-medium">Job Category</th>
                <th className="text-right px-3 py-2 font-medium"># Jobs</th>
                <th className="text-right px-3 py-2 font-medium">Time to Hire</th>
                <th className="text-right px-3 py-2 font-medium">Time to Find a Hire</th>
                <th className="text-right px-3 py-2 font-medium">Time to Fill</th>
              </tr>
            </thead>
            <tbody>
              {byCategory.map(row => (
                <React.Fragment key={row.category}>
                  <tr onClick={() => setDrillCategory(drillCategory === row.category ? null : row.category)}
                    className="border-t border-gray-700 hover:bg-gray-700 cursor-pointer">
                    <td className="px-4 py-2 text-white">
                      <span className="inline-block w-3 text-gray-400">{drillCategory === row.category ? '\u25BE' : '\u25B8'}</span> {row.category || '(no category)'}
                    </td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.jobs}</td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.tth ?? '-'}</td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.t2find ?? '-'}</td>
                    <td className="px-3 py-2 text-right text-gray-300">{row.t2fill ?? '-'}</td>
                  </tr>
                  {drillCategory === row.category && row.subs.map(sub => (
                    <tr key={sub.subcategory} className="border-t border-gray-700/50 bg-gray-900/40 text-xs">
                      <td className="px-4 py-1.5 pl-10 text-gray-300">{sub.subcategory}</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{sub.jobs}</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{sub.tth ?? '-'}</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{sub.t2find ?? '-'}</td>
                      <td className="px-3 py-1.5 text-right text-gray-400">{sub.t2fill ?? '-'}</td>
                    </tr>
                  ))}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      </div>

      <div className="text-xs text-gray-500 mt-2">
        Computed from <code className="text-gray-400">job.date_first_hired / date_first_hired_contacted / date_created</code> &mdash;
        excludes jobs where diff = 0 (per Andy&apos;s rule). ~95%+ match vs Power BI Time to Hire page (small drift from data refresh lag).
      </div>
    </div>
  );
};

// ============================================================
// KPI - TS Summary Tab
// Full port of Andy's legacy Power BI "KPI - TS Summary" page (Bucket A in legacy-pbix/PAGE_INVENTORY.md).
// Sourcer-only — visual 6's job_recruiter field in the homework reference is vestigial config; PBI exports
// (data (2).xlsx, Pipelines without Hires by Official Sourcers.xlsx) are all sourcer-keyed.
// All aggregates computed client-side from project_dashboard.rows + project_dashboard_hires + tth_jobs + jobs.
// DAX semantics preserved: ratios capped at 1.0, USERELATIONSHIP-equivalent date attribution honored,
// Tech Role flag from job_category (subset of calc-col logic — Jacopo owns the full list).
// ============================================================

// Tech Role classifier — matches DAX `# Tech Roles Hired` filter (job_category subset).
// The full `Tech Role` calc column also includes Product Manager, IT, QA, Engineering Management,
// and IT/Tech Project Manager subcategories — but PD rows don't carry job_subcategory, so we use
// the simpler hires-side filter. Per memory reference_metric_ownership.md, Jacopo owns this list.
const TECH_ROLE_CATEGORIES = new Set([
  'Data Analytics', 'DevOps', 'Software Engineering', 'Software', 'Design',
  'Product Manager', 'Information Technology', 'Quality Assurance (QA) ', 'Engineering Management',
]);

// Current TS roster — matches the hardcoded list in Keboola block b0.c6 (ts_summary_per_sourcer SQL).
// Used to filter the fallback path (project_dashboard.rows aggregation) so we don't show TAs
// when the dedicated ts_summary table hasn't been populated yet by Flow.
const TS_SUMMARY_ROSTER = new Set([
  'Andrea Akovic', 'Elena Petrovska', 'Gustavo Loureiro Castro', 'Jovana Drakula',
  'Marina Lazarevic', 'Mia Gjorgievska', 'Milica Veselinovic', 'Naledi Ngwenya',
  'Nare Avetisyan', 'Rodrigo Gomes', 'Valeriia Yurykova', 'Zelimir Stajcic',
]);
const isTechRole = (jobCategory) => TECH_ROLE_CATEGORIES.has(jobCategory) ? 'Yes' : 'No';

// ISO week → first day of that week (Monday). Used to bucket weekly rows into months.
const isoWeekToDate = (year, week) => {
  // ISO week 1 = the week containing Jan 4. Day 1 of week 1 is Monday.
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const jan4Day = (jan4.getUTCDay() + 6) % 7; // Mon=0
  const week1Start = new Date(jan4.getTime() - jan4Day * 86400000);
  return new Date(week1Start.getTime() + (week - 1) * 7 * 86400000);
};
const isoWeekToMonth = (year, week) => {
  const d = isoWeekToDate(year, week);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
};
const isoWeekToQuarter = (year, week) => {
  const d = isoWeekToDate(year, week);
  return `Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
};

const TSSummaryTab = ({ data }) => {
  // 2026-06-03 (Andrea/Sanja feedback): rebuilt to derive from project_dashboard.rows so
  // this tab matches Project Dashboard numbers for the same period. Attribution =
  // candidate_sourcer (same as PD). Archive filter is now a UI toggle (default INCLUDE
  // — matches PBI Weekly Progress canon + Gustavo's Sourcing Dashboard rule).
  // Year/Quarter/Month dropdowns map to calendar ranges via pdWeekSetFor: picking
  // "2026-04" produces {2026-W14..2026-W18}, exactly matching PD's 01/04 → 30/04.
  // Weekly grain means boundary weeks spill (W14 Mar 30-Apr 5, W18 Apr 27-May 3).
  // Same behaviour as PD. PD's Actual Screens / ATS are now event-gated (Keboola fix
  // 2026-06-03), so the two tabs reconcile to the unit.
  const tsSummary = data.ts_summary || []; // legacy, no longer used for aggregation
  const useTsSummary = false; // forced PD-rows path
  const rows = (data.project_dashboard && data.project_dashboard.rows) || [];
  const hires = data.project_dashboard_hires || [];
  const jobs = data.jobs || [];
  const tthJobs = data.tth_jobs || [];

  // Filters — default Year='2026' so KPI cards + per-sourcer table show YTD
  // numbers that match PBI (e.g. 6 hires, 4 tech roles, 108 jobs). Trend area
  // charts further down ALWAYS show full history regardless of this filter
  // so users still see Jan 2025-present context. Set Year='All' to apply
  // history to KPIs too.
  const [year, setYear] = useState('2026');
  const [quarter, setQuarter] = useState('All');
  const [month, setMonth] = useState('All');
  const [client, setClient] = useState('All');
  const [sourcer, setSourcer] = useState('All');
  const [techRoleFilter, setTechRoleFilter] = useState('All');
  const [includeExternal, setIncludeExternal] = useState(false);
  const [includeArchived, setIncludeArchived] = useState(true);
  // Per-sourcer drill-down expansion. Click a sourcer row in the Per-Sourcer
  // table to reveal that sourcer's breakdown by role. Stored as a Set so
  // multiple sourcers can be expanded simultaneously.
  const [expandedSourcers, setExpandedSourcers] = useState(() => new Set());
  const toggleSourcer = (ts) => setExpandedSourcers(prev => {
    const next = new Set(prev);
    if (next.has(ts)) next.delete(ts); else next.add(ts);
    return next;
  });

  // Set of archived job_ids — drives the "Include archived jobs" toggle.
  const archivedJobIds = useMemo(() => {
    const s = new Set();
    jobs.forEach(j => { if (String(j.is_job_archived || '').toLowerCase() === 'true') s.add(j.job_id); });
    return s;
  }, [jobs]);

  // Map job_id → tech_role flag (from tth_jobs which has the canonical calc column)
  const techRoleByJob = useMemo(() => {
    const m = {};
    tthJobs.forEach(j => { if (j.job_id) m[j.job_id] = j.tech_role; });
    return m;
  }, [tthJobs]);

  // Map job_id → job_category (used to fall back when tth_jobs lacks the job)
  const categoryByJob = useMemo(() => {
    const m = {};
    rows.forEach(r => { if (r.job_id && !m[r.job_id]) m[r.job_id] = r.job_category; });
    tthJobs.forEach(j => { if (j.job_id && !m[j.job_id]) m[j.job_id] = j.job_category; });
    return m;
  }, [rows, tthJobs]);

  const techRoleFor = (jobId) => techRoleByJob[jobId] || isTechRole(categoryByJob[jobId]);

  // Filter option sets
  const years = useMemo(() => {
    const ys = new Set();
    rows.forEach(r => { if (r.iso_year >= 2024 && r.iso_year <= 2030) ys.add(r.iso_year); });
    hires.forEach(h => { if (h.date_hired) ys.add(Number(h.date_hired.slice(0, 4))); });
    return ['All', ...Array.from(ys).filter(y => y >= 2024 && y <= 2030).sort().reverse().map(String)];
  }, [rows, hires]);

  const months = useMemo(() => {
    if (year === 'All') return ['All'];
    const ms = new Set();
    // Each ISO week spans 7 days — add months of Mon AND Sun so boundary weeks
    // (e.g. W14 Mar 30-Apr 5) appear under both March and April.
    rows.forEach(r => {
      if (r.iso_year !== Number(year)) return;
      const mon = isoWeekToDate(r.iso_year, r.iso_week);
      const sun = new Date(mon.getTime() + 6 * 86400000);
      const ym = (d) => `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      ms.add(ym(mon));
      ms.add(ym(sun));
    });
    hires.forEach(h => {
      if (!h.date_hired || h.date_hired.slice(0, 4) !== year) return;
      ms.add(h.date_hired.slice(0, 7));
    });
    return ['All', ...Array.from(ms).filter(m => m.startsWith(year)).sort()];
  }, [rows, hires, year]);

  const clients = useMemo(() => {
    const cs = new Set();
    rows.forEach(r => { if (r.client) cs.add(r.client); });
    return ['All', ...Array.from(cs).sort()];
  }, [rows]);

  const sourcers = useMemo(() => {
    const ss = new Set();
    rows.forEach(r => { if (r.ts) ss.add(r.ts); });
    return ['All', ...Array.from(ss).sort()];
  }, [rows]);

  // Year/Quarter/Month → calendar date range → ISO week set via pdWeekSetFor.
  // Picking "2026-04" produces {2026-W14..2026-W18}, exactly matching PD's
  // 01/04 → 30/04 picker. Boundary weeks spill (W14 starts Mar 30, W18 ends May 3).
  const [periodStart, periodEnd] = useMemo(() => {
    if (year === 'All') return [null, null];
    const y = Number(year);
    if (month !== 'All' && /^\d{4}-\d{2}$/.test(month)) {
      const m = Number(month.slice(5, 7));
      return [new Date(Date.UTC(y, m - 1, 1)), new Date(Date.UTC(y, m, 0))];
    }
    if (quarter !== 'All') {
      const q = Number(quarter.replace('Q', ''));
      return [new Date(Date.UTC(y, (q - 1) * 3, 1)), new Date(Date.UTC(y, q * 3, 0))];
    }
    return [new Date(Date.UTC(y, 0, 1)), new Date(Date.UTC(y, 11, 31))];
  }, [year, quarter, month]);

  const periodWeekSet = useMemo(() => {
    if (!periodStart || !periodEnd) return null;
    return pdWeekSetFor(periodStart, periodEnd);
  }, [periodStart, periodEnd]);

  const rowMatchesPeriod = (r) => {
    if (r.iso_year < 2024 || r.iso_year > 2030) return false;
    if (!periodWeekSet) return true;
    return periodWeekSet.has(`${r.iso_year}-W${String(r.iso_week).padStart(2, '0')}`);
  };
  const hireMatchesPeriod = (h) => {
    if (!h.date_hired) return false;
    if (!periodStart || !periodEnd) return true;
    const startStr = pdIsoDate(periodStart);
    const endStr = pdIsoDate(periodEnd);
    return h.date_hired >= startStr && h.date_hired <= endStr;
  };

  // Apply all filters to PD rows
  const filteredRows = useMemo(() => rows.filter(r => {
    if (!rowMatchesPeriod(r)) return false;
    if (client !== 'All' && r.client !== client) return false;
    if (sourcer !== 'All' && r.ts !== sourcer) return false;
    if (techRoleFilter !== 'All' && techRoleFor(r.job_id) !== techRoleFilter) return false;
    if (!includeExternal && r.is_external_recruiter) return false;
    if (!includeArchived && archivedJobIds.has(r.job_id)) return false;
    return true;
  }), [rows, periodWeekSet, client, sourcer, techRoleFilter, includeExternal, includeArchived, archivedJobIds, techRoleByJob, categoryByJob]);

  // Apply filters to hires. Hard-capped to TS_SUMMARY_ROSTER so the Hires KPI
  // counts the sourcing team only (~6 in 2026), not all-company (~1,320).
  const filteredHires = useMemo(() => hires.filter(h => {
    if (!h.ts || !TS_SUMMARY_ROSTER.has(h.ts)) return false;
    if (!hireMatchesPeriod(h)) return false;
    if (client !== 'All' && h.client !== client) return false;
    if (sourcer !== 'All' && h.ts !== sourcer) return false;
    if (techRoleFilter !== 'All' && techRoleFor(h.job_id) !== techRoleFilter) return false;
    if (!includeExternal && h.is_external_recruiter) return false;
    if (!includeArchived && archivedJobIds.has(h.job_id)) return false;
    return true;
  }), [hires, periodStart, periodEnd, client, sourcer, techRoleFilter, includeExternal, includeArchived, archivedJobIds, techRoleByJob, categoryByJob]);

  // KPI cards. Prefer ts_summary aggregate (PBI-aligned) when available.
  // Without it, fall back to project_dashboard_hires filtered to the TS roster
  // (lower fidelity — uses job-level attribution).
  const kpis = useMemo(() => {
    const periodMatch = (r) => {
      if (year !== 'All' && r.iso_year !== Number(year)) return false;
      // iso_week=0 is the year-aggregate sentinel from the baked YTD totals.
      // Treat it as matching ANY quarter/month (since we don't have weekly
      // breakdowns yet). Once Flow populates real weekly data, iso_week>0 rows
      // will use the proper quarter/month checks below.
      if (r.iso_week !== 0) {
        if (month !== 'All' && isoWeekToMonth(r.iso_year, r.iso_week) !== month) return false;
        if (quarter !== 'All' && isoWeekToQuarter(r.iso_year, r.iso_week) !== quarter) return false;
      }
      if (sourcer !== 'All' && r.ts !== sourcer) return false;
      return true;
    };
    let totalHires = 0, techHires = 0, totalJobs = 0;
    if (useTsSummary) {
      const jobAccumulator = {}; // {ts: jobs} but we need distinct (ts) Jobs sum
      tsSummary.forEach(r => {
        if (!periodMatch(r)) return;
        totalHires += r.hires || 0;
        techHires += r.hires_tech || 0;
        // Sum distinct jobs by adding per-week jobs (rough — overcounts if same job spans weeks)
        // For PBI-matching # Jobs, we need distinct count of job_ids across the period — we don't
        // have job_ids in ts_summary. As an approximation, take MAX across weeks per sourcer
        // (Jobs is roughly stable per sourcer). Sum across sourcers.
        const k = r.ts;
        if (!jobAccumulator[k] || r.jobs > jobAccumulator[k]) jobAccumulator[k] = r.jobs || 0;
      });
      totalJobs = Object.values(jobAccumulator).reduce((a, b) => a + b, 0);
    } else {
      totalHires = filteredHires.length;
      techHires = filteredHires.filter(h => techRoleFor(h.job_id) === 'Yes').length;
      // Distinct job_ids across filtered rows in scope (PD path)
      const jobSet = new Set();
      filteredRows.forEach(r => { if (TS_SUMMARY_ROSTER.has(r.ts) && r.job_id) jobSet.add(r.job_id); });
      totalJobs = jobSet.size;
    }
    // Candidate Time to Find a Hire — avg days from job created to date_contacted.
    // PBI DAX: AVERAGE(candidate_stage[Diff Concated - Job created]) for hired candidates.
    // Computed from project_dashboard_hires regardless of ts_summary (need per-hire dates).
    const jobCreatedById = {};
    jobs.forEach(j => { if (j.job_id) jobCreatedById[j.job_id] = j.date_created; });
    tthJobs.forEach(j => { if (j.job_id && !jobCreatedById[j.job_id]) jobCreatedById[j.job_id] = j.date_created; });
    const diffs = [];
    filteredHires.forEach(h => {
      const created = jobCreatedById[h.job_id];
      if (!created || !h.date_contacted) return;
      const d1 = new Date(created), d2 = new Date(h.date_contacted);
      const diff = Math.round((d2 - d1) / 86400000);
      if (diff > 0) diffs.push(diff);
    });
    const avgDiff = diffs.length ? Math.round(diffs.reduce((a, b) => a + b, 0) / diffs.length) : null;
    return { totalHires, techHires, candidateTimeToFind: avgDiff, totalJobs };
  }, [useTsSummary, tsSummary, filteredHires, filteredRows, year, quarter, month, sourcer, jobs, tthJobs, techRoleByJob, categoryByJob]);

  // Per-Sourcer ratio table — matches PBI `data (2).xlsx`.
  // Primary path: ts_summary aggregate (PBI-aligned attribution).
  // Fallback path: project_dashboard.rows + project_dashboard_hires (job-level TS — wrong
  // attribution; only used until Flow refreshes after the new SQL block deploy).
  const perSourcer = useMemo(() => {
    const agg = {};
    const ensure = (ts) => {
      if (!agg[ts]) agg[ts] = {
        sourcer: ts, viewed: 0, contacted: 0, positive_response: 0, screens: 0,
        actual_screens: 0, ats: 0, offered: 0, hires: 0, jobs: 0,
      };
      return agg[ts];
    };

    if (useTsSummary) {
      // ts_summary path: filter rows by year/quarter/month, sum per-TS
      tsSummary.forEach(r => {
        if (year !== 'All' && r.iso_year !== Number(year)) return;
        if (month !== 'All' && isoWeekToMonth(r.iso_year, r.iso_week) !== month) return;
        if (quarter !== 'All' && isoWeekToQuarter(r.iso_year, r.iso_week) !== quarter) return;
        if (sourcer !== 'All' && r.ts !== sourcer) return;
        const a = ensure(r.ts);
        a.contacted += r.contacted;
        a.positive_response += r.positive_response;
        a.screens += r.screens;
        a.actual_screens += r.actual_screens;
        a.ats += r.ats;
        a.offered += r.offers;
        a.hires += r.hires;
        a.jobs += r.jobs;
      });
    } else {
      // Fallback (pre-Flow-refresh): job-level TS attribution from PD rows.
      // Hard-cap to the Current_TS roster so we don't accidentally include TAs
      // who happen to be listed as job_sourcer on some jobs.
      filteredRows.forEach(r => {
        if (!r.ts || !TS_SUMMARY_ROSTER.has(r.ts)) return;
        const a = ensure(r.ts);
        a.viewed += r.viewed || 0;
        a.contacted += r.contacted || 0;
        a.positive_response += r.positive_response || 0;
        a.screens += r.screens || 0;
        a.actual_screens += r.actual_screens || 0;
        a.ats += r.ats || 0;
        a.offered += r.offered || 0;
      });
      filteredHires.forEach(h => {
        if (!h.ts || !TS_SUMMARY_ROSTER.has(h.ts)) return;
        ensure(h.ts).hires += 1;
      });
    }

    return Object.values(agg)
      .map(a => ({
        ...a,
        pctContPR: a.contacted ? Math.min(1, a.positive_response / a.contacted) : null,
        pctScrActual: a.screens ? Math.min(1, a.actual_screens / a.screens) : null,
        pctActualATS: a.actual_screens ? Math.min(1, a.ats / a.actual_screens) : null,
      }))
      .filter(x => x.contacted + x.actual_screens + x.hires > 0)
      .sort((a, b) => a.sourcer.localeCompare(b.sourcer));
  }, [tsSummary, useTsSummary, filteredRows, filteredHires, year, quarter, month, sourcer]);

  // Per-(sourcer, job) breakdown for the drill-down rows in the Per-Sourcer
  // table. Sums weekly PD rows by (ts, job_id), attaches hire counts and the
  // archived flag, and groups them under each sourcer sorted by actual_screens
  // desc. Same attribution + filters as `perSourcer` above. Answers Gustavo's
  // 2026-06-04 question — "on which roles is Andrea's pipeline actually
  // happening, and which of them got archived?".
  const perSourcerJobs = useMemo(() => {
    const byTs = {}; // ts -> { job_id -> agg }
    filteredRows.forEach(r => {
      if (!r.ts || !TS_SUMMARY_ROSTER.has(r.ts)) return;
      if (!r.job_id) return;
      if (!byTs[r.ts]) byTs[r.ts] = {};
      const bucket = byTs[r.ts];
      if (!bucket[r.job_id]) bucket[r.job_id] = {
        job_id: r.job_id,
        job_title: r.job_title || '(untitled)',
        client: r.client || '',
        archived: archivedJobIds.has(r.job_id),
        viewed: 0, contacted: 0, positive_response: 0, screens: 0,
        actual_screens: 0, ats: 0, offered: 0, hires: 0,
      };
      const a = bucket[r.job_id];
      a.viewed += r.viewed || 0;
      a.contacted += r.contacted || 0;
      a.positive_response += r.positive_response || 0;
      a.screens += r.screens || 0;
      a.actual_screens += r.actual_screens || 0;
      a.ats += r.ats || 0;
      a.offered += r.offered || 0;
    });
    filteredHires.forEach(h => {
      if (!h.ts || !TS_SUMMARY_ROSTER.has(h.ts)) return;
      if (!h.job_id) return;
      if (!byTs[h.ts]) byTs[h.ts] = {};
      const bucket = byTs[h.ts];
      if (!bucket[h.job_id]) bucket[h.job_id] = {
        job_id: h.job_id,
        job_title: h.job_title || '(untitled)',
        client: h.client || '',
        archived: archivedJobIds.has(h.job_id),
        viewed: 0, contacted: 0, positive_response: 0, screens: 0,
        actual_screens: 0, ats: 0, offered: 0, hires: 0,
      };
      bucket[h.job_id].hires += 1;
    });
    const out = {};
    Object.keys(byTs).forEach(ts => {
      out[ts] = Object.values(byTs[ts])
        .filter(j => j.contacted + j.actual_screens + j.hires > 0)
        .sort((a, b) => b.actual_screens - a.actual_screens || b.contacted - a.contacted);
    });
    return out;
  }, [filteredRows, filteredHires, archivedJobIds]);

  // Funnel — 8 PBI stages in order:
  // Viewed -> Contacted -> Reacted -> Positive Response -> Actual Screens -> Move to ATS -> Offers -> Hired
  // Each row shows count + % conversion FROM the previous stage.
  // Sourced from ts_summary (PBI-aligned). For Viewed we fall back to summing PD rows.viewed
  // when ts_summary doesn't carry it yet (older Flow runs); same for Reacted.
  const funnel = useMemo(() => {
    const periodMatch = (r) => {
      if (year !== 'All' && r.iso_year !== Number(year)) return false;
      // iso_week=0 is the year-aggregate sentinel from the baked YTD totals.
      // Treat it as matching ANY quarter/month (since we don't have weekly
      // breakdowns yet). Once Flow populates real weekly data, iso_week>0 rows
      // will use the proper quarter/month checks below.
      if (r.iso_week !== 0) {
        if (month !== 'All' && isoWeekToMonth(r.iso_year, r.iso_week) !== month) return false;
        if (quarter !== 'All' && isoWeekToQuarter(r.iso_year, r.iso_week) !== quarter) return false;
      }
      if (sourcer !== 'All' && r.ts !== sourcer) return false;
      return true;
    };
    const t = { viewed: 0, contacted: 0, reacted: 0, positive_response: 0, actual_screens: 0, ats: 0, offers: 0, hires: 0 };
    if (useTsSummary) {
      tsSummary.forEach(r => {
        if (!periodMatch(r)) return;
        t.viewed += r.viewed || 0;
        t.contacted += r.contacted || 0;
        t.reacted += r.reacted || 0;
        t.positive_response += r.positive_response || 0;
        t.actual_screens += r.actual_screens || 0;
        t.ats += r.ats || 0;
        t.offers += r.offers || 0;
        t.hires += r.hires || 0;
      });
      // If the new viewed/reacted columns aren't populated yet (older Flow run),
      // augment with PD rows for Viewed at least.
      if (t.viewed === 0) {
        filteredRows.forEach(r => { if (TS_SUMMARY_ROSTER.has(r.ts)) t.viewed += r.viewed || 0; });
      }
    } else {
      // PD-rows path. 2026-06-04: viewed + reacted now from ts_summary_by_client
      // when a client filter is active (it's a small per-(sourcer, client, week)
      // aggregate so the data ALREADY scoped by client). Falls back to ts_summary
      // (sourcer total) when no client filter — same numbers either way for All.
      filteredRows.forEach(r => {
        if (!TS_SUMMARY_ROSTER.has(r.ts)) return;
        t.contacted += r.contacted || 0;
        t.positive_response += r.positive_response || 0;
        t.actual_screens += r.actual_screens || 0;
        t.ats += r.ats || 0;
        t.offers += r.offered || 0;
      });
      const tsByClient = data.ts_summary_by_client || [];
      const useByClient = client !== 'All' && tsByClient.length > 0;
      const viewedSource = useByClient ? tsByClient : tsSummary;
      viewedSource.forEach(r => {
        if (r.iso_year < 2024 || r.iso_year > 2030) return;
        if (periodWeekSet && !periodWeekSet.has(`${r.iso_year}-W${String(r.iso_week).padStart(2, '0')}`)) return;
        if (sourcer !== 'All' && r.ts !== sourcer) return;
        if (useByClient && r.client !== client) return;
        if (!TS_SUMMARY_ROSTER.has(r.ts)) return;
        t.viewed += r.viewed || 0;
        t.reacted += r.reacted || 0;
      });
      t.hires = filteredHires.filter(h => TS_SUMMARY_ROSTER.has(h.ts)).length;
    }
    const stages = [
      { stage: 'Viewed',            count: t.viewed },
      { stage: 'Contacted',         count: t.contacted },
      { stage: 'Reacted',           count: t.reacted },
      { stage: 'Positive Response', count: t.positive_response },
      { stage: 'Actual Screens',    count: t.actual_screens },
      { stage: 'Move to ATS',       count: t.ats },
      { stage: 'Offers',            count: t.offers },
      { stage: 'Hired',             count: t.hires },
    ];
    return stages.map((s, i) => {
      const prev = i > 0 ? stages[i - 1].count : null;
      const conv = (prev != null && prev > 0) ? s.count / prev : null;
      return { ...s, conv };
    });
  }, [useTsSummary, tsSummary, filteredRows, filteredHires, year, quarter, month, sourcer, periodWeekSet]);

  // Monthly trends — three ratios over time. Ignores year/quarter/month filters
  // (always shows full 2024-present history) so the user sees historical context
  // even when KPI cards are scoped to current year. Still respects sourcer +
  // client + tech-role filters.
  // NOTE: when iso_week=0 (year-aggregate sentinel from baked YTD totals),
  // skip — the trend chart needs real weekly data which only the Flow can produce.
  const trend = useMemo(() => {
    const groups = {};
    const src = useTsSummary
      ? tsSummary.filter(r => {
          if (r.iso_year < 2024) return false;
          if (r.iso_week === 0) return false; // year-aggregate sentinel
          if (sourcer !== 'All' && r.ts !== sourcer) return false;
          return true;
        })
      : filteredRows;
    src.forEach(r => {
      const key = isoWeekToMonth(r.iso_year, r.iso_week);
      if (!groups[key]) groups[key] = { contacted: 0, positive_response: 0, screens: 0, actual_screens: 0, ats: 0 };
      groups[key].contacted += r.contacted || 0;
      groups[key].positive_response += r.positive_response || 0;
      groups[key].screens += r.screens || 0;
      groups[key].actual_screens += r.actual_screens || 0;
      groups[key].ats += r.ats || 0;
    });
    return Object.entries(groups).map(([m, t]) => {
      const monthLabel = new Date(m + '-01').toLocaleString('en-US', { month: 'short', year: '2-digit' });
      return {
        month: m,
        monthLabel,
        pctContPR: t.contacted ? Math.min(100, Math.round(100 * t.positive_response / t.contacted)) : null,
        pctScrActual: t.screens ? Math.min(100, Math.round(100 * t.actual_screens / t.screens)) : null,
        pctActualATS: t.actual_screens ? Math.min(100, Math.round(100 * t.ats / t.actual_screens)) : null,
      };
    }).sort((a, b) => a.month.localeCompare(b.month));
  }, [tsSummary, useTsSummary, filteredRows, year, sourcer]);

  // Pipelines without hires — CURRENT snapshot of active jobs not yet hired.
  // Intentionally ignores Year/Quarter/Month filters because pipelines is a
  // point-in-time view, not a time-window aggregation. A job created in 2025
  // but still open today is "currently a pipeline" regardless of whether the
  // user filters to Q1 2025. Matches PBI's behavior on this section.
  // Filters that DO apply: sourcer, client, tech_role, includeExternal.
  const pipelinesNoHire = useMemo(() => {
    const baked = data.ts_summary_pipelines;
    if (Array.isArray(baked) && baked.length > 0) {
      return baked
        .filter(j => sourcer === 'All' || j.sourcer === sourcer)
        .filter(j => client === 'All' || j.client === client)
        .filter(j => techRoleFilter === 'All' || j.tech_role === techRoleFilter)
        .filter(j => includeExternal || j.is_external !== 'true')
        .map(j => ({
          job_id: j.job_id,
          job_title: j.job_title,
          client: j.client,
          sourcer: j.sourcer,
          days: j.days,
        }));
    }
    // Fallback (data.jobs) — narrower set
    const hiredJobIds = new Set(tthJobs.map(j => j.job_id));
    const today = new Date();
    const dayDiff = (dStr) => dStr ? Math.floor((today - new Date(dStr)) / 86400000) : null;
    return jobs
      .filter(j => String(j.is_job_archived || '').toLowerCase() !== 'true')
      .filter(j => !hiredJobIds.has(j.job_id))
      .filter(j => TS_SUMMARY_ROSTER.has(j.job_sourcer))
      .filter(j => sourcer === 'All' || j.job_sourcer === sourcer)
      .filter(j => client === 'All' || j.client_name === client)
      .filter(j => includeExternal || String(j.is_external_recruiter || '').toLowerCase() !== 'true')
      .filter(j => techRoleFilter === 'All' || techRoleFor(j.job_id) === techRoleFilter)
      .map(j => ({
        job_id: j.job_id,
        job_title: j.job_title,
        client: j.client_name,
        sourcer: j.job_sourcer || '(not assigned)',
        days: dayDiff(j.date_created),
      }))
      .filter(j => j.days != null);
  }, [data, jobs, tthJobs, sourcer, client, includeExternal, techRoleFilter, techRoleByJob, categoryByJob]);

  // Per-sourcer age buckets
  const noHireBySourcer = useMemo(() => {
    const agg = {};
    pipelinesNoHire.forEach(j => {
      const s = j.sourcer || '(not assigned)';
      if (!agg[s]) agg[s] = { sourcer: s, jobs: 0, '0-30': 0, '30-60': 0, '60-90': 0, '>90': 0 };
      agg[s].jobs += 1;
      const d = j.days;
      if (d <= 30) agg[s]['0-30'] += 1;
      else if (d <= 60) agg[s]['30-60'] += 1;
      else if (d <= 90) agg[s]['60-90'] += 1;
      else agg[s]['>90'] += 1;
    });
    return Object.values(agg).sort((a, b) => a.sourcer.localeCompare(b.sourcer));
  }, [pipelinesNoHire]);

  // Helpers for percent formatting.
  // Using Tailwind v2-compatible bg + bg-opacity classes (memory:
  // reference_dashboard_tailwind_v2.md — `bg-red-900/30` v3+ syntax silently
  // renders as no-op in v2). Per-metric thresholds match PBI heatmap intent.
  const fmtPct = (v) => v == null ? '—' : `${Math.round(v * 100)}%`;
  // metric-specific thresholds: pctContPR (0-30%), pctScrActual (40-80%), pctActualATS (40-70%)
  const cellStyle = (v, metric) => {
    if (v == null) return {};
    const thresholds = {
      pctContPR:    [0.10, 0.20],   // <10% red, 10-20% mid, >20% green
      pctScrActual: [0.50, 0.75],
      pctActualATS: [0.50, 0.65],
    }[metric] || [0.30, 0.60];
    if (v < thresholds[0]) return { backgroundColor: '#dc2626', color: '#fee2e2' };  // red-600
    if (v < thresholds[1]) return { backgroundColor: '#fbbf24', color: '#451a03' };  // amber-400
    return { backgroundColor: '#16a34a', color: '#dcfce7' };                          // green-600
  };

  const Kpi = ({ label, value, sub }) => (
    <div className="bg-gradient-to-br from-blue-900 to-blue-800 text-white rounded-lg px-4 py-3 min-w-[120px] flex-1 border border-blue-700">
      <div className="text-3xl font-bold">{value == null ? '-' : value}</div>
      <div className="text-xs text-blue-200 mt-1">{label}</div>
      {sub && <div className="text-xs text-blue-300/70 mt-1">{sub}</div>}
    </div>
  );

  const Select = ({ label, value, onChange, options }) => (
    <div>
      <div className="text-xs text-gray-400 mb-1">{label}</div>
      <select value={value} onChange={e => onChange(e.target.value)}
        className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded px-2 py-1">
        {options.map(o => <option key={o} value={o}>{o}</option>)}
      </select>
    </div>
  );

  // Table component reused for the trend area-charts
  const TrendChart = ({ title, dataKey, color }) => (
    <div className="bg-gray-800 border border-gray-700 rounded-lg p-3">
      <div className="text-xs font-semibold text-gray-200 mb-2">{title}</div>
      <ResponsiveContainer width="100%" height={160}>
        <AreaChart data={trend} margin={{ top: 5, right: 10, bottom: 0, left: -25 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="monthLabel" stroke="#9CA3AF" fontSize={10} />
          <YAxis stroke="#9CA3AF" fontSize={10} domain={[0, 100]} tickFormatter={v => `${v}%`} />
          <Tooltip contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: '6px', fontSize: 12 }}
            labelStyle={{ color: '#F3F4F6' }} formatter={v => `${v}%`} />
          <Area type="monotone" dataKey={dataKey} stroke={color} fill={color} fillOpacity={0.4} strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );

  return (
    <div className="space-y-5">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">KPI &mdash; TS Summary</h2>
          <div className="text-sm text-gray-400 mt-1 max-w-3xl leading-relaxed">
            Sourcer-level performance: contacted &rarr; positive response &rarr; screens &rarr; actual screens &rarr; ATS &rarr; offers &rarr; hires.
            Port of Andy's legacy Power BI page (sourcer-only; the PBI page does not show TAs).
          </div>
        </div>
        <div className="text-xs text-right">
          <span className="text-green-400">PD-rows aggregation &middot; matches Project Dashboard</span><br />
          <span className="text-gray-500">
            {filteredRows.length.toLocaleString()} PD rows &middot; {filteredHires.length.toLocaleString()} hires
          </span>
        </div>
      </div>

      <div className="bg-gray-800 border border-gray-700 rounded-lg p-4 grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
        <Select label="Year" value={year} onChange={v => { setYear(v); setQuarter('All'); setMonth('All'); }} options={years} />
        <Select label="Quarter" value={quarter} onChange={v => { setQuarter(v); setMonth('All'); }} options={year === 'All' ? ['All'] : ['All', 'Q1', 'Q2', 'Q3', 'Q4']} />
        <Select label="Month" value={month} onChange={setMonth} options={months} />
        <Select label="Client" value={client} onChange={setClient} options={clients} />
        <Select label="Sourcer" value={sourcer} onChange={setSourcer} options={sourcers} />
        <Select label="Tech Role" value={techRoleFilter} onChange={setTechRoleFilter} options={['All', 'Yes', 'No']} />
        <div className="flex items-end">
          <label className="flex items-center gap-2 text-xs text-gray-300 pb-1.5 cursor-pointer">
            <input type="checkbox" checked={includeExternal} onChange={e => setIncludeExternal(e.target.checked)}
              className="rounded" />
            Include external recruiter
          </label>
        </div>
        <div className="flex items-end">
          <label className="flex items-center gap-2 text-xs text-gray-300 pb-1.5 cursor-pointer">
            <input type="checkbox" checked={includeArchived} onChange={e => setIncludeArchived(e.target.checked)}
              className="rounded" />
            Include archived jobs
          </label>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <Kpi label="Candidates Hired" value={kpis.totalHires.toLocaleString()} />
        <Kpi label="Tech Roles Hired" value={kpis.techHires.toLocaleString()} sub="Engineering, Design, Data, DevOps" />
        <Kpi label="Find a Hire (Days)" value={kpis.candidateTimeToFind == null ? '—' : kpis.candidateTimeToFind}
          sub="Avg days job-created &rarr; contacted" />
        <Kpi label="Jobs" value={kpis.totalJobs.toLocaleString()} sub="Distinct jobs in scope" />
        <Kpi label="Pipelines without Hires" value={pipelinesNoHire.length.toLocaleString()} sub="Active jobs, never hired" />
      </div>

      {/* Trend area charts removed — required per-week data which the baked YTD aggregates
          don't have. They'll re-appear once Flow refreshes with proper weekly ts_summary. */}

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden xl:col-span-1">
          <div className="px-4 py-3 bg-gray-700 text-sm font-semibold text-white">Funnel</div>
          <div className="p-4 space-y-1">
            {(() => {
              // Use MAX across all stages as denominator (not funnel[0]) so the funnel
              // tapers correctly even if Viewed is 0 (old Flow data).
              const maxCount = Math.max(...funnel.map(f => f.count), 1);
              return funnel.map((f, i) => {
                const widthPct = Math.max(8, (100 * f.count / maxCount));
                const convPct = f.conv != null ? `${(f.conv * 100).toFixed(1)}%` : '100%';
                const label = `${f.count.toLocaleString()} (${convPct})`;
                return (
                  <div key={f.stage} className="text-xs">
                    <div className="text-gray-200 font-medium mb-1">{f.stage}</div>
                    <div style={{ width: '100%', textAlign: 'center', lineHeight: 0 }}>
                      <div style={{
                        display: 'inline-block',
                        backgroundColor: '#3b82f6',
                        width: `${widthPct}%`,
                        minWidth: '120px',
                        height: '32px',
                        borderRadius: '3px',
                        color: 'white',
                        fontSize: '13px',
                        fontWeight: 600,
                        lineHeight: '32px',
                        textAlign: 'center',
                        verticalAlign: 'top',
                        whiteSpace: 'nowrap',
                        padding: '0 8px',
                        boxSizing: 'border-box',
                      }}>
                        {label}
                      </div>
                    </div>
                  </div>
                );
              });
            })()}
          </div>
          <div className="px-4 py-2 text-xs text-gray-500 border-t border-gray-700 leading-relaxed">
            Conversion % is from the previous stage. Recruiter Screens omitted (PBI funnel chains Pos Response &rarr; Actual Screens directly).
            {funnel[0] && funnel[0].count === 0 && sourcer !== 'All' && <span className="text-yellow-400"> Viewed = 0 — LinkedIn views are currently attributed to TA only; sourcer-level views fix in flight.</span>}
          </div>
        </div>

        {/* Per-Sourcer table — main workhorse, matches PBI data (2).xlsx */}
        <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden xl:col-span-2">
          <div className="px-4 py-3 bg-gray-700 text-sm font-semibold text-white flex justify-between">
            <span>Per Sourcer</span>
            <span className="text-xs text-gray-400 font-normal">{perSourcer.length} sourcers</span>
          </div>
          <div className="overflow-x-auto">
            <CsvBtn fname="ts_summary_per_sourcer" />
            <table className="min-w-full text-xs">
              <thead className="bg-gray-900 text-gray-300">
                <tr>
                  <th className="text-left px-2 py-2 font-medium sticky left-0 bg-gray-900 w-6"></th>
                  <th className="text-left px-3 py-2 font-medium sticky left-6 bg-gray-900">Sourcer</th>
                  <th className="text-right px-2 py-2 font-medium">% Cont&rarr;PR</th>
                  <th className="text-right px-2 py-2 font-medium">% Scr&rarr;Actual</th>
                  <th className="text-right px-2 py-2 font-medium">% Actual&rarr;ATS</th>
                  <th className="text-right px-2 py-2 font-medium">Cont</th>
                  <th className="text-right px-2 py-2 font-medium">PR</th>
                  <th className="text-right px-2 py-2 font-medium">Scr</th>
                  <th className="text-right px-2 py-2 font-medium">Actual</th>
                  <th className="text-right px-2 py-2 font-medium">ATS</th>
                  <th className="text-right px-2 py-2 font-medium">Off</th>
                  <th className="text-right px-2 py-2 font-medium">Hires</th>
                  <th className="text-right px-2 py-2 font-medium">Jobs</th>
                </tr>
              </thead>
              <tbody>
                {perSourcer.map(r => {
                  const isOpen = expandedSourcers.has(r.sourcer);
                  const drilldown = perSourcerJobs[r.sourcer] || [];
                  return (
                    <React.Fragment key={r.sourcer}>
                      <tr
                        className="border-t border-gray-700 hover:bg-gray-700 cursor-pointer"
                        onClick={() => toggleSourcer(r.sourcer)}
                        title="Click to see this sourcer's breakdown by role"
                      >
                        <td className="px-2 py-1.5 text-gray-400 sticky left-0 bg-gray-800 text-center select-none">{isOpen ? '▼' : '▶'}</td>
                        <td className="px-3 py-1.5 text-white sticky left-6 bg-gray-800">{r.sourcer}</td>
                        <td className="px-2 py-1.5 text-right font-semibold" style={cellStyle(r.pctContPR, 'pctContPR')}>{fmtPct(r.pctContPR)}</td>
                        <td className="px-2 py-1.5 text-right font-semibold" style={cellStyle(r.pctScrActual, 'pctScrActual')}>{fmtPct(r.pctScrActual)}</td>
                        <td className="px-2 py-1.5 text-right font-semibold" style={cellStyle(r.pctActualATS, 'pctActualATS')}>{fmtPct(r.pctActualATS)}</td>
                        <td className="px-2 py-1.5 text-right text-gray-300">{r.contacted.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-gray-300">{r.positive_response.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-gray-300">{r.screens.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-gray-300">{r.actual_screens.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-gray-300">{r.ats.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-gray-300">{r.offered.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-green-300 font-semibold">{r.hires.toLocaleString()}</td>
                        <td className="px-2 py-1.5 text-right text-gray-400">{r.jobs}</td>
                      </tr>
                      {isOpen && drilldown.length > 0 && drilldown.map(j => {
                        const pctContPR = j.contacted ? Math.min(1, j.positive_response / j.contacted) : null;
                        const pctScrActual = j.screens ? Math.min(1, j.actual_screens / j.screens) : null;
                        const pctActualATS = j.actual_screens ? Math.min(1, j.ats / j.actual_screens) : null;
                        return (
                          <tr key={r.sourcer + '::' + j.job_id} className="border-t border-gray-800 bg-gray-900/40 text-[11px]">
                            <td className="px-2 py-1 sticky left-0 bg-gray-900/40"></td>
                            <td className="px-3 py-1 sticky left-6 bg-gray-900/40 text-gray-300 max-w-[260px] truncate" title={`${j.client} — ${j.job_title}${j.archived ? ' (archived)' : ''}`}>
                              <span className="text-gray-500">↳</span> {j.client ? <span className="text-gray-500">{j.client} · </span> : null}{j.job_title}
                              {j.archived && <span className="ml-1 px-1 py-0.5 text-[9px] rounded bg-gray-700 text-gray-400 align-middle">archived</span>}
                            </td>
                            <td className="px-2 py-1 text-right text-gray-400" style={cellStyle(pctContPR, 'pctContPR')}>{fmtPct(pctContPR)}</td>
                            <td className="px-2 py-1 text-right text-gray-400" style={cellStyle(pctScrActual, 'pctScrActual')}>{fmtPct(pctScrActual)}</td>
                            <td className="px-2 py-1 text-right text-gray-400" style={cellStyle(pctActualATS, 'pctActualATS')}>{fmtPct(pctActualATS)}</td>
                            <td className="px-2 py-1 text-right text-gray-400">{j.contacted.toLocaleString()}</td>
                            <td className="px-2 py-1 text-right text-gray-400">{j.positive_response.toLocaleString()}</td>
                            <td className="px-2 py-1 text-right text-gray-400">{j.screens.toLocaleString()}</td>
                            <td className="px-2 py-1 text-right text-gray-400">{j.actual_screens.toLocaleString()}</td>
                            <td className="px-2 py-1 text-right text-gray-400">{j.ats.toLocaleString()}</td>
                            <td className="px-2 py-1 text-right text-gray-400">{j.offered.toLocaleString()}</td>
                            <td className="px-2 py-1 text-right text-green-400/80">{j.hires ? j.hires.toLocaleString() : ''}</td>
                            <td className="px-2 py-1 text-right text-gray-500"></td>
                          </tr>
                        );
                      })}
                      {isOpen && drilldown.length === 0 && (
                        <tr className="border-t border-gray-800 bg-gray-900/40">
                          <td colSpan={13} className="px-6 py-2 text-[11px] text-gray-500 italic">No per-role activity for {r.sourcer} in current filter.</td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
                {perSourcer.length === 0 && (
                  <tr><td colSpan={13} className="px-3 py-6 text-center text-gray-500">No sourcer activity in current filter.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden">
          <div className="px-4 py-3 bg-gray-700 text-sm font-semibold text-white flex justify-between items-center">
            <div>
              <span>Pipelines without Hires &mdash; by Sourcer</span>
              <span className="ml-2 text-xs text-gray-400 font-normal">(current snapshot — Year/Q/Month don't apply)</span>
            </div>
            <span className="text-xs text-gray-400 font-normal">{noHireBySourcer.length} sourcers</span>
          </div>
          <div className="overflow-x-auto max-h-[500px]">
            <CsvBtn fname="ts_pipelines_no_hires_by_sourcer" />
            <table className="min-w-full text-xs">
              <thead className="bg-gray-900 text-gray-300 sticky top-0">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Sourcer</th>
                  <th className="text-right px-3 py-2 font-medium">Jobs</th>
                  <th className="text-right px-3 py-2 font-medium">0-30 d</th>
                  <th className="text-right px-3 py-2 font-medium">30-60 d</th>
                  <th className="text-right px-3 py-2 font-medium">60-90 d</th>
                  <th className="text-right px-3 py-2 font-medium">&gt;90 d</th>
                </tr>
              </thead>
              <tbody>
                {noHireBySourcer.map(r => (
                  <tr key={r.sourcer} className="border-t border-gray-700 hover:bg-gray-700/50">
                    <td className="px-3 py-1.5 text-white">{r.sourcer}</td>
                    <td className="px-3 py-1.5 text-right text-gray-300 font-semibold">{r.jobs}</td>
                    <td className="px-3 py-1.5 text-right text-green-300">{r['0-30'] || ''}</td>
                    <td className="px-3 py-1.5 text-right text-yellow-300">{r['30-60'] || ''}</td>
                    <td className="px-3 py-1.5 text-right text-orange-300">{r['60-90'] || ''}</td>
                    <td className="px-3 py-1.5 text-right text-red-300">{r['>90'] || ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-gray-800 border border-gray-700 rounded-lg overflow-hidden">
          <div className="px-4 py-3 bg-gray-700 text-sm font-semibold text-white flex justify-between">
            <span>Pipelines without Hires &mdash; Job Detail</span>
            <span className="text-xs text-gray-400 font-normal">{pipelinesNoHire.length} jobs</span>
          </div>
          <div className="overflow-x-auto max-h-[500px]">
            <CsvBtn fname="ts_pipelines_no_hires_job_detail" />
            <table className="min-w-full text-xs">
              <thead className="bg-gray-900 text-gray-300 sticky top-0">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Job Title</th>
                  <th className="text-left px-3 py-2 font-medium">Client</th>
                  <th className="text-left px-3 py-2 font-medium">Sourcer</th>
                  <th className="text-right px-3 py-2 font-medium">Days Open</th>
                </tr>
              </thead>
              <tbody>
                {pipelinesNoHire.slice().sort((a, b) => (a.job_title || '').localeCompare(b.job_title || '')).map(j => {
                  const dColor = j.days > 90 ? 'text-red-300' : j.days > 60 ? 'text-orange-300' : j.days > 30 ? 'text-yellow-300' : 'text-green-300';
                  return (
                    <tr key={j.job_id} className="border-t border-gray-700 hover:bg-gray-700/50">
                      <td className="px-3 py-1.5 text-gray-200 max-w-[260px] truncate" title={j.job_title}>{j.job_title}</td>
                      <td className="px-3 py-1.5 text-gray-300">{j.client}</td>
                      <td className="px-3 py-1.5 text-gray-300">{j.sourcer}</td>
                      <td className={`px-3 py-1.5 text-right font-semibold ${dColor}`}>{j.days}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="text-xs text-gray-500 leading-relaxed">
        Per-Sourcer table + funnel sourced from <code>project_dashboard.rows</code> (attribution = <code>candidate.candidate_sourcer</code>; same as Project Dashboard tab). Year/Quarter/Month picker uses calendar dates mapped to ISO week set via <code>pdWeekSetFor</code> — picking "2026-04" matches PD's 01/04 → 30/04. Numbers reconcile to the unit with Project Dashboard.<br />
        Pipelines, hires, KPIs from project_dashboard_hires, tth_jobs, jobs. Tech Role from tth_jobs.tech_role with job_category fallback. Disqualification matrix deferred.
      </div>
    </div>
  );
};

// Leadership-only tabs (WBR + MBR) are hidden unless the URL contains
// ?role=leadership. Auth is enforced by Cloudflare Access (Google SSO,
// @tribe.xyz) in front of recruiting.tribe.xyz and tribe-recruiting.pages.dev,
// so the prior in-app sessionStorage password gate has been removed.

// ── Weekly Summary Tab ────────────────────────────────────────────────────
// PBI "Weekly Progress" port. Person/client/company weekly+monthly funnel from
// data.weekly_summary; drill into a person's clients/jobs via data.weekly_summary_byjob.
// Person headline stays de-duplicated (matches person view); per-job rows can sum
// to slightly more than the person total (a candidate on >1 job counts in each).
const WSUM_WINDOWS = [
  ['12', 'Last 12 weeks'], ['26', 'Last 26 weeks'], ['2026', '2026 only'], ['2025', '2025 only'], ['all', 'All weeks (2025–26)'],
];
const WSUM_METRICS = ['viewed','contacted','reacted','positive_response','screens','actual_screens','ats','offered','hired'];

const WeeklySummaryTab = ({ data }) => {
  const rows = data.weekly_summary || [];
  const byjob = data.weekly_summary_byjob || [];
  const [fClient, setFClient] = useState('');
  const [fTa, setFTa] = useState('');
  const [fTs, setFTs] = useState('');
  const [subClient, setSubClient] = useState('');
  const [subJob, setSubJob] = useState('');
  const [win, setWin] = useState('12');

  const clients = useMemo(() => Array.from(new Set(rows.filter((r) => r.dim_type === 'client').map((r) => r.dim_value).filter(Boolean))).sort(), [rows]);
  const tas = useMemo(() => Array.from(new Set(rows.filter((r) => r.dim_type === 'ta').map((r) => r.dim_value).filter(Boolean))).sort(), [rows]);
  const tses = useMemo(() => Array.from(new Set(rows.filter((r) => r.dim_type === 'ts').map((r) => r.dim_value).filter(Boolean))).sort(), [rows]);

  const primaryType = fTs ? 'ts' : fTa ? 'ta' : fClient ? 'client' : 'company';
  const primaryVal = fTs || fTa || fClient || '';
  const isPerson = primaryType === 'ta' || primaryType === 'ts';

  const personByjob = useMemo(() => (isPerson ? byjob.filter((r) => r.dim_type === primaryType && r.person === primaryVal) : []), [byjob, isPerson, primaryType, primaryVal]);
  const subClients = useMemo(() => Array.from(new Set(personByjob.map((r) => r.client).filter(Boolean))).sort(), [personByjob]);
  const subJobs = useMemo(() => {
    const seen = new Map();
    personByjob.forEach((r) => { if (subClient && r.client !== subClient) return; if (!seen.has(r.job_id)) seen.set(r.job_id, { job_id: r.job_id, label: `${r.job_title}${subClient ? '' : ' — ' + r.client}` }); });
    return Array.from(seen.values()).sort((a, b) => a.label.localeCompare(b.label));
  }, [personByjob, subClient]);

  const drilled = isPerson && (subClient || subJob);

  const selRows = useMemo(() => {
    if (drilled) {
      return personByjob.filter((r) => (subJob ? r.job_id === subJob : true) && (subClient ? r.client === subClient : true) && r.iso_week >= 1 && (r.iso_year === 2025 || r.iso_year === 2026));
    }
    return rows.filter((r) => r.dim_type === primaryType && r.dim_value === primaryVal && r.iso_week >= 1 && (r.iso_year === 2025 || r.iso_year === 2026));
  }, [drilled, personByjob, subJob, subClient, rows, primaryType, primaryVal]);

  const blank = () => { const o = {}; WSUM_METRICS.forEach((m) => { o[m] = 0; }); return o; };
  const addInto = (a, r) => { WSUM_METRICS.forEach((m) => { a[m] += (r[m] || 0); }); };

  const weekly = useMemo(() => {
    const m = new Map();
    selRows.forEach((r) => { const k = `${r.iso_year}-${String(r.iso_week).padStart(2, '0')}`; if (!m.has(k)) m.set(k, { key: k, year: r.iso_year, week: r.iso_week, ...blank() }); addInto(m.get(k), r); });
    let arr = Array.from(m.values()).sort((a, b) => b.key.localeCompare(a.key));
    if (win === '12') arr = arr.slice(0, 12); else if (win === '26') arr = arr.slice(0, 26); else if (win === '2026') arr = arr.filter((w) => w.year === 2026); else if (win === '2025') arr = arr.filter((w) => w.year === 2025);
    return arr;
  }, [selRows, win]);
  const monthly = useMemo(() => {
    const m = new Map();
    selRows.forEach((r) => { const k = isoWeekToMonth(r.iso_year, r.iso_week); if (!m.has(k)) m.set(k, { key: k, ...blank() }); addInto(m.get(k), r); });
    return Array.from(m.values()).sort((a, b) => b.key.localeCompare(a.key));
  }, [selRows]);

  const totalsOf = (arr) => arr.reduce((t, r) => { addInto(t, r); return t; }, blank());
  const rate = (n, d) => (d ? Math.min(n / d, 1) : null);
  const weekLabel = (y, w) => { const mon = isoWeekToDate(y, w); const sun = new Date(mon.getTime() + 6 * 86400000); const f = (d) => d.toLocaleString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }); return `${y} W${String(w).padStart(2, '0')} · ${f(mon)}–${sun.getUTCDate()}`; };
  const monthLabel = (k) => new Date(k + '-01').toLocaleString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' });

  const header = (firstCol) => (
    <thead><tr className="text-gray-300 border-b border-gray-700 text-xs">
      <th className="text-left px-2 py-1 whitespace-nowrap">{firstCol}</th>
      <th className="text-center px-1 py-1">LinkedIn Viewed</th><th className="text-center px-1 py-1">Contacted</th>
      <th className="text-center px-1 py-1">Reacted</th><th className="text-center px-1 py-1">Pos Resp</th>
      <th className="text-center px-1 py-1">Rec Screens</th><th className="text-center px-1 py-1">Actual Screens</th>
      <th className="text-center px-1 py-1">Moved to ATS</th><th className="text-center px-1 py-1">Offered</th><th className="text-center px-1 py-1">Hired</th>
      <th className="text-center px-1 py-1">% V→C</th><th className="text-center px-1 py-1">% C→R</th><th className="text-center px-1 py-1">% C→PR</th>
      <th className="text-center px-1 py-1">% PR→S</th><th className="text-center px-1 py-1">% S→ATS</th><th className="text-center px-1 py-1">% AS→ATS</th>
    </tr></thead>
  );
  const renderRow = (label, c, opts = {}) => {
    const base = opts.total ? 'text-white font-semibold' : 'text-gray-200';
    const cls = opts.total ? 'border-t-2 border-gray-600' : 'border-t border-gray-700 hover:bg-gray-700';
    return (
      <tr key={label} className={cls} style={opts.total ? { backgroundColor: '#1F2937' } : {}}>
        <td className="px-2 py-1 text-white whitespace-nowrap">{label}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.viewed.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base} font-medium`}>{c.contacted.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.reacted.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.positive_response.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.screens.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.actual_screens.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.ats.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base}`}>{c.offered.toLocaleString()}</td>
        <td className={`text-center px-1 py-1 ${base} font-medium`}>{c.hired.toLocaleString()}</td>
        <td className="text-center px-1 py-1 text-gray-400">{pdPct(rate(c.contacted, c.viewed))}</td>
        <td className="text-center px-1 py-1 text-gray-400">{pdPct(rate(c.reacted, c.contacted))}</td>
        <td className="text-center px-1 py-1 text-gray-400">{pdPct(rate(c.positive_response, c.contacted))}</td>
        <td className="text-center px-1 py-1 text-gray-400">{pdPct(rate(c.screens, c.positive_response))}</td>
        <td className="text-center px-1 py-1 text-gray-400">{pdPct(rate(c.ats, c.screens))}</td>
        <td className="text-center px-1 py-1 text-gray-400">{pdPct(rate(c.ats, c.actual_screens))}</td>
      </tr>
    );
  };

  const wTot = totalsOf(weekly); const mTot = totalsOf(monthly);
  const setPrimary = (which, val) => { setFClient(which === 'client' ? val : ''); setFTa(which === 'ta' ? val : ''); setFTs(which === 'ts' ? val : ''); setSubClient(''); setSubJob(''); };
  const jobLabel = (subJobs.find((j) => j.job_id === subJob) || {}).label || '';
  const scope = primaryType === 'company' ? 'Company-wide (all clients · all recruiters)'
    : primaryType === 'client' ? `Client: ${primaryVal}`
    : `${primaryType === 'ts' ? 'Sourcer' : 'TA'}: ${primaryVal}${subClient ? ' · ' + subClient : ''}${subJob ? ' · ' + jobLabel : ''}`;

  return (
    <div className="space-y-6">
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex flex-wrap items-center gap-3 mb-1">
          <h2 className="text-xl font-bold text-white">Weekly Summary</h2>
          <span className="text-xs text-gray-400">Full funnel by week &amp; month · includes archived jobs · {scope}</span>
        </div>
        <div className="flex flex-wrap gap-2 mt-3">
          <select value={fClient} onChange={(e) => setPrimary('client', e.target.value)} className="bg-gray-700 text-white text-sm rounded px-2 py-1 border border-gray-600">
            <option value="">All Clients</option>{clients.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={fTa} onChange={(e) => setPrimary('ta', e.target.value)} className="bg-gray-700 text-white text-sm rounded px-2 py-1 border border-gray-600">
            <option value="">All TAs</option>{tas.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={fTs} onChange={(e) => setPrimary('ts', e.target.value)} className="bg-gray-700 text-white text-sm rounded px-2 py-1 border border-gray-600">
            <option value="">All Sourcers (TS)</option>{tses.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
          {(fClient || fTa || fTs) && <button onClick={() => setPrimary('', '')} className="text-xs text-gray-400 hover:text-white px-2 py-1">Clear</button>}
        </div>
        {isPerson && (
          <div className="flex flex-wrap items-center gap-2 mt-3 pt-3 border-t border-gray-700">
            <span className="text-xs text-gray-400">Drill into {primaryVal}'s jobs:</span>
            <select value={subClient} onChange={(e) => { setSubClient(e.target.value); setSubJob(''); }} className="bg-gray-700 text-white text-sm rounded px-2 py-1 border border-gray-600">
              <option value="">All their clients</option>{subClients.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
            <select value={subJob} onChange={(e) => setSubJob(e.target.value)} className="bg-gray-700 text-white text-sm rounded px-2 py-1 border border-gray-600">
              <option value="">All their positions</option>{subJobs.map((j) => <option key={j.job_id} value={j.job_id}>{j.label}</option>)}
            </select>
            {(subClient || subJob) && <button onClick={() => { setSubClient(''); setSubJob(''); }} className="text-xs text-gray-400 hover:text-white px-2 py-1">Clear drill</button>}
          </div>
        )}
        {drilled && <div className="mt-2 text-xs text-gray-500">Drill view from per-job data. A candidate worked on multiple jobs is counted in each, so these rows can sum to slightly more than {primaryVal}'s de-duplicated total.</div>}
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-white">Weekly Performance</h3>
          <select value={win} onChange={(e) => setWin(e.target.value)} className="bg-gray-700 text-white text-xs rounded px-2 py-1 border border-gray-600">
            {WSUM_WINDOWS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </select>
        </div>
        <div className="overflow-x-auto"><CsvBtn fname="weekly_performance" /><table className="w-full text-sm">{header('Week')}
          <tbody>{weekly.map((w) => renderRow(weekLabel(w.year, w.week), w))}{weekly.length > 0 && renderRow('Total', wTot, { total: true })}</tbody>
        </table></div>
        {weekly.length === 0 && <div className="text-sm text-gray-500 py-4 text-center">No data for this selection.</div>}
      </div>

      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-3">Monthly Performance</h3>
        <div className="overflow-x-auto"><CsvBtn fname="monthly_performance" /><table className="w-full text-sm">{header('Month')}
          <tbody>{monthly.map((mo) => renderRow(monthLabel(mo.key), mo))}{monthly.length > 0 && renderRow('Total', mTot, { total: true })}</tbody>
        </table></div>
        {monthly.length === 0 && <div className="text-sm text-gray-500 py-4 text-center">No data for this selection.</div>}
      </div>

      <div className="text-xs text-gray-500">
        Source: data.weekly_summary{drilled ? ' / weekly_summary_byjob' : ''} — PBI "Weekly Progress" port, includes archived jobs. Sourcer = who_created_event_first; TA = who_event_created_for. Conversion rates capped at 100%, blank when denominator is 0.
      </div>
    </div>
  );
};


// ---------- New Project Health Tab ----------
// Gated (Blake + Jacopo). Q2 OKR tracking for new client roles.
//   KR2: first move to ATS within 4 business days on 90% of new projects.
//   KR3: Actual Screen -> ATS conversion above 60% by week 4.
// Top = OKR scorecard over the current-quarter cohort (roles started in the
// quarter, never dropped — a Q2 role maturing in Q3 stays in Q2). Bottom = live
// operational list of roles open <=30 days. Data: data.new_project_health.
const nphPillStyles = {
  green: { background: 'rgba(34,197,94,0.18)',  color: '#4ade80' },
  amber: { background: 'rgba(245,158,11,0.18)', color: '#fbbf24' },
  red:   { background: 'rgba(239,68,68,0.18)',  color: '#f87171' },
  gray:  { background: 'rgba(148,163,184,0.15)', color: '#94a3b8' },
};
const NphPill = ({ text, kind }) => (
  <span style={{ ...nphPillStyles[kind], padding: '2px 9px', borderRadius: '6px', fontWeight: 500, fontSize: '12px', display: 'inline-block', minWidth: '54px', textAlign: 'center' }}>{text}</span>
);
const nphConvKind = (v) => (v == null ? 'gray' : v >= 60 ? 'green' : v >= 40 ? 'amber' : 'red');
const nphSortRows = (rows, key, dir) => {
  const mul = dir === 'desc' ? -1 : 1;
  return [...rows].sort((a, b) => {
    const av = a[key], bv = b[key];
    const an = (av == null), bn = (bv == null);
    if (an && bn) return 0;
    if (an) return 1;
    if (bn) return -1;
    if (typeof av === 'string') return mul * av.localeCompare(bv);
    return mul * (av - bv);
  });
};
const NphTh = ({ label, k, sort, setSort, align }) => (
  <th
    onClick={() => setSort((sp) => ({ key: k, dir: sp.key === k && sp.dir === 'asc' ? 'desc' : 'asc' }))}
    className={`px-3 py-2 font-medium cursor-pointer select-none hover:text-white ${align === 'right' ? 'text-right' : 'text-left'}`}
  >
    {label}{sort.key === k ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ''}
  </th>
);
// Business days strictly after `a` up to and including `b` (weekends excluded).
const nphBizDays = (a, b) => {
  if (!a || !b) return null;
  if (b <= a) return 0;
  let n = 0; const cur = new Date(a);
  while (cur < b) { cur.setDate(cur.getDate() + 1); const d = cur.getDay(); if (d !== 0 && d !== 6) n += 1; }
  return n;
};
const nphQuarterKey = (d) => `${d.getFullYear()}-Q${Math.floor(d.getMonth() / 3) + 1}`;

const NewProjectHealthTab = ({ data }) => {
  const [expanded, setExpanded] = useState(null);
  const [sort, setSort] = useState({ key: 'client', dir: 'asc' });
  const [roleSort, setRoleSort] = useState({ key: 'daysOpen', dir: 'asc' });

  const Kpi = ({ label, value }) => (
    <div className="bg-blue-100 text-blue-900 rounded-lg px-4 py-3 min-w-[120px] flex-1">
      <div className="text-2xl font-bold">{value == null ? '-' : value}</div>
      <div className="text-xs text-blue-800 mt-1">{label}</div>
    </div>
  );

  const today = new Date();
  const MS = 86400000;

  const roles = useMemo(() => {
    return (data.new_project_health || []).map((r) => {
      const dc = new Date(r.date_created);
      const daysOpen = Math.floor((today - dc) / MS);
      const dfa = r.date_first_ats ? new Date(r.date_first_ats) : null;
      const bd = dfa ? nphBizDays(dc, dfa) : null;                 // business days to first ATS (KR2)
      const bizElapsed = nphBizDays(dc, today);
      const conv = r.w4_actual_screens > 0 ? Math.round((100 * r.w4_ats) / r.w4_actual_screens) : null;
      const mature = daysOpen >= 28;                               // reached week 4
      const weekNum = Math.min(4, Math.floor(daysOpen / 7) + 1);
      return { ...r, dc, daysOpen, dfa, bd, bizElapsed, conv, mature, weekNum, qkey: nphQuarterKey(dc) };
    });
  }, [data]);

  // ---- OKR scorecard: current-quarter cohort ----
  const score = useMemo(() => {
    const qk = nphQuarterKey(today);
    const cohort = roles.filter((r) => r.qkey === qk);
    // KR2 — decidable once the role got an ATS OR the 4-business-day window has closed.
    const kr2Decidable = cohort.filter((r) => r.dfa || r.bizElapsed > 4);
    const kr2Within = kr2Decidable.filter((r) => r.dfa && r.bd <= 4);
    const gotAts = cohort.filter((r) => r.dfa);
    const kr2Pct = kr2Decidable.length ? Math.round((100 * kr2Within.length) / kr2Decidable.length) : null;
    const kr2AvgBd = gotAts.length ? (gotAts.reduce((s, r) => s + r.bd, 0) / gotAts.length).toFixed(1) : null;
    // KR3 — mature roles with >0 actual screens (0-screen roles excluded: usually a named/direct hire).
    const kr3Pool = cohort.filter((r) => r.mature && r.w4_actual_screens > 0);
    const kr3Hit = kr3Pool.filter((r) => r.conv >= 60);
    const kr3Pct = kr3Pool.length ? Math.round((100 * kr3Hit.length) / kr3Pool.length) : null;
    const sAs = kr3Pool.reduce((s, r) => s + r.w4_actual_screens, 0);
    const sAts = kr3Pool.reduce((s, r) => s + r.w4_ats, 0);
    const kr3Blend = sAs > 0 ? Math.round((100 * sAts) / sAs) : null;
    return { qk, cohortN: cohort.length, kr2Pct, kr2AvgBd, kr2Den: kr2Decidable.length, kr3Pct, kr3Blend, kr3Den: kr3Pool.length };
  }, [roles]);

  // ---- Operational list: roles open <=30 days, grouped by client ----
  const activeRoles = useMemo(() => roles.filter((r) => r.daysOpen >= 0 && r.daysOpen <= 30), [roles]);
  const clients = useMemo(() => {
    const m = {};
    activeRoles.forEach((r) => { (m[r.client] = m[r.client] || []).push(r); });
    return Object.entries(m).map(([client, rs]) => {
      const conv = rs.filter((r) => r.conv != null);
      const as = rs.reduce((a, r) => a + r.w4_actual_screens, 0);
      const ats = rs.reduce((a, r) => a + r.w4_ats, 0);
      return {
        client, rolesList: rs, n: rs.length,
        avgOpen: Math.round(rs.reduce((a, r) => a + r.daysOpen, 0) / rs.length),
        convNow: as > 0 ? Math.round((100 * ats) / as) : null,
      };
    });
  }, [activeRoles]);
  const sortedClients = useMemo(() => nphSortRows(clients, sort.key, sort.dir), [clients, sort]);

  const firstAtsPill = (r) => {
    if (r.dfa) { const k = r.bd <= 4 ? 'green' : r.bd <= 8 ? 'amber' : 'red'; return <NphPill text={`${r.bd} bd${r.bd <= 4 ? ' ✓' : ''}`} kind={k} />; }
    if (r.bizElapsed > 4) return <NphPill text="not yet" kind="red" />;
    return <NphPill text="pending" kind="gray" />;
  };
  const week4Pill = (r) => {
    if (!r.mature) return <NphPill text={`wk ${r.weekNum} of 4`} kind="gray" />;
    if (r.conv == null) return <NphPill text="no screens" kind="gray" />;
    const ok = r.conv >= 60;
    return <NphPill text={`${r.conv}% ${ok ? '✓' : '✗'}`} kind={ok ? 'green' : 'red'} />;
  };

  return (
    <div>
      <div className="flex items-start justify-between mb-1">
        <h2 className="text-2xl font-bold text-white">New Project Health</h2>
        <div className="text-xs text-gray-500 text-right mt-1">Leadership only &middot; client delivery &middot; new roles since Q2 start (1 Apr 2026)</div>
      </div>

      {/* OKR scorecard */}
      <div className="text-sm font-medium text-gray-300 mt-4 mb-1">{score.qk} OKR scorecard</div>
      <div className="text-xs text-gray-500 mb-3">{score.cohortN} new projects this quarter &middot; {score.kr3Den} have reached week 4</div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-7">
        <div className="bg-gray-800 border border-gray-700 rounded-lg px-5 py-4">
          <div className="text-sm font-medium text-white">KR2 &middot; First move to ATS</div>
          <div className="text-xs text-gray-500 mb-3">Goal: 90% of projects within 4 business days</div>
          <div className="flex gap-8">
            <div>
              <div className="text-3xl font-bold" style={{ color: (score.kr2Pct ?? 0) >= 90 ? '#4ade80' : '#fbbf24' }}>{score.kr2Pct == null ? '—' : score.kr2Pct + '%'}</div>
              <div className="text-xs text-gray-400 mt-1">within 4 business days</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-white">{score.kr2AvgBd == null ? '—' : score.kr2AvgBd}</div>
              <div className="text-xs text-gray-400 mt-1">avg business days to ATS</div>
            </div>
          </div>
        </div>
        <div className="bg-gray-800 border border-gray-700 rounded-lg px-5 py-4">
          <div className="text-sm font-medium text-white">KR3 &middot; Actual Screen &rarr; ATS by week 4</div>
          <div className="text-xs text-gray-500 mb-3">Goal: conversion above 60% by week 4</div>
          <div className="flex gap-8">
            <div>
              <div className="text-3xl font-bold" style={{ color: (score.kr3Pct ?? 0) >= 60 ? '#4ade80' : '#fbbf24' }}>{score.kr3Pct == null ? '—' : score.kr3Pct + '%'}</div>
              <div className="text-xs text-gray-400 mt-1">of roles hit &ge;60%</div>
            </div>
            <div>
              <div className="text-3xl font-bold" style={{ color: (score.kr3Blend ?? 0) >= 60 ? '#4ade80' : '#fbbf24' }}>{score.kr3Blend == null ? '—' : score.kr3Blend + '%'}</div>
              <div className="text-xs text-gray-400 mt-1">blended conversion</div>
            </div>
          </div>
        </div>
      </div>

      {/* Operational list */}
      <div className="text-sm font-medium text-gray-300 mb-1">Active new roles</div>
      <div className="text-xs text-gray-500 mb-3">Open &le;30 days &middot; live tracker &middot; click a client to drill into its roles</div>
      {sortedClients.length === 0 ? (
        <div className="bg-gray-800 rounded-lg p-8 text-center text-gray-400">No roles opened in the last 30 days.</div>
      ) : (
        <div className="bg-gray-800 rounded-lg overflow-hidden">
          <CsvBtn fname="project_health_kr2_first_ats" />
          <table className="w-full text-sm">
            <thead className="text-gray-400 border-b border-gray-700">
              <tr>
                <NphTh label="Client" k="client" sort={sort} setSort={setSort} />
                <NphTh label="Roles" k="n" sort={sort} setSort={setSort} align="right" />
                <NphTh label="Avg days open" k="avgOpen" sort={sort} setSort={setSort} align="right" />
                <NphTh label="Actual Screen &rarr; ATS now" k="convNow" sort={sort} setSort={setSort} align="right" />
              </tr>
            </thead>
            <tbody>
              {sortedClients.map((c) => {
                const open = expanded === c.client;
                const sortedRoles = nphSortRows(c.rolesList, roleSort.key, roleSort.dir);
                return (
                  <React.Fragment key={c.client}>
                    <tr onClick={() => setExpanded(open ? null : c.client)} className="border-b border-gray-700 hover:bg-gray-700 cursor-pointer" style={{ backgroundColor: open ? 'rgba(255,255,255,0.03)' : undefined }}>
                      <td className="px-3 py-3 font-medium text-white">
                        <span className="inline-block text-gray-500 mr-2" style={{ transform: open ? 'rotate(90deg)' : 'none', transition: 'transform .15s' }}>{'▶'}</span>
                        {c.client}
                      </td>
                      <td className="px-3 py-3 text-right text-gray-300">{c.n}</td>
                      <td className="px-3 py-3 text-right text-gray-300">{c.avgOpen}d</td>
                      <td className="px-3 py-3 text-right"><NphPill text={c.convNow == null ? '—' : c.convNow + '%'} kind={nphConvKind(c.convNow)} /></td>
                    </tr>
                    {open && (
                      <tr className="bg-gray-900">
                        <td colSpan={4} className="px-3 py-3">
                          <CsvBtn fname="project_health_kr3_as_to_ats" />
                          <table className="w-full text-sm">
                            <thead className="text-gray-500 border-b border-gray-800">
                              <tr>
                                <NphTh label="Role" k="job_title" sort={roleSort} setSort={setRoleSort} />
                                <NphTh label="Days open" k="daysOpen" sort={roleSort} setSort={setRoleSort} align="right" />
                                <NphTh label="First ATS" k="bd" sort={roleSort} setSort={setRoleSort} align="right" />
                                <NphTh label="AS &rarr; ATS now" k="conv" sort={roleSort} setSort={setRoleSort} align="right" />
                                <NphTh label="Week 4" k="daysOpen" sort={roleSort} setSort={setRoleSort} align="right" />
                              </tr>
                            </thead>
                            <tbody>
                              {sortedRoles.map((r) => (
                                <tr key={r.job_id} className="border-b border-gray-800">
                                  <td className="px-3 py-2 text-gray-200">{r.job_title}<div className="text-xs text-gray-500">{r.ta}{r.is_external ? ' · ext' : ''}</div></td>
                                  <td className="px-3 py-2 text-right text-gray-300">{r.daysOpen}d</td>
                                  <td className="px-3 py-2 text-right">{firstAtsPill(r)}</td>
                                  <td className="px-3 py-2 text-right"><NphPill text={r.conv == null ? '—' : r.conv + '%'} kind={nphConvKind(r.conv)} /></td>
                                  <td className="px-3 py-2 text-right">{week4Pill(r)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="text-xs text-gray-500 mt-4 leading-relaxed">
        First ATS = business days from role creation to first candidate reaching ATS (green &le;4). Week 4 stays &ldquo;wk N of 4&rdquo; until a role is 28 days old, then locks to its final Actual Screen&rarr;ATS result vs 60%. Roles with no actual screens by week 4 are excluded from KR3 (typically a named/direct hire). Mature roles keep feeding the scorecard after they leave this list at 30 days.
      </div>
    </div>
  );
};

const LEADERSHIP_TABS = new Set(['wbr', 'mbr']);
const DIRECTOR_TABS = new Set(['profitability']);

const IRTab = ({ data }) => {
  const funnelRows      = data.ir_funnel_jobweek      || [];
  const sourcedRows     = data.ir_sourced_jobweek     || [];
  const interviewedRows = data.ir_interviewed_jobweek || [];
  const dqByStage       = data.ir_dq_by_stage         || [];
  const jobsActive      = data.ir_jobs_active         || [];
  const dqByJobReason   = data.ir_dq_byjob_reason     || [];
  // Phase 2b: Ashby-derived right side of funnel. Empty if extractor not yet plumbed in.
  const ashbyFunnel     = data.ir_ashby_funnel_jobweek  || [];
  const ashbyActive     = data.ir_ashby_active_pipeline || [];
  const ashbyDQReasons  = data.ir_ashby_dq_reasons      || [];
  const ashbyHires      = data.ir_ashby_hires           || [];
  const hasAshby        = ashbyFunnel.length > 0 || ashbyHires.length > 0;
  // Set of Bubble job_ids that have linked Ashby data (via the atsID crosswalk).
  const ashbyJobIdSet = useMemo(() => {
    const s = new Set();
    for (const r of ashbyFunnel) if (r.bubble_job_id) s.add(r.bubble_job_id);
    for (const r of ashbyHires)  if (r.bubble_job_id) s.add(r.bubble_job_id);
    for (const r of ashbyActive) if (r.bubble_job_id) s.add(r.bubble_job_id);
    return s;
  }, [ashbyFunnel, ashbyHires, ashbyActive]);

  const [jobFilter, setJobFilter]     = useState('all');
  const [windowFilter, setWindowFilter] = useState('last4');
  const [highlightSourcer, setHighlightSourcer] = useState(null);
  const [sourcerFilter, setSourcerFilter] = useState('all');
  const [taFilter, setTaFilter] = useState('all');
  const [highlightTA, setHighlightTA] = useState(null);

  // Compute the active week range from the filter
  // Use TODAY's ISO week as the anchor, not the max week present in the data.
  // That way "Last week" always means the previous completed week, even if
  // the current week has no data yet.
  const todayISOWeek = useMemo(() => {
    const d = new Date();
    const target = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    target.setUTCDate(target.getUTCDate() + 4 - (target.getUTCDay() || 7));
    const yearStart = new Date(Date.UTC(target.getUTCFullYear(), 0, 1));
    return Math.ceil((((target - yearStart) / 86400000) + 1) / 7);
  }, []);
  const thisWeek = todayISOWeek;
  const lastWeek = todayISOWeek - 1;

  const allWeeks = useMemo(() => {
    const ws = new Set(funnelRows.map(r => r.iso_week));
    return [...ws].sort((a, b) => a - b);
  }, [funnelRows]);
  const maxWeek = allWeeks.length ? Math.max(...allWeeks) : thisWeek;

  const inWindow = (week) => {
    if (windowFilter === 'all') return true;
    if (windowFilter === 'this_week') return week === thisWeek;
    if (windowFilter === 'last_week') return week === lastWeek;
    if (windowFilter === 'last4')  return week >= thisWeek - 4 && week <= thisWeek;   // 4 most recent completed weeks + this week
    if (windowFilter === 'last12') return week >= thisWeek - 12 && week <= thisWeek;
    if (windowFilter === 'last26') return week >= thisWeek - 26 && week <= thisWeek;  // ~6 months
    if (windowFilter === 'ytd')    return week >= 1 && week <= thisWeek;              // assumes single-year data
    return true;
  };

  const matchesJob = (jobId) => jobFilter === 'all' || jobId === jobFilter;

  // Filtered datasets
  const f_funnel = useMemo(() =>
    funnelRows.filter(r => matchesJob(r.job_id) && inWindow(r.iso_week)),
    [funnelRows, jobFilter, windowFilter, maxWeek]);
  const f_sourced = useMemo(() =>
    sourcedRows.filter(r => matchesJob(r.job_id) && inWindow(r.iso_week)
      && (sourcerFilter === 'all' || r.sourcer === sourcerFilter)),
    [sourcedRows, jobFilter, windowFilter, sourcerFilter, thisWeek]);
  const f_interviewed = useMemo(() =>
    interviewedRows.filter(r => matchesJob(r.job_id) && inWindow(r.iso_week)
      && (taFilter === 'all' || r.ta === taFilter)),
    [interviewedRows, jobFilter, windowFilter, taFilter, thisWeek]);
  const f_dqReason = useMemo(() =>
    dqByJobReason.filter(r => matchesJob(r.job_id)),
    [dqByJobReason, jobFilter]);
  const f_dqByStage = useMemo(() =>
    dqByStage.filter(r => matchesJob(r.job_id)),
    [dqByStage, jobFilter]);

  // Funnel totals (across filtered rows)
  const totals = useMemo(() => f_funnel.reduce((a, r) => ({
    contacted: a.contacted + r.contacted,
    pos_response: a.pos_response + r.pos_response,
    rec_screens: a.rec_screens + r.rec_screens,
    actual_screens: a.actual_screens + r.actual_screens,
    ats: a.ats + r.ats,
    onsite: a.onsite + r.onsite,
    culture: a.culture + r.culture,
    call_w_client: a.call_w_client + r.call_w_client,
    offered: a.offered + r.offered,
    hired: a.hired + r.hired,
  }), {contacted:0,pos_response:0,rec_screens:0,actual_screens:0,ats:0,onsite:0,culture:0,call_w_client:0,offered:0,hired:0}), [f_funnel]);

  // Sourced By: group by sourcer
  const sourcedAgg = useMemo(() => {
    const m = new Map();
    for (const r of f_sourced) {
      const cur = m.get(r.sourcer) || { sourcer: r.sourcer, contacted: 0, pos_response: 0, hired: 0 };
      cur.contacted += r.contacted; cur.pos_response += r.pos_response; cur.hired += r.hired;
      m.set(r.sourcer, cur);
    }
    return [...m.values()].sort((a, b) => b.contacted - a.contacted);
  }, [f_sourced]);

  // Interviewed By: group by TA
  const interviewedAgg = useMemo(() => {
    const m = new Map();
    for (const r of f_interviewed) {
      const cur = m.get(r.ta) || { ta: r.ta, actual_screens: 0 };
      cur.actual_screens += r.actual_screens;
      m.set(r.ta, cur);
    }
    return [...m.values()].sort((a, b) => b.actual_screens - a.actual_screens);
  }, [f_interviewed]);

  // Weekly Performance: group filtered funnel by week
  const weeklyAgg = useMemo(() => {
    const m = new Map();
    for (const r of f_funnel) {
      const cur = m.get(r.iso_week) || {iso_week: r.iso_week, contacted:0, pos_response:0, rec_screens:0, actual_screens:0, ats:0, onsite:0, culture:0, call_w_client:0, offered:0, hired:0};
      ['contacted','pos_response','rec_screens','actual_screens','ats','onsite','culture','call_w_client','offered','hired'].forEach(k => cur[k] += r[k]);
      m.set(r.iso_week, cur);
    }
    return [...m.values()].sort((a, b) => b.iso_week - a.iso_week);
  }, [f_funnel]);

  // DQ Reasons: group by reason
  const dqReasonAgg = useMemo(() => {
    const m = new Map();
    for (const r of f_dqReason) {
      m.set(r.reason, (m.get(r.reason) || 0) + r.count);
    }
    return [...m.entries()].map(([reason, count]) => ({reason, count})).sort((a, b) => b.count - a.count);
  }, [f_dqReason]);

  // Highlight metrics
  const highlightSourcerData = useMemo(() => {
    if (!highlightSourcer) return null;
    const rows = f_sourced.filter(r => r.sourcer === highlightSourcer);
    return {
      sourcer: highlightSourcer,
      contacted: rows.reduce((s, r) => s + r.contacted, 0),
      pos_response: rows.reduce((s, r) => s + r.pos_response, 0),
      hired: rows.reduce((s, r) => s + r.hired, 0),
    };
  }, [highlightSourcer, f_sourced]);
  const highlightTAData = useMemo(() => {
    if (!highlightTA) return null;
    const rows = f_interviewed.filter(r => r.ta === highlightTA);
    return {
      ta: highlightTA,
      actual_screens: rows.reduce((s, r) => s + r.actual_screens, 0),
    };
  }, [highlightTA, f_interviewed]);

  // Job dropdown / Active Jobs panel — ACTIVITY-DRIVEN.
  // A job is "active in the selected window" iff it has at least one row in
  // funnelRows / sourcedRows / interviewedRows (within the window) where the
  // contacted/screen/ats/etc. count is non-zero. This is the only honest
  // definition: an Ashby req that's "Open" but nobody has touched is NOT
  // active. Bubble's ir_jobs_active is a snapshot of "currently open" but
  // includes stale reqs too — we don't trust that as the gate either.
  const jobOptions = useMemo(() => {
    const FUNNEL_KEYS = ['contacted','pos_response','rec_screens','actual_screens','ats','onsite','culture','call_w_client','offered','hired'];
    const activeIds = new Set();
    const titleByJobId = new Map();

    for (const r of funnelRows) {
      if (!inWindow(r.iso_week)) continue;
      const has = FUNNEL_KEYS.some(k => (r[k] || 0) > 0);
      if (has) activeIds.add(r.job_id);
    }
    for (const r of sourcedRows) {
      if (!inWindow(r.iso_week)) continue;
      if ((r.contacted || 0) > 0 || (r.pos_response || 0) > 0 || (r.hired || 0) > 0) activeIds.add(r.job_id);
    }
    for (const r of interviewedRows) {
      if (!inWindow(r.iso_week)) continue;
      if ((r.actual_screens || 0) > 0) activeIds.add(r.job_id);
    }

    // Resolve titles + Bubble metadata from ir_jobs_active when available
    const bubbleById = new Map();
    for (const j of jobsActive) bubbleById.set(j.job_id, j);

    return [...activeIds].map(jid => {
      const bj = bubbleById.get(jid);
      return {
        job_id: jid,
        job_title: bj?.job_title || titleByJobId.get(jid) || '(historic Bubble job)',
        days_open: bj?.days_open ?? 0,
        hires_total: bj?.hires_total ?? 0,
        job_recruiter: bj?.job_recruiter || '—',
        job_sourcer: bj?.job_sourcer || '—',
        in_bubble: !!bj,
        in_ashby: ashbyJobIdSet.has(jid),
      };
    }).sort((a, b) => (b.days_open || 0) - (a.days_open || 0));
  }, [funnelRows, sourcedRows, interviewedRows, jobsActive, windowFilter, thisWeek, ashbyJobIdSet]);
  const selectedJob = jobsActive.find(j => j.job_id === jobFilter);

  // Sourced hired count for KPI
  const sourcedHired = sourcedAgg.reduce((s, r) => s + r.hired, 0);

  // Ashby override for right-side stages — when Ashby data is present, replace
  // Bubble's sparse counts with Ashby's authoritative ones for the same window.
  // Stage names mapped: Onsite, Culture Interview, Call with Client, Offered, Hired
  // (case-insensitive, includes substring match for variants like "Final Interview - Onsite")
  const ashbyStageMap = useMemo(() => {
    if (!ashbyFunnel.length) return null;
    if (jobFilter !== 'all' && !ashbyJobIdSet.has(jobFilter)) return null;
    const matches = (stage, target) => {
      const s = (stage || '').toLowerCase();
      return s.includes(target.toLowerCase());
    };
    const inJob = (r) => jobFilter === 'all' || r.bubble_job_id === jobFilter;
    const inWk = (r) => inWindow(r.iso_week);
    const sumIf = (pred) => ashbyFunnel.filter(r => inJob(r) && inWk(r) && pred(r.stage)).reduce((s, r) => s + r.count, 0);
    const L = (x) => (x || '').toLowerCase();
    return {
      application_review: sumIf(x => L(x) === 'application review'),
      initial_screen:     sumIf(x => L(x) === 'initial screen' || L(x) === 'recruiter screen'),
      who:                sumIf(x => L(x).startsWith('who')),
      case_study:         sumIf(x => L(x).includes('case study')),
      onsite:             sumIf(x => L(x).includes('final interview') || L(x).includes('call with martin') || L(x) === 'onsite'),
      culture:            sumIf(x => L(x).includes('culture')),
      call_w_client:      sumIf(x => L(x).includes('call with client') || L(x).includes('client prep') || L(x).includes('presented to client')),
      offered:            sumIf(x => L(x).includes('offer')),
      hired:              ashbyHires.filter(r => inJob(r) && inWindow(r.iso_week)).reduce((s, r) => s + r.count, 0),
    };
  }, [ashbyFunnel, ashbyHires, ashbyActive, ashbyJobIdSet, jobFilter, windowFilter, maxWeek]);

  const sourcingStages = [
    { label: 'Contacted',         n: totals.contacted },
    { label: 'Positive response', n: totals.pos_response },
    { label: 'Recruiter screens', n: totals.rec_screens },
    { label: 'Actual screens',    n: totals.actual_screens },
    { label: 'Moved to ATS',      n: totals.ats },
  ];
  const ashbyStages = ashbyStageMap ? [
    { label: 'Application Review', n: ashbyStageMap.application_review },
    { label: 'Initial Screen',     n: ashbyStageMap.initial_screen },
    { label: 'WHO',                n: ashbyStageMap.who },
    { label: 'Case Study',         n: ashbyStageMap.case_study },
    { label: 'Onsite',             n: ashbyStageMap.onsite },
    { label: 'Culture interview',  n: ashbyStageMap.culture },
    { label: 'Call with client',   n: ashbyStageMap.call_w_client },
    { label: 'Offer',              n: ashbyStageMap.offered },
    { label: 'Hired',              n: ashbyStageMap.hired },
  ] : [];
  const maxN = Math.max(1, totals.contacted);
  const ashbyMax = Math.max(1, ...ashbyStages.map(s => s.n));
  const dqTotal = dqReasonAgg.reduce((s, r) => s + r.count, 0);
  const dqMax = Math.max(1, ...dqReasonAgg.map(r => r.count));

  const windowOptions = [
    ['this_week', `This week (W${thisWeek})`],
    ['last_week', `Last week (W${lastWeek})`],
    ['last4',     'Last 4 weeks'],
    ['last12',    'Last 12 weeks'],
    ['last26',    'Last 6 months'],
    ['ytd',       'Year to date'],
    ['all',       'All time'],
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-2xl font-bold text-white">Internal Recruiting</h2>
          <p className="text-sm text-gray-400 mt-1">
          Tribe.xyz (IR) jobs &middot; Sourcing funnel (Bubble) + Ashby funnel {(jobFilter === 'all' ? hasAshby : ashbyJobIdSet.has(jobFilter)) ? <span className="text-teal-400">linked ✓</span> : <span className="text-gray-500">— not linked to Ashby</span>}
        </p>
        </div>
        <div className="flex gap-2 flex-wrap items-center">
          <select value={jobFilter} onChange={e => setJobFilter(e.target.value)}
            className="px-3 py-1 text-sm rounded-md bg-gray-800 border border-gray-700 text-gray-200 hover:border-gray-600 focus:outline-none focus:border-gray-500">
            <option value="all">All jobs ({jobOptions.length})</option>
            {jobOptions.map(j => (
              <option key={j.job_id} value={j.job_id}>{j.job_title} ({j.days_open}d)</option>
            ))}
          </select>
          <select value={windowFilter} onChange={e => setWindowFilter(e.target.value)}
            className="px-3 py-1 text-sm rounded-md bg-gray-800 border border-gray-700 text-gray-200 hover:border-gray-600 focus:outline-none focus:border-gray-500">
            {windowOptions.map(([v, label]) => (
              <option key={v} value={v}>{label}</option>
            ))}
          </select>
          <select value={sourcerFilter} onChange={e => setSourcerFilter(e.target.value)}
            className="px-3 py-1 text-sm rounded-md bg-gray-800 border border-gray-700 text-gray-200 hover:border-gray-600 focus:outline-none focus:border-gray-500">
            <option value="all">All sourcers</option>
            {[...new Set(sourcedRows.map(r => r.sourcer))].sort().map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <select value={taFilter} onChange={e => setTaFilter(e.target.value)}
            className="px-3 py-1 text-sm rounded-md bg-gray-800 border border-gray-700 text-gray-200 hover:border-gray-600 focus:outline-none focus:border-gray-500">
            <option value="all">All TAs</option>
            {[...new Set(interviewedRows.map(r => r.ta))].sort().map(t => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
          {(jobFilter !== 'all' || windowFilter !== 'last4' || sourcerFilter !== 'all' || taFilter !== 'all' || highlightSourcer || highlightTA) && (
            <button onClick={() => { setJobFilter('all'); setWindowFilter('last4'); setSourcerFilter('all'); setTaFilter('all'); setHighlightSourcer(null); setHighlightTA(null); }}
              className="px-3 py-1 text-sm rounded-md bg-blue-600 hover:bg-blue-500 text-white">
              Clear filters
            </button>
          )}
        </div>
      </div>

      {selectedJob && (
        <div className="bg-blue-900 bg-opacity-30 border border-blue-700 rounded-lg px-4 py-3 text-sm text-blue-100">
          <span className="font-medium">{selectedJob.job_title}</span>
          <span className="text-blue-300"> &middot; {selectedJob.days_open} days open &middot; {selectedJob.job_recruiter} (TA) &middot; {selectedJob.job_sourcer} (sourcer)</span>
        </div>
      )}
      {(highlightSourcerData || highlightTAData) && (
        <div className="bg-amber-900 bg-opacity-30 border border-amber-700 rounded-lg px-4 py-3 text-sm text-amber-100 flex items-center justify-between">
          <span>
            {highlightSourcerData && (
              <>Highlighting <span className="font-medium">{highlightSourcerData.sourcer}</span>: contributed {highlightSourcerData.contacted.toLocaleString()} contacted / {highlightSourcerData.pos_response} positive response / {highlightSourcerData.hired} hired
                {totals.contacted > 0 && ` (${(highlightSourcerData.contacted / totals.contacted * 100).toFixed(1)}% of filtered total)`}.</>
            )}
            {highlightTAData && (
              <>Highlighting <span className="font-medium">{highlightTAData.ta}</span>: conducted {highlightTAData.actual_screens} actual screens
                {totals.actual_screens > 0 && ` (${(highlightTAData.actual_screens / totals.actual_screens * 100).toFixed(1)}% of filtered total)`}.</>
            )}
          </span>
          <button onClick={() => { setHighlightSourcer(null); setHighlightTA(null); }}
            className="text-amber-300 hover:text-white text-xs">clear</button>
        </div>
      )}

      <div className="grid grid-cols-5 gap-3">
        {[
          ['Active jobs', jobOptions.length],
          ['Contacted', totals.contacted.toLocaleString()],
          ['Actual screens', totals.actual_screens],
          ['Sourced hired', sourcedHired],
          ['DQ reasons logged', dqTotal],
        ].map(([k, v], i) => (
          <div key={i} className="bg-gray-800 rounded-lg px-4 py-3 border border-gray-700">
            <div className="text-xs text-gray-400">{k}</div>
            <div className="text-2xl font-semibold text-white mt-1">{v}</div>
          </div>
        ))}
      </div>

      <div className="grid gap-3" style={{gridTemplateColumns: '1.6fr 1fr'}}>
        <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
          <div className="text-sm font-medium text-white mb-1">Sourcing funnel <span className="text-xs text-gray-500 font-normal">· Bubble (outbound)</span></div>
          <div className="space-y-1">
            {sourcingStages.map((st, i) => {
              const pct = maxN > 0 ? Math.max(0.04, st.n / maxN) : 0.04;
              const pctOfTop = totals.contacted > 0 ? (st.n / totals.contacted * 100) : 0;
              let overlay = 0;
              if (highlightSourcerData) {
                if (st.label === 'Contacted')              overlay = highlightSourcerData.contacted;
                else if (st.label === 'Positive response') overlay = highlightSourcerData.pos_response;
              } else if (highlightTAData && st.label === 'Actual screens') {
                overlay = highlightTAData.actual_screens;
              }
              const overlayPct = st.n > 0 ? Math.max(0, Math.min(1, overlay / st.n)) : 0;
              const colors = ['bg-blue-300','bg-blue-400','bg-blue-500','bg-blue-600','bg-blue-700'];
              return (
                <div key={i} className="grid items-center gap-2" style={{gridTemplateColumns: '120px 1fr 50px'}}>
                  <span className="text-xs text-gray-400 text-right">{st.label}</span>
                  <div className="flex justify-center">
                    <div className={`${st.n === 0 ? 'bg-gray-700' : colors[i]} rounded relative`}
                         style={{height: '20px', width: `${(pct * 100).toFixed(1)}%`, minWidth: '36px', display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
                      <span className={`text-xs font-medium ${st.n === 0 ? 'text-gray-500' : 'text-white'} relative z-10`}>{st.n.toLocaleString()}</span>
                      {overlay > 0 && (<div className="absolute left-0 top-0 h-full bg-amber-400 rounded" style={{width: `${(overlayPct * 100).toFixed(1)}%`, opacity: 0.5}} />)}
                    </div>
                  </div>
                  <span className="text-xs text-gray-500">{pctOfTop < 1 ? pctOfTop.toFixed(1) : pctOfTop.toFixed(0)}%</span>
                </div>
              );
            })}
          </div>

          <div className="text-sm font-medium text-white mt-5 mb-1">Ashby funnel <span className="text-xs text-gray-500 font-normal">· interviews · all sources</span></div>
          {ashbyStages.length === 0 ? (
            <div className="text-xs text-gray-500 italic py-2">No Ashby data for this selection{jobFilter === 'all' ? '' : ' (job not linked to Ashby)'}.</div>
          ) : (
          <div className="space-y-1">
            {ashbyStages.map((st, i) => {
              const top = ashbyStages[0] ? ashbyStages[0].n : 0;
              const pct = ashbyMax > 0 ? Math.max(0.04, st.n / ashbyMax) : 0.04;
              const pctOfTop = top > 0 ? (st.n / top * 100) : 0;
              return (
                <div key={i} className="grid items-center gap-2" style={{gridTemplateColumns: '120px 1fr 50px'}}>
                  <span className="text-xs text-gray-400 text-right">{st.label}</span>
                  <div className="flex justify-center">
                    <div className={`${st.n === 0 ? 'bg-gray-700' : 'bg-teal-600'} rounded relative`}
                         style={{height: '20px', width: `${(pct * 100).toFixed(1)}%`, minWidth: '36px', display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
                      <span className={`text-xs font-medium ${st.n === 0 ? 'text-gray-500' : 'text-white'} relative z-10`}>{st.n.toLocaleString()}</span>
                    </div>
                  </div>
                  <span className="text-xs text-gray-500">{pctOfTop < 1 ? pctOfTop.toFixed(1) : pctOfTop.toFixed(0)}%</span>
                </div>
              );
            })}
          </div>
          )}
          <div className="text-xs text-gray-500 mt-3"><span className="text-blue-300">Sourcing</span> = outbound effort (Bubble) · <span className="text-teal-400">Ashby</span> = interview funnel from all sources. These track largely different candidates — don\'t sum across them.</div>
        </div>

        <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
          <div className="text-sm font-medium text-white mb-3">Active jobs ({jobOptions.length})</div>
          <CsvBtn fname="ir_active_jobs" />
          <table className="w-full text-xs">
            <thead><tr className="text-gray-400 text-left">
              <th className="pb-2 font-normal">Job</th>
              <th className="pb-2 font-normal text-right">Days</th>
              <th className="pb-2 font-normal text-right">Hired</th>
            </tr></thead>
            <tbody>
              {jobOptions.map(j => (
                <tr key={j.job_id}
                    onClick={() => setJobFilter(jobFilter === j.job_id ? 'all' : j.job_id)}
                    className={`border-t border-gray-700 cursor-pointer hover:bg-gray-700 ${jobFilter === j.job_id ? 'bg-blue-900 bg-opacity-40' : ''}`}>
                  <td className="py-1 text-gray-200 truncate" title={j.in_ashby && !j.in_bubble ? `${j.job_title} (Ashby only — not tagged Tribe.xyz (IR) in Bubble)` : j.job_title}>
                    {j.job_title}
                    {j.in_ashby && !j.in_bubble && <span className="ml-1 text-xs text-amber-400" title="Ashby only — Bubble sourcing data unavailable">⚠</span>}
                  </td>
                  <td className="py-1 text-right text-gray-300">{j.days_open}</td>
                  <td className="py-1 text-right text-gray-500">{j.hires_total}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="text-xs text-gray-500 mt-2">Click a row to filter &middot; ⚠ = Ashby-only (not yet tagged Tribe.xyz (IR) in Bubble — sourcing data won&apos;t show)</div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
          <div className="flex items-center justify-between mb-3">
            <div className="text-sm font-medium text-white">Sourced by ({sourcedAgg.length})</div>
            <div className="text-xs text-gray-500">Click row to highlight in funnel</div>
          </div>
          <CsvBtn fname="ir_sourced_by" />
          <table className="w-full text-xs">
            <thead><tr className="text-gray-400">
              <th className="pb-2 font-normal text-left">Sourcer</th>
              <th className="pb-2 font-normal text-right">Contacted</th>
              <th className="pb-2 font-normal text-right">+Resp</th>
              <th className="pb-2 font-normal text-right">Hired</th>
            </tr></thead>
            <tbody>
              {sourcedAgg.map((r, i) => (
                <tr key={i}
                    onClick={() => setHighlightSourcer(highlightSourcer === r.sourcer ? null : r.sourcer)}
                    className={`border-t border-gray-700 cursor-pointer hover:bg-gray-700 ${highlightSourcer === r.sourcer ? 'bg-amber-900 bg-opacity-40' : ''}`}>
                  <td className="py-1 text-gray-200">{r.sourcer}</td>
                  <td className="py-1 text-right text-gray-300">{r.contacted.toLocaleString()}</td>
                  <td className="py-1 text-right text-gray-300">{r.pos_response.toLocaleString()}</td>
                  <td className="py-1 text-right text-gray-500">{r.hired}</td>
                </tr>
              ))}
              <tr className="border-t-2 border-gray-600">
                <td className="py-1 font-medium text-white">Total</td>
                <td className="py-1 text-right font-medium text-white">{totals.contacted.toLocaleString()}</td>
                <td className="py-1 text-right font-medium text-white">{totals.pos_response.toLocaleString()}</td>
                <td className="py-1 text-right font-medium text-white">{sourcedHired}</td>
              </tr>
            </tbody>
          </table>
          <div className="text-xs text-gray-500 mt-2 italic">Sourcer attribution from event.who_created_event_first. Deactivated sourcers may show reduced historicals.</div>
        </div>

        <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
          <div className="flex items-center justify-between mb-3">
            <div className="text-sm font-medium text-white">Interviewed by &middot; Actual Screens</div>
            <div className="text-xs text-gray-500">Click to highlight</div>
          </div>
          <CsvBtn fname="ir_interviewed_by" />
          <table className="w-full text-xs">
            <thead><tr className="text-gray-400">
              <th className="pb-2 font-normal text-left">TA</th>
              <th className="pb-2 font-normal text-right">Actual Screens</th>
            </tr></thead>
            <tbody>
              {interviewedAgg.map((r, i) => (
                <tr key={i}
                    onClick={() => setHighlightTA(highlightTA === r.ta ? null : r.ta)}
                    className={`border-t border-gray-700 cursor-pointer hover:bg-gray-700 ${highlightTA === r.ta ? 'bg-amber-900 bg-opacity-40' : ''}`}>
                  <td className={`py-1 ${r.ta === '(unattributed)' ? 'text-gray-500 italic' : 'text-gray-200'}`}>{r.ta}</td>
                  <td className={`py-1 text-right ${r.ta === '(unattributed)' ? 'text-gray-500' : 'text-gray-300'}`}>{r.actual_screens}</td>
                </tr>
              ))}
              <tr className="border-t-2 border-gray-600">
                <td className="py-1 font-medium text-white">Total</td>
                <td className="py-1 text-right font-medium text-white">{interviewedAgg.reduce((s,r) => s + r.actual_screens, 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
        <div className="text-sm font-medium text-white mb-3">Weekly performance ({weeklyAgg.length} weeks shown)</div>
        {weeklyAgg.length === 0 ? (
          <div className="text-xs text-gray-500 italic">No data for the selected filter.</div>
        ) : (<>
          <CsvBtn fname="ir_weekly_performance" />
          <table className="w-full text-xs">
            <thead><tr className="text-gray-400">
              <th className="pb-2 font-normal text-left">Week</th>
              <th className="pb-2 font-normal text-right">Contacted</th>
              <th className="pb-2 font-normal text-right">Positive Response</th>
              <th className="pb-2 font-normal text-right">Recruiter Screens</th>
              <th className="pb-2 font-normal text-right">Actual Screens</th>
              <th className="pb-2 font-normal text-right">Moved to ATS</th>
              <th className="pb-2 font-normal text-right">Onsite</th>
              <th className="pb-2 font-normal text-right">Culture Interview</th>
              <th className="pb-2 font-normal text-right">Call with Client</th>
              <th className="pb-2 font-normal text-right">Offered</th>
              <th className="pb-2 font-normal text-right">Hired</th>
            </tr></thead>
            <tbody>
              {weeklyAgg.map((w, i) => (
                <tr key={i} className="border-t border-gray-700">
                  <td className="py-1 text-gray-300">W{w.iso_week}</td>
                  {['contacted','pos_response','rec_screens','actual_screens','ats','onsite','culture','call_w_client','offered','hired'].map(k => (
                    <td key={k} className={`py-1 text-right ${w[k] === 0 ? 'text-gray-600' : 'text-gray-300'}`}>{w[k] === 0 ? '—' : w[k]}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table></>
        )}
      </div>

      <div className="grid gap-3" style={{gridTemplateColumns: '1.2fr 1fr'}}>
      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
        <div className="text-sm font-medium text-white mb-3">Disqualified by stage (per job) &middot; total {f_dqByStage.reduce((s, r) => s + r.total, 0)}</div>
        {f_dqByStage.length === 0 ? (
          <div className="text-xs text-gray-500 italic">No DQs logged for the selected filter.</div>
        ) : (<>
          <CsvBtn fname="ir_dq_by_stage" />
          <table className="w-full text-xs">
            <thead><tr className="text-gray-400">
              <th className="pb-2 font-normal text-left">Job</th>
              <th className="pb-2 font-normal text-right">Contacted</th>
              <th className="pb-2 font-normal text-right">Recruiter Screen</th>
              <th className="pb-2 font-normal text-right">Actual Screen</th>
              <th className="pb-2 font-normal text-right">Moved to ATS</th>
              <th className="pb-2 font-normal text-right">Onsite</th>
              <th className="pb-2 font-normal text-right">Offer</th>
              <th className="pb-2 font-normal text-right">Total</th>
            </tr></thead>
            <tbody>
              {f_dqByStage.map((j, i) => (
                <tr key={i} className="border-t border-gray-700">
                  <td className="py-1 text-gray-200 truncate" title={j.job_title}>{j.job_title}</td>
                  <td className="py-1 text-right text-gray-300">{j.stage_contacted || '—'}</td>
                  <td className="py-1 text-right text-gray-300">{j.stage_rec_screen || '—'}</td>
                  <td className="py-1 text-right text-gray-300">{j.stage_actual_screen || '—'}</td>
                  <td className="py-1 text-right text-gray-300">{j.stage_move_to_ats || '—'}</td>
                  <td className="py-1 text-right text-gray-300">{j.stage_onsite || '—'}</td>
                  <td className="py-1 text-right text-gray-300">{j.stage_offer || '—'}</td>
                  <td className="py-1 text-right font-medium text-white">{j.total}</td>
                </tr>
              ))}
            </tbody>
          </table></>
        )}
      </div>

      <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
        <div className="text-sm font-medium text-white mb-1">Disqualified reasons ({(ashbyDQReasons.length > 0 ? ashbyDQReasons : dqReasonAgg).length})</div>
        <div className="text-xs text-gray-500 italic mb-3">{ashbyDQReasons.length > 0 ? `From Ashby archive_reason · ${ashbyDQReasons.reduce((s,r) => s + r.count, 0)} total archives` : `From candidate.reason_not_interested · ${dqTotal} total DQs · v2 will use Ashby's structured taxonomy`}</div>
        {dqReasonAgg.length === 0 ? (
          <div className="text-xs text-gray-500 italic">No DQ reasons for the selected filter.</div>
        ) : (
          <div className="space-y-1.5">
            {(ashbyDQReasons.length > 0 ? ashbyDQReasons : dqReasonAgg).map((r, i, arr) => {
              const localMax = Math.max(1, ...arr.map(x => x.count));
              const localTotal = arr.reduce((s, x) => s + x.count, 0) || 1;
              const pct = (r.count / localMax * 100).toFixed(1);
              const pctOfTotal = (r.count / localTotal * 100).toFixed(1);
              return (
                <div key={i} className="grid items-center gap-2" style={{gridTemplateColumns: '180px 1fr 70px'}}>
                  <span className="text-xs text-gray-200" title={r.reason}>{r.reason}</span>
                  <div className="bg-blue-400 rounded" style={{height: '12px', width: `${pct}%`, minWidth: '4px'}} />
                  <span className="text-xs text-gray-400 text-right">{r.count} &middot; {pctOfTotal}%</span>
                </div>
              );
            })}
          </div>
        )}
      </div>
      </div>
    </div>
  );
};

// Profitability Tab — director-only. Embedded port of the finance dashboard's
// Client Profitability card. Own month dropdown (defaults to the latest period
// in the snapshot); no drill-down. Data file regenerated from finance
// dashboard-src/data.json by recruiting-pipeline/build_client_profitability.py.
const fmtEUR = (n) => {
  if (n == null || Number.isNaN(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `€${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `€${Math.round(n / 1_000)}K`;
  return `€${Math.round(n)}`;
};
const BU_BADGE_COLORS = {
  Martin: '#3b82f6',
  Kristjana: '#10b981',
  Jacopo: '#f59e0b',
  Salem: '#8b5cf6',
  Tijana: '#ec4899',
};
const formatPeriodLabel = (ym) => {
  if (!ym) return '';
  const [y, m] = ym.split('-');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${months[parseInt(m, 10) - 1]} ${y}`;
};

const ProfitabilityTab = () => {
  const D = clientProfitabilityData;
  const periods = useMemo(() => (D.periods || []).slice().sort(), []);
  const latestPeriod = periods.length ? periods[periods.length - 1] : null;
  const [selectedPeriod, setSelectedPeriod] = useState(latestPeriod);
  const [sort, setSort] = useState({ col: 'rev', asc: false });

  const computed = useMemo(() => {
    const pf = selectedPeriod;
    if (!pf) return { rows: [], totals: null };
    const agg = {};
    // Revenue from cr (per-client revenue ledger). NET = r + v (v is negative
    // vacation deduction). Matches finance dashboard KPI.
    (D.cr || []).filter(c => c.p === pf).forEach(c => {
      if (!agg[c.c]) agg[c.c] = { client: c.c, bu: c.bu || '', rev: 0, directCost: 0, sourcingCost: 0 };
      agg[c.c].rev += (c.r || 0) + (c.v || 0);
      if (c.bu) agg[c.c].bu = c.bu;
    });
    // Cost from ea (Client rows only, filtered upstream by build script).
    (D.ea || []).filter(e => e.p === pf).forEach(e => {
      if (!agg[e.d]) agg[e.d] = { client: e.d, bu: e.bu || '', rev: 0, directCost: 0, sourcingCost: 0 };
      const a = agg[e.d];
      // Same rule as finance dashboard (changed 2026-06-23): Direct Cost = anyone
      // assigned to this client — alloc_type 'default' (BambooHR division IS the client),
      // 'sourcer_client' (job history shows them on it), or 'ta' (assigned via the TA tab
      // for holiday cover / notice-period) — or anyone billing it (rev>0, safety net).
      // Sourcing Cost = bench sourcers ('sourcer_bench') with no revenue, spread by %.
      const at = e.alloc_type;
      if (at === 'default' || at === 'sourcer_client' || at === 'ta' || (e.rev || 0) > 0) a.directCost += (e.pr || 0);
      else a.sourcingCost += (e.pr || 0);
      if (e.bu) a.bu = e.bu;
    });
    const rowList = Object.values(agg)
      .filter(r => r.rev > 0 || r.directCost > 0 || r.sourcingCost > 0)
      .map(r => {
        const directProfit = r.rev - r.directCost;
        const directMargin = r.rev > 0 ? Math.round((directProfit / r.rev) * 100) : 0;
        const netProfit = directProfit - r.sourcingCost;
        const netMargin = r.rev > 0 ? Math.round((netProfit / r.rev) * 100) : 0;
        return { ...r, directProfit, directMargin, netProfit, netMargin };
      })
      .sort((a, b) => {
        const { col, asc } = sort;
        let va = typeof a[col] === 'number' ? a[col] : (a[col] || '').toString().toLowerCase();
        let vb = typeof b[col] === 'number' ? b[col] : (b[col] || '').toString().toLowerCase();
        if (va < vb) return asc ? -1 : 1;
        if (va > vb) return asc ? 1 : -1;
        return 0;
      });
    const tot = rowList.reduce((s, r) => ({
      rev: s.rev + r.rev,
      directCost: s.directCost + r.directCost,
      directProfit: s.directProfit + r.directProfit,
      sourcingCost: s.sourcingCost + r.sourcingCost,
      netProfit: s.netProfit + r.netProfit,
    }), { rev: 0, directCost: 0, directProfit: 0, sourcingCost: 0, netProfit: 0 });
    tot.directMargin = tot.rev > 0 ? Math.round((tot.directProfit / tot.rev) * 100) : 0;
    tot.netMargin = tot.rev > 0 ? Math.round((tot.netProfit / tot.rev) * 100) : 0;
    return { rows: rowList, totals: tot };
  }, [selectedPeriod, sort]);

  const directMarginColor = (m) => m >= 60 ? 'text-green-400' : m >= 55 ? 'text-yellow-400' : 'text-red-400';

  const cols = [
    { col: 'client', l: 'Client' },
    { col: 'bu', l: 'BU' },
    { col: 'rev', l: 'Revenue', r: true },
    { col: 'directCost', l: 'Direct Cost', r: true },
    { col: 'directProfit', l: 'Direct Profit', r: true },
    { col: 'directMargin', l: 'Margin %', r: true },
    { col: 'sourcingCost', l: 'Sourcing Cost', r: true },
    { col: 'netMargin', l: 'Net Margin %', r: true },
  ];

  const onHeaderClick = (h) =>
    setSort(s => s.col === h.col ? { col: h.col, asc: !s.asc } : { col: h.col, asc: h.r ? false : true });

  if (!periods.length) {
    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-white mb-1">Client Profitability</h3>
        <p className="text-xs text-gray-500">No data available.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="bg-gray-800 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
          <div>
            <h3 className="text-lg font-semibold text-white">
              Client Profitability — {formatPeriodLabel(selectedPeriod)}
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Director-only view. Synced from the finance dashboard (revenue is NET — gross minus vacation).
            </p>
          </div>
          <div className="flex items-center gap-2">
            <label className="text-gray-400 text-xs">Month:</label>
            <select
              value={selectedPeriod || ''}
              onChange={(e) => setSelectedPeriod(e.target.value)}
              className="bg-gray-700 border border-gray-600 rounded-lg px-2 py-1.5 text-sm text-white min-w-[140px]"
            >
              {periods.slice().reverse().map(p => (
                <option key={p} value={p}>{formatPeriodLabel(p)}</option>
              ))}
            </select>
          </div>
        </div>

        <div className="overflow-x-auto max-h-[640px] overflow-y-auto">
          <CsvBtn fname="profitability_by_client" />
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-gray-800 z-10">
              <tr className="text-gray-400 border-b border-gray-700">
                {cols.map(h => (
                  <th
                    key={h.col}
                    className={`py-2 px-2 cursor-pointer select-none hover:text-blue-400 transition-colors ${h.r ? 'text-right' : 'text-left'}`}
                    onClick={() => onHeaderClick(h)}
                  >
                    {h.l} {sort.col === h.col ? (sort.asc ? '▲' : '▼') : <span className="text-gray-600">⇅</span>}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {computed.rows.map((r, i) => (
                <tr key={i} className="border-b border-gray-800 hover:bg-gray-700">
                  <td className="py-1.5 px-2 font-medium text-white">{r.client}</td>
                  <td className="py-1.5 px-2">
                    {r.bu && (
                      <span
                        className="px-2 py-0.5 rounded-full text-xs"
                        style={{
                          backgroundColor: (BU_BADGE_COLORS[r.bu] || '#64748b') + '25',
                          color: BU_BADGE_COLORS[r.bu] || '#94a3b8',
                        }}
                      >
                        {r.bu}
                      </span>
                    )}
                  </td>
                  <td className="py-1.5 px-2 text-right text-blue-400">{fmtEUR(r.rev)}</td>
                  <td className="py-1.5 px-2 text-right text-red-400">{fmtEUR(r.directCost)}</td>
                  <td className={`py-1.5 px-2 text-right font-medium ${r.directProfit >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmtEUR(r.directProfit)}</td>
                  <td className={`py-1.5 px-2 text-right ${directMarginColor(r.directMargin)}`}>{r.directMargin}%</td>
                  <td className="py-1.5 px-2 text-right text-orange-400">{fmtEUR(r.sourcingCost)}</td>
                  <td className={`py-1.5 px-2 text-right ${directMarginColor(r.netMargin)}`}>{r.netMargin}%</td>
                </tr>
              ))}
              {computed.totals && (
                <tr className="bg-gray-800 font-semibold border-t-2 border-gray-600">
                  <td className="py-2 px-2 text-white">Total ({computed.rows.length} clients)</td>
                  <td></td>
                  <td className="py-2 px-2 text-right text-blue-400">{fmtEUR(computed.totals.rev)}</td>
                  <td className="py-2 px-2 text-right text-red-400">{fmtEUR(computed.totals.directCost)}</td>
                  <td className={`py-2 px-2 text-right ${computed.totals.directProfit >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmtEUR(computed.totals.directProfit)}</td>
                  <td className={`py-2 px-2 text-right ${directMarginColor(computed.totals.directMargin)}`}>{computed.totals.directMargin}%</td>
                  <td className="py-2 px-2 text-right text-orange-400">{fmtEUR(computed.totals.sourcingCost)}</td>
                  <td className={`py-2 px-2 text-right ${directMarginColor(computed.totals.netMargin)}`}>{computed.totals.netMargin}%</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <p className="text-gray-500 text-xs mt-2">
          Direct Cost = everyone assigned to the client (placed staff, holiday cover, notice-period wind-down, or anyone billing) even before revenue starts. Direct Profit = Revenue − Direct Cost. Sourcing Cost = bench sourcers whose spare time is spread across clients. Net Margin = (Direct Profit − Sourcing Cost) ÷ Revenue. Overhead is excluded.
        </p>
      </div>
    </div>
  );
};

// Main Dashboard

// ── Candidate Finder ─────────────────────────────────────────────────────────
// Lazy-loads /finder_data.json.gz (engaged candidates, ~92k) only when the tab
// opens. Cascading filters + searchable table. Data built by the candidate_finder
// Snowflake transform -> render_json (build_finder) -> finder_data.json.gz.
const FINDER_KEYS = ['function', 'role_type', 'client', 'country', 'stage', 'reason'];
const FINDER_FILTERS = [['function', 'Function'], ['role_type', 'Role type'], ['client', 'Client'], ['country', 'Country'], ['stage', 'Stage'], ['reason', 'Reason']];
const FINDER_CSV_CAP = 5000;
const finderStageClass = (s) => ({
  'Recruiter Screen': 'bg-blue-900 text-blue-200',
  'Offsite': 'bg-teal-900 text-teal-200',
  'Final Interview': 'bg-purple-900 text-purple-200',
  'Offer': 'bg-amber-900 text-amber-200',
  'Hired': 'bg-green-900 text-green-200',
}[s] || 'bg-gray-700 text-gray-200');

const finderCsvCell = (v) => {
  const t = (v == null ? '' : String(v));
  return /[",\n]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t;
};

const FinderMultiSelect = ({ fkey, label, options, selected, onToggle, onClear, isOpen, onOpen }) => {
  const [q, setQ] = useState('');
  const shown = q ? options.filter((o) => o.toLowerCase().includes(q.toLowerCase())) : options;
  const btnText = selected.length === 0 ? label : selected.length === 1 ? selected[0] : `${label}: ${selected.length}`;
  return (
    <div className="relative" data-ms={fkey}>
      <button type="button" onClick={onOpen}
        className="w-full text-left bg-gray-800 border border-gray-700 text-sm rounded px-2 py-1.5 flex items-center justify-between">
        <span className={`truncate ${selected.length ? 'text-white' : 'text-gray-400'}`}>{btnText}</span>
        <span className="text-gray-500 ml-2 shrink-0">▾</span>
      </button>
      {isOpen && (
        <div className="absolute z-30 mt-1 w-full bg-gray-800 border border-gray-700 rounded shadow-xl max-h-72 overflow-hidden flex flex-col">
          <div className="p-2 border-b border-gray-700 flex gap-2 items-center">
            <input autoFocus value={q} onChange={(e) => setQ(e.target.value)} placeholder="Filter…"
              className="flex-1 bg-gray-900 border border-gray-700 text-white text-xs rounded px-2 py-1" />
            {selected.length > 0 && <button type="button" onClick={onClear} className="text-xs text-blue-400 hover:underline shrink-0">Clear</button>}
          </div>
          <div className="overflow-y-auto">
            {shown.length === 0 && <div className="px-3 py-2 text-xs text-gray-500">No matches</div>}
            {shown.map((o) => (
              <label key={o} className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-200 hover:bg-gray-700/50 cursor-pointer">
                <input type="checkbox" checked={selected.includes(o)} onChange={() => onToggle(o)} />
                <span className="truncate">{o}</span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

const CandidateFinderTab = () => {
  const [rows, setRows] = useState(null);
  const [err, setErr] = useState(null);
  const [sel, setSel] = useState({ function: [], role_type: [], client: [], country: [], stage: [], reason: [] });
  const [q, setQ] = useState('');
  const [onlyLi, setOnlyLi] = useState(false);
  const [limit, setLimit] = useState(300);
  const [openKey, setOpenKey] = useState(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch('finder_data.json.gz', { cache: 'no-cache' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const buf = await res.arrayBuffer();
        let text;
        try {
          if (typeof DecompressionStream === 'undefined') throw new Error('no DecompressionStream');
          const stream = new Response(buf).body.pipeThrough(new DecompressionStream('gzip'));
          text = await new Response(stream).text();
        } catch (_) {
          text = new TextDecoder().decode(buf);
        }
        const obj = JSON.parse(text);
        if (!cancelled) setRows(obj.candidates || []);
      } catch (e) {
        if (!cancelled) setErr(String(e && e.message ? e.message : e));
      }
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    const onDoc = (e) => { if (!e.target || !e.target.closest || !e.target.closest('[data-ms]')) setOpenKey(null); };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);

  if (err) return <div className="text-gray-400">Couldn't load finder data: <span className="font-mono text-gray-500">{err}</span></div>;
  if (!rows) return <div className="text-gray-400">Loading candidates…</div>;

  const toggleVal = (k, v) => setSel((prev) => ({ ...prev, [k]: prev[k].includes(v) ? prev[k].filter((x) => x !== v) : [...prev[k], v] }));
  const clearKey = (k) => setSel((prev) => ({ ...prev, [k]: [] }));
  const clearAll = () => { setSel({ function: [], role_type: [], client: [], country: [], stage: [], reason: [] }); setQ(''); setOnlyLi(false); };
  const matchExcept = (r, skip) => FINDER_KEYS.every((k) => k === skip || sel[k].length === 0 || sel[k].includes(r[k]));
  const optsFor = (k) => [...new Set(rows.filter((r) => matchExcept(r, k)).map((r) => r[k]).filter(Boolean))].sort();

  const out = rows.filter((r) => {
    if (!FINDER_KEYS.every((k) => sel[k].length === 0 || sel[k].includes(r[k]))) return false;
    if (onlyLi && !r.linkedin) return false;
    if (q) {
      const blob = ((r.name || '') + ' ' + (r.current_title || '') + ' ' + (r.company || '') + ' ' + (r.sourced_role || '') + ' ' + (r.role_type || '')).toLowerCase();
      if (!blob.includes(q.toLowerCase())) return false;
    }
    return true;
  });
  const shown = out.slice(0, limit);
  const withLi = out.filter((r) => r.linkedin).length;
  const anyFilter = FINDER_KEYS.some((k) => sel[k].length) || q || onlyLi;
  const canExport = out.length > 0 && out.length <= FINDER_CSV_CAP;

  const exportCsv = () => {
    if (!canExport) return;
    const cols = ['name', 'current_title', 'company', 'location', 'country', 'function', 'role_type', 'client', 'sourced_role', 'stage', 'reason', 'linkedin'];
    const csv = [cols.join(','), ...out.map((r) => cols.map((c) => finderCsvCell(r[c])).join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'candidate_finder_' + new Date().toISOString().slice(0, 10) + '.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div>
      <p className="text-sm text-gray-400 mb-4">Candidates we have engaged (reached a recruiter screen or further). Filters are multi-select; pick several values in any of them. Names link to LinkedIn.</p>
      <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-3">
        {FINDER_FILTERS.map(([k, label]) => (
          <FinderMultiSelect key={k} fkey={k} label={label} options={optsFor(k)} selected={sel[k]}
            onToggle={(v) => toggleVal(k, v)} onClear={() => clearKey(k)}
            isOpen={openKey === k} onOpen={() => setOpenKey(openKey === k ? null : k)} />
        ))}
      </div>
      <div className="flex flex-wrap gap-4 items-center mb-3">
        <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search name, title, company…"
          className="bg-gray-800 border border-gray-700 text-white text-sm rounded px-3 py-1.5 w-72" />
        <label className="flex items-center gap-2 text-sm text-gray-300">
          <input type="checkbox" checked={onlyLi} onChange={(e) => setOnlyLi(e.target.checked)} /> Only with LinkedIn
        </label>
        {anyFilter && <button type="button" onClick={clearAll} className="text-sm text-blue-400 hover:underline">Clear all</button>}
        <div className="flex items-center gap-3 ml-auto">
          {!canExport && out.length > FINDER_CSV_CAP && <span className="text-xs text-gray-500">Filter to ≤{FINDER_CSV_CAP.toLocaleString()} to export ({out.length.toLocaleString()})</span>}
          <button type="button" onClick={exportCsv} disabled={!canExport}
            className={`text-sm rounded px-3 py-1.5 border ${canExport ? 'border-gray-600 text-white hover:bg-gray-800' : 'border-gray-800 text-gray-600 cursor-not-allowed'}`}>
            Export CSV
          </button>
          <div className="text-sm text-gray-400">
            <span className="text-white font-semibold">{out.length.toLocaleString()}</span> candidates · {withLi.toLocaleString()} with LinkedIn{out.length > limit ? ` · showing first ${limit}` : ''}
          </div>
        </div>
      </div>
      <div className="overflow-x-auto border border-gray-800 rounded-lg">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-400 text-xs uppercase border-b border-gray-800 bg-gray-800/40">
              <th className="text-left px-3 py-2">Candidate</th>
              <th className="text-left px-3 py-2">Current title</th>
              <th className="text-left px-3 py-2">Company</th>
              <th className="text-left px-3 py-2">Location</th>
              <th className="text-left px-3 py-2">Client</th>
              <th className="text-left px-3 py-2">Role (sourced for)</th>
              <th className="text-left px-3 py-2">Stage</th>
              <th className="text-left px-3 py-2">Reason</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((r, i) => (
              <tr key={i} className="border-b border-gray-800 hover:bg-gray-800/50">
                <td className="px-3 py-2 font-medium">
                  {r.linkedin ? <a href={'https://' + r.linkedin} target="_blank" rel="noreferrer" className="text-blue-400 hover:underline">{r.name}</a> : <span>{r.name}</span>}
                </td>
                <td className="px-3 py-2">{r.current_title || <span className="text-gray-600">—</span>}</td>
                <td className="px-3 py-2">{r.company || <span className="text-gray-600">—</span>}</td>
                <td className="px-3 py-2">{r.location || <span className="text-gray-600">unknown</span>}</td>
                <td className="px-3 py-2">{r.client || '—'}{r.role_type ? <div className="text-xs text-gray-500">{r.role_type}</div> : null}</td>
                <td className="px-3 py-2 text-gray-300">{r.sourced_role || '—'}</td>
                <td className="px-3 py-2"><span className={'px-2 py-0.5 rounded text-xs font-medium ' + finderStageClass(r.stage)}>{r.stage}</span></td>
                <td className="px-3 py-2 text-red-300 text-xs">{r.reason || ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {out.length > limit && (
        <button onClick={() => setLimit(limit + 500)} className="mt-3 text-sm text-blue-400 hover:underline">Show more ({(out.length - limit).toLocaleString()} hidden)</button>
      )}
    </div>
  );
};

const RecruitingDashboard = () => {
  // Leadership tabs (WBR/MBR) are gated by the Pages Functions auth flow:
  // /functions/api/login.ts sets a non-HttpOnly `tribe_role=leadership` cookie
  // for the 23 leadership emails. We also keep the legacy ?role=leadership URL
  // param as a fallback for direct testing.
  // Cookie-based role detection. Director > leadership > member.
  // Director cookie unlocks the Profitability tab in addition to WBR/MBR.
  const role = (() => {
    try {
      if (typeof document !== 'undefined') {
        const roleCookie = document.cookie
          .split(';')
          .map(c => c.trim())
          .find(c => c.startsWith('tribe_role='));
        if (roleCookie) return roleCookie.split('=')[1];
      }
      if (typeof window !== 'undefined') {
        return new URLSearchParams(window.location.search).get('role') || 'member';
      }
      return 'member';
    } catch (_) { return 'member'; }
  })();
  const isDirector = role === 'director';
  const isLeadership = isDirector || role === 'leadership';
  // New Project Health tab — strictly Blake + Jacopo via tribe_ph=1 cookie
  // (set by /functions/api/login.ts). ?ph=1 URL param kept as a test fallback.
  const canProjectHealth = (() => {
    try {
      if (typeof document !== 'undefined') {
        const c = document.cookie.split(';').map((x) => x.trim()).find((x) => x.startsWith('tribe_ph='));
        if (c && c.split('=')[1] === '1') return true;
      }
      if (typeof window !== 'undefined' && new URLSearchParams(window.location.search).get('ph') === '1') return true;
      return false;
    } catch (_) { return false; }
  })();
  let visibleTabs = isDirector
    ? ['project', 'weekly', 'wbr', 'mbr', 'profitability', 'tth', 'ts_summary', 'ir', 'finder']
    : isLeadership
      ? ['project', 'weekly', 'wbr', 'mbr', 'tth', 'ts_summary', 'ir', 'finder']
      : ['project', 'weekly', 'tth', 'ts_summary', 'ir', 'finder'];
  if (canProjectHealth) visibleTabs = [...visibleTabs, 'project_health'];
  const [activeTab, setActiveTab] = useState('project');
  // Load the heavy Snowflake data file at runtime from a gzipped /public asset
  // (see scripts/gzip-data.mjs). Keeps it out of the JS bundle so deploys stay
  // under Cloudflare's 25 MiB limit and the initial download is ~5MB not ~32MB.
  const [dashboardData, setDashboardData] = useState(null);
  const [dataError, setDataError] = useState(null);
  const [dataUpdatedAt, setDataUpdatedAt] = useState(null);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch('dashboard_data_snowflake.json.gz', { cache: 'no-cache' });
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const lastMod = res.headers.get('last-modified');
        const buf = await res.arrayBuffer();
        let text;
        try {
          if (typeof DecompressionStream === 'undefined') throw new Error('no DecompressionStream');
          const stream = new Response(buf).body.pipeThrough(new DecompressionStream('gzip'));
          text = await new Response(stream).text();
        } catch (_) {
          // Fallback: CDN may have transparently decompressed the asset.
          text = new TextDecoder().decode(buf);
        }
        const obj = JSON.parse(text);
        if (!cancelled) { setDashboardData(obj); if (lastMod) setDataUpdatedAt(lastMod); }
      } catch (e) {
        if (!cancelled) setDataError(String(e && e.message ? e.message : e));
      }
    })();
    return () => { cancelled = true; };
  }, []);
  // Snap-back: if state lands on a tab the current role doesn't see, fall back
  // to Project Dashboard. Covers both leadership-only and director-only tabs.
  const safeActiveTab =
    (!isLeadership && LEADERSHIP_TABS.has(activeTab)) ||
    (!isDirector && DIRECTOR_TABS.has(activeTab)) ||
    (!canProjectHealth && activeTab === 'project_health')
      ? 'project'
      : activeTab;
  if (dataError) {
    return (
      <div className="min-h-screen bg-gray-900 text-white flex items-center justify-center">
        <div className="text-center">
          <div className="text-lg mb-2">Couldn't load dashboard data</div>
          <div className="text-sm text-gray-400 font-mono">{dataError}</div>
        </div>
      </div>
    );
  }
  if (!dashboardData) {
    return (
      <div className="min-h-screen bg-gray-900 text-white flex items-center justify-center">
        <div className="text-gray-400">Loading dashboard data…</div>
      </div>
    );
  }
  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="bg-gray-800 border-b border-gray-700 px-6 py-6 flex items-start justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">Tribe.xyz Recruiting Dashboard</h1>
          <p className="text-sm text-gray-400 mt-1">Snowflake pipeline</p>
          <p className="text-xs text-gray-500 mt-1">
            Refreshes ~4×/day · data fresh by 09:00, 11:00, 14:00 &amp; 16:30 CET
            {dataUpdatedAt ? ` · last updated ${new Date(dataUpdatedAt).toLocaleString('en-GB', { timeZone: 'Europe/Berlin', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })} CET` : ''}
          </p>
        </div>
      </div>
      <div className="bg-gray-800 border-b border-gray-700 px-6">
        <div className="flex gap-8">
          {visibleTabs.map((tab) => (
            <button key={tab} onClick={() => setActiveTab(tab)}
              className={`py-4 px-2 font-medium border-b-2 transition-colors ${safeActiveTab === tab ? 'text-white border-white' : 'text-gray-400 border-transparent hover:text-gray-300'}`}>
              {tab === 'wbr' ? 'WBR' : tab === 'mbr' ? 'MBR' : tab === 'profitability' ? 'Profitability' : tab === 'project' ? 'Project Dashboard' : tab === 'weekly' ? 'Weekly Summary' : tab === 'tth' ? 'Time to Hire' : tab === 'ts_summary' ? 'KPI - TS Summary' : tab === 'ir' ? 'Internal Recruiting' : tab === 'finder' ? 'Candidate Finder' : 'New Project Health'}
            </button>
          ))}
        </div>
      </div>
      <div className="px-6 py-6">
        {safeActiveTab === 'wbr' && isLeadership && <WBRTab data={dashboardData} />}
        {safeActiveTab === 'mbr' && isLeadership && <MBRTab data={dashboardData} />}
        {safeActiveTab === 'profitability' && isDirector && <ProfitabilityTab />}
        {safeActiveTab === 'project' && <ProjectDashboardTab data={dashboardData} />}
        {safeActiveTab === 'weekly' && <WeeklySummaryTab data={dashboardData} />}
        {safeActiveTab === 'tth' && <TTHTab data={dashboardData} />}
        {safeActiveTab === 'ts_summary' && <TSSummaryTab data={dashboardData} />}
        {safeActiveTab === 'ir' && <IRTab data={dashboardData} />}
        {safeActiveTab === 'project_health' && canProjectHealth && <NewProjectHealthTab data={dashboardData} />}
        {safeActiveTab === 'finder' && <CandidateFinderTab />}
      </div>
    </div>
  );
};

export default RecruitingDashboard;

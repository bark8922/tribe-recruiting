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
    return Array.from(deduped.values()).sort((a, b) => {
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

    // Enrich all with actuals and job data, then drop rows with no activity
    // AND no comment for the week (same rule we apply to TA Detail). Catches
    // weekly-note placeholders like Mia Gjorgievska who show up on the roster
    // but have nothing to review.
    return Object.values(tsMap).map((t) => {
      const tsName = t.ts;
      const actuals = data.ts_actuals?.[tsName]?.[weekKey] || {};
      // Prefer the per-week ts_jobs_weekly (new pipeline metric) when present;
      // fall back to the static ts_jobs dict for older snapshots.
      const weeklyJobs = data.ts_jobs_weekly?.[weekKey]?.[tsName];
      const jobs = weeklyJobs || data.ts_jobs?.[tsName] || {};
      const hires12w = data.ts_hires_12w?.[tsName] || 0;

      // Derived targets: the WBR TS Weekly Note sheet only has a `Contacted Target`
      // column. PBI shows colors on Recruiter Screens / Actual Screens / ATS
      // too — derived from the same per-TS target via typical funnel ratios.
      // TSes without an explicit contacted target get a 100 default so cells
      // still receive a color (matches PBI's behaviour of colouring every cell).
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
        recruiter_screens_target: Math.round(contactedTarget * 0.15),
        actual_screens_target:    Math.round(contactedTarget * 0.10),
        ats_target:               Math.round(contactedTarget * 0.05),
        _contacted_color_target:  contactedTarget, // always non-null for getCellStyle
      };
    }).filter((r) => {
      const anyActivity = (r.contacted||0) + (r.recruiter_screens||0) + (r.actual_screens||0)
        + (r.ats||0) + (r.offers||0) + (r.hires||0) + (r.num_jobs||0) > 0;
      const hasNote = !!((r.comment && r.comment.trim()) || (r.reasoning && r.reasoning.trim()));
      return anyActivity || hasNote;
    }).sort((a, b) => a.ts.localeCompare(b.ts));
  }, [data, selectedWeek]);

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
        // PBI treats 0-numerator rows as blank (no red colour), e.g. Valeriia w16 has PR=0 → blank cell.
        pct_contacted_to_pr:  (contacted        > 0 && positiveResponse > 0) ? Math.round(positiveResponse / contacted        * 1000) / 10 : null,
        pct_screen_to_actual: (recruiterScreens > 0 && actualScreens    > 0) ? Math.round(actualScreens    / recruiterScreens * 1000) / 10 : null,
        pct_actual_to_ats:    (actualScreens    > 0 && ats              > 0) ? Math.round(ats              / actualScreens    * 1000) / 10 : null,
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
                <td className="text-center px-2 py-2 text-white" title="Sum of 12w hires across ALL active clients (includes Wolt Volume + other hidden rows), to match PBI's Total behaviour">{Object.values(data.mbr_client_totals || {}).reduce((s, v) => s + (v.hires_12w || 0), 0) || clientSummary.reduce((sum, r) => sum + r.hires_12w, 0)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* TA Detail */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TA Weekly Detail — Week {selectedWeek}</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm" style={{ minWidth: '1400px', borderCollapse: 'separate', borderSpacing: 0 }}>
            <thead className="sticky top-0 z-20">
              <tr className="text-gray-300 bg-gray-800">
                <th className="text-left px-2 py-2 sticky left-0 bg-gray-800 z-30 border-b border-gray-600">Client</th>
                <th className="text-left px-2 py-2 bg-gray-800 border-b border-gray-600">TA</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Last 12 Weeks Hires">12w H</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Last 12 Weeks ATS">12w ATS</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Last 12 Weeks Screens">12w Scr</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Last 12w % Actual Screens to Hires">12w %S→H</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Last 12w Time to Fill (days)">12w TTF</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Weekly Hires">Hires</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Weekly Contacted">Cntd</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Weekly Actual Screens">Scrn</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Weekly ATS">ATS</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="% Actual Screens to ATS">%S→A</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="# Active Roles"># Jobs</th>
                <th className="text-center px-1 py-2 text-xs bg-gray-800 border-b border-gray-600" title="Jobs Opened > 60 days">{'>'}60d</th>
                <th className="text-left px-2 py-2 text-xs min-w-[120px] bg-gray-800 border-b border-gray-600">Comment</th>
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
                    <th className="text-left px-2 py-2 sticky left-0 bg-gray-800">Client</th>
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
                    <td className="text-center px-2 py-2" style={getCellStyle(row.contacted, row._contacted_color_target)}>
                      {row.contacted}
                    </td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.contacted_target || '—'}</td>
                    <td className="text-center px-2 py-2" style={getCellStyle(row.recruiter_screens, row.recruiter_screens_target)}>
                      {row.recruiter_screens}
                    </td>
                    <td className="text-center px-2 py-2" style={getCellStyle(row.actual_screens, row.actual_screens_target)}>
                      {row.actual_screens}
                    </td>
                    <td className="text-center px-2 py-2" style={getCellStyle(row.ats, row.ats_target)}>
                      {row.ats}
                    </td>
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
                  <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                    <td className="text-left px-2 py-2 text-white font-medium">{row.ts}</td>
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
          Source: <code className="text-gray-400">ts_conversion.sql</code> · Andy Hsu logic · Calibrated vs PBI w16 2026-04-20 — 12/12 Active Pipelines exact, 12/12 colour triplets exact, 98.99% aggregate volume accuracy
        </p>
      </div>

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

    const result = [];
    targets.forEach(t => {
      const displayClient = mbrAbbrevClient(t.client);
      const key = `${displayClient}|${normalizeTa(t.ta)}`;
      const a = data.mbr_ta_actuals?.[key] || {};
      const note = latestNote[normalizeTa(t.ta)];
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
        contacted_target: t.contacted || 0,
        actual_screens_target: t.actual_screens || 0,
        ats_target: t.moved_to_ats || 0,
        hires_target: t.hires || 0,
        pct_screens_to_hires: a.screens_12w > 0 ? Math.round((a.hires_12w || 0) / a.screens_12w * 100) : null,
        comment: note?.comment || note?.reasoning || '',
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
        recruiter_screens_target: Math.round(contactedTarget * 0.15),
        actual_screens: a.actual_screens_4w || 0,
        actual_screens_target: Math.round(contactedTarget * 0.10),
        ats: a.ats_4w || 0,
        ats_target: Math.round(contactedTarget * 0.05),
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
          <table className="w-full text-sm" style={{ minWidth: '1300px', tableLayout: 'fixed' }}>
            <colgroup>
              <col style={{ width: '85px' }} />
              <col style={{ width: '120px' }} />
              <col style={{ width: '42px' }} />
              <col style={{ width: '52px' }} />
              <col style={{ width: '52px' }} />
              <col style={{ width: '56px' }} />
              <col style={{ width: '46px' }} />
              <col style={{ width: '40px' }} />
              <col style={{ width: '48px' }} />
              <col style={{ width: '40px' }} />
              <col style={{ width: '46px' }} />
              <col style={{ width: '40px' }} />
              <col style={{ width: '42px' }} />
              <col style={{ width: '40px' }} />
              <col style={{ width: '44px' }} />
              <col style={{ minWidth: '320px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-1 sticky left-0 bg-gray-800 z-10">Client</th>
                <th className="text-left px-2 py-1">TA</th>
                <th className="text-center px-1 py-1 text-xs" title="Last 12w Hires">12w H</th>
                <th className="text-center px-1 py-1 text-xs" title="Last 12w ATS">12w ATS</th>
                <th className="text-center px-1 py-1 text-xs" title="Last 12w Screens">12w Scr</th>
                <th className="text-center px-1 py-1 text-xs" title="12w % Screens → Hires">12w %S→H</th>
                <th className="text-center px-1 py-1 text-xs" title="4w Hires">Hires</th>
                <th className="text-center px-1 py-1 text-xs">Tgt</th>
                <th className="text-center px-1 py-1 text-xs" title="4w Contacted">Cntd</th>
                <th className="text-center px-1 py-1 text-xs">Tgt</th>
                <th className="text-center px-1 py-1 text-xs" title="4w Actual Screens">Scrn</th>
                <th className="text-center px-1 py-1 text-xs">Tgt</th>
                <th className="text-center px-1 py-1 text-xs" title="4w Moved to ATS">ATS</th>
                <th className="text-center px-1 py-1 text-xs">Tgt</th>
                <th className="text-center px-1 py-1 text-xs" title="Jobs Opened &gt; 60 days">{'>'}60d</th>
                <th className="text-left px-2 py-1 text-xs">Latest Comment</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, idx) => {
                const prev = idx > 0 ? rows[idx - 1] : null;
                const clientChange = !prev || prev.client !== r.client;
                return (
                  <tr key={idx} className={`${idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'} ${clientChange ? 'border-t border-gray-600' : ''}`}>
                    <td className="text-left px-2 py-1 text-white font-medium sticky left-0 bg-inherit z-10 text-xs whitespace-normal align-top">{clientChange ? r.client : ''}</td>
                    <td className="text-left px-2 py-1 text-gray-300 text-xs whitespace-normal align-top">{r.ta}</td>
                    <td className="text-center px-1 py-1 text-gray-300">{r.hires_12w || '—'}</td>
                    <td className="text-center px-1 py-1 text-gray-300">{r.ats_12w || '—'}</td>
                    <td className="text-center px-1 py-1 text-gray-300">{r.screens_12w || '—'}</td>
                    <td className="text-center px-1 py-1 text-gray-400">{r.pct_screens_to_hires != null ? `${r.pct_screens_to_hires}%` : '—'}</td>
                    <td className="text-center px-1 py-1 text-gray-300">{r.hires || ''}</td>
                    <td className="text-center px-1 py-1 text-gray-500">{r.hires_target ? r.hires_target.toFixed(1) : '—'}</td>
                    <td className="text-center px-1 py-1" style={getCellStyle(r.contacted, r.contacted_target)}>{r.contacted || ''}</td>
                    <td className="text-center px-1 py-1 text-gray-500">{r.contacted_target || '—'}</td>
                    <td className="text-center px-1 py-1" style={getCellStyle(r.actual_screens, r.actual_screens_target)}>{r.actual_screens || ''}</td>
                    <td className="text-center px-1 py-1 text-gray-500">{r.actual_screens_target || '—'}</td>
                    <td className="text-center px-1 py-1" style={getCellStyle(r.ats, r.ats_target)}>{r.ats || ''}</td>
                    <td className="text-center px-1 py-1 text-gray-500">{r.ats_target || '—'}</td>
                    <td className="text-center px-1 py-1 text-gray-300">{r.jobs_60d || ''}</td>
                    <td className="text-left px-2 py-1 text-gray-300 text-xs align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word' }}>{r.comment || '—'}</td>
                  </tr>
                );
              })}
              <tr className="bg-gray-700 font-semibold" style={{ borderTop: '2px solid #6B7280' }}>
                <td className="text-left px-2 py-1 text-white sticky left-0 bg-gray-700 z-10 text-xs">{group} Total</td>
                <td className="text-left px-2 py-1 text-gray-300">—</td>
                <td className="text-center px-1 py-1 text-white">{totals.hires_12w}</td>
                <td className="text-center px-1 py-1 text-white">{totals.ats_12w}</td>
                <td className="text-center px-1 py-1 text-white">{totals.screens_12w}</td>
                <td className="text-center px-1 py-1 text-white">—</td>
                <td className="text-center px-1 py-1 text-white">{totals.hires}</td>
                <td className="text-center px-1 py-1 text-white">{totals.hires_target.toFixed(1)}</td>
                <td className="text-center px-1 py-1 text-white">{totals.contacted}</td>
                <td className="text-center px-1 py-1 text-white">{totals.contacted_target}</td>
                <td className="text-center px-1 py-1 text-white">{totals.actual_screens}</td>
                <td className="text-center px-1 py-1 text-white">{totals.actual_screens_target}</td>
                <td className="text-center px-1 py-1 text-white">{totals.ats}</td>
                <td className="text-center px-1 py-1 text-white">{totals.ats_target}</td>
                <td className="text-center px-1 py-1 text-white">{totals.jobs_60d}</td>
                <td className="text-left px-2 py-1 text-gray-400">—</td>
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
          <table className="text-sm" style={{ width: '540px', margin: '0 auto', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '110px' }} />
              <col style={{ width: '55px' }} />
              <col style={{ width: '55px' }} />
              <col style={{ width: '85px' }} />
              <col style={{ width: '80px' }} />
              <col style={{ width: '60px' }} />
              <col style={{ width: '65px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-1">Client</th>
                <th className="text-center px-1 py-1 text-xs" title="Last 12w Hires">12w H</th>
                <th className="text-center px-1 py-1">Hires</th>
                <th className="text-center px-1 py-1">Contacted</th>
                <th className="text-center px-1 py-1">Act Scrn</th>
                <th className="text-center px-1 py-1">ATS</th>
                <th className="text-center px-1 py-1">Offers</th>
              </tr>
            </thead>
            <tbody>
              {clientRows.map((row, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-1 text-white font-medium whitespace-nowrap">{row.client}</td>
                  <td className="text-center px-1 py-1 text-gray-300">{row.hires_12w}</td>
                  <td className="text-center px-1 py-1" style={getCellStyle(row.hires, row.hires_target)}>{row.hires}</td>
                  <td className="text-center px-1 py-1" style={getCellStyle(row.contacted, row.contacted_target)}>{row.contacted}</td>
                  <td className="text-center px-1 py-1" style={getCellStyle(row.actual_screens, row.actual_screens_target)}>{row.actual_screens}</td>
                  <td className="text-center px-1 py-1" style={getCellStyle(row.ats, row.ats_target)}>{row.ats}</td>
                  <td className="text-center px-1 py-1 text-gray-300">{row.offers}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-1 text-white">Total</td>
                <td className="text-center px-1 py-1 text-white">{clientTotals.hires_12w}</td>
                <td className="text-center px-1 py-1 text-white">{clientTotals.hires}</td>
                <td className="text-center px-1 py-1 text-white">{clientTotals.contacted}</td>
                <td className="text-center px-1 py-1 text-white">{clientTotals.actual_screens}</td>
                <td className="text-center px-1 py-1 text-white">{clientTotals.ats}</td>
                <td className="text-center px-1 py-1 text-white">{clientTotals.offers}</td>
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
          <table className="text-sm" style={{ width: '100%', tableLayout: 'fixed', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '140px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '80px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '65px' }} />
              <col style={{ width: '50px' }} />
              <col style={{ minWidth: '400px' }} />
            </colgroup>
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-1">Sourcer</th>
                <th className="text-center px-1 py-1 text-xs" title="Last 12w Hires">12w H</th>
                <th className="text-center px-1 py-1 text-xs" title="Last 12w % Actual Screens → ATS">12w %S→A</th>
                <th className="text-center px-1 py-1">Contacted</th>
                <th className="text-center px-1 py-1 text-xs">Tgt</th>
                <th className="text-center px-1 py-1 text-xs">Rec Scrn</th>
                <th className="text-center px-1 py-1 text-xs">Act Scrn</th>
                <th className="text-center px-1 py-1 text-xs">ATS</th>
                <th className="text-left px-2 py-1 text-xs">Latest Comment</th>
              </tr>
            </thead>
            <tbody>
              {tsRows.map((r, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-1 text-white font-medium whitespace-normal align-top">{r.ts}</td>
                  <td className="text-center px-1 py-1 text-gray-300 align-top">{r.hires_12w || '—'}</td>
                  <td className="text-center px-1 py-1 text-gray-400 align-top">{r.pct_actual_to_ats_12w != null ? `${r.pct_actual_to_ats_12w}%` : '—'}</td>
                  <td className="text-center px-1 py-1 align-top" style={getCellStyle(r.contacted, r._contacted_color_target)}>{r.contacted}</td>
                  <td className="text-center px-1 py-1 text-gray-500 align-top">{r.contacted_target || '—'}</td>
                  <td className="text-center px-1 py-1 align-top" style={getCellStyle(r.recruiter_screens, r.recruiter_screens_target)}>{r.recruiter_screens}</td>
                  <td className="text-center px-1 py-1 align-top" style={getCellStyle(r.actual_screens, r.actual_screens_target)}>{r.actual_screens}</td>
                  <td className="text-center px-1 py-1 align-top" style={getCellStyle(r.ats, r.ats_target)}>{r.ats}</td>
                  <td className="text-left px-2 py-1 text-gray-300 text-xs align-top" style={{ whiteSpace: 'normal', wordBreak: 'break-word' }}>{r.comment || '—'}</td>
                </tr>
              ))}
              <tr className="bg-gray-700 border-t border-gray-600 font-semibold">
                <td className="text-left px-2 py-1 text-white">Total</td>
                <td className="text-center px-1 py-1 text-white">{tsRows.reduce((s, r) => s + r.hires_12w, 0)}</td>
                <td className="text-center px-1 py-1 text-white">—</td>
                <td className="text-center px-1 py-1 text-white">{tsRows.reduce((s, r) => s + r.contacted, 0)}</td>
                <td className="text-center px-1 py-1 text-white">{tsRows.reduce((s, r) => s + r.contacted_target, 0)}</td>
                <td className="text-center px-1 py-1 text-white">{tsRows.reduce((s, r) => s + r.recruiter_screens, 0)}</td>
                <td className="text-center px-1 py-1 text-white">{tsRows.reduce((s, r) => s + r.actual_screens, 0)}</td>
                <td className="text-center px-1 py-1 text-white">{tsRows.reduce((s, r) => s + r.ats, 0)}</td>
                <td className="text-left px-2 py-1 text-gray-400">—</td>
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
    default:           return [new Date('2026-01-01'), today];
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

const pdPct = (v) => v == null ? '—' : `${(v * 100).toFixed(0)}%`;

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
  const [hiresOpen, setHiresOpen] = useState(false);
  const [expandedJobs, setExpandedJobs] = useState(new Set());
  const [expandedTas, setExpandedTas] = useState(new Set());
  const [expandedTses, setExpandedTses] = useState(new Set());

  const pdRows = (data.project_dashboard && data.project_dashboard.rows) || [];
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
  }, [pdRows, weekSet, searchText, filterClient, filterTa, filterTs, filterCategory, filterSource, showExternal]);

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
    return Array.from(m.values()).sort((a, b) => a.client.localeCompare(b.client));
  }, [filtered]);

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
  const uniqueClients    = useMemo(() => Array.from(new Set(pdRows.map((r) => normalizeClientPD(r.client)))).sort(), [pdRows]);
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
  }, [pdHires, startStr, endStr, searchText, filterClient, filterTa, filterTs, filterSource, showExternal]);

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
          <span className="text-xs text-gray-400 ml-auto">window: {startStr} → {endStr}</span>
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
    </div>
  );
};


// Main Dashboard
const RecruitingDashboard = () => {
  const [activeTab, setActiveTab] = useState('wbr');
  // Data source toggle: 'snowflake' = Keboola-Snowflake pipeline (accurate, refreshed 3x/day)
  //                     'pbi' = legacy Power BI / Bubble pipeline (kept for comparison)
  const [dataSource, setDataSource] = useState('snowflake');
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

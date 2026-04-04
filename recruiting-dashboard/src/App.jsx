import React, { useMemo, useState, useEffect } from 'react';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell, FunnelChart, Funnel, ScatterChart, Scatter, ComposedChart } from 'recharts';
import * as duckdb from '@duckdb/duckdb-wasm';

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1'];

// ─── DuckDB-WASM Initialization ───────────────────────────────────────────────
let _dbInstance = null;
let _connInstance = null;
let _initPromise = null;

async function initDuckDB() {
  if (_initPromise) return _initPromise;

  _initPromise = (async () => {
    try {
      const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
      const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
      );
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      _dbInstance = new duckdb.AsyncDuckDB(logger, worker);
      await _dbInstance.instantiate(bundle.mainModule, bundle.pthreadWorker);
      _connInstance = await _dbInstance.connect();

      // Compute base URL for Parquet files
      const base = new URL('./', window.location.href).href;

      // Register all Parquet files via HTTP (supports range requests)
      const files = ['candidates', 'events_monthly', 'events_detail', 'jobs', 'users', 'clients', 'screens', 'job_goals'];
      for (const file of files) {
        try {
          await _dbInstance.registerFileURL(
            `${file}.parquet`,
            `${base}data/${file}.parquet`,
            duckdb.DuckDBDataProtocol.HTTP,
            false
          );
        } catch (e) {
          console.warn(`Failed to register ${file}.parquet:`, e);
        }
      }

      return _connInstance;
    } catch (error) {
      console.error('Failed to initialize DuckDB:', error);
      throw error;
    }
  })();

  return _initPromise;
}

async function queryDB(sqlQuery) {
  try {
    const conn = await initDuckDB();
    const result = await conn.query(sqlQuery);
    const rows = result.toArray().map(row => {
      const obj = row.toJSON();
      for (const k of Object.keys(obj)) {
        if (typeof obj[k] === 'bigint') {
          obj[k] = Number(obj[k]);
        }
      }
      return obj;
    });
    return rows;
  } catch (error) {
    console.error('Query error:', sqlQuery, error);
    throw error;
  }
}

// ─── Shared Components ────────────────────────────────────────────────────────

const Tip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-gray-900 border border-gray-600 rounded-lg p-2 shadow-xl text-xs max-w-xs">
      <p className="text-gray-300 font-medium mb-1">{label}</p>
      {payload.filter(p => p.value != null && p.value !== 0).map((p, i) => (
        <p key={i} style={{ color: p.color || p.fill }}>{p.name}: {typeof p.value === 'number' ? p.value.toLocaleString() : p.value}</p>
      ))}
    </div>
  );
};

const Card = ({ title, children, className = "" }) => (
  <div className={`bg-gray-800 rounded-xl border border-gray-700 p-4 ${className}`}>
    {title && <h3 className="text-gray-300 text-sm font-medium mb-3">{title}</h3>}
    {children}
  </div>
);

const KPI = ({ label, value, sub, color = "text-white" }) => (
  <div className="bg-gray-800 rounded-xl border border-gray-700 p-3">
    <p className="text-gray-400 text-xs uppercase tracking-wider">{label}</p>
    <p className={`text-xl font-bold mt-1 ${color}`}>{value != null ? value.toLocaleString() : '—'}</p>
    {sub && <p className="text-gray-500 text-xs mt-1">{sub}</p>}
  </div>
);

const Badge = ({ text, color }) => (
  <span className="px-2 py-0.5 rounded-full text-xs" style={{ backgroundColor: color + '25', color }}>
    {text}
  </span>
);

const Select = ({ value, onChange, options, label }) => (
  <div className="flex flex-col gap-1">
    {label && <label className="text-gray-500 text-xs">{label}</label>}
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className="bg-gray-800 border border-gray-600 rounded-lg px-2 py-1.5 text-sm text-white min-w-[120px]"
    >
      {options.map(o => (
        <option key={o.v} value={o.v}>{o.l}</option>
      ))}
    </select>
  </div>
);

// ─── Helpers ──────────────────────────────────────────────────────────────────

const daysBetween = (d1, d2) => {
  if (!d1 || !d2) return null;
  return Math.floor((new Date(d2) - new Date(d1)) / (1000 * 60 * 60 * 24));
};

const formatDate = (dateStr) => {
  if (!dateStr) return '';
  return new Date(dateStr).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
};

const formatPeriod = (ym) => {
  if (!ym) return '';
  const [y, m] = ym.split('-');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return months[parseInt(m, 10) - 1] + ' ' + y.slice(2);
};

// SQL escape helper (prevent injection from filter values)
const esc = (val) => val.replace(/'/g, "''");

// ─── Main Dashboard ───────────────────────────────────────────────────────────

export default function Dashboard() {
  const [dbReady, setDbReady] = useState(false);
  const [dbError, setDbError] = useState(null);
  const [activeTab, setActiveTab] = useState('overview');
  const [period, setPeriod] = useState('all');
  const [recruiter, setRecruiter] = useState('all');
  const [client, setClient] = useState('all');
  const [expandedJob, setExpandedJob] = useState(null);
  const [loading, setLoading] = useState(false);

  // Tab data states
  const [overviewData, setOverviewData] = useState(null);
  const [pipelineData, setPipelineData] = useState(null);
  const [performanceData, setPerformanceData] = useState(null);
  const [deliveryData, setDeliveryData] = useState(null);
  const [timeToHireData, setTimeToHireData] = useState(null);
  const [jobsData, setJobsData] = useState(null);

  // Filter options
  const [filterOptions, setFilterOptions] = useState({ periods: [], recruiters: [], clients: [] });

  // ── DuckDB Init ──
  useEffect(() => {
    initDuckDB()
      .then(() => setDbReady(true))
      .catch(err => {
        console.error(err);
        setDbError(err.message || 'Failed to initialize DuckDB');
      });
  }, []);

  // ── Load filter options once DB is ready ──
  useEffect(() => {
    if (!dbReady) return;
    (async () => {
      try {
        const [periods, recruiters, clients] = await Promise.all([
          queryDB(`
            SELECT DISTINCT SUBSTRING(date_created, 1, 7) AS period
            FROM 'candidates.parquet'
            WHERE date_created IS NOT NULL
            ORDER BY period DESC
          `),
          queryDB(`
            SELECT user_id, user_name FROM 'users.parquet'
            WHERE user_name IS NOT NULL
            ORDER BY user_name
          `),
          queryDB(`
            SELECT DISTINCT client_id, client_name FROM 'jobs.parquet'
            WHERE client_name IS NOT NULL AND (test IS NULL OR test = false)
            ORDER BY client_name
          `)
        ]);

        setFilterOptions({
          periods: periods.filter(p => p.period).map(p => ({ v: p.period, l: formatPeriod(p.period) })),
          recruiters: recruiters.map(r => ({ v: r.user_id, l: r.user_name })),
          clients: clients.filter(c => c.client_id).map(c => ({ v: c.client_id, l: c.client_name }))
        });
      } catch (err) {
        console.error('Error loading filter options:', err);
      }
    })();
  }, [dbReady]);

  // ── Load tab data when filters or tab change ──
  useEffect(() => {
    if (!dbReady) return;
    loadTabData(activeTab);
  }, [dbReady, activeTab, period, recruiter, client]);

  // ── SQL filter fragments ──
  function buildFilters() {
    const cFilter = client !== 'all' ? `AND j.client_id = '${esc(client)}'` : '';
    const rFilter = recruiter !== 'all' ? `AND j.job_recruiter = '${esc(recruiter)}'` : '';
    const pFilter = period !== 'all' ? `AND SUBSTRING(c.date_created, 1, 7) = '${esc(period)}'` : '';
    return { cFilter, rFilter, pFilter };
  }

  // ── Tab Data Loaders ──

  async function loadTabData(tab) {
    setLoading(true);
    try {
      switch (tab) {
        case 'overview': setOverviewData(await loadOverviewData()); break;
        case 'pipeline': setPipelineData(await loadPipelineData()); break;
        case 'recruiter': setPerformanceData(await loadPerformanceData()); break;
        case 'client': setDeliveryData(await loadDeliveryData()); break;
        case 'tth': setTimeToHireData(await loadTimeToHireData()); break;
        case 'jobs': setJobsData(await loadJobsData()); break;
      }
    } catch (err) {
      console.error(`Error loading ${tab}:`, err);
    } finally {
      setLoading(false);
    }
  }

  // ── OVERVIEW ──
  async function loadOverviewData() {
    const { cFilter, rFilter, pFilter } = buildFilters();
    const jobFilters = `WHERE (j.test IS NULL OR j.test = false) AND j.is_job_archived = false ${client !== 'all' ? `AND j.client_id = '${esc(client)}'` : ''} ${recruiter !== 'all' ? `AND j.job_recruiter = '${esc(recruiter)}'` : ''}`;
    const candBase = `FROM 'candidates.parquet' c LEFT JOIN 'jobs.parquet' j ON c.job_id = j.job_id WHERE c.is_candidate_archived = false AND c.is_candidate_disqualified = false ${cFilter} ${rFilter} ${pFilter}`;

    const [stats, funnel, hiringTrend, activity, tth] = await Promise.all([
      // KPIs
      queryDB(`
        SELECT
          (SELECT COUNT(*)::INTEGER FROM 'jobs.parquet' j ${jobFilters}) AS open_roles,
          (SELECT COUNT(*)::INTEGER ${candBase}) AS active_candidates,
          (SELECT COUNT(*)::INTEGER ${candBase} AND c.stage_current_type = 'Hired') AS hires,
          (SELECT COUNT(*)::INTEGER FROM 'screens.parquet') AS screens
      `),
      // Funnel by stage
      queryDB(`
        SELECT c.stage_current_type AS name, COUNT(*)::INTEGER AS value
        ${candBase}
        AND c.stage_current_type IN ('Contacted', 'Positive Response', 'Recruiter Screen', 'Interview', 'Offer', 'Hired')
        GROUP BY c.stage_current_type
      `),
      // Monthly hiring trend
      queryDB(`
        SELECT SUBSTRING(c.date_hired, 1, 7) AS month, COUNT(*)::INTEGER AS hires
        FROM 'candidates.parquet' c
        WHERE c.date_hired IS NOT NULL AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        GROUP BY month
        ORDER BY month
      `),
      // Activity by recruiter (from events_monthly)
      queryDB(`
        SELECT u.user_name AS name, e.event_type, SUM(e.count)::INTEGER AS total
        FROM 'events_monthly.parquet' e
        LEFT JOIN 'users.parquet' u ON e.recruiter = u.user_id
        WHERE e.period >= '2025-01'
        GROUP BY u.user_name, e.event_type
      `),
      // Avg TTH
      queryDB(`
        SELECT AVG(DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)))::INTEGER AS avg_tth
        FROM 'candidates.parquet' c
        LEFT JOIN 'jobs.parquet' j ON c.job_id = j.job_id
        WHERE c.stage_current_type = 'Hired' AND c.date_hired IS NOT NULL AND c.date_created IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
          ${cFilter} ${rFilter}
      `)
    ]);

    // Build funnel in correct stage order
    const stageOrder = ['Contacted', 'Positive Response', 'Recruiter Screen', 'Interview', 'Offer', 'Hired'];
    const funnelData = stageOrder.map((name, i) => {
      const found = funnel.find(r => r.name === name);
      return { name, value: found ? found.value : 0, fill: COLORS[i] };
    });

    // Build activity chart grouped by recruiter
    const activityMap = {};
    activity.forEach(r => {
      if (!r.name) return;
      if (!activityMap[r.name]) activityMap[r.name] = {};
      activityMap[r.name][r.event_type] = r.total;
    });
    const activityChart = Object.entries(activityMap)
      .map(([name, events]) => ({ name, ...events }))
      .sort((a, b) => {
        const sumA = Object.values(a).reduce((s, v) => s + (typeof v === 'number' ? v : 0), 0);
        const sumB = Object.values(b).reduce((s, v) => s + (typeof v === 'number' ? v : 0), 0);
        return sumB - sumA;
      })
      .slice(0, 8);

    return {
      stats: stats[0] || {},
      avgTTH: tth[0]?.avg_tth || 0,
      funnelData,
      hiringTrend: hiringTrend.map(r => ({ month: r.month, hires: r.hires })),
      activityChart
    };
  }

  // ── PIPELINE ──
  async function loadPipelineData() {
    const { cFilter, rFilter, pFilter } = buildFilters();

    const jobs = await queryDB(`
      SELECT
        j.job_id, j.job_title, j.client_name, j.date_created,
        u_rec.user_name AS recruiter_name,
        u_src.user_name AS sourcer_name,
        COUNT(CASE WHEN c.stage_current_type = 'Contacted' THEN 1 END)::INTEGER AS contacted,
        COUNT(CASE WHEN c.stage_current_type = 'Recruiter Screen' THEN 1 END)::INTEGER AS screen,
        COUNT(CASE WHEN c.stage_current_type = 'Interview' THEN 1 END)::INTEGER AS interview,
        COUNT(CASE WHEN c.stage_current_type = 'Offer' THEN 1 END)::INTEGER AS offer,
        COUNT(CASE WHEN c.stage_current_type = 'Hired' THEN 1 END)::INTEGER AS hired,
        DATEDIFF('day', CAST(j.date_created AS DATE), CURRENT_DATE)::INTEGER AS days_open
      FROM 'jobs.parquet' j
      LEFT JOIN 'candidates.parquet' c
        ON c.job_id = j.job_id
        AND c.is_candidate_archived = false
        AND c.is_candidate_disqualified = false
        ${pFilter}
      LEFT JOIN 'users.parquet' u_rec ON j.job_recruiter = u_rec.user_id
      LEFT JOIN 'users.parquet' u_src ON j.job_sourcer = u_src.user_id
      WHERE (j.test IS NULL OR j.test = false)
        AND j.is_job_archived = false
        ${cFilter} ${rFilter}
      GROUP BY j.job_id, j.job_title, j.client_name, j.date_created, u_rec.user_name, u_src.user_name
      ORDER BY j.date_created DESC
    `);

    return jobs.map(job => ({
      ...job,
      health: (job.screen >= 25 && job.hired === 0) ? 'red' :
              (job.screen > 0 && job.hired > 0 && (job.screen / job.hired) > 32) ? 'red' : 'green'
    }));
  }

  // ── RECRUITER PERFORMANCE ──
  async function loadPerformanceData() {
    // Get all users first
    const users = await queryDB(`
      SELECT user_id, user_name FROM 'users.parquet'
      WHERE user_name IS NOT NULL
    `);
    const userMap = {};
    users.forEach(u => { userMap[u.user_id] = u.user_name; });

    const [screens, sourced, hires, events] = await Promise.all([
      // Screens per recruiter
      queryDB(`
        SELECT user_recruiter AS uid, COUNT(*)::INTEGER AS cnt
        FROM 'screens.parquet'
        GROUP BY user_recruiter
      `),
      // Sourced candidates per sourcer
      queryDB(`
        SELECT candidate_sourcer AS uid, COUNT(*)::INTEGER AS cnt
        FROM 'candidates.parquet'
        WHERE is_candidate_archived = false AND is_candidate_disqualified = false
        GROUP BY candidate_sourcer
      `),
      // Hires per recruiter (via job)
      queryDB(`
        SELECT j.job_recruiter AS uid, COUNT(*)::INTEGER AS cnt
        FROM 'candidates.parquet' c
        JOIN 'jobs.parquet' j ON c.job_id = j.job_id
        WHERE c.stage_current_type = 'Hired'
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        GROUP BY j.job_recruiter
      `),
      // Events per recruiter
      queryDB(`
        SELECT recruiter AS uid, SUM(count)::INTEGER AS cnt
        FROM 'events_monthly.parquet'
        GROUP BY recruiter
      `)
    ]);

    // Merge into per-user rows
    const metrics = {};
    users.forEach(u => {
      metrics[u.user_id] = { name: u.user_name, screens: 0, sourced: 0, hires: 0, events: 0 };
    });
    screens.forEach(r => { if (metrics[r.uid]) metrics[r.uid].screens = r.cnt; });
    sourced.forEach(r => { if (metrics[r.uid]) metrics[r.uid].sourced = r.cnt; });
    hires.forEach(r => { if (metrics[r.uid]) metrics[r.uid].hires = r.cnt; });
    events.forEach(r => { if (metrics[r.uid]) metrics[r.uid].events = r.cnt; });

    const recruiterPerf = Object.values(metrics)
      .filter(r => r.screens > 0 || r.sourced > 0 || r.hires > 0 || r.events > 0)
      .map(r => ({
        ...r,
        ratio: r.screens > 0 ? (r.hires / r.screens).toFixed(2) : '0.00'
      }))
      .sort((a, b) => b.hires - a.hires);

    return { recruiterPerf };
  }

  // ── CLIENT DELIVERY ──
  async function loadDeliveryData() {
    const { cFilter, rFilter } = buildFilters();

    const [clientMetrics, goalData] = await Promise.all([
      queryDB(`
        SELECT
          cl.client_id,
          cl.client_name AS name,
          COUNT(DISTINCT CASE WHEN j.is_job_archived = false THEN j.job_id END)::INTEGER AS open_roles,
          COUNT(DISTINCT c.candidate_id)::INTEGER AS pipeline_candidates,
          COUNT(DISTINCT CASE WHEN c.stage_current_type = 'Hired' THEN c.candidate_id END)::INTEGER AS hires
        FROM 'clients.parquet' cl
        LEFT JOIN 'jobs.parquet' j ON j.client_id = cl.client_id AND (j.test IS NULL OR j.test = false)
          ${recruiter !== 'all' ? `AND j.job_recruiter = '${esc(recruiter)}'` : ''}
        LEFT JOIN 'candidates.parquet' c ON c.job_id = j.job_id
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        ${client !== 'all' ? `WHERE cl.client_id = '${esc(client)}'` : ''}
        GROUP BY cl.client_id, cl.client_name
        HAVING open_roles > 0 OR pipeline_candidates > 0 OR hires > 0
        ORDER BY hires DESC
      `),
      // Goals per client
      queryDB(`
        SELECT j.client_id, SUM(g.goal_number)::INTEGER AS total_goal,
          COUNT(DISTINCT CASE WHEN c.stage_current_type = 'Hired' THEN c.candidate_id END)::INTEGER AS filled
        FROM 'job_goals.parquet' g
        JOIN 'jobs.parquet' j ON g.job_id = j.job_id
        LEFT JOIN 'candidates.parquet' c ON c.job_id = g.job_id AND c.stage_current_type = 'Hired'
        GROUP BY j.client_id
      `)
    ]);

    const goalMap = {};
    goalData.forEach(g => { goalMap[g.client_id] = g; });

    const clientPerf = clientMetrics.map(c => {
      const goal = goalMap[c.client_id];
      const goals = goal ? goal.total_goal : 0;
      const filled = goal ? goal.filled : 0;
      const fillRate = goals > 0 ? ((filled / goals) * 100).toFixed(1) : '0.0';

      // Avg TTF: compute from hired candidates for this client
      return { ...c, goals, filled, fillRate };
    });

    return { clientPerf };
  }

  // ── TIME TO HIRE ──
  async function loadTimeToHireData() {
    const { cFilter, rFilter } = buildFilters();

    const [tthByMonth, stageBreakdown, overallStats] = await Promise.all([
      // Monthly TTH trend (avg, min, max)
      queryDB(`
        SELECT
          SUBSTRING(c.date_hired, 1, 7) AS month,
          AVG(DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)))::INTEGER AS avg,
          MIN(DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)))::INTEGER AS min,
          MAX(DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)))::INTEGER AS max
        FROM 'candidates.parquet' c
        LEFT JOIN 'jobs.parquet' j ON c.job_id = j.job_id
        WHERE c.stage_current_type = 'Hired'
          AND c.date_hired IS NOT NULL AND c.date_created IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
          AND DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)) > 0
          AND DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)) < 365
          ${cFilter} ${rFilter}
        GROUP BY month
        ORDER BY month
      `),
      // Stage duration breakdown (avg days per stage transition)
      queryDB(`
        SELECT
          'Created → Contacted' AS stage,
          AVG(DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_contacted AS DATE)))::INTEGER AS avg_days
        FROM 'candidates.parquet' c
        WHERE c.stage_current_type = 'Hired' AND c.date_created IS NOT NULL AND c.date_contacted IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        UNION ALL
        SELECT 'Contacted → Screen',
          AVG(DATEDIFF('day', CAST(c.date_contacted AS DATE), CAST(c.date_screen AS DATE)))::INTEGER
        FROM 'candidates.parquet' c
        WHERE c.stage_current_type = 'Hired' AND c.date_contacted IS NOT NULL AND c.date_screen IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        UNION ALL
        SELECT 'Screen → Interview',
          AVG(DATEDIFF('day', CAST(c.date_screen AS DATE), CAST(c.date_interview AS DATE)))::INTEGER
        FROM 'candidates.parquet' c
        WHERE c.stage_current_type = 'Hired' AND c.date_screen IS NOT NULL AND c.date_interview IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        UNION ALL
        SELECT 'Interview → Offer',
          AVG(DATEDIFF('day', CAST(c.date_interview AS DATE), CAST(c.date_offer AS DATE)))::INTEGER
        FROM 'candidates.parquet' c
        WHERE c.stage_current_type = 'Hired' AND c.date_interview IS NOT NULL AND c.date_offer IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
        UNION ALL
        SELECT 'Offer → Hired',
          AVG(DATEDIFF('day', CAST(c.date_offer AS DATE), CAST(c.date_hired AS DATE)))::INTEGER
        FROM 'candidates.parquet' c
        WHERE c.stage_current_type = 'Hired' AND c.date_offer IS NOT NULL AND c.date_hired IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
      `),
      // Overall KPIs
      queryDB(`
        SELECT
          AVG(DATEDIFF('day', CAST(c.date_created AS DATE), CAST(c.date_hired AS DATE)))::INTEGER AS overall_avg,
          AVG(DATEDIFF('day', CAST(c.date_contacted AS DATE), CAST(c.date_screen AS DATE)))::INTEGER AS avg_to_screen,
          AVG(DATEDIFF('day', CAST(c.date_interview AS DATE), CAST(c.date_offer AS DATE)))::INTEGER AS avg_to_offer
        FROM 'candidates.parquet' c
        LEFT JOIN 'jobs.parquet' j ON c.job_id = j.job_id
        WHERE c.stage_current_type = 'Hired'
          AND c.date_hired IS NOT NULL AND c.date_created IS NOT NULL
          AND c.is_candidate_archived = false AND c.is_candidate_disqualified = false
          ${cFilter} ${rFilter}
      `)
    ]);

    return {
      tthByMonth,
      stageBreakdown: stageBreakdown.filter(s => s.avg_days != null && s.avg_days > 0),
      overallAvg: overallStats[0]?.overall_avg || 0,
      overallScreen: overallStats[0]?.avg_to_screen || 0,
      overallOffer: overallStats[0]?.avg_to_offer || 0
    };
  }

  // ── JOBS ──
  async function loadJobsData() {
    const { cFilter, rFilter } = buildFilters();

    const jobs = await queryDB(`
      SELECT
        j.job_id, j.job_title, j.client_name, j.date_created,
        u_rec.user_name AS recruiter_name,
        u_src.user_name AS sourcer_name,
        COUNT(DISTINCT c.candidate_id)::INTEGER AS candidate_count,
        COUNT(CASE WHEN c.stage_current_type = 'Contacted' THEN 1 END)::INTEGER AS contacted,
        COUNT(CASE WHEN c.stage_current_type = 'Recruiter Screen' THEN 1 END)::INTEGER AS screen,
        COUNT(CASE WHEN c.stage_current_type = 'Interview' THEN 1 END)::INTEGER AS interview,
        COUNT(CASE WHEN c.stage_current_type = 'Offer' THEN 1 END)::INTEGER AS offer,
        COUNT(CASE WHEN c.stage_current_type = 'Hired' THEN 1 END)::INTEGER AS hired,
        DATEDIFF('day', CAST(j.date_created AS DATE), CURRENT_DATE)::INTEGER AS days_open
      FROM 'jobs.parquet' j
      LEFT JOIN 'candidates.parquet' c
        ON c.job_id = j.job_id
        AND c.is_candidate_archived = false
        AND c.is_candidate_disqualified = false
      LEFT JOIN 'users.parquet' u_rec ON j.job_recruiter = u_rec.user_id
      LEFT JOIN 'users.parquet' u_src ON j.job_sourcer = u_src.user_id
      WHERE (j.test IS NULL OR j.test = false)
        AND j.is_job_archived = false
        ${cFilter} ${rFilter}
      GROUP BY j.job_id, j.job_title, j.client_name, j.date_created, u_rec.user_name, u_src.user_name
      ORDER BY j.date_created DESC
    `);

    return { jobs };
  }

  // ─── RENDER ─────────────────────────────────────────────────────────────────

  if (!dbReady) {
    return (
      <div className="w-screen h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
          <p className="text-gray-400">Loading DuckDB and Parquet files...</p>
          {dbError && <p className="text-red-400 text-sm mt-2">{dbError}</p>}
        </div>
      </div>
    );
  }

  const periodOptions = [{ v: 'all', l: 'All Periods' }, ...filterOptions.periods];
  const recruiterOptions = [{ v: 'all', l: 'All Recruiters' }, ...filterOptions.recruiters];
  const clientOptions = [{ v: 'all', l: 'All Clients' }, ...filterOptions.clients];

  // ── Tab Renderers ──

  const renderOverview = () => {
    if (!overviewData) return null;
    const d = overviewData;
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-5 gap-3">
          <KPI label="Open Roles" value={d.stats.open_roles} />
          <KPI label="Active Candidates" value={d.stats.active_candidates} />
          <KPI label="Hires (TTD)" value={d.stats.hires} color="text-green-400" />
          <KPI label="Screens Completed" value={d.stats.screens} />
          <KPI label="Avg TTH (days)" value={d.avgTTH} color="text-blue-400" sub="for hired candidates" />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <Card title="Pipeline Funnel (All Stages)">
            <ResponsiveContainer width="100%" height={300}>
              <FunnelChart>
                <Tooltip content={<Tip />} />
                <Funnel dataKey="value" data={d.funnelData} fill="#3b82f6">
                  {d.funnelData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.fill} />
                  ))}
                </Funnel>
              </FunnelChart>
            </ResponsiveContainer>
          </Card>

          <Card title="Hiring Trend (Monthly)">
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={d.hiringTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="month" stroke="#9ca3af" style={{ fontSize: '12px' }} tickFormatter={formatPeriod} />
                <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
                <Tooltip content={<Tip />} />
                <Line type="monotone" dataKey="hires" stroke="#10b981" strokeWidth={2} dot={{ r: 4 }} />
              </LineChart>
            </ResponsiveContainer>
          </Card>
        </div>

        <Card title="Activity by Recruiter">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={d.activityChart}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" stroke="#9ca3af" style={{ fontSize: '12px' }} angle={-45} textAnchor="end" height={80} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip content={<Tip />} />
              <Legend />
              <Bar dataKey="Email Sent" fill="#3b82f6" />
              <Bar dataKey="Email Read" fill="#10b981" />
              <Bar dataKey="Email Replied" fill="#f59e0b" />
              <Bar dataKey="Linkedin Visited Profile" fill="#ef4444" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
    );
  };

  const renderPipeline = () => {
    if (!pipelineData) return null;
    return (
      <div className="space-y-4">
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-gray-600">
                <tr className="text-gray-400">
                  <th className="text-left py-2 px-2">Job Title</th>
                  <th className="text-left py-2 px-2">Client</th>
                  <th className="text-left py-2 px-2">Recruiter</th>
                  <th className="text-left py-2 px-2">Sourcer</th>
                  <th className="text-center py-2 px-2">Contacted</th>
                  <th className="text-center py-2 px-2">Screen</th>
                  <th className="text-center py-2 px-2">Interview</th>
                  <th className="text-center py-2 px-2">Offer</th>
                  <th className="text-center py-2 px-2">Hired</th>
                  <th className="text-center py-2 px-2">Days Open</th>
                </tr>
              </thead>
              <tbody>
                {pipelineData.map(job => (
                  <tr key={job.job_id} className={`border-b border-gray-700 hover:bg-gray-700/50 ${job.health === 'red' ? 'bg-red-900/10' : ''}`}>
                    <td className="py-2 px-2 text-white">{job.job_title}</td>
                    <td className="py-2 px-2 text-gray-300">{job.client_name}</td>
                    <td className="py-2 px-2 text-gray-300">{job.recruiter_name}</td>
                    <td className="py-2 px-2 text-gray-300">{job.sourcer_name}</td>
                    <td className="py-2 px-2 text-center text-blue-400">{job.contacted}</td>
                    <td className="py-2 px-2 text-center text-amber-400">{job.screen}</td>
                    <td className="py-2 px-2 text-center text-purple-400">{job.interview}</td>
                    <td className="py-2 px-2 text-center text-pink-400">{job.offer}</td>
                    <td className="py-2 px-2 text-center text-green-400 font-bold">{job.hired}</td>
                    <td className="py-2 px-2 text-center text-gray-400">{job.days_open}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </div>
    );
  };

  const renderRecruiterPerformance = () => {
    if (!performanceData) return null;
    const d = performanceData;
    return (
      <div className="space-y-6">
        <Card title="Recruiter Metrics">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-gray-600">
                <tr className="text-gray-400">
                  <th className="text-left py-2 px-2">Recruiter</th>
                  <th className="text-center py-2 px-2">Screens</th>
                  <th className="text-center py-2 px-2">Sourced</th>
                  <th className="text-center py-2 px-2">Hires</th>
                  <th className="text-center py-2 px-2">Events</th>
                  <th className="text-center py-2 px-2">H/S Ratio</th>
                </tr>
              </thead>
              <tbody>
                {d.recruiterPerf.map(r => (
                  <tr key={r.name} className="border-b border-gray-700 hover:bg-gray-700/50">
                    <td className="py-2 px-2 text-white">{r.name}</td>
                    <td className="py-2 px-2 text-center text-blue-400">{r.screens}</td>
                    <td className="py-2 px-2 text-center text-amber-400">{r.sourced}</td>
                    <td className="py-2 px-2 text-center text-green-400 font-bold">{r.hires}</td>
                    <td className="py-2 px-2 text-center text-purple-400">{r.events}</td>
                    <td className="py-2 px-2 text-center text-gray-300">{r.ratio}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <Card title="Hires by Recruiter">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={d.recruiterPerf.slice(0, 10)}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" stroke="#9ca3af" style={{ fontSize: '12px' }} angle={-45} textAnchor="end" height={80} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip content={<Tip />} />
              <Bar dataKey="hires" fill="#10b981" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
    );
  };

  const renderClientDelivery = () => {
    if (!deliveryData) return null;
    const d = deliveryData;
    return (
      <div className="space-y-6">
        <Card title="Client Delivery Metrics">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-gray-600">
                <tr className="text-gray-400">
                  <th className="text-left py-2 px-2">Client</th>
                  <th className="text-center py-2 px-2">Open Roles</th>
                  <th className="text-center py-2 px-2">Pipeline</th>
                  <th className="text-center py-2 px-2">Hires</th>
                  <th className="text-center py-2 px-2">Fill Rate</th>
                </tr>
              </thead>
              <tbody>
                {d.clientPerf.map(c => {
                  let fillColor = '#ef4444'; // red <50%
                  const fr = parseFloat(c.fillRate);
                  if (fr >= 120) fillColor = '#10b981';       // green
                  else if (fr >= 100) fillColor = '#84cc16';  // light green
                  else if (fr >= 75) fillColor = '#f59e0b';   // yellow
                  else if (fr >= 50) fillColor = '#f97316';   // orange

                  return (
                    <tr key={c.client_id} className="border-b border-gray-700 hover:bg-gray-700/50">
                      <td className="py-2 px-2 text-white">{c.name}</td>
                      <td className="py-2 px-2 text-center text-blue-400">{c.open_roles}</td>
                      <td className="py-2 px-2 text-center text-amber-400">{c.pipeline_candidates}</td>
                      <td className="py-2 px-2 text-center text-green-400 font-bold">{c.hires}</td>
                      <td className="py-2 px-2 text-center">
                        <span style={{ color: fillColor, fontWeight: 'bold' }}>
                          {c.goals > 0 ? `${c.fillRate}%` : '—'}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>

        <Card title="Hires by Client">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={d.clientPerf.filter(c => c.hires > 0).slice(0, 10)}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" stroke="#9ca3af" style={{ fontSize: '12px' }} angle={-45} textAnchor="end" height={80} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip content={<Tip />} />
              <Bar dataKey="hires" fill="#10b981" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
    );
  };

  const renderTimeToHire = () => {
    if (!timeToHireData) return null;
    const d = timeToHireData;
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-3 gap-3">
          <KPI label="Avg Time to Hire" value={`${d.overallAvg}d`} color="text-blue-400" />
          <KPI label="Avg Time to Screen" value={`${d.overallScreen}d`} color="text-amber-400" />
          <KPI label="Avg Time to Offer" value={`${d.overallOffer}d`} color="text-pink-400" />
        </div>

        <Card title="TTH Trend (Monthly)">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={d.tthByMonth}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" stroke="#9ca3af" style={{ fontSize: '12px' }} tickFormatter={formatPeriod} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip content={<Tip />} />
              <Legend />
              <Line type="monotone" dataKey="avg" stroke="#3b82f6" strokeWidth={2} name="Average" />
              <Line type="monotone" dataKey="min" stroke="#10b981" strokeWidth={1} strokeDasharray="5 5" name="Min" />
              <Line type="monotone" dataKey="max" stroke="#ef4444" strokeWidth={1} strokeDasharray="5 5" name="Max" />
            </LineChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Days per Stage (Avg)">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={d.stageBreakdown}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="stage" stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip content={<Tip />} />
              <Bar dataKey="avg_days" fill="#3b82f6" name="Avg Days" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
    );
  };

  const renderJobs = () => {
    if (!jobsData) return null;
    return (
      <div className="space-y-4">
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-gray-600">
                <tr className="text-gray-400">
                  <th className="text-left py-2 px-2">Job Title</th>
                  <th className="text-left py-2 px-2">Client</th>
                  <th className="text-center py-2 px-2">Days Open</th>
                  <th className="text-center py-2 px-2">Candidates</th>
                  <th className="text-center py-2 px-2">Contacted</th>
                  <th className="text-center py-2 px-2">Screen</th>
                  <th className="text-center py-2 px-2">Interview</th>
                  <th className="text-center py-2 px-2">Offer</th>
                  <th className="text-center py-2 px-2">Hired</th>
                </tr>
              </thead>
              <tbody>
                {jobsData.jobs.map(job => (
                  <tr key={job.job_id} className="border-b border-gray-700 hover:bg-gray-700/50">
                    <td className="py-2 px-2 text-white">{job.job_title}</td>
                    <td className="py-2 px-2 text-gray-300">{job.client_name}</td>
                    <td className="py-2 px-2 text-center text-gray-400">{job.days_open}</td>
                    <td className="py-2 px-2 text-center text-blue-400">{job.candidate_count}</td>
                    <td className="py-2 px-2 text-center text-blue-400">{job.contacted}</td>
                    <td className="py-2 px-2 text-center text-amber-400">{job.screen}</td>
                    <td className="py-2 px-2 text-center text-purple-400">{job.interview}</td>
                    <td className="py-2 px-2 text-center text-pink-400">{job.offer}</td>
                    <td className="py-2 px-2 text-center text-green-400 font-bold">{job.hired}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </div>
    );
  };

  // ── Main Layout ──
  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-2">Recruiting Dashboard</h1>
          <p className="text-gray-400">Tribe.xyz Talent Acquisition Platform</p>
        </div>

        {/* Global Filters */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 mb-6 flex gap-4 flex-wrap items-end">
          <Select
            label="Period"
            value={period}
            onChange={setPeriod}
            options={periodOptions}
          />
          <Select
            label="Recruiter"
            value={recruiter}
            onChange={setRecruiter}
            options={recruiterOptions}
          />
          <Select
            label="Client"
            value={client}
            onChange={setClient}
            options={clientOptions}
          />
        </div>

        {/* Tab Navigation */}
        <div className="flex gap-2 mb-6 overflow-x-auto pb-2 border-b border-gray-700">
          {[
            { key: 'overview', label: 'Overview' },
            { key: 'pipeline', label: 'Pipeline' },
            { key: 'recruiter', label: 'Recruiter Performance' },
            { key: 'client', label: 'Client Delivery' },
            { key: 'tth', label: 'Time to Hire' },
            { key: 'jobs', label: 'Jobs' }
          ].map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-4 py-2 rounded-t-lg font-medium whitespace-nowrap ${
                activeTab === tab.key
                  ? 'bg-blue-600 text-white border-b-2 border-blue-400'
                  : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Loading indicator */}
        {loading && (
          <div className="flex items-center gap-2 mb-4 text-gray-400 text-sm">
            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
            Loading data...
          </div>
        )}

        {/* Tab Content */}
        <div className="mb-6">
          {activeTab === 'overview' && renderOverview()}
          {activeTab === 'pipeline' && renderPipeline()}
          {activeTab === 'recruiter' && renderRecruiterPerformance()}
          {activeTab === 'client' && renderClientDelivery()}
          {activeTab === 'tth' && renderTimeToHire()}
          {activeTab === 'jobs' && renderJobs()}
        </div>
      </div>
    </div>
  );
}

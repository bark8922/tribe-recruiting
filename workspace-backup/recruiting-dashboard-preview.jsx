import React, { useState, useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid, Legend
} from 'recharts';
import { ChevronDown, Search } from 'lucide-react';
import dashboardData from './dashboard_data.json';

const WEEKS = [10, 11, 12, 13, 14, 15];
const WEEKLY_DIVISOR = 4.33; // Monthly targets divided by weeks per month

// Client name mapping
const normalizeClient = (client) => {
  if (!client) return client;
  const trimmed = client.trim();
  if (trimmed.toLowerCase() === 'aviv') return 'Aviv';
  if (trimmed.toLowerCase() === 'doordash') return 'DoorDash';
  if (trimmed.toLowerCase() === 'nexi') return 'Nexi';
  return trimmed;
};

// Calculate color based on percentage of target
const getTargetColor = (actual, target) => {
  if (target === 0) return 'bg-gray-700';
  const pct = (actual / target) * 100;
  if (pct < 50) return 'bg-red-900';
  if (pct < 75) return 'bg-orange-900';
  if (pct < 100) return 'bg-yellow-900';
  if (pct <= 120) return 'bg-green-900';
  return 'bg-green-700';
};

// Format number for display
const formatNum = (val) => {
  if (val === null || val === undefined) return '0';
  return Math.round(val * 100) / 100;
};

// Calculate percentage
const calcPct = (actual, target) => {
  if (target === 0) return '—';
  return Math.round((actual / target) * 100) + '%';
};

// WBR Tab Component
const WBRTab = ({ data }) => {
  const [selectedWeek, setSelectedWeek] = useState(15);

  // Get unique clients from targets
  const clients = useMemo(() => {
    const clientSet = new Set();
    data.targets.forEach((t) => {
      clientSet.add(normalizeClient(t.client));
    });
    return Array.from(clientSet).sort();
  }, [data]);

  // Build client summary table for selected week
  const clientSummary = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const summary = {};

    // Initialize with targets
    data.targets.forEach((target) => {
      const normalized = normalizeClient(target.client);
      if (!summary[normalized]) {
        summary[normalized] = {
          client: normalized,
          contacted_actual: 0,
          contacted_target: target.contacted / WEEKLY_DIVISOR,
          screened_actual: 0,
          screened_target: target.actual_screens / WEEKLY_DIVISOR,
          ats_actual: 0,
          ats_target: target.moved_to_ats / WEEKLY_DIVISOR,
          hires_actual: 0,
          hires_target: target.hires / WEEKLY_DIVISOR,
        };
      } else {
        summary[normalized].contacted_target += target.contacted / WEEKLY_DIVISOR;
        summary[normalized].screened_target += target.actual_screens / WEEKLY_DIVISOR;
        summary[normalized].ats_target += target.moved_to_ats / WEEKLY_DIVISOR;
        summary[normalized].hires_target += target.hires / WEEKLY_DIVISOR;
      }
    });

    // Add actuals from wbr_actuals
    Object.keys(data.wbr_actuals).forEach((key) => {
      const [clientKey] = key.split('|');
      const normalized = normalizeClient(clientKey);

      if (summary[normalized] && data.wbr_actuals[key][weekKey] !== undefined) {
        summary[normalized].contacted_actual += data.wbr_actuals[key][weekKey] || 0;
      }
    });

    return Object.values(summary).sort((a, b) => a.client.localeCompare(b.client));
  }, [data, selectedWeek]);

  // Build TA weekly detail table
  const taWeeklyDetail = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const details = [];

    data.targets.forEach((target) => {
      const normalized = normalizeClient(target.client);
      const key = `${normalizeClient(target.client)}|${target.ta}`;

      const weeklyTarget = target.contacted / WEEKLY_DIVISOR;
      const actualValue = data.wbr_actuals[key]?.[weekKey] || 0;

      details.push({
        client: normalized,
        ta: target.ta,
        contacted_actual: actualValue,
        contacted_target: weeklyTarget,
        comment: '',
        reasoning: '',
      });
    });

    return details.sort((a, b) => {
      if (a.client !== b.client) return a.client.localeCompare(b.client);
      return a.ta.localeCompare(b.ta);
    });
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
            <option key={w} value={w}>
              Week {w}
            </option>
          ))}
        </select>
      </div>

      {/* Section 1: Client Target Summary */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Client Target Summary</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700">
                <th className="text-left px-3 py-2">Client</th>
                <th className="text-center px-3 py-2">Contacted (A/T/%)</th>
                <th className="text-center px-3 py-2">Actual Screens (A/T/%)</th>
                <th className="text-center px-3 py-2">Moved to ATS (A/T/%)</th>
                <th className="text-center px-3 py-2">Hires (A/T/%)</th>
              </tr>
            </thead>
            <tbody>
              {clientSummary.map((row, idx) => (
                <tr
                  key={idx}
                  className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}
                >
                  <td className="text-left px-3 py-2 text-white font-medium">{row.client}</td>
                  <td className={`text-center px-3 py-2 ${getTargetColor(row.contacted_actual, row.contacted_target)}`}>
                    {formatNum(row.contacted_actual)} / {formatNum(row.contacted_target)} / {calcPct(row.contacted_actual, row.contacted_target)}
                  </td>
                  <td className={`text-center px-3 py-2 ${getTargetColor(row.screened_actual, row.screened_target)}`}>
                    {formatNum(row.screened_actual)} / {formatNum(row.screened_target)} / {calcPct(row.screened_actual, row.screened_target)}
                  </td>
                  <td className={`text-center px-3 py-2 ${getTargetColor(row.ats_actual, row.ats_target)}`}>
                    {formatNum(row.ats_actual)} / {formatNum(row.ats_target)} / {calcPct(row.ats_actual, row.ats_target)}
                  </td>
                  <td className={`text-center px-3 py-2 ${getTargetColor(row.hires_actual, row.hires_target)}`}>
                    {formatNum(row.hires_actual)} / {formatNum(row.hires_target)} / {calcPct(row.hires_actual, row.hires_target)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Section 2: TA Weekly Detail */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TA Weekly Detail</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700">
                <th className="text-left px-3 py-2">Client</th>
                <th className="text-left px-3 py-2">TA</th>
                <th className="text-center px-3 py-2">Contacted (A/T)</th>
                <th className="text-left px-3 py-2">Comment</th>
                <th className="text-left px-3 py-2">Reasoning</th>
              </tr>
            </thead>
            <tbody>
              {taWeeklyDetail.map((row, idx) => (
                <tr
                  key={idx}
                  className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}
                >
                  <td className="text-left px-3 py-2 text-white">{row.client}</td>
                  <td className="text-left px-3 py-2 text-white">{row.ta}</td>
                  <td className="text-center px-3 py-2 text-gray-300">
                    {formatNum(row.contacted_actual)} / {formatNum(row.contacted_target)}
                  </td>
                  <td className="text-left px-3 py-2 text-gray-400">{row.comment || '—'}</td>
                  <td className="text-left px-3 py-2 text-gray-400">{row.reasoning || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

// Project Dashboard Tab Component
const ProjectDashboardTab = ({ data }) => {
  const [selectedClient, setSelectedClient] = useState('');
  const [searchTitle, setSearchTitle] = useState('');
  const [sortField, setSortField] = useState('client');

  // Filter jobs (exclude archived and external recruiters)
  const activeJobs = useMemo(() => {
    return data.jobs.filter((job) => !job.is_ext);
  }, [data]);

  // Get unique clients
  const uniqueClients = useMemo(() => {
    const clients = new Set();
    activeJobs.forEach((job) => {
      clients.add(normalizeClient(job.client));
    });
    return Array.from(clients).sort();
  }, [activeJobs]);

  // KPI Cards
  const kpis = useMemo(() => {
    const totals = activeJobs.reduce(
      (acc, job) => ({
        openRoles: acc.openRoles + 1,
        contacted: acc.contacted + (job.contacted || 0),
        screened: acc.screened + (job.screened || 0),
        hires: acc.hires + (job.hires || 0),
      }),
      { openRoles: 0, contacted: 0, screened: 0, hires: 0 }
    );
    return totals;
  }, [activeJobs]);

  // Calculate days open
  const getDaysOpen = (dateCreated) => {
    const created = new Date(dateCreated);
    const today = new Date('2026-04-10');
    const diff = today - created;
    return Math.floor(diff / (1000 * 60 * 60 * 24));
  };

  // Job Performance Table with filters
  const filteredJobs = useMemo(() => {
    let jobs = activeJobs;

    if (selectedClient) {
      jobs = jobs.filter((job) => normalizeClient(job.client) === selectedClient);
    }

    if (searchTitle) {
      jobs = jobs.filter((job) =>
        job.title.toLowerCase().includes(searchTitle.toLowerCase())
      );
    }

    return jobs.sort((a, b) => {
      let aVal, bVal;
      switch (sortField) {
        case 'client':
          aVal = normalizeClient(a.client);
          bVal = normalizeClient(b.client);
          break;
        case 'title':
          aVal = a.title;
          bVal = b.title;
          break;
        case 'ta':
          aVal = a.ta || '';
          bVal = b.ta || '';
          break;
        case 'contacted':
          aVal = a.contacted || 0;
          bVal = b.contacted || 0;
          break;
        case 'screened':
          aVal = a.screened || 0;
          bVal = b.screened || 0;
          break;
        case 'hires':
          aVal = a.hires || 0;
          bVal = b.hires || 0;
          break;
        case 'days_open':
          aVal = getDaysOpen(a.date_created);
          bVal = getDaysOpen(b.date_created);
          break;
        default:
          return 0;
      }
      return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    });
  }, [activeJobs, selectedClient, searchTitle, sortField]);

  // Per-Client Summary for chart
  const clientSummaryChart = useMemo(() => {
    const summary = {};
    activeJobs.forEach((job) => {
      const client = normalizeClient(job.client);
      if (!summary[client]) {
        summary[client] = 0;
      }
      summary[client] += job.hires || 0;
    });

    return Object.entries(summary)
      .map(([client, hires]) => ({ client, hires }))
      .sort((a, b) => b.hires - a.hires)
      .slice(0, 10);
  }, [activeJobs]);

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4">
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-gray-400 text-sm font-medium">Open Roles</div>
          <div className="text-3xl font-bold text-white mt-2">{kpis.openRoles}</div>
        </div>
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-gray-400 text-sm font-medium">Total Contacted</div>
          <div className="text-3xl font-bold text-white mt-2">{kpis.contacted}</div>
        </div>
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-gray-400 text-sm font-medium">Total Screened</div>
          <div className="text-3xl font-bold text-white mt-2">{kpis.screened}</div>
        </div>
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <div className="text-gray-400 text-sm font-medium">Total Hires</div>
          <div className="text-3xl font-bold text-white mt-2">{kpis.hires}</div>
        </div>
      </div>

      {/* Weekly Trend Chart */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Weekly Contacted Trend</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart
            data={data.weekly_trend}
            margin={{ top: 5, right: 30, left: 0, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#444" />
            <XAxis
              dataKey="week"
              stroke="#999"
              label={{ value: 'Week', position: 'insideBottomRight', offset: -5, fill: '#999' }}
            />
            <YAxis stroke="#999" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #444' }}
              labelStyle={{ color: '#fff' }}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="contacted"
              stroke="#60a5fa"
              dot={{ fill: '#60a5fa', r: 4 }}
              activeDot={{ r: 6 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Job Performance Table */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Job Performance</h3>

        {/* Filters */}
        <div className="flex gap-3 mb-4">
          <div className="flex-1 relative">
            <Search className="absolute left-2 top-2.5 w-4 h-4 text-gray-500" />
            <input
              type="text"
              placeholder="Search job title..."
              value={searchTitle}
              onChange={(e) => setSearchTitle(e.target.value)}
              className="w-full pl-8 pr-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm placeholder-gray-500"
            />
          </div>
          <select
            value={selectedClient}
            onChange={(e) => setSelectedClient(e.target.value)}
            className="px-3 py-2 bg-gray-700 text-white rounded border border-gray-600 text-sm"
          >
            <option value="">All Clients</option>
            {uniqueClients.map((client) => (
              <option key={client} value={client}>
                {client}
              </option>
            ))}
          </select>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-700">
                <th
                  className="text-left px-3 py-2 cursor-pointer hover:text-white"
                  onClick={() => setSortField('client')}
                >
                  Client
                </th>
                <th
                  className="text-left px-3 py-2 cursor-pointer hover:text-white"
                  onClick={() => setSortField('title')}
                >
                  Job Title
                </th>
                <th
                  className="text-left px-3 py-2 cursor-pointer hover:text-white"
                  onClick={() => setSortField('ta')}
                >
                  TA
                </th>
                <th className="text-left px-3 py-2">Category</th>
                <th
                  className="text-center px-3 py-2 cursor-pointer hover:text-white"
                  onClick={() => setSortField('contacted')}
                >
                  Contacted
                </th>
                <th
                  className="text-center px-3 py-2 cursor-pointer hover:text-white"
                  onClick={() => setSortField('screened')}
                >
                  Screened
                </th>
                <th className="text-center px-3 py-2">ATS</th>
                <th className="text-center px-3 py-2">Offers</th>
                <th className="text-center px-3 py-2">Hires</th>
                <th
                  className="text-center px-3 py-2 cursor-pointer hover:text-white"
                  onClick={() => setSortField('days_open')}
                >
                  Days Open
                </th>
              </tr>
            </thead>
            <tbody>
              {filteredJobs.map((job, idx) => {
                const daysOpen = getDaysOpen(job.date_created);
                const isRedRow = job.screened > 25 && job.hires === 0;
                return (
                  <tr
                    key={idx}
                    className={
                      isRedRow
                        ? 'bg-red-900 bg-opacity-20'
                        : idx % 2 === 0
                        ? 'bg-gray-800'
                        : 'bg-gray-750'
                    }
                  >
                    <td className="text-left px-3 py-2 text-white font-medium">
                      {normalizeClient(job.client)}
                    </td>
                    <td className="text-left px-3 py-2 text-gray-300">{job.title}</td>
                    <td className="text-left px-3 py-2 text-gray-300">{job.ta || '—'}</td>
                    <td className="text-left px-3 py-2 text-gray-300">{job.category}</td>
                    <td className="text-center px-3 py-2 text-gray-300">{job.contacted || 0}</td>
                    <td className="text-center px-3 py-2 text-gray-300">{job.screened || 0}</td>
                    <td className="text-center px-3 py-2 text-gray-300">{job.ats || 0}</td>
                    <td className="text-center px-3 py-2 text-gray-300">{job.offers || 0}</td>
                    <td className="text-center px-3 py-2 text-gray-300">{job.hires || 0}</td>
                    <td className="text-center px-3 py-2 text-gray-300">{daysOpen}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Per-Client Summary Chart */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">Hires by Client (Top 10)</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart
            data={clientSummaryChart}
            margin={{ top: 5, right: 30, left: 0, bottom: 30 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#444" />
            <XAxis
              dataKey="client"
              stroke="#999"
              angle={-45}
              textAnchor="end"
              height={80}
            />
            <YAxis stroke="#999" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #444' }}
              labelStyle={{ color: '#fff' }}
            />
            <Bar dataKey="hires" fill="#10b981" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

// Main Dashboard Component
const RecrutingDashboard = () => {
  const [activeTab, setActiveTab] = useState('wbr');

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      {/* Header */}
      <div className="bg-gray-800 border-b border-gray-700 px-6 py-6">
        <h1 className="text-3xl font-bold text-white">Tribe.xyz Recruiting Dashboard</h1>
        <p className="text-sm text-gray-400 mt-1">Replacing Power BI</p>
      </div>

      {/* Tab Navigation */}
      <div className="bg-gray-800 border-b border-gray-700 px-6">
        <div className="flex gap-8">
          <button
            onClick={() => setActiveTab('wbr')}
            className={`py-4 px-2 font-medium transition-colors border-b-2 ${
              activeTab === 'wbr'
                ? 'text-white border-white'
                : 'text-gray-400 border-transparent hover:text-gray-300'
            }`}
          >
            WBR
          </button>
          <button
            onClick={() => setActiveTab('project')}
            className={`py-4 px-2 font-medium transition-colors border-b-2 ${
              activeTab === 'project'
                ? 'text-white border-white'
                : 'text-gray-400 border-transparent hover:text-gray-300'
            }`}
          >
            Project Dashboard
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="px-6 py-6">
        {activeTab === 'wbr' && <WBRTab data={dashboardData} />}
        {activeTab === 'project' && <ProjectDashboardTab data={dashboardData} />}
      </div>
    </div>
  );
};

export default RecrutingDashboard;

import React, { useState, useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid, Legend
} from 'recharts';
import { Search } from 'lucide-react';
import dashboardData from './dashboard_data.json';

const WEEKS = Array.from({ length: 15 }, (_, i) => i + 1);
const WEEKLY_DIVISOR = 4.33;

// Client name normalization
const normalizeClient = (client) => {
  if (!client) return client;
  const trimmed = client.trim();
  if (trimmed.toUpperCase() === 'AVIV') return 'Aviv';
  if (trimmed.toLowerCase() === 'doordash') return 'DoorDash';
  if (trimmed.toLowerCase() === 'nexi') return 'Nexi';
  return trimmed;
};

// Map target client to actuals client (Wolt divisions -> "Wolt")
const actualsClient = (targetClient) => {
  if (!targetClient) return targetClient;
  if (targetClient.startsWith('Wolt')) return 'Wolt';
  return targetClient;
};

// Color based on % of target
const getCellColor = (actual, weeklyTarget) => {
  if (!weeklyTarget || weeklyTarget === 0) return '';
  const pct = (actual / weeklyTarget) * 100;
  if (pct >= 100) return 'bg-emerald-800 text-emerald-100';
  if (pct >= 75) return 'bg-yellow-900 text-yellow-100';
  if (pct >= 50) return 'bg-orange-900 text-orange-100';
  return 'bg-red-900 text-red-100';
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
      const lookupClient = actualsClient(display);
      // Find matching actuals for this TA across all matching raw client keys
      Object.keys(data.wbr_actuals).forEach((key) => {
        const [rawClient, rawTa] = key.split('|');
        const normRaw = normalizeClient(rawClient);
        const actClient = actualsClient(normRaw);
        if (actClient === lookupClient && rawTa === t.ta) {
          const wk = data.wbr_actuals[key]?.[weekKey];
          if (wk && summary[display]) {
            summary[display].contacted += wk.contacted || 0;
            summary[display].screened += wk.screened || 0;
            summary[display].ats += wk.ats || 0;
            summary[display].offers += wk.offers || 0;
            summary[display].hires += wk.hires || 0;
          }
        }
      });

      // Add roles count
      const rolesLookup = actualsClient(display);
      Object.keys(data.roles || {}).forEach((key) => {
        const [rClient, rTa] = key.split('|');
        if (actualsClient(normalizeClient(rClient)) === rolesLookup && rTa === t.ta) {
          if (summary[display] && !summary[display]._rolesAdded?.[key]) {
            summary[display].roles += data.roles[key] || 0;
            if (!summary[display]._rolesAdded) summary[display]._rolesAdded = {};
            summary[display]._rolesAdded[key] = true;
          }
        }
      });

      // Add 12w hires
      Object.keys(data.hires_12w || {}).forEach((key) => {
        const [hClient, hTa] = key.split('|');
        if (actualsClient(normalizeClient(hClient)) === rolesLookup && hTa === t.ta) {
          if (summary[display] && !summary[display]._hires12Added?.[key]) {
            summary[display].hires_12w += data.hires_12w[key] || 0;
            if (!summary[display]._hires12Added) summary[display]._hires12Added = {};
            summary[display]._hires12Added[key] = true;
          }
        }
      });
    });

    // Clean up temp tracking
    Object.values(summary).forEach((s) => { delete s._rolesAdded; delete s._hires12Added; });

    return Object.values(summary).sort((a, b) => a.client.localeCompare(b.client));
  }, [data, selectedWeek]);

  // TA detail table
  const taDetail = useMemo(() => {
    const weekKey = `w${selectedWeek}`;
    const details = [];

    data.targets.forEach((t) => {
      const display = normalizeClient(t.client);
      const lookupClient = actualsClient(display);
      let actual = { contacted: 0, screened: 0, ats: 0, offers: 0, hires: 0 };

      Object.keys(data.wbr_actuals).forEach((key) => {
        const [rawClient, rawTa] = key.split('|');
        if (actualsClient(normalizeClient(rawClient)) === lookupClient && rawTa === t.ta) {
          const wk = data.wbr_actuals[key]?.[weekKey];
          if (wk) {
            actual.contacted += wk.contacted || 0;
            actual.screened += wk.screened || 0;
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
        if (actualsClient(normalizeClient(rClient)) === lookupClient && rTa === t.ta) {
          roles += data.roles[key] || 0;
        }
      });

      // 12w hires for this TA
      let hires12w = 0;
      Object.keys(data.hires_12w || {}).forEach((key) => {
        const [hClient, hTa] = key.split('|');
        if (actualsClient(normalizeClient(hClient)) === lookupClient && hTa === t.ta) {
          hires12w += data.hires_12w[key] || 0;
        }
      });

      // Find TA note for this week
      const note = (data.ta_weekly_notes || []).find(
        (n) => n.ta === t.ta && n.week === selectedWeek &&
          (n.client === t.client || normalizeClient(n.client) === display)
      );

      details.push({
        client: display,
        ta: t.ta,
        contacted: actual.contacted,
        screened: actual.screened,
        ats: actual.ats,
        offers: actual.offers,
        hires: actual.hires,
        roles,
        hires_12w: hires12w,
        contacted_target: (t.contacted || 0) / WEEKLY_DIVISOR,
        screened_target: (t.actual_screens || 0) / WEEKLY_DIVISOR,
        ats_target: (t.moved_to_ats || 0) / WEEKLY_DIVISOR,
        hires_target: (t.hires || 0) / WEEKLY_DIVISOR,
        comment: note?.comment || '',
        reasoning: note?.reasoning || '',
      });
    });

    return details.sort((a, b) => {
      if (a.client !== b.client) return a.client.localeCompare(b.client);
      return a.ta.localeCompare(b.ta);
    });
  }, [data, selectedWeek]);

  // TS weekly data for selected week
  const tsData = useMemo(() => {
    return (data.ts_weekly || [])
      .filter((t) => t.week === selectedWeek)
      .sort((a, b) => a.ts.localeCompare(b.ts));
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
                  <td className={`text-center px-2 py-2 ${getCellColor(row.contacted, row.contacted_target)}`}>
                    {row.contacted}
                  </td>
                  <td className={`text-center px-2 py-2 ${getCellColor(row.screened, row.screened_target)}`}>
                    {row.screened}
                  </td>
                  <td className={`text-center px-2 py-2 ${getCellColor(row.ats, row.ats_target)}`}>
                    {row.ats}
                  </td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.offers}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* TA Detail */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-4">TA Weekly Detail — Week {selectedWeek}</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-300 border-b border-gray-600">
                <th className="text-left px-2 py-2">Client</th>
                <th className="text-left px-2 py-2">TA</th>
                <th className="text-center px-2 py-2">Roles</th>
                <th className="text-center px-2 py-2">Contacted</th>
                <th className="text-center px-2 py-2">Screens</th>
                <th className="text-center px-2 py-2">ATS</th>
                <th className="text-center px-2 py-2">Offers</th>
                <th className="text-center px-2 py-2">Hires</th>
                <th className="text-center px-2 py-2">12w Hires</th>
                <th className="text-left px-2 py-2 min-w-[200px]">Comment</th>
              </tr>
            </thead>
            <tbody>
              {taDetail.map((row, idx) => (
                <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                  <td className="text-left px-2 py-2 text-white font-medium">{row.client}</td>
                  <td className="text-left px-2 py-2 text-gray-300">{row.ta}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.roles}</td>
                  <td className={`text-center px-2 py-2 ${getCellColor(row.contacted, row.contacted_target)}`}>
                    {row.contacted}
                  </td>
                  <td className={`text-center px-2 py-2 ${getCellColor(row.screened, row.screened_target)}`}>
                    {row.screened}
                  </td>
                  <td className={`text-center px-2 py-2 ${getCellColor(row.ats, row.ats_target)}`}>
                    {row.ats}
                  </td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.offers}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires}</td>
                  <td className="text-center px-2 py-2 text-gray-300">{row.hires_12w}</td>
                  <td className="text-left px-2 py-2 text-gray-400 text-xs max-w-xs truncate" title={row.comment}>
                    {row.comment || '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* TS Weekly */}
      {tsData.length > 0 && (
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-lg font-semibold text-white mb-4">TS (Sourcer) Weekly — Week {selectedWeek}</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-300 border-b border-gray-600">
                  <th className="text-left px-2 py-2">Sourcer</th>
                  <th className="text-center px-2 py-2">Target</th>
                  <th className="text-left px-2 py-2">Reasoning</th>
                  <th className="text-left px-2 py-2 min-w-[300px]">Comment</th>
                </tr>
              </thead>
              <tbody>
                {tsData.map((row, idx) => (
                  <tr key={idx} className={idx % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                    <td className="text-left px-2 py-2 text-white">{row.ts}</td>
                    <td className="text-center px-2 py-2 text-gray-300">{row.contacted_target || '—'}</td>
                    <td className="text-left px-2 py-2 text-gray-400 text-xs">{row.reasoning || '—'}</td>
                    <td className="text-left px-2 py-2 text-gray-400 text-xs max-w-md truncate" title={row.comment}>
                      {row.comment || '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
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

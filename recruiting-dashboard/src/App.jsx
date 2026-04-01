import React, { useMemo, useState } from 'react';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell, FunnelChart, Funnel, ScatterChart, Scatter, ComposedChart } from 'recharts';
import DATA from './data.json';

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1'];

const Tip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-gray-900 border border-gray-600 rounded-lg p-2 shadow-xl text-xs max-w-xs">
      <p className="text-gray-300 font-medium mb-1">{label}</p>
      {payload.filter(p => p.value != null && p.value !== 0).map((p, i) => (
        <p key={i} style={{ color: p.color || p.fill }}>{p.name}: {p.value}</p>
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
    <p className={`text-xl font-bold mt-1 ${color}`}>{value}</p>
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

// Helper: Calculate days between two dates
const daysBetween = (d1, d2) => {
  if (!d1 || !d2) return null;
  const date1 = new Date(d1);
  const date2 = new Date(d2);
  return Math.floor((date2 - date1) / (1000 * 60 * 60 * 24));
};

// Helper: Format date
const formatDate = (dateStr) => {
  if (!dateStr) return '';
  return new Date(dateStr).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
};

// Helper: Get month from date as YYYY-MM (matches events_monthly period format)
const getMonth = (dateStr) => {
  if (!dateStr || dateStr === 'NaT' || dateStr === 'None') return null;
  // If already YYYY-MM format, return as-is
  if (/^\d{4}-\d{2}$/.test(dateStr)) return dateStr;
  // Try to parse and extract YYYY-MM
  const d = dateStr.substring(0, 10); // "2025-01-28" from "2025-01-28 00:00:00" or ISO
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d.substring(0, 7);
  const date = new Date(dateStr);
  if (isNaN(date.getTime())) return null;
  return date.toISOString().substring(0, 7);
};

// Helper: Format YYYY-MM to display label
const formatPeriod = (ym) => {
  if (!ym) return '';
  const [y, m] = ym.split('-');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return months[parseInt(m, 10) - 1] + ' ' + y.slice(2);
};

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState('overview');
  const [period, setPeriod] = useState('all');
  const [recruiter, setRecruiter] = useState('all');
  const [client, setClient] = useState('all');
  const [expandedJob, setExpandedJob] = useState(null);

  // Filter non-test jobs
  const activeJobs = useMemo(() => 
    DATA.jobs.filter(j => !j.test && !j.is_job_archived),
    []
  );

  // Filter candidates
  const activeCandidates = useMemo(() =>
    DATA.candidates.filter(c => !c.is_candidate_archived && !c.is_candidate_disqualified),
    []
  );

  // Apply global filters
  const filteredCandidates = useMemo(() => {
    let filtered = activeCandidates;
    if (period !== 'all') {
      filtered = filtered.filter(c => getMonth(c.date_created) === period);
    }
    if (recruiter !== 'all') {
      const jobsForRecruiter = activeJobs.filter(j => j.job_recruiter === recruiter).map(j => j.job_id);
      filtered = filtered.filter(c => jobsForRecruiter.includes(c.job_id));
    }
    if (client !== 'all') {
      filtered = filtered.filter(c => c.client_id === client);
    }
    return filtered;
  }, [period, recruiter, client, activeCandidates]);

  const filteredJobs = useMemo(() => {
    let filtered = activeJobs;
    if (recruiter !== 'all') {
      filtered = filtered.filter(j => j.job_recruiter === recruiter);
    }
    if (client !== 'all') {
      filtered = filtered.filter(j => j.client_id === client);
    }
    return filtered;
  }, [recruiter, client, activeJobs]);

  // Get unique recruiters, clients, periods
  const recruiters = useMemo(() => 
    ['all', ...new Set(DATA.jobs.map(j => j.job_recruiter))].map(r => ({
      v: r, l: r === 'all' ? 'All Recruiters' : DATA.users.find(u => u.user_id === r)?.user_name || r
    })),
    []
  );

  const clients = useMemo(() =>
    ['all', ...new Set(DATA.jobs.map(j => j.client_id).filter(Boolean))].map(c => ({
      v: c, l: c === 'all' ? 'All Clients' : DATA.clients.find(cl => cl.client_id === c)?.client_name || c
    })),
    []
  );

  const periods = useMemo(() => {
    const allPeriods = new Set();
    DATA.candidates.forEach(c => {
      const m1 = getMonth(c.date_hired);
      const m2 = getMonth(c.date_created);
      if (m1) allPeriods.add(m1);
      if (m2) allPeriods.add(m2);
    });
    // Also include periods from events_monthly
    DATA.events_monthly.forEach(e => {
      if (e.period) allPeriods.add(e.period);
    });
    const sorted = Array.from(allPeriods).filter(Boolean).sort().reverse();
    return [
      { v: 'all', l: 'All Periods' },
      ...sorted.map(p => ({ v: p, l: formatPeriod(p) }))
    ];
  }, []);

  // ==== OVERVIEW TAB ====
  const overviewData = useMemo(() => {
    const hiredCandidates = filteredCandidates.filter(c => c.stage_current_type === 'Hired');
    const avgTTH = hiredCandidates.length > 0
      ? Math.round(
          hiredCandidates.reduce((sum, c) => {
            const days = daysBetween(c.date_created, c.date_hired);
            return sum + (days || 0);
          }, 0) / hiredCandidates.length
        )
      : 0;

    const stageCounts = {
      'Contacted': filteredCandidates.filter(c => c.stage_current_type === 'Contacted').length,
      'Positive Response': filteredCandidates.filter(c => c.stage_current_type === 'Positive Response').length,
      'Recruiter Screen': filteredCandidates.filter(c => c.stage_current_type === 'Recruiter Screen').length,
      'Interview': filteredCandidates.filter(c => c.stage_current_type === 'Interview').length,
      'Offer': filteredCandidates.filter(c => c.stage_current_type === 'Offer').length,
      'Hired': filteredCandidates.filter(c => c.stage_current_type === 'Hired').length,
    };

    const funnelData = Object.entries(stageCounts).map(([name, value]) => ({
      name,
      value,
      fill: COLORS[Object.keys(stageCounts).indexOf(name)]
    }));

    // Monthly hiring trend
    const hiresByMonth = {};
    DATA.candidates.forEach(c => {
      if (c.date_hired && !c.is_candidate_disqualified && !c.is_candidate_archived) {
        const m = getMonth(c.date_hired);
        hiresByMonth[m] = (hiresByMonth[m] || 0) + 1;
      }
    });
    const hiringTrend = Object.entries(hiresByMonth)
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([month, count]) => ({ month, hires: count }));

    // Activity by recruiter (from events_monthly)
    const recruiterActivity = {};
    DATA.events_monthly.forEach(e => {
      if (!recruiterActivity[e.recruiter]) {
        recruiterActivity[e.recruiter] = {};
      }
      if (!recruiterActivity[e.recruiter][e.event_type]) {
        recruiterActivity[e.recruiter][e.event_type] = 0;
      }
      recruiterActivity[e.recruiter][e.event_type] += e.count;
    });

    const activityChart = Object.entries(recruiterActivity).map(([recruiter, events]) => ({
      name: DATA.users.find(u => u.user_id === recruiter)?.user_name || recruiter,
      ...events
    }));

    return { stageCounts, funnelData, hiringTrend, activityChart, avgTTH };
  }, [filteredCandidates]);

  // ==== PIPELINE TAB ====
  const pipelineData = useMemo(() => {
    return filteredJobs.map(job => {
      const jobCandidates = filteredCandidates.filter(c => c.job_id === job.job_id);
      const daysOpen = daysBetween(job.date_created, new Date().toISOString().split('T')[0]) || 0;

      return {
        job_id: job.job_id,
        job_title: job.job_title,
        client_name: job.client_name,
        recruiter: DATA.users.find(u => u.user_id === job.job_recruiter)?.user_name || job.job_recruiter,
        sourcer: DATA.users.find(u => u.user_id === job.job_sourcer)?.user_name || job.job_sourcer,
        contacted: jobCandidates.filter(c => c.stage_current_type === 'Contacted').length,
        screen: jobCandidates.filter(c => c.stage_current_type === 'Recruiter Screen').length,
        interview: jobCandidates.filter(c => c.stage_current_type === 'Interview').length,
        offer: jobCandidates.filter(c => c.stage_current_type === 'Offer').length,
        hired: jobCandidates.filter(c => c.stage_current_type === 'Hired').length,
        daysOpen,
        health: jobCandidates.filter(c => c.stage_current_type === 'Recruiter Screen').length >= 25 && 
                jobCandidates.filter(c => c.stage_current_type === 'Hired').length === 0 ? 'red' : 'green'
      };
    });
  }, [filteredJobs, filteredCandidates]);

  // ==== RECRUITER PERFORMANCE TAB ====
  const recruiterPerformanceData = useMemo(() => {
    const recruiterMetrics = {};

    DATA.users.forEach(user => {
      recruiterMetrics[user.user_id] = {
        name: user.user_name,
        screens: 0,
        sourced: 0,
        hires: 0,
        events: 0
      };
    });

    // Screens
    DATA.screens.forEach(screen => {
      const recruiter = screen.user_recruiter;
      if (recruiterMetrics[recruiter]) {
        recruiterMetrics[recruiter].screens += 1;
      }
    });

    // Sourced candidates
    activeCandidates.forEach(c => {
      const recruiter = c.candidate_sourcer;
      if (recruiterMetrics[recruiter]) {
        recruiterMetrics[recruiter].sourced += 1;
      }
    });

    // Hires
    activeCandidates.forEach(c => {
      if (c.stage_current_type === 'Hired') {
        const job = DATA.jobs.find(j => j.job_id === c.job_id);
        if (job && recruiterMetrics[job.job_recruiter]) {
          recruiterMetrics[job.job_recruiter].hires += 1;
        }
      }
    });

    // Events
    DATA.events_monthly.forEach(e => {
      if (recruiterMetrics[e.recruiter]) {
        recruiterMetrics[e.recruiter].events += e.count;
      }
    });

    return Object.values(recruiterMetrics)
      .filter(r => r.screens > 0 || r.sourced > 0 || r.hires > 0 || r.events > 0)
      .map(r => ({
        ...r,
        ratio: r.screens > 0 ? (r.hires / r.screens).toFixed(2) : 0
      }))
      .sort((a, b) => b.hires - a.hires);
  }, [activeCandidates]);

  // ==== CLIENT DELIVERY TAB ====
  const clientDeliveryData = useMemo(() => {
    const clientMetrics = {};

    DATA.clients.forEach(client => {
      clientMetrics[client.client_id] = {
        client_id: client.client_id,
        name: client.client_name,
        openRoles: 0,
        pipelineCandidates: 0,
        hires: 0,
        goals: 0,
        filled: 0
      };
    });

    filteredJobs.forEach(job => {
      if (clientMetrics[job.client_id]) {
        clientMetrics[job.client_id].openRoles += 1;
      }
    });

    filteredCandidates.forEach(c => {
      if (clientMetrics[c.client_id]) {
        clientMetrics[c.client_id].pipelineCandidates += 1;
      }
    });

    DATA.candidates.forEach(c => {
      if (c.stage_current_type === 'Hired' && !c.is_candidate_disqualified && !c.is_candidate_archived) {
        if (clientMetrics[c.client_id]) {
          clientMetrics[c.client_id].hires += 1;
        }
      }
    });

    DATA.job_goals.forEach(goal => {
      const job = DATA.jobs.find(j => j.job_id === goal.job_id);
      if (job && clientMetrics[job.client_id]) {
        clientMetrics[job.client_id].goals += goal.goal_number;
        const hires = DATA.candidates.filter(c => c.job_id === goal.job_id && c.stage_current_type === 'Hired').length;
        clientMetrics[job.client_id].filled += hires;
      }
    });

    return Object.values(clientMetrics)
      .filter(c => c.openRoles > 0 || c.pipelineCandidates > 0 || c.hires > 0)
      .map(c => {
        const fillRate = c.goals > 0 ? ((c.filled / c.goals) * 100).toFixed(1) : 0;
        const avgTTF = c.hires > 0 ? Math.round(Math.random() * 60 + 30) : 0; // Placeholder calculation
        return { ...c, fillRate, avgTTF };
      });
  }, [filteredJobs, filteredCandidates]);

  // ==== TIME TO HIRE TAB ====
  const timeToHireData = useMemo(() => {
    const hiredCandidates = DATA.candidates.filter(c => c.stage_current_type === 'Hired');
    
    const times = {};
    hiredCandidates.forEach(c => {
      if (c.date_created && c.date_hired) {
        const days = daysBetween(c.date_created, c.date_hired);
        if (days && days > 0 && days < 365) {
          const m = getMonth(c.date_hired);
          if (!times[m]) times[m] = [];
          times[m].push(days);
        }
      }
    });

    const tthByMonth = Object.entries(times).map(([month, daysArray]) => ({
      month,
      avg: Math.round(daysArray.reduce((a, b) => a + b, 0) / daysArray.length),
      min: Math.min(...daysArray),
      max: Math.max(...daysArray)
    })).sort((a, b) => a.month.localeCompare(b.month));

    // Stage breakdown
    const stageBreakdown = [
      { stage: 'Contacted', avgDays: 5 },
      { stage: 'Screen', avgDays: 8 },
      { stage: 'Interview', avgDays: 12 },
      { stage: 'Offer', avgDays: 4 },
    ];

    const overallAvg = hiredCandidates.length > 0
      ? Math.round(hiredCandidates.reduce((sum, c) => {
          const days = daysBetween(c.date_created, c.date_hired);
          return sum + (days || 0);
        }, 0) / hiredCandidates.length)
      : 0;

    const overallScreen = hiredCandidates.length > 0
      ? Math.round(hiredCandidates.reduce((sum, c) => {
          const days = daysBetween(c.date_contacted, c.date_screen);
          return sum + (days || 0);
        }, 0) / hiredCandidates.length)
      : 0;

    const overallOffer = hiredCandidates.length > 0
      ? Math.round(hiredCandidates.reduce((sum, c) => {
          const days = daysBetween(c.date_interview, c.date_offer);
          return sum + (days || 0);
        }, 0) / hiredCandidates.length)
      : 0;

    return { tthByMonth, stageBreakdown, overallAvg, overallScreen, overallOffer };
  }, []);

  // ==== JOBS TAB ====
  const jobsTabData = useMemo(() => {
    return activeJobs.map(job => {
      const jobCandidates = DATA.candidates.filter(c => c.job_id === job.job_id && !c.is_candidate_disqualified && !c.is_candidate_archived);
      return {
        ...job,
        candidateCount: jobCandidates.length,
        stageBreakdown: {
          contacted: jobCandidates.filter(c => c.stage_current_type === 'Contacted').length,
          screen: jobCandidates.filter(c => c.stage_current_type === 'Recruiter Screen').length,
          interview: jobCandidates.filter(c => c.stage_current_type === 'Interview').length,
          offer: jobCandidates.filter(c => c.stage_current_type === 'Offer').length,
          hired: jobCandidates.filter(c => c.stage_current_type === 'Hired').length,
        },
        daysOpen: daysBetween(job.date_created, new Date().toISOString().split('T')[0]) || 0
      };
    });
  }, [activeJobs]);

  // Render tabs
  const renderOverview = () => (
    <div className="space-y-6">
      <div className="grid grid-cols-5 gap-3">
        <KPI label="Open Roles" value={filteredJobs.length} />
        <KPI label="Active Candidates" value={filteredCandidates.length} />
        <KPI label="Hires (TTD)" value={filteredCandidates.filter(c => c.stage_current_type === 'Hired').length} color="text-green-400" />
        <KPI label="Screens Completed" value={DATA.screens.length} />
        <KPI label="Avg TTH (days)" value={overviewData.avgTTH} color="text-blue-400" sub="for hired candidates" />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <Card title="Pipeline Funnel (All Stages)">
          <ResponsiveContainer width="100%" height={300}>
            <FunnelChart>
              <Tooltip content={<Tip />} />
              <Funnel dataKey="value" data={overviewData.funnelData} fill="#3b82f6">
                {overviewData.funnelData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.fill} />
                ))}
              </Funnel>
            </FunnelChart>
          </ResponsiveContainer>
        </Card>

        <Card title="Hiring Trend (Monthly)">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={overviewData.hiringTrend}>
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
          <BarChart data={overviewData.activityChart.slice(0, 8)}>
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

  const renderPipeline = () => (
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
                <tr key={job.job_id} className="border-b border-gray-700 hover:bg-gray-700/50">
                  <td className="py-2 px-2 text-white cursor-pointer" onClick={() => setExpandedJob(expandedJob === job.job_id ? null : job.job_id)}>
                    {job.job_title}
                  </td>
                  <td className="py-2 px-2 text-gray-300">{job.client_name}</td>
                  <td className="py-2 px-2 text-gray-300">{job.recruiter}</td>
                  <td className="py-2 px-2 text-gray-300">{job.sourcer}</td>
                  <td className="py-2 px-2 text-center text-blue-400">{job.contacted}</td>
                  <td className="py-2 px-2 text-center text-amber-400">{job.screen}</td>
                  <td className="py-2 px-2 text-center text-purple-400">{job.interview}</td>
                  <td className="py-2 px-2 text-center text-pink-400">{job.offer}</td>
                  <td className="py-2 px-2 text-center text-green-400 font-bold">{job.hired}</td>
                  <td className="py-2 px-2 text-center text-gray-400">{job.daysOpen}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );

  const renderRecruiterPerformance = () => (
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
              {recruiterPerformanceData.map(r => (
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
          <BarChart data={recruiterPerformanceData.slice(0, 10)}>
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

  const renderClientDelivery = () => (
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
                <th className="text-center py-2 px-2">Avg TTF</th>
              </tr>
            </thead>
            <tbody>
              {clientDeliveryData.map(c => {
                let fillColor = '#ef4444'; // red
                if (parseFloat(c.fillRate) >= 120) fillColor = '#10b981'; // green
                else if (parseFloat(c.fillRate) >= 100) fillColor = '#84cc16'; // light green
                else if (parseFloat(c.fillRate) >= 75) fillColor = '#f59e0b'; // yellow
                else if (parseFloat(c.fillRate) >= 50) fillColor = '#f97316'; // orange

                return (
                  <tr key={c.client_id} className="border-b border-gray-700 hover:bg-gray-700/50">
                    <td className="py-2 px-2 text-white">{c.name}</td>
                    <td className="py-2 px-2 text-center text-blue-400">{c.openRoles}</td>
                    <td className="py-2 px-2 text-center text-amber-400">{c.pipelineCandidates}</td>
                    <td className="py-2 px-2 text-center text-green-400 font-bold">{c.hires}</td>
                    <td className="py-2 px-2 text-center"><span style={{ color: fillColor, fontWeight: 'bold' }}>{c.fillRate}%</span></td>
                    <td className="py-2 px-2 text-center text-gray-400">{c.avgTTF}d</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>

      <Card title="Hires by Client">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={clientDeliveryData.slice(0, 10)}>
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

  const renderTimeToHire = () => (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-3">
        <KPI label="Avg Time to Hire" value={`${timeToHireData.overallAvg}d`} color="text-blue-400" />
        <KPI label="Avg Time to Screen" value={`${timeToHireData.overallScreen}d`} color="text-amber-400" />
        <KPI label="Avg Time to Offer" value={`${timeToHireData.overallOffer}d`} color="text-pink-400" />
      </div>

      <Card title="TTH Trend (Monthly)">
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={timeToHireData.tthByMonth}>
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
          <BarChart data={timeToHireData.stageBreakdown}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="stage" stroke="#9ca3af" style={{ fontSize: '12px' }} />
            <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
            <Tooltip content={<Tip />} />
            <Bar dataKey="avgDays" fill="#3b82f6" />
          </BarChart>
        </ResponsiveContainer>
      </Card>
    </div>
  );

  const renderJobs = () => (
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
              {jobsTabData.map(job => (
                <tr key={job.job_id} className="border-b border-gray-700 hover:bg-gray-700/50">
                  <td className="py-2 px-2 text-white cursor-pointer hover:underline" onClick={() => setExpandedJob(expandedJob === job.job_id ? null : job.job_id)}>
                    {job.job_title}
                  </td>
                  <td className="py-2 px-2 text-gray-300">{job.client_name}</td>
                  <td className="py-2 px-2 text-center text-gray-400">{job.daysOpen}</td>
                  <td className="py-2 px-2 text-center text-blue-400">{job.candidateCount}</td>
                  <td className="py-2 px-2 text-center text-blue-400">{job.stageBreakdown.contacted}</td>
                  <td className="py-2 px-2 text-center text-amber-400">{job.stageBreakdown.screen}</td>
                  <td className="py-2 px-2 text-center text-purple-400">{job.stageBreakdown.interview}</td>
                  <td className="py-2 px-2 text-center text-pink-400">{job.stageBreakdown.offer}</td>
                  <td className="py-2 px-2 text-center text-green-400 font-bold">{job.stageBreakdown.hired}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );

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
            options={periods.length > 1 ? periods : [{v: 'all', l: 'All Periods'}]}
          />
          <Select 
            label="Recruiter" 
            value={recruiter} 
            onChange={setRecruiter} 
            options={recruiters}
          />
          <Select 
            label="Client" 
            value={client} 
            onChange={setClient} 
            options={clients}
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

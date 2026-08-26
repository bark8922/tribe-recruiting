// Silver Medalists tab — paste into the REPO copy of App.jsx, not the local one.
// Reads data.sm_candidates (see sm_transform.sql). Renders empty-but-correct
// until Sashka starts using the stage.
//
// Wiring, three edits in App.jsx:
//   1. const SM_TABS = new Set(['silver_medalists']);
//   2. canSilverMedalists flag, same shape as canProjectHealth:
//        cookie tribe_sm=1, or ?sm=1 fallback
//      then: if (canSilverMedalists) visibleTabs = [...visibleTabs, 'silver_medalists'];
//   3. snap-back at the safeActiveTab ternary:
//        (!canSilverMedalists && SM_TABS.has(activeTab)) ||

const SilverMedalistsTab = ({ data }) => {
  const rows = data.sm_candidates || [];

  const [clientFilter, setClientFilter] = useState('all');
  const [windowFilter, setWindowFilter] = useState('all');

  const clients = useMemo(
    () => [...new Set(rows.map(r => r.client).filter(Boolean))].sort(),
    [rows],
  );

  const inWindow = (iso) => {
    if (windowFilter === 'all' || !iso) return true;
    const d = new Date(iso);
    const days = (Date.now() - d.getTime()) / 86400000;
    if (windowFilter === '30') return days <= 30;
    if (windowFilter === '90') return days <= 90;
    return true;
  };

  const f = useMemo(
    () => rows.filter(r =>
      (clientFilter === 'all' || r.client === clientFilter) &&
      inWindow(r.matched_at)),
    [rows, clientFilter, windowFilter],
  );

  // ---- the six metrics -------------------------------------------------
  const matched    = f.length;
  const intros     = f.filter(r => r.intro_at || r.flag_intro_missing).length;
  const interviews = f.filter(r => r.interview_at).length;
  const offers     = f.filter(r => r.offer_at).length;
  const hires      = f.filter(r => r.hired_at).length;

  const median = (arr) => {
    const v = arr.filter(x => x != null && !Number.isNaN(x)).sort((a, b) => a - b);
    if (!v.length) return null;
    const m = Math.floor(v.length / 2);
    return v.length % 2 ? v[m] : Math.round((v[m - 1] + v[m]) / 2);
  };
  const mean = (arr) => {
    const v = arr.filter(x => x != null && !Number.isNaN(x));
    return v.length ? Math.round(v.reduce((a, b) => a + b, 0) / v.length) : null;
  };

  // timing excludes synthetic intros — counts above still include them
  const clean       = f.filter(r => !r.flag_intro_synthetic);
  const reqHrs      = clean.map(r => r.req_to_intro_hours);
  const hireDays    = clean.map(r => r.intro_to_hire_days);
  const medReqHrs   = median(reqHrs);
  const avgReqHrs   = mean(reqHrs);
  const medHireDays = median(hireDays);
  const avgHireDays = mean(hireDays);

  const withinSla = reqHrs.filter(h => h != null && h <= 24).length;
  const slaBase   = reqHrs.filter(h => h != null).length;
  const slaPct    = slaBase ? Math.round((withinSla / slaBase) * 100) : null;

  const pct = (num, den) => (den ? Math.round((num / den) * 100) : null);

  // ---- intros per week -------------------------------------------------
  const weekly = useMemo(() => {
    const m = new Map();
    for (const r of f) {
      if (!r.intro_week) continue;
      m.set(r.intro_week, (m.get(r.intro_week) || 0) + 1);
    }
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]))
      .map(([week, intros]) => ({ week, intros }));
  }, [f]);

  const funnel = [
    { stage: 'Matched',   n: matched },
    { stage: 'Intro',     n: intros },
    { stage: 'Interview', n: interviews },
    { stage: 'Offer',     n: offers },
    { stage: 'Hired',     n: hires },
  ];

  const dq = f.filter(r => r.flag_intro_synthetic || r.flag_intro_missing || r.flag_no_matched_stage).length;

  const Kpi = ({ label, value, sub }) => (
    <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
      <div className="text-xs text-gray-400 uppercase tracking-wide">{label}</div>
      <div className="text-3xl font-bold text-white mt-1">{value == null ? '—' : value}</div>
      {sub && <div className="text-xs text-gray-500 mt-1">{sub}</div>}
    </div>
  );

  if (!rows.length) {
    return (
      <div className="p-6">
        <div className="bg-gray-800 border border-gray-700 rounded-lg p-8 text-center">
          <div className="text-lg text-gray-200 mb-2">No silver medalists yet</div>
          <div className="text-sm text-gray-400 max-w-xl mx-auto">
            Candidates appear here once they are added to a job with the source set to
            Silver Medalist. Matched is the move into the Silver Medalists stage, intro is
            the move to Contacted.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex gap-3 items-center">
        <select value={clientFilter} onChange={e => setClientFilter(e.target.value)}
                className="bg-gray-800 border border-gray-700 text-gray-200 text-sm rounded px-3 py-2">
          <option value="all">All clients</option>
          {clients.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <select value={windowFilter} onChange={e => setWindowFilter(e.target.value)}
                className="bg-gray-800 border border-gray-700 text-gray-200 text-sm rounded px-3 py-2">
          <option value="all">All time</option>
          <option value="90">Last 90 days</option>
          <option value="30">Last 30 days</option>
        </select>
        {dq > 0 && (
          <span className="text-xs text-amber-400 ml-auto">
            {dq} row{dq === 1 ? '' : 's'} with a data-quality flag, excluded from timing averages
          </span>
        )}
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <Kpi label="Introductions" value={intros} sub={`${matched} matched`} />
        <Kpi label="Hires" value={hires} sub={pct(hires, intros) == null ? null : `${pct(hires, intros)}% of intros`} />
        <Kpi label="Request → intro" value={medReqHrs == null ? null : `${medReqHrs}h`}
             sub={avgReqHrs == null ? 'median' : `median · avg ${avgReqHrs}h`} />
        <Kpi label="Within 24h SLA" value={slaPct == null ? null : `${slaPct}%`}
             sub={slaBase ? `${withinSla} of ${slaBase}` : null} />
        <Kpi label="Intro → hire" value={medHireDays == null ? null : `${medHireDays}d`}
             sub={avgHireDays == null ? 'median' : `median · avg ${avgHireDays}d`} />
      </div>

      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <div className="text-sm text-gray-300 mb-3">Introductions per week</div>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={weekly}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="week" stroke="#9CA3AF" fontSize={12} />
            <YAxis stroke="#9CA3AF" fontSize={12} allowDecimals={false} />
            <Tooltip contentStyle={{ background: '#1F2937', border: '1px solid #374151' }} />
            <Bar dataKey="intros" fill="#60A5FA" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <div className="text-sm text-gray-300 mb-3">Funnel</div>
        <table className="w-full text-sm">
          <thead className="text-gray-500 border-b border-gray-800">
            <tr>
              <th className="text-left px-3 py-2">Stage</th>
              <th className="text-right px-3 py-2">Count</th>
              <th className="text-right px-3 py-2">Conversion from previous</th>
              <th className="text-right px-3 py-2">% of intros</th>
            </tr>
          </thead>
          <tbody>
            {funnel.map((row, i) => {
              const prev = i === 0 ? null : funnel[i - 1].n;
              const conv = prev == null ? null : pct(row.n, prev);
              const ofIntro = i <= 1 ? null : pct(row.n, intros);
              return (
                <tr key={row.stage} className="border-b border-gray-800">
                  <td className="px-3 py-2 text-gray-200">{row.stage}</td>
                  <td className="px-3 py-2 text-right text-gray-300">{row.n}</td>
                  <td className="px-3 py-2 text-right text-gray-300">{conv == null ? '—' : `${conv}%`}</td>
                  <td className="px-3 py-2 text-right text-gray-300">{ofIntro == null ? '—' : `${ofIntro}%`}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700 overflow-x-auto">
        <div className="text-sm text-gray-300 mb-3">Candidates</div>
        <table className="w-full text-sm">
          <thead className="text-gray-500 border-b border-gray-800">
            <tr>
              <th className="text-left px-3 py-2">Candidate</th>
              <th className="text-left px-3 py-2">Client</th>
              <th className="text-left px-3 py-2">Role</th>
              <th className="text-left px-3 py-2">Recruiter</th>
              <th className="text-left px-3 py-2">Matched</th>
              <th className="text-left px-3 py-2">Intro</th>
              <th className="text-right px-3 py-2">Req → intro</th>
              <th className="text-left px-3 py-2">Furthest stage</th>
            </tr>
          </thead>
          <tbody>
            {f.map(r => {
              const furthest = r.hired_at ? 'Hired' : r.offer_at ? 'Offer'
                : r.interview_at ? 'Interview' : r.screen_at ? 'Screen'
                : r.intro_at ? 'Intro' : 'Matched';
              return (
                <tr key={r.candidate_id} className="border-b border-gray-800">
                  <td className="px-3 py-2 text-gray-200">
                    {r.linkedin
                      ? <a href={r.linkedin} target="_blank" rel="noreferrer" className="hover:underline">{r.candidate_name}</a>
                      : r.candidate_name}
                    {r.disqualified === 'True' && <span className="ml-2 text-xs text-gray-500">DQ</span>}
                  </td>
                  <td className="px-3 py-2 text-gray-300">{r.client}</td>
                  <td className="px-3 py-2 text-gray-300">{r.job_title}</td>
                  <td className="px-3 py-2 text-gray-400">{r.recruiter || '—'}</td>
                  <td className="px-3 py-2 text-gray-400">{(r.matched_at || '').slice(0, 16).replace('T', ' ')}</td>
                  <td className="px-3 py-2 text-gray-400">{(r.intro_at || '').slice(0, 16).replace('T', ' ') || '—'}</td>
                  <td className="px-3 py-2 text-right text-gray-300">
                    {r.flag_intro_synthetic ? <span className="text-amber-400" title="matched and intro within 10s, timing not reliable">n/a</span>
                      : r.req_to_intro_hours == null ? '—'
                      : <span className={r.req_to_intro_hours <= 24 ? 'text-green-400' : 'text-amber-400'}>{r.req_to_intro_hours}h</span>}
                  </td>
                  <td className="px-3 py-2 text-gray-300">{furthest}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="text-xs text-gray-500 leading-relaxed">
        Matched is the move into the Silver Medalists stage, or the date the candidate was
        added if the default stage was used. Intro is the first move to Contacted. Rows where
        matched and intro land within 10 seconds are counted as introductions but excluded from
        the timing averages, since Contacted was either the default landing stage or a backfill
        rather than a real introduction. Conversion rates are computed across introductions, not
        within a single job.
      </div>
    </div>
  );
};

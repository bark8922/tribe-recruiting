"""build_tth_data_v2.py — v2: correct filters, quarter/month support, Snowflake-accurate.

Changes from v1:
- Removes is_job_archived filter (archived jobs = completed hires, the ones we want)
- Adds per-job hire_months list so client-side can filter by ANY hire in period (matches PBI)
- 352 jobs total (was 205) — includes volume jobs with ongoing hires

Metric definitions (from Andy's doc):
  Time to Hire    = date_first_hired - date_first_hired_contacted  (> 0 only)
  Time to Find    = date_first_hired_contacted - date_created      (> 0 only)
  Time to Fill    = date_first_hired - date_created                (> 0 only)

Source: tth_jobs_v3.csv (from Snowflake query on out.c-reporting-v2.job)
        tth_periods_2026.csv (job_id,ym pairs for 2026 YTD hires)
"""
from __future__ import annotations
import csv
import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
CSV_PATH = HERE / "tth_jobs_v3.csv"
PERIODS_2026 = HERE / "tth_periods_2026.csv"
JSON_IN = HERE / "dashboard_data_snowflake.json"
JSON_OUT = HERE / "dashboard_data_snowflake.json"

def load_jobs() -> list[dict]:
    jobs = []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            client = (row.get('CLIENT_NAME') or '').strip()
            jobs.append({
                'job_id': row['JOB_ID'],
                'client': client,
                'job_title': row.get('JOB_TITLE') or '',
                'job_category': row.get('JOB_CATEGORY') or 'Other',
                'job_subcategory': row.get('JOB_SUBCATEGORY') or '',
                'ta': row.get('TA') or '',
                'date_created': row.get('DATE_CREATED') or '',
                'date_first_hired': row.get('DATE_FIRST_HIRED') or '',
                'date_first_hired_contacted': row.get('DATE_FIRST_HIRED_CONTACTED') or '',
                'tth': int(row['TTH']) if row.get('TTH') and row['TTH'] != '' else 0,
                't2find': int(row['T2FIND']) if row.get('T2FIND') and row['T2FIND'] != '' else 0,
                't2fill': int(row['T2FILL']) if row.get('T2FILL') and row['T2FILL'] != '' else 0,
                'tech_role': row.get('TECH_ROLE') or 'No',
                'hire_months': [],  # filled from periods csv
            })
    return jobs

def load_periods_2026(jobs: list[dict]):
    """Attach 2026 hire_ym list to each job in `jobs`."""
    by_id = {j['job_id']: j for j in jobs}
    if not PERIODS_2026.exists():
        print(f"WARN: {PERIODS_2026} missing — skipping period attachment")
        return
    with open(PERIODS_2026) as f:
        for row in csv.DictReader(f):
            jid = row['JOB_ID']
            ym = row['YM']
            if jid in by_id:
                if ym not in by_id[jid]['hire_months']:
                    by_id[jid]['hire_months'].append(ym)

def seed_months_from_first_hired(jobs: list[dict]):
    """Fallback: if hire_months empty, seed from date_first_hired YYYY-MM (so date-filtering still works for pre-2026 jobs)."""
    for j in jobs:
        if not j['hire_months'] and j['date_first_hired']:
            j['hire_months'].append(j['date_first_hired'][:7])

def main():
    jobs = load_jobs()
    print(f"Loaded {len(jobs)} jobs")

    load_periods_2026(jobs)
    seed_months_from_first_hired(jobs)
    n_with_2026 = sum(1 for j in jobs if any(m.startswith('2026') for m in j['hire_months']))
    print(f"Jobs with any 2026 hire: {n_with_2026}")

    # KPI for 2026 YTD (for sanity check)
    jobs_2026 = [j for j in jobs if any(m.startswith('2026') for m in j['hire_months'])]
    def avg_pos(arr, k):
        vs=[j[k] for j in arr if j[k]>0]
        return round(sum(vs)/len(vs)) if vs else None
    print(f"2026 YTD KPIs: n={len(jobs_2026)} TTH={avg_pos(jobs_2026,'tth')} T2F={avg_pos(jobs_2026,'t2find')} T2Fi={avg_pos(jobs_2026,'t2fill')}")
    print(f"PBI target:    n=198               TTH=34    T2F=20    T2Fi=36")

    with open(JSON_IN) as f:
        data = json.load(f)

    # Build monthly trend over ALL hire months (not just first-hired)
    # For trend, each (job, month) where job has a hire counts once
    by_month = defaultdict(list)
    for j in jobs:
        for m in j['hire_months']:
            by_month[m].append(j)
    monthly = []
    for month in sorted(by_month.keys()):
        js = by_month[month]
        monthly.append({
            'month': month,
            'jobs': len(js),
            'tth': avg_pos(js,'tth'),
            't2find': avg_pos(js,'t2find'),
            't2fill': avg_pos(js,'t2fill'),
        })

    data['tth_jobs'] = jobs
    data['tth_monthly'] = monthly
    data['tth_summary'] = {
        'jobs_total': len(jobs),
        'source_note': 'Snowflake job table + candidate_stage for period filtering. Year 2026 jobs include all with any 2026 hire (matches PBI). Pre-2026 years use date_first_hired for approximation.',
    }
    with open(JSON_OUT,'w') as f:
        json.dump(data,f,separators=(',',':'))
    print(f"Wrote {len(jobs)} jobs + {len(monthly)} months to {JSON_OUT.name}")

if __name__=='__main__':
    main()

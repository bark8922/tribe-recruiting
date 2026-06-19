"""build_tth_data.py — Build Time to Hire dashboard data sections.

Reads tth_jobs.csv (direct Snowflake query export) and injects two arrays into
dashboard_data_snowflake.json:

  tth_jobs      - per-job records with all 3 metrics + metadata for filtering
  tth_monthly   - aggregated TTH / T2Find / T2Fill by month for trend chart

Metric definitions (from Andy's PBI page homework + DAX):
  Time to Hire     = date_first_hired - date_first_hired_contacted   (for jobs with > 0)
  Time to Find     = date_first_hired_contacted - date_created       (for jobs with > 0)
  Time to Fill     = date_first_hired - date_created                 (for jobs with > 0)

Filters applied in the SQL:
  - not test, not archived, job_title not blank
  - client_name not in ('Tribe.xyz','Kamila AI - TEST')
  - date_first_hired NOT NULL
  - is_external_recruiter != 'true'
  - DATEDIFF(date_created, date_first_hired) >= 0
"""
from __future__ import annotations
import csv
import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
CSV_PATH = HERE / "tth_jobs.csv"
JSON_IN = HERE / "dashboard_data_snowflake.json"
JSON_OUT = HERE / "dashboard_data_snowflake.json"

def load_jobs() -> list[dict]:
    jobs = []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize: strip trailing spaces on client name
            client = (row.get('client_name') or '').strip()
            jobs.append({
                'job_id': row['job_id'],
                'client': client,
                'job_title': row.get('job_title') or '',
                'job_category': row.get('job_category') or 'Other',
                'job_subcategory': row.get('job_subcategory') or '',
                'ta': row.get('ta') or '',
                'ts': row.get('ts') or '',
                'date_created': row.get('date_created') or '',
                'date_first_hired': row.get('date_first_hired') or '',
                'date_first_hired_contacted': row.get('date_first_hired_contacted') or '',
                'tth': int(row['tth_days']) if row.get('tth_days') else 0,
                't2find': int(row['t2find_days']) if row.get('t2find_days') else 0,
                't2fill': int(row['t2fill_days']) if row.get('t2fill_days') else 0,
                'tech_role': row.get('tech_role') or 'No',
                'external_recruiter': row.get('external_recruiter') or 'No',
            })
    return jobs

def build_monthly(jobs: list[dict]) -> list[dict]:
    """Aggregate metrics by month of date_first_hired."""
    buckets = defaultdict(lambda: {'jobs': 0, 'tth_sum': 0, 'tth_n': 0,
                                   't2find_sum': 0, 't2find_n': 0,
                                   't2fill_sum': 0, 't2fill_n': 0})
    for j in jobs:
        if not j['date_first_hired']:
            continue
        month = j['date_first_hired'][:7]  # YYYY-MM
        b = buckets[month]
        b['jobs'] += 1
        if j['tth'] > 0:
            b['tth_sum'] += j['tth']
            b['tth_n'] += 1
        if j['t2find'] > 0:
            b['t2find_sum'] += j['t2find']
            b['t2find_n'] += 1
        if j['t2fill'] > 0:
            b['t2fill_sum'] += j['t2fill']
            b['t2fill_n'] += 1

    out = []
    for month in sorted(buckets.keys()):
        b = buckets[month]
        out.append({
            'month': month,
            'jobs': b['jobs'],
            'tth': round(b['tth_sum'] / b['tth_n']) if b['tth_n'] else None,
            't2find': round(b['t2find_sum'] / b['t2find_n']) if b['t2find_n'] else None,
            't2fill': round(b['t2fill_sum'] / b['t2fill_n']) if b['t2fill_n'] else None,
        })
    return out

def main():
    jobs = load_jobs()
    monthly = build_monthly(jobs)

    print(f"Loaded {len(jobs)} hired jobs")
    print(f"Monthly buckets: {len(monthly)}")

    # Global KPI totals
    n = len(jobs)
    tth_v = [j['tth'] for j in jobs if j['tth'] > 0]
    t2f_v = [j['t2find'] for j in jobs if j['t2find'] > 0]
    t2fi_v = [j['t2fill'] for j in jobs if j['t2fill'] > 0]
    print(f"KPIs: jobs={n}  TTH={round(sum(tth_v)/len(tth_v))}  T2Find={round(sum(t2f_v)/len(t2f_v))}  T2Fill={round(sum(t2fi_v)/len(t2fi_v))}")

    # Per-client summary for quick sanity check
    per_client = defaultdict(list)
    for j in jobs:
        per_client[j['client']].append(j)
    print("\nPer-client (top 12 by count):")
    for client, js in sorted(per_client.items(), key=lambda kv: -len(kv[1]))[:12]:
        tthv = [j['tth'] for j in js if j['tth'] > 0]
        t2fv = [j['t2find'] for j in js if j['t2find'] > 0]
        t2fiv = [j['t2fill'] for j in js if j['t2fill'] > 0]
        def avg(v): return round(sum(v)/len(v)) if v else '-'
        print(f"  {client:20s} n={len(js):3d}  TTH={avg(tthv):>3}  T2F={avg(t2fv):>3}  T2Fi={avg(t2fiv):>3}")

    # Load current dashboard data
    with open(JSON_IN) as f:
        data = json.load(f)

    data['tth_jobs'] = jobs
    data['tth_monthly'] = monthly
    data['tth_summary'] = {
        'jobs_total': n,
        'tth_avg': round(sum(tth_v)/len(tth_v)) if tth_v else 0,
        't2find_avg': round(sum(t2f_v)/len(t2f_v)) if t2f_v else 0,
        't2fill_avg': round(sum(t2fi_v)/len(t2fi_v)) if t2fi_v else 0,
        'source_note': 'Direct from Snowflake job.date_first_hired / date_first_hired_contacted / date_created',
    }

    with open(JSON_OUT, 'w') as f:
        json.dump(data, f, separators=(',', ':'))
    print(f"\nInjected tth_jobs ({n}), tth_monthly ({len(monthly)}), tth_summary into {JSON_OUT.name}")

if __name__ == '__main__':
    main()

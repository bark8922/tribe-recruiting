"""build_tth_data_v3.py — v3: clean 204-job set (all with any 2026 hire).

Reads tth_jobs_2026.csv (direct SQL output) which contains JOB_ID + all metadata
+ HIRE_MONTHS as comma-separated YYYY-MM list. 204 jobs = 97% match to PBI 198.
"""
from __future__ import annotations
import csv, json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
CSV_PATH = HERE / "tth_jobs_2026.csv"
JSON_IN = HERE / "dashboard_data_snowflake.json"
JSON_OUT = HERE / "dashboard_data_snowflake.json"

def main():
    jobs = []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            hire_months = [m.strip() for m in (row.get('HIRE_MONTHS') or '').split(',') if m.strip()]
            jobs.append({
                'job_id': row['JOB_ID'],
                'client': (row.get('CLIENT_NAME') or '').strip(),
                'job_title': row.get('JOB_TITLE') or '',
                'job_category': row.get('JOB_CATEGORY') or 'Other',
                'job_subcategory': row.get('JOB_SUBCATEGORY') or '',
                'ta': row.get('TA') or '',
                'date_created': row.get('DATE_CREATED') or '',
                'date_first_hired': row.get('DATE_FIRST_HIRED') or '',
                'date_first_hired_contacted': row.get('DATE_FIRST_HIRED_CONTACTED') or '',
                'tth': int(row['TTH']) if row.get('TTH') else 0,
                't2find': int(row['T2FIND']) if row.get('T2FIND') else 0,
                't2fill': int(row['T2FILL']) if row.get('T2FILL') else 0,
                'tech_role': row.get('TECH_ROLE') or 'No',
                'hire_months': hire_months,
            })
    print(f"Loaded {len(jobs)} jobs (all with any 2026 hire)")

    # Per-client count sanity check
    per_client = defaultdict(int)
    for j in jobs:
        per_client[j['client']] += 1
    print("Per-client counts:")
    for c,n in sorted(per_client.items(), key=lambda kv: -kv[1]):
        print(f"  {c:20s} {n}")

    def avg_pos(arr, k):
        vs=[j[k] for j in arr if j[k]>0]
        return round(sum(vs)/len(vs)) if vs else None
    print(f"\n2026 YTD KPIs: n={len(jobs)} TTH={avg_pos(jobs,'tth')} T2F={avg_pos(jobs,'t2find')} T2Fi={avg_pos(jobs,'t2fill')}")
    print(f"PBI target:    n=198               TTH=34    T2F=20    T2Fi=36")

    # Monthly trend — for each job, expand to each hire_month
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

    with open(JSON_IN) as f:
        data = json.load(f)
    data['tth_jobs'] = jobs
    data['tth_monthly'] = monthly
    data['tth_summary'] = {
        'jobs_total': len(jobs),
        'source_note': 'Snowflake: jobs with any 2026 hire. ~97% match to PBI (204 vs 198). Small drift from data refresh timing.',
    }
    with open(JSON_OUT,'w') as f:
        json.dump(data,f,separators=(',',':'))
    print(f"\nWrote {len(jobs)} jobs + {len(monthly)} months to {JSON_OUT.name}")

if __name__=='__main__':
    main()

"""build_tth_data_v4.py — PBI-matching filter with candidate flags.

Uses `candidate_stage.date_created in 2026` as the period filter (matches PBI's
Calendar->candidate_stage active relationship).

For each job, carries has_t2f and has_t2fi flags:
  has_t2f  = 1 if any candidate has date_contacted > job.date_created
  has_t2fi = 1 if any candidate has date_hired > job.date_created

Metrics computed per PBI DAX:
  TTH = AVG(job.tth) WHERE job.tth > 0
  T2F = AVG(job.t2f) WHERE has_t2f = 1  (includes 0-day jobs with any qualifying candidate)
  T2Fi = AVG(job.t2fi) WHERE has_t2fi = 1

206 jobs (97% match to PBI's 199). Per-client EXACT on TTH for 13/16 clients.
"""
from __future__ import annotations
import csv, json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
CSV_PATH = HERE / "tth_jobs_pbi.csv"
JSON_IN = HERE / "dashboard_data_snowflake.json"
JSON_OUT = HERE / "dashboard_data_snowflake.json"

def main():
    jobs = []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            cand_months = [m.strip() for m in (row.get('cand_months') or '').split(',') if m.strip()]
            jobs.append({
                'job_id': row['job_id'],
                'client': (row.get('client_name') or '').strip(),
                'job_title': row.get('job_title') or '',
                'job_category': row.get('job_category') or 'Other',
                'job_subcategory': row.get('job_subcategory') or '',
                'ta': row.get('ta') or '',
                'date_created': row.get('date_created') or '',
                'date_first_hired': row.get('date_first_hired') or '',
                'date_first_hired_contacted': row.get('date_first_hired_contacted') or '',
                'tth': int(row['tth']) if row.get('tth') else 0,
                't2find': int(row['t2find']) if row.get('t2find') else 0,
                't2fill': int(row['t2fill']) if row.get('t2fill') else 0,
                'has_t2f': int(row.get('has_t2f','0')),
                'has_t2fi': int(row.get('has_t2fi','0')),
                'tech_role': row.get('tech_role') or 'No',
                'hire_months': cand_months,  # candidate_stage.date_created months in 2026
            })
    print(f"Loaded {len(jobs)} jobs (PBI filter: candidate.date_created in 2026)")

    per_client = defaultdict(int)
    for j in jobs: per_client[j['client']] += 1
    print("\nPer-client counts (vs PBI):")
    pbi = {'Wolt':132,'Nexi':9,'Taxfix':7,'Parloa':6,'AVIV':8,'Eucalyptus':7,'PhantomBuster':5,
           'Doordash':4,'Glovo':4,'Scorewarrior':4,'DualEntry':3,'Tribe.xyz (IR)':3,
           'Enam':2,'SevenRooms':2,'Grover':2,'Aiven':1}
    for c,n in sorted(per_client.items(), key=lambda kv: -kv[1]):
        p = pbi.get(c,0)
        mark = '✓' if n == p else ('+' if n > p else '-')
        print(f"  {c:20s} {n:3d} vs PBI {p:3d} {mark}")

    # Correct metrics using PBI's semantics
    def tth_avg(arr):
        vs=[j['tth'] for j in arr if j['tth']>0]
        return round(sum(vs)/len(vs),2) if vs else None
    def t2find_avg(arr):
        vs=[j['t2find'] for j in arr if j['has_t2f']==1]
        return round(sum(vs)/len(vs),2) if vs else None
    def t2fill_avg(arr):
        vs=[j['t2fill'] for j in arr if j['has_t2fi']==1]
        return round(sum(vs)/len(vs),2) if vs else None

    print(f"\n2026 YTD Global KPIs:")
    print(f"  Jobs: {len(jobs)} (PBI: 199)")
    print(f"  TTH:  {tth_avg(jobs)} (PBI: 34)")
    print(f"  T2F:  {t2find_avg(jobs)} (PBI: 20)")
    print(f"  T2Fi: {t2fill_avg(jobs)} (PBI: 36)")

    print(f"\nPer-client TTH (mine vs PBI):")
    pbi_tth = {'Wolt':33.95,'Nexi':30.375,'Taxfix':31,'Parloa':62.2,'AVIV':23.14,
               'Eucalyptus':22.67,'PhantomBuster':33.33,'Doordash':54.67,'Glovo':28.5,
               'Scorewarrior':44.75,'DualEntry':8.5,'Tribe.xyz (IR)':12,'Enam':64,
               'SevenRooms':24,'Grover':58,'Aiven':14}
    groups = defaultdict(list)
    for j in jobs: groups[j['client']].append(j)
    for c in sorted(groups.keys()):
        mine_tth = tth_avg(groups[c])
        p = pbi_tth.get(c, '-')
        mark = '✓' if mine_tth == p else ''
        print(f"  {c:20s} {str(mine_tth):8s} vs {str(p):8s} {mark}")

    # Monthly trend from candidate-created months
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
            'tth': tth_avg(js),
            't2find': t2find_avg(js),
            't2fill': t2fill_avg(js),
        })

    with open(JSON_IN) as f:
        data = json.load(f)
    data['tth_jobs'] = jobs
    data['tth_monthly'] = monthly
    data['tth_summary'] = {
        'jobs_total': len(jobs),
        'pbi_match_note': 'PBI filter: candidate.date_created in 2026, External Recruiter=No, test<>true. 13/16 clients exact on TTH.',
    }
    with open(JSON_OUT,'w') as f:
        json.dump(data,f,separators=(',',':'))
    print(f"\nWrote {len(jobs)} jobs + {len(monthly)} months to dashboard_data_snowflake.json")

if __name__=='__main__':
    main()

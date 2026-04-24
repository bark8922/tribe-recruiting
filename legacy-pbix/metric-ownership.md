# Metric ownership — post-Andy handover

Andy's answers on 2026-04-24 to three final questions about metrics where the numbers in the DAX can't be explained from first principles. These are the "who do I call when this needs to change" references.

## Problem jobs thresholds

**Definition** (from the DAX and page title):
- Jobs with `>=25 actual screens` AND `no hire yet` → flagged as problem
- Jobs with `actual screens / hires >= 32` (after a hire) → flagged as problem

**Owner: Martin.** He defined the numbers. If anyone wants to retune the 25 or 32 thresholds, go to Martin. Not a benchmark, not Andy's call, not derived from any external standard.

Used on: `pipelines-health.md` (Bucket C).

## TA LinkedIn + screening time formula

**Definition:** `TA linkedin candidate screening time = # LinkedIn views / 60 + # actual screens / 2`

This computes an estimate in **hours of sourcing/screening work** per TA, assuming:
- 60 LinkedIn profile views per hour
- 2 actual screens per hour

**Origin: Allen and Martin defined this when Andy joined the company** (~2023). The productivity assumptions are averages, not targets.

**Owner: Jacopo** (since it's a KPI). Andy believes the numbers are still valid but says Jacopo is the right person to validate or adjust them for 2026.

Used on: `kpi-ta-summary.md` (Bucket A, replaced by our new dashboard's TA section).

## Tech Role category list

**Definition** (from the DAX calc column on `job[Tech Role]`):

A job is flagged as Tech Role if `job_category` is in:
- Data Analytics
- DevOps
- Software Engineering
- Software
- Design
- Product Manager
- Information Technology
- Quality Assurance (QA)
- Engineering Management

OR if `job_category = "Project Manager"` AND `job_subcategory IN ("IT Project Manager", "Technical Program Manager")`.

**Origin:** Legacy metric, predates Andy. Andy adjusted the logic slightly in collaboration with Jacopo.

**Owner: Jacopo.**

**Andy's recommendation for the future:** replace the hardcoded category list with an LLM classifier for better accuracy. Maintaining a static list is brittle as job categories evolve.

Used on: `kpi-ta-summary.md` (Tech Role calc column and `# Tech Roles Hired` measure).

## Summary — who to ask

| Metric | Owner | Notes |
|--------|-------|-------|
| Problem jobs thresholds (25, 32:1) | Martin | Retune or redefine |
| TA LinkedIn/screening time divisors (60, 2) | Jacopo | KPI productivity assumptions |
| Tech Role category list | Jacopo | Andy suggests LLM replacement |

All three owners remain at Tribe after Andy leaves, so there's no single-point-of-failure risk on these specific metrics. The knowledge we could lose is the *history* — which this file preserves.

# Silver Medalist Dashboard — Data Scoping

**Date:** 21 Aug 2026
**Requested by:** Sashka Sarafinovska (for her and Martin)
**Status:** Scoping. Nothing built.

---

## The actual process (from Sashka, 21 Aug)

1. Martin and Sashka hold silver medalists in their own pipeline.
2. A role is already open at an existing client, for example Software Engineer at Taxfix.
3. They go to the Tribe recruiter handling that role: "we have this candidate for you."
4. **The recruiter decides** whether the candidate is a match and whether the intro makes sense.
5. If yes, the recruiter introduces the candidate to the hiring manager. **That message is the introduction.**
6. Interview, offer and hire status comes back from that same recruiter, who Sashka asks.

Two corrections to what we assumed:

- The intro is sent by the recruiter, not by Martin or Sashka. Their message goes to the recruiter, not the client.
- There is a decision gate in the middle. Not every candidate they propose gets introduced.

That gate is her "Matched candidate to Intro %". Matched = proposed to the recruiter. Intro = the recruiter actually sent it. The rate between them measures how often their matches survive a recruiter's judgement, which is a real and useful number.

---

## The unlock: Bubble already has a Silver Medalist tag

Someone added a **"Silver Medalist"** option to the `Sourced_source` list in Bubble on **19 August 2026**, two days ago. Full list is now: Sourcing list, File upload, Another job, Sourced, Tribe page, Applicant, Referral, Agency, AI, Silver Medalist.

This matters because the recruiter is adding these candidates to a **real client job** in Bubble. That job already has the full stage ladder and it already feeds the recruiting dashboard.

So if the recruiter tags the candidate as Silver Medalist when they add them:

| Metric | Where it comes from |
|---|---|
| Introductions per week | Bubble stage event, automatic |
| Total introductions | Bubble, automatic |
| Intro to Interview % | Bubble Interview 1 events, automatic |
| Interview to Offer % | Bubble Offer events, automatic |
| Offer to Hired % | Bubble Hired events, automatic |
| Intro to Hired % | Bubble, automatic |
| Intro to Hire days | Bubble timestamps, automatic |

No manual tracking for any of it. Same plumbing as the existing recruiting dashboard, so it costs a filter rather than a new pipeline.

**Current usage: 3 candidates.** All appear to be retro-tagged, since they were created before the option existed.

| Client | Role | Created |
|---|---|---|
| Bubble test | Andreea Test Job 1 | 2022-09-09 |
| AVIV | Senior Backend Engineer, Platform Team, Berlin (Wlad) | 2026-05-11 |
| Enam | Fullstack Engineer | 2026-08-01 |

Worth asking Martin or Mikhail who added it and what the intended workflow is, because it looks like this is already half solved.

---

## What is left manual

Only the front of the funnel, which Bubble does not see:

- **Matched:** Martin and Sashka proposing a candidate to a recruiter.
- **Request date:** still undefined. See open question below.

That is a much smaller ask than tracking the whole funnel by hand. Sashka logs what she and Martin do. Everything after the recruiter takes over comes from Bubble.

---

## Open questions

**1. What is a "request"?** Her Request to Intro metric needs a start date. Candidates: the client opening the role, or Martin and Sashka handing the candidate to the recruiter. The second is more useful, since it measures recruiter turnaround on their proposals.

**2. Which stage in Bubble equals the intro?** Probably Moved to ATS, but needs confirming with a recruiter. Whichever stage the recruiter sets when they send the candidate to the hiring manager.

**3. Scope.** Sashka's closing line: "this will be a dashboard we'll use for getting new clients as well." That is a different motion. No existing role, no recruiter, no Bubble job. Introducing a candidate cold to a prospect company to open a door is not the flow above and would not be captured by the tag. Decide whether v1 covers existing clients only.

---

## Main risk

The whole automatic path depends on recruiters tagging the source field consistently. Same discipline problem as the intake eligibility tracker. Worth deciding up front how that gets enforced, or at least monitored, before promising the numbers are complete.

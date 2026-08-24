# Silver Medalist — Proposed Data Model

**Date:** 24 Aug 2026
**Problem:** the outreach pipeline holds hiring managers. Interview, offer and hire belong to the candidate. One pipeline cannot carry both without the stages meaning the wrong thing.

---

## The proposal: two stages, one handoff

Do not try to make one pipeline hold both. Split it where the business actually splits.

### Stage 1 — Outreach pipeline (what exists today)

The `Engineering and Talent profiles` pipelines, unchanged. The person in the pipeline is the hiring manager. Stages are outreach stages.

| Event | Meaning | Metric it gives Sashka |
|---|---|---|
| Candidate created | HM added to the pipeline | Pool size |
| `Linkedin Sent Contact` | Connection request | The **request** |
| `Linkedin Connected` | They accepted | |
| `Message sent` | "We have a candidate for you" | The **intro** |
| `Linkedin Responded` | They replied | Intro to reply rate |

Gives: introductions per week, total introductions, request to intro time, reply rate. All automatic, no manual entry, no new objects.

### The handoff

When a hiring manager says yes, send me that person.

### Stage 2 — A real job for that company

Create a job under the prospect company and put the silver medalist in it as the candidate. This is the normal Bubble model, so the stages mean what they say. Interview 1, Interview 2, Onsite, Offer, Hired all belong to the candidate, which is correct.

Tag the candidate's source as **Silver Medalist**, the option added to Bubble on 19 August. That is what makes these filterable and separates them from ordinary sourcing.

Gives: interview, offer and hire counts, all the conversion rates below the intro, intro to hire time. It also feeds the existing recruiting dashboard for free, since the machinery already reads these stages.

---

## Why not force it into one pipeline

Moving a hiring manager through Interview 1, Offer and Hired means those stages describe the wrong person. It would read fine on a dashboard and be wrong in the database, and every later question ("who did we actually place at Parloa") would be unanswerable.

The alternative, one pipeline per silver medalist with hiring managers as the rows, works mathematically but inverts the meaning of every stage and needs a new job per candidate. Not worth it.

---

## Do not over-engineer the bottom half

Current volume: **394 intros and 2 replies in the last month.** At that rate the number of candidates reaching interview this year is small.

The top of the funnel needs automation because it is high volume. The bottom does not. A handful of real opportunities a month can be tracked with a couple of fields and a note. Build the linked model later if the volume justifies it.

---

## Naming convention (answers Sashka's own request)

She asked for a tag or format so she can pick out the right jobs. Prefix every job in this program with `SM -`:

```
SM - Engineering and Talent profiles (Sashka)
SM - Engineering and Talent profiles (Martin)
SM - Parloa (Martin)
```

Cheap, solves the "which jobs are in scope" problem permanently, and makes the dashboard filter one line rather than a hardcoded list that goes stale.

---

## Open items for Martin

1. **His outreach dashboard.** Sashka mentioned "an outreach dashboard which Martin created." Nobody has looked at it. It may already hold the messages that are missing from Bubble, in which case it is the source for the intro metric and this gets much easier.

2. **Message logging.** 91 connections in the last month, 0 messages logged in Bubble. Sashka says Martin has sent some. If those can run through Bubble the intro metric is automatic. If not, we need his dashboard or another source.

3. **The Silver Medalist source tag** he or someone added on 19 August. What was the intended workflow?

4. **Naming convention.** Confirm `SM -` or pick another prefix.

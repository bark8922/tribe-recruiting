# Silver Medalist — Decisions from the 25 Aug call

Martin, Sashka, Blake. 20 minutes.

---

## Scope narrowed

Sashka, 05:41: *"Right now we have matching candidates we already have with currently open roles we have with our clients. So that will be like only one dashboard."*

The dashboard covers the **existing-client matching motion only**. The BD outreach to potential clients' hiring managers is a separate thing and not what this measures. The `BD - Tribe` pipelines, the 394 connection requests, the missing messages — all out of scope for v1.

---

## No new jobs. The source tag is the key.

Martin, 09:44: *"You're not going to have separate jobs for Silver Medalists. It's going to be the same jobs. The only difference is that the candidates will have a source of Silver Medalist. We're not gonna open another role for Taxfix full stack just for the Silver Medalists. It doesn't make sense."*

So the filter is `Sourcedsource = Silver Medalist` across all existing client jobs. No `SM -` job naming needed. That idea is dead and it is the right call.

---

## The stage model

One new stage, named **Silver Medalists**, with stage **type = Prospects** (point_of_process 0).

| Stage | Type | Means |
|---|---|---|
| Silver Medalists | Prospects | **Matched.** Sashka found a fit and asked the recruiter |
| Contacted | Contacted | **Intro happened.** The recruiter made the introduction |
| Recruiter Screen | Recruiter Screen | They actually talked |
| Interview, Offer, Hired | existing | unchanged |

Martin, 08:23: *"Matched means we found a match. Contacted means introduction happened and Recruiter Screen means they talked."*

No extra "introduced" stage. Matched to Contacted is the intro conversion.

**Three requirements for the stage:** hidden, cannot be deleted, prospect stage type.

---

## SLA agreed

Martin, 17:58: *"The SLA or the goal for this is going to be 24 hours. If you don't get it to 24 hours then we might as well practically [drop] that."*

Matched to Contacted has a 24-hour target. That is Sashka's "request to intro time" with a real threshold to chart against.

---

## Every metric she asked for now works

| Metric | How |
|---|---|
| Introductions per week | Count of Silver-Medalist-sourced candidates moved to Contacted, by week |
| Request to intro, avg/median days | Silver Medalists stage date to Contacted date. 24h SLA line |
| Total introductions | Same, weekly / monthly / all-time |
| Total hires | Hired stage, filtered by source |
| Matched to Intro % | Entered Silver Medalists stage vs reached Contacted |
| Intro to Interview / Offer / Hired % | Existing stage events, source filter |
| Intro to hire, avg/median days | Contacted date to Hired date |

All six. Nothing manual, once the stage exists and the tag gets applied.

---

## The problem nobody raised in the call

**15 of the 21 active 2026 clients have no Prospects stage at all.**

| Has Prospects | Missing Prospects |
|---|---|
| Taxfix, Wolt, Glovo, Enam, Tribe.xyz, BD - Tribe | AVIV, Aiven, Doordash, DualEntry, Eucalyptus, Nexi, No Isolation, Parloa, Pliant, Reaktor, Scorewarrior, SevenRooms, Voize, Tribe - Marketing, Tribe.xyz (IR) |

Sashka's example in the call was Taxfix to Enam, and both happen to have it. Parloa and AVIV, two of the most active, do not.

So this is not one stage for Mikhail to create. It is a stage that has to exist on **every client Sashka might introduce into**, or the matched step cannot be recorded there and those intros go missing.

Worth raising with Mikhail as part of the same request.

---

## Blake's actions

1. **Ask Mikhail** to create the Silver Medalists stage: prospect type, hidden, undeletable. Raise the 15 missing clients above.
2. **Backend role notification.** Slack DM to Sashka when a new backend role opens. Martin's warning at 14:44: do not just match "backend engineer", it needs full stack, Python engineer, and similar. Pull the last 20 to 30 jobs in the relevant categories, share with Sashka, confirm the filter before building.
3. **Start the dashboard.** Prototype promised by end of week.

## Later, not now

Martin, 18:27: *"I think we will automate it down the line and we will just take control of their LinkedIn and just make the introductions ourselves."*

# Silver Medalist — What the Data Actually Shows

**Date:** 24 Aug 2026
**Definition in use:** an introduction is a message we send to someone saying we have a candidate for them. The candidate does not need to be in the data. Martin handles the matching separately.

---

## 1. Their pipelines are under a fake client called `BD - Tribe`

Every job Martin or Sashka created in 2026 sits there. The people in them are the buyers: Engineering Managers, Directors of Engineering, CTOs, VPs of Engineering, Heads of Talent Acquisition, Chief People Officers. That is the right unit for counting intros.

| Job | Created | People |
|---|---|---|
| Engineering and Talent profiles (Sashka) | 21 Jul | 220 |
| Engineering and Talent profiles (Martin) | 21 Jul | 217 |
| Engineering Manager: No Message | 8 Jul | 113 |
| Engineering Manager: No message (Sashka) | 8 Jul | 101 |
| Engineering Manager: Message (Sashka) | 8 Jul | 100 |
| Engineering Manager: W/ Message (Martin) | 8 Jul | 98 |
| Engineering profiles Parloa (Sashka) / (Martin) | 21 Jul | 27 / 27 |
| TA Leadership: Growing Network | 12 Mar | 786 |
| TL sales | 17 Feb | 97 |
| Head of TA (Claude) | 11 Feb | 81 |

The July "Message" versus "No message" pairs look like an A/B test on whether to attach a note to the connection request.

---

## 2. Bubble has three events that could be the intro

| Event | What it is |
|---|---|
| `Linkedin Sent Contact` | Connection request sent |
| `Message sent` | LinkedIn DM, after connecting |
| `Linkedin inMail sent` | InMail |

Which one counts as an intro is the decision to make. The counts are very different.

---

## 3. The current push logs connection requests and zero messages

Engineering and Talent profiles, 22 Jul to 21 Aug:

| | Sashka | Martin |
|---|---|---|
| People added | 220 | 217 |
| Connection requests sent | 212 | 182 |
| Connected | 45 | 46 |
| Connections withdrawn | 194 | 175 |
| **Messages sent** | **0** | **0** |
| Replies | 1 | 1 |

They connected with 91 people in the last month and logged no messages to any of them.

This is not a Bubble limitation. Other pipelines log messages fine: TA Leadership: Growing Network has 128 messages against 146 connections, TL sales has 24. So when messaging runs through Bubble it gets recorded.

**The likely explanation is that the actual pitch is being sent natively on LinkedIn or by email, outside Bubble.** If so, the intros are happening and nothing is capturing them.

One more thing: the `Content` field is empty on every event in these pipelines, so even where a message is logged the text is not stored. There is no way to verify from data whether a given message offered a candidate.

---

## 4. What this makes measurable today

| Metric | Status |
|---|---|
| Introductions per week | Works immediately **if** the intro is the connection request. 394 in the last month |
| Introductions per week | Returns zero **if** the intro is the follow-up message |
| Total introductions | Same dependency |
| Intro to reply rate | Available: `Linkedin Responded`, currently 2 replies from 394 |
| Everything below the intro | Not in these pipelines. Needs the client-side tracking discussed separately |

Connection acceptance is running at roughly 23%, 91 of 394. If the intro is the follow-up message rather than the request, intro volume is capped by that.

---

## 5. The one question that settles the build

**Where does the "we have a candidate for you" message actually get sent, and does it go through Bubble?**

- Through Bubble's messaging: the metric is `Message sent`, it works from today, no new tooling.
- Natively on LinkedIn or by email: nothing is captured today and that gap needs solving before any number is real.
- Attached as a note on the connection request: the metric is `Linkedin Sent Contact` and it already works, 394 last month.

Everything else in the brief follows from that answer.

---

## 6. Separately: the Silver Medalist tag, built 19 August

A `Silver Medalist` option was added to Bubble's source dropdown on 19 Aug 2026. Three candidates carry it, all modified that same day, so retro-tagged as a test. One is a "Bubble test" client row. The two real ones were added by Rodrigo Gomes (AVIV) and Aleksandra Vistac (Enam, since disqualified).

This is for the other motion, intros into existing client roles. If a recruiter tags a silver medalist when adding them to a real client job, the interview, offer and hire stages come free from the existing recruiting dashboard pipeline. Worth asking who built it and what they had in mind.

---

## 7. The pool, for context

There is no silver medalist list anywhere, but it is derivable. Since Jan 2025, **1,896 people reached Interview 2, Interview 3, Onsite or Offer and were never hired anywhere.** 668 of those in 2026.

| Client | Onsite | Offer |
|---|---|---|
| Wolt | 646 | 155 |
| Glovo | 194 | 71 |
| Doordash | 127 | 6 |
| AVIV | 123 | 14 |
| Taxfix | 100 | |
| Parloa | 80 | 19 |
| Tribe.xyz | 65 | 26 |
| Aiven | 39 | 6 |

Martin's OKR was "100+ active profiles". The addressable pool is closer to 1,900.

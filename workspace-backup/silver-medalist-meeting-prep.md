# Silver Medalist — Prep for today's call

**Meeting:** 25 Aug, 16:45 CEST. Martin, Sashka, Blake. Blake joining the first few minutes.
**Sources:** Blake & Martin 1:1, 24 Aug 15:30 (Gemini transcript in Drive). Friday Team Meeting, 21 Aug, 46:00 to 56:00.

---

## What Martin said in the 1:1 yesterday

**On the data constraint, and he said it first.**

> "The fact is that you don't have other data. That's what she kind of needs to learn on how it all works. You cannot come up with data out of thin air."

**On the Silver Medalist source tag. He knows about it and is counting on it.**

> "They have updated a source. Let's see if it actually is there. Source of a candidate. So we know when they came through silver medalists, which is good, and so you'll be able to track the progression."

**On what is missing.**

> Martin: "But there's the very initial step doesn't exist in Tribe at all."
> Blake: "And this is the connection request."
> Martin: "Yeah. Exactly."

**On how he plans to run the call.**

> "We need to, let's talk about it tomorrow. I'll help her through it because I know she needs to learn how to do this properly." … "She will learn. She will make mistakes."

So Martin is going to walk Sashka through the constraints himself. He is on your side of this. Your few minutes are for stating what exists, what does not, and what decision you need.

---

## One correction worth making

Martin believes the connection request step does not exist in Tribe. It does.

In the two live pipelines, 21 Jul to 21 Aug: **437 people added, 394 connection requests, 91 connections, 2 replies.** All timestamped in Bubble.

What is actually missing is narrower and more specific:

1. **The message offering a candidate.** 91 connections, zero messages logged. Sashka says Martin has sent some. They are going out somewhere Bubble cannot see.
2. **Which candidate was offered to whom.** The pipeline holds the hiring manager. Nothing records the candidate on the other side of the intro.
3. **The link from an intro to the job that later opens.** Needed for every timing metric.

Getting this right matters, because "the initial step doesn't exist" leads to building something new, and "the initial step exists but the message and the candidate link don't" leads to a much smaller fix.

---

## What the tag buys, and the caveat

Martin is right that the source tag gives you progression. Once a silver medalist sits in a real client job tagged Silver Medalist, interview, offer, hire and every timing between them come free from the existing recruiting dashboard.

Caveat: three candidates carry the tag today, all retro-tagged on 19 Aug, one on a "Bubble test" client. The mechanism exists and has never been used in anger. Adoption is the risk, not the build.

---

## What to say in your few minutes

**Works today, no new tooling:** introductions per week, total introductions, request to intro time, reply rate. All from the connection request and message events, provided the message runs through Bubble.

**Works once a job is opened and tagged:** interview, offer, hire counts, conversion rates across intros, intro to hire time.

**Does not exist and needs a decision:** where the "we have a candidate for you" message is sent, and how an intro connects to the job that follows.

**Naming:** `SM -` prefix on outreach pipelines and jobs. Sashka asked for exactly this on Sunday.

---

## Questions to land

1. Martin, where are you sending the messages to hiring managers? If they can run through Bubble the intro metric is automatic from today.
2. When a hiring manager says yes, do we open a job under their company and put the candidate in it? That is the only way interview, offer and hire stay semantically correct.
3. How do we link that job back to the intro? At two conversions a month this is a note, not a system, but someone has to own it.
4. Confirm `SM -` naming.
5. Commercial model for intros into existing clients: per-hire fee or goodwill? Sashka bounced this to you last week.

---

## Two things from Friday worth raising if there is time

**Martin's idea at 48:51:** put roles Tribe is not working on at existing clients into the matching pool, and ask the recruiter to broker an intro to the colleague who owns it. That is a third motion with its own data path. Ask whether it is v1 or later.

**Martin's idea at 55:05:** a trigger when a new role opens in Tribe so Sashka can offer leads before sourcing starts. He suggested Mikhail build it. **You already have this** — the new-role briefing bot in `tribe-job-intel`, dry run to you 22 Jul. Say so before a second one gets built.

---

## The risk Martin already named

From Friday, 49:33:

> "The recruiters will not feel comfortable about pitching something. They don't always know how to do that. Unfortunately. That's the problem."

Every intro at an existing client goes through a recruiter who has to agree to make it. Matched to Intro % is really a measure of recruiter buy-in, and Martin expects that to be the weak link. Worth naming, because if that gate stays shut the dashboard will show a lot of outreach and no outcomes.

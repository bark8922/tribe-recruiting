# Team Lead Audit — 2026-05-28

Read-only audit. No pipeline or dashboard changes.

## Inputs

- BambooHR employee directory: 68 active employees (pulled live)
- `dashboard_data.json`: 46 unique TA names (`wbr_actuals` keys) + 16 unique TS names (`ts_actuals` keys) = 56 distinct people in the WBR funnel

## Match rate

| Bucket | Count | % |
|---|---|---|
| Exact match (after diacritic fold) | 37 | 66% |
| Fuzzy match (first/last name) | 7 | 13% |
| **Unmatched** | **12** | **21%** |

79% match overall. The 21% unmatched rate is the biggest flag — well below the >95% bar we'd want before this is production-ready. Details below.

## 12 unmatched names

Not present in current Bamboo directory at all. Most likely external recruiters or terminated employees who still have recent funnel activity.

```
Adis Prepoljac          (TA+TS — both sections)
Akash Singh             (TA)
Andreas Weins           (TA)
Danish Shams            (TA)
Evagelina Rapanaki      (TA)
Ketevan Khorava         (TA)
Mark Kandaurov          (TA)
Mia Gjorgievska         (TS)
Nidhi Raina             (TA)
Rafael Rosa             (TA)
Tinatini Karaulashvili  (TA)
Zarina Amanbekova       (TA)
```

You said "I don't think there are any [external recruiters] in the WBR section." There are 12 candidates here. Worth confirming:
- Which are externals (intentional — they should never appear under a team lead, only in "All teams")
- Which are terminated Tribe employees (need the last-known supervisor cached so historical data still attributes)
- Which (if any) are current Tribe employees the matcher missed

Mia Gjorgievska in particular jumps out — she has TS data but no Bamboo record. If she's still here she needs to be added; if she left we need her last-known supervisor.

## 7 fuzzy matches — eyeball before trusting

| Dashboard name | Matched to Bamboo | Why fuzzy | Confidence |
|---|---|---|---|
| Lejla Silva | Lejla Dizdarevic | only one "Lejla" | **Verify — name change or different person?** |
| Tina Aramouni | Tina Abdul-Karim | only one "Tina" | Email = tinaaramouni@tribe.xyz so same person, married name |
| Maria Desiree Gerbore | Maria Gerbore | middle name absent in Bamboo | Same person |
| Valeriia Yurykova | Valeriia Yurikova | "y" vs "i" spelling | Same person |
| Ejla Suljcic | Ejla Suljčić | diacritics | Same person |
| Jelena Lacmanovic | Jelena Lacmanović | diacritics + extra space | Same person |
| Marina Lazarevic | Marina Lazarević | diacritics | Same person |
| Mateja Jokovic | Mateja Joković | diacritics | Same person |
| Zelimir Stajcic | Želimir Stajčić | diacritics | Same person |
| Simon Siew | Simon Siew | extra space in dashboard name | Same person |

(That's 10 actually, not 7 — diacritic-only ones got bucketed as exact_after_fold; only the first 4 are true fuzzy and need confirmation.)

## Proposed team-lead list (matched names only)

Using the rule "person has at least one direct report in the WBR funnel data":

| Lead | # reports | Reports |
|---|---|---|
| Chené Elliot | 8 | Adelya Khakimova, Alexandra Richiteanu, Elena Petrovska, Jelena Lacmanovic, Jovana Drakula, Nenad Skoko, Tina Aramouni, Zelimir Stajcic |
| Lejla Dizdarevic | 5 | Aleksandra Markovic, Aleksandra Vistac, Dušan Špica, Maria Gerbore, Marina Nikolic |
| Vladimir Stankovic | 5 | Ejla Suljcic, Jan Dokulil, Lisa Gargulinska, Milica Veselinovic, Niki Vokalkova |
| Meho Saracevic | 4 | Ekaterina Boyprav, Filip Nogowski, Jonaed Iqbal, Mateja Jokovic |
| Niki Vokalkova | 3 | Alisa Liddell, Dora Vrbanić, Fuad Safarov |
| Kristina Colovic | 3 | Anna Tyulpanova, Samantha Nel, Wladyslaw Gadomski |
| Kristjana Thorarinsdottir | 3 | Chené Elliot, Simon Siew, Vladimir Stankovic |
| Salem Mansuri | 3 | Kristina Colovic, Lejla Silva, Meho Saracevic |
| Gustavo Loureiro Castro | 3 | Nare Avetisyan, Rodrigo Gomes, Valeriia Yurykova |
| Jacopo Lupo Ferrari | 2 | Andrea Akovic, Gustavo Loureiro Castro |
| Simon Siew | 2 | Chantal Bozkurt, Dolores Palotas |
| Andrea Akovic | 2 | Marina Lazarevic, Naledi Ngwenya |
| Sanja Pavlovikj | 1 | Iryna Dyda |

13 candidate leads total.

## Two things to decide before building

**1. The "exclude directors" rule isn't auto-clean.** You said Martin and Jacopo shouldn't be in the dropdown because their reports are directors/leads. But:

- **Jacopo** has 2 reports who *are* in the funnel as TS contributors (Andrea Akovic, Gustavo Loureiro Castro). Strict "has a report in funnel data" rule keeps him in.
- **Salem Mansuri** has 3 reports who appear in funnel data — but they're all Team Leads themselves (Kristina, Lejla, Meho).
- **Kristjana Thorarinsdottir** has Simon Siew (Staff TA, an IC contributor) plus two managers (Chené, Vladimir).

Cleanest rule I can think of: a person qualifies as a team lead only if their reports' Bamboo `department` is `Talent Acquisition`, `Sourcing`, or `Recruitment Ops` — **not** `Leadership` or `Senior Leadership`. Under that rule:
- Jacopo → excluded ✓ (Andrea/Gustavo are Leadership)
- Salem → excluded ✓ (Kristina/Lejla/Meho are Leadership)
- Kristjana → excluded (Chené/Vladimir are Leadership; Simon is in TA but only one IC report)
- Martin → would never have qualified anyway

That collapses the list from 13 → 10 leads. Worth confirming you want this rule.

**2. What to do with the 12 unmatched names.** Drop them entirely (they only appear under "All teams")? Maintain a hand-curated `name_overrides.json`? Need a decision before this is production-quality.

## Files

- `team_lead_audit.csv` — every dashboard name with match type + Bamboo info + supervisor
- `team_lead_list.csv` — flat lead → reports table

Both live in `_team_lead_audit/` next to this file.

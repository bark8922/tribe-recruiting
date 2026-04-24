# Client name mappings

Canonical list of client-name merges, splits, renames, and exclusions. This matters because client names drift between systems (Bubble, Snowflake, Google Sheets) and any rebuild that doesn't handle the mappings correctly gets wrong numbers.

## How to fill this in

We've pre-listed what we've figured out. **Correct anything wrong, fill in the `?` rows, and add anything we missed.** Any level of detail is welcome — one-liners are fine.

---

## Known merges (multiple names → one canonical)

| Canonical name | Also appears as | Since when | Why |
|----------------|-----------------|------------|-----|
| Wolt HQ | Wolt, Wolt Helsinki | ? | ? |
| Wolt NBB | Wolt New Business, Wolt NBB | ? | Separate BU, separate targets |
| DoorDash | DoorDash, DoorDash Inc | ? | ? |
| 7Rooms / SevenRooms | 7Rooms, SevenRooms | ? | ? |

**Add more rows as needed.** If there are WBR vs PBI client-name renames, note them here.

---

## Known splits (one legal entity → multiple canonical names)

> Example: "Wolt HQ and Wolt NBB are both Wolt the company but tracked as separate clients because they have separate TA teams and targets."

| Parent | Splits into | Why |
|--------|-------------|-----|
| Wolt | Wolt HQ, Wolt NBB | Separate TA teams + targets |
| ? | ? | ? |

---

## Clients that are excluded from key metrics

> Test clients, sandboxes, internal dogfooding, anything that shouldn't appear in WBR/MBR.

- `?`
- `?`

---

## Clients with special handling you should know about

> Anything non-obvious. E.g., "Eucalyptus ATS double-counts events so we dedupe by (candidate_id, event_type, DATE(date_created)).", "Company X pays a separate fee structure so revenue calc differs."

_Your answer:_

---

## Renamed clients (historical)

> Clients that used to go by a different name. If the old name still appears anywhere in the data, worth flagging.

| Current name | Previous name(s) | Renamed around |
|--------------|------------------|----------------|
| ? | ? | ? |

---

## The "obvious test clients" list

> What are the client names that look real but are actually test data we should filter out? (e.g., "Test Client", "Tribe Internal", etc.)

_Your answer:_

# WBR Detailed Diff Report (ISO weeks 1-15, 2026)

Live source: `dashboard_data.json.wbr_actuals`  •  Snow source: fresh Keboola Snowflake query

Total live non-Wolt keys: **27** • Snow non-Wolt keys: **51** • Common: **27**

Keys in snow but not in live WBR roster (expected — WBR is a fixed roster): **24**


## Non-Wolt per-key summary

| Client | TA | Exact cells | Small drift (±1-3) | Big delta | Status |
|---|---|---|---|---|---|
| Aiven | Fuad Safarov | 3 | 1 | 0 | DRIFT |
| Aiven | Vladimir Stankovic | 4 | 0 | 0 | MATCH |
| Aviv | Alexandra Richiteanu | 38 | 0 | 0 | MATCH |
| Aviv | Anna Tyulpanova | 42 | 0 | 0 | MATCH |
| Aviv | Jovana Drakula | 33 | 1 | 0 | DRIFT |
| Aviv | Kristina Colovic | 45 | 0 | 0 | MATCH |
| Aviv | Lejla Silva | 41 | 4 | 0 | DRIFT |
| Aviv | Wladyslaw Gadomski | 43 | 0 | 0 | MATCH |
| DoorDash | Akash Singh | 58 | 0 | 0 | MATCH |
| DoorDash | Danish Shams | 39 | 0 | 0 | MATCH |
| DoorDash | Nidhi Raina | 32 | 0 | 0 | MATCH |
| Enam | Aleksandra Vistac | 57 | 1 | 2 | MISMATCH |
| Eucalyptus | Alisa Liddell | 3 | 1 | 0 | DRIFT |
| Eucalyptus | Dušan Špica | 5 | 0 | 0 | MATCH |
| Eucalyptus | Meho Saracevic | 66 | 2 | 0 | DRIFT |
| Fever | Andrea Akovic | 16 | 0 | 0 | MATCH |
| Glovo | Chené Elliot | 23 | 0 | 0 | MATCH |
| Glovo | Samantha Nel | 25 | 0 | 0 | MATCH |
| Grover | Eduardo Moral | 45 | 0 | 4 | MISMATCH |
| Grover | Rodrigo Gomes | 8 | 0 | 0 | MATCH |
| Nexi | Maria Desiree Gerbore | 65 | 0 | 0 | MATCH |
| Parloa | Filip Nogowski | 66 | 0 | 0 | MATCH |
| Parloa | Jonaed Iqbal | 61 | 2 | 0 | DRIFT |
| PhantomBuster | Mateja Jokovic | 30 | 0 | 0 | MATCH |
| Scorewarrior | Ekaterina Boyprav | 53 | 0 | 0 | MATCH |
| SevenRooms | Zelimir Stajcic | 57 | 0 | 0 | MATCH |
| Taxfix | Marina Nikolic | 65 | 0 | 0 | MATCH |

### Non-Wolt mismatch detail (only keys with drift or big delta)


**Aiven | Fuad Safarov**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w15 | contacted | 56 | 55 | -1 | small |

**Aviv | Jovana Drakula**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w15 | actual_screens | 11 | 10 | -1 | small |

**Aviv | Lejla Silva**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w12 | contacted | 42 | 41 | -1 | small |
| w13 | screened | 19 | 18 | -1 | small |
| w14 | contacted | 303 | 301 | -2 | small |
| w14 | screened | 32 | 31 | -1 | small |

**Enam | Aleksandra Vistac**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w1 | contacted | 0 | 18 | +18 | big |
| w1 | screened | 0 | 4 | +4 | big |
| w1 | actual_screens | 0 | 3 | +3 | small |

**Eucalyptus | Alisa Liddell**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w15 | actual_screens | 4 | 3 | -1 | small |

**Eucalyptus | Meho Saracevic**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w13 | contacted | 210 | 209 | -1 | small |
| w14 | contacted | 40 | 39 | -1 | small |

**Grover | Eduardo Moral**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w1 | contacted | 19 | 74 | +55 | big |
| w1 | screened | 6 | 15 | +9 | big |
| w1 | actual_screens | 2 | 9 | +7 | big |
| w1 | ats | 2 | 8 | +6 | big |

**Parloa | Jonaed Iqbal**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w1 | contacted | 4 | 5 | +1 | small |
| w1 | screened | 8 | 11 | +3 | small |

## Wolt roll-up (sum of 6 sub-BUs in live vs Snowflake 'Wolt' per TA)

Live Wolt TAs: **10** • Snow Wolt TAs: **33** • Common: **10**

| TA | Exact cells | Small drift | Big delta | Status |
|---|---|---|---|---|
| Adelya Khakimova | 46 | 0 | 0 | MATCH |
| Ejla Suljcic | 53 | 0 | 0 | MATCH |
| Elena Petrovska | 51 | 0 | 0 | MATCH |
| Jan Dokulil | 62 | 0 | 0 | MATCH |
| Jelena Lacmanovic | 67 | 0 | 1 | MISMATCH |
| Lisa Gargulinska | 60 | 1 | 0 | DRIFT |
| Milica Veselinovic | 45 | 0 | 0 | MATCH |
| Nenad Skoko | 67 | 1 | 1 | MISMATCH |
| Niki Vokalkova | 65 | 0 | 0 | MATCH |
| Tina Aramouni | 68 | 0 | 0 | MATCH |

### Wolt mismatch detail


**Wolt (all sub-BUs) | Jelena Lacmanovic**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w1 | contacted | 0 | 4 | +4 | big |

**Wolt (all sub-BUs) | Lisa Gargulinska**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w1 | screened | 2 | 5 | +3 | small |

**Wolt (all sub-BUs) | Nenad Skoko**
| Week | Metric | Live | Snow | Δ | Class |
|---|---|---|---|---|---|
| w1 | contacted | 0 | 270 | +270 | big |
| w1 | screened | 0 | 1 | +1 | small |

## TAs in Snowflake with activity but NOT in live WBR roster
(These are not 'mismatches' — the live WBR is a fixed roster from Andy's WBR Target sheet. Listed for completeness.)

- ABOUT YOU | Gustavo Loureiro Castro
- BD - Tribe | Martin Bernard
- Bubble test | Andreea Vrancianu
- Circula | Maurice Stuart
- DoorDash | Milica Veselinovic
- DualEntry | Chantal Bozkurt
- DualEntry | Chené Elliot
- DualEntry | Samantha Nel
- DualEntry | Simon Siew
- FTAPI | Naledi Ngwenya
- Fever | Lejla Silva
- Glovo | Maria Desiree Gerbore
- Nexi | Andreas Weins
- Parloa | Gustavo Loureiro Castro
- Scorewarrior | Mark Kandaurov
- SevenRooms | Adelya Khakimova
- SevenRooms | Milica Veselinovic
- Statista | Gustavo Loureiro Castro
- Taxfix | Alexandra Felea
- Taxfix | Wladyslaw Gadomski
- Tribe - Marketing | Caroline Murphy
- Tribe - Marketing | Salem Mansuri
- Tribe - Marketing | Simon Siew
- Tribe.xyz (IR) | Ella Darie
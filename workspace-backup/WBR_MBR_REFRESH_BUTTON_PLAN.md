# WBR/MBR "Refresh comments" button

Status: BUILT, on a preview branch. Not merged to main. Inert until configured.
Date: 2026-08-17

## What it does

Adds a "Refresh comments" button to the WBR and MBR tabs. It re-reads the two
note tabs from Andy's WBR target sheet and swaps them into the already-loaded
dashboard bundle. About 10 seconds, no Keboola render, no commit, no deploy, no
page reload.

Replaces the 5-to-6 minute manual runbook for the "recruiters filled in their
comments late and the meeting has already started" case.

## SETUP RUNBOOK

Until both parts are done the endpoint returns 503 and the button shows an
error. Nothing else on the dashboard is affected, so merging early is safe.

### The secret (used in both parts, must match exactly)

```
OvR3og1qDEvyem_zfKac9TdP33xm6BohOG5R20pVG4rNUV_vImXXtBpFiMNY8yTP
```

Header name, also used in both parts:

```
x-wbr-secret
```

---

### PART 1 — n8n (about 2 minutes)

Go to: https://blakebarkley.app.n8n.cloud/workflow/hezapl9m66uD5E7j

1. Double-click the node called **Notes Webhook** (leftmost node).
2. You will see **Credential for Header Auth** with `Header Auth account`
   already selected. Open that dropdown and choose **+ Create new credential**.
   - Do not keep `Header Auth account`. Its header name is `Cookie`, which is a
     reserved browser header and the wrong thing to authenticate with.
   - Note: n8n does not show a "Used by" list on credentials, so there is no way
     to check what else uses it from the UI. Creating a new one sidesteps the
     question entirely.
3. In the credential dialog, fill in exactly two fields:
   - **Name**: `x-wbr-secret`
   - **Value**: `OvR3og1qDEvyem_zfKac9TdP33xm6BohOG5R20pVG4rNUV_vImXXtBpFiMNY8yTP`
4. Top-left of the dialog, rename the credential from "Header Auth account 2" to
   `WBR Notes Secret`. Click **Save**, then close the dialog.
5. Still in the Notes Webhook node, find the **Production URL** field near the
   top and confirm it reads:
   `https://blakebarkley.app.n8n.cloud/webhook/wbr-notes`
   Toggle to Production URL if it is showing the Test URL. Test URLs only work
   while the editor is open, so the button would break as soon as you close it.
6. Close the node. Top-right, flip the **Inactive** toggle to **Active**.
   Confirm the save prompt. The workflow is currently inactive.

---

### PART 2 — Cloudflare (about 3 minutes)

Go to: https://dash.cloudflare.com

1. Left sidebar, click **Workers & Pages** (in the newer UI this is **Compute
   (Workers)**).
2. In the project list click **tribe-recruiting**.
   - It is a **Pages** project, listed with a "Pages" type label.
   - Do NOT pick `tribe-dashboard` or `tribe-dashboard-leaders`. Those are the
     separate finance and leadership dashboards and have nothing to do with this.
3. Click the **Settings** tab.
4. Find **Variables and secrets** (labelled **Environment variables** in the
   older UI).
5. There are two environments, **Production** and **Preview**. Start with
   **Preview**, because that is where the branch deploys.
6. Click **Add variable** twice, creating these two. Set the type to **Secret**
   (or tick **Encrypt**) on the second one:

   | Type | Variable name | Value |
   |---|---|---|
   | Text | `N8N_NOTES_URL` | `https://blakebarkley.app.n8n.cloud/webhook/wbr-notes` |
   | Secret | `N8N_NOTES_SECRET` | `OvR3og1qDEvyem_zfKac9TdP33xm6BohOG5R20pVG4rNUV_vImXXtBpFiMNY8yTP` |

7. Click **Save**.
8. **Redeploy, or the variables will not apply.** Pages binds environment
   variables at build time, so an existing deployment will not pick them up.
   Go to the **Deployments** tab, find the newest
   `feat/wbr-notes-refresh-button` deployment, click the **...** menu on the
   right, and choose **Retry deployment**.
9. When you merge to main, repeat steps 5 to 8 for the **Production**
   environment.

You do not need `N8N_NOTES_HEADER`. The code defaults to `x-wbr-secret`, which
is what you set in Part 1.

---

### Testing it

1. Open the preview URL from the Deployments tab and sign in.
2. Go to the WBR tab. You should see a blue **Refresh comments** button at the
   top with the note "Comments from the last scheduled refresh".
3. In Andy's WBR target sheet, type something into a Comment cell for the
   current week and any TA already showing on the tab.
4. Back on the dashboard, click **Refresh comments**. Within about 10 seconds
   the label should change to "Comments as of HH:MM CET, metrics unchanged" and
   your text should appear. No page reload.
5. Confirm the numbers did not move. Only comments and row visibility change.

If it errors:

- "Refresh is not configured on this deployment" means the env vars are missing
  or you have not redeployed since adding them. Repeat Part 2 step 8.
- "n8n returned 403" means the secret or header name does not match between
  Part 1 and Part 2.
- "n8n returned 404" means the workflow is not Active, or you used the Test URL.

## Where the code is

Branch `feat/wbr-notes-refresh-button` on bark8922/tribe-recruiting, commit
`080aae8`. Two files:

- `recruiting-dashboard/functions/api/wbr-notes.ts` (new, 97 lines)
- `recruiting-dashboard/src/App.jsx` (+69 / -4)

PR: https://github.com/bark8922/tribe-recruiting/pull/new/feat/wbr-notes-refresh-button

## How it hangs together

```
Browser, WBR or MBR tab
  │  fetch('/api/wbr-notes')        same origin, session cookie sent automatically
  ▼
functions/api/wbr-notes.ts         already gated by _middleware.ts;
  │                                re-checks LEADERSHIP_EMAILS server-side
  │  POST + secret header
  ▼
n8n "WBR Notes JSON" hezapl9m66uD5E7j
  Read "TA Weekly Note" + "TS Weekly Note", Code node, Respond to Webhook
  ▼
{ta_weekly_notes, ts_weekly}  merged over the loaded bundle in React state
```

No new Cloudflare Worker and no CORS involved: the endpoint is a Pages Function
in the same project as the dashboard, so the browser calls a relative path on
its own domain.

Access is now stricter than the tabs themselves. WBR/MBR are currently hidden
client-side by the `tribe_role` cookie; this endpoint checks the signed session
against `LEADERSHIP_EMAILS` on the server.

## Why swapping those two arrays is safe

Both are pure projections of the sheet, verified in `render_json.py`:

| Bundle key | Built by | Source |
|---|---|---|
| `ta_weekly_notes` | `load_ta_weekly_notes()` L518 | `wbr_static/wbr_ta_weekly_note.csv` |
| `ts_weekly` | `build_ts_weekly_from_csv()` L572 | `wbr_static/wbr_ts_weekly.csv` |

Every metric lives in a separate key (`wbr_actuals`, `ts_actuals`,
`ts_conversion`, ...) and is never touched.

## Verification done

1. Ported the Python parsing to JS and diffed both against the same CSVs:
   **byte-identical on all 1545 rows** (1180 TA + 365 TS).
2. Rebuilt the arrays from commit `aa2a7bc`, the exact commit the last Keboola
   render consumed, and compared to the live `dashboard_data_snowflake.json.gz`:
   **identical, 1182 rows**. So the port reproduces production output exactly.
3. Ran the n8n workflow against the live sheet: 1180 TA + 365 TS, matching key
   sets and order, `contacted_target` typed number-or-null with zero strings.
4. `npm run build` passes.
5. `esbuild` parses the Pages Function; all four imports exist in `_lib/session.ts`.

An earlier 2-row discrepancy turned out to be real sheet drift, not a bug:
someone removed the W33 rows for Nenad Skoko and Elena Petrovska between 10:44
and 12:00 today.

## Behaviour to know

**Rows move, not just text.** Comments drive visibility: both tabs hide a TA with
zero activity unless they have a comment or reasoning that week (`hasNote`,
App.jsx L611 and L1447), and `ts_weekly` is the sourcer roster that also filters
`tsConversion` (L755). So a refresh can add and remove rows and the table will
visibly reshuffle. Intended, but do not let it surprise the room mid-meeting.

**Comments get fresher than the metrics beside them.** After a click, comments
reflect the sheet right now while every number still reflects the last scheduled
render. The button prints "Comments as of HH:MM CET, metrics unchanged" so this
is not misread as a full refresh.

**Guards.** 60 second cache plus in-flight dedupe, so a room clicking at once
causes one sheet read. Empty arrays from n8n are rejected rather than applied,
because an empty `ts_weekly` would blank the sourcer roster. On upstream error a
stale copy is served if one exists.

## Not done

- Not merged to main.
- Not tested against a live preview deployment, because that needs the env vars
  from step 2. Worth one click on the preview URL before merging: edit a cell in
  the sheet, press the button, confirm it appears without a reload and that the
  numbers do not move.

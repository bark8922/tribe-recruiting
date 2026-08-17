// On-demand WBR/MBR comment refresh.
//
// Recruiters fill in Andy's WBR target sheet after the scheduled Keboola render
// has already run, so comments are missing during the meeting. This endpoint
// re-reads just the two note tabs and returns them as JSON, so the dashboard can
// swap them into the loaded bundle without a render, a commit, or a deploy.
//
// It returns ONLY the two sheet-derived arrays. Every metric (wbr_actuals,
// ts_actuals, ts_conversion, ...) lives in separate keys and is untouched.
//
// Auth: _middleware.ts already blocks unauthenticated requests to every path.
// We re-check the session here and additionally require leadership, so the data
// is gated server-side rather than by the client hiding a tab.

import { LEADERSHIP_EMAILS, SESSION_COOKIE, parseCookies, verifySession } from "../_lib/session";

interface Env {
  SESSION_SECRET: string;
  N8N_NOTES_URL: string;
  N8N_NOTES_SECRET: string;
  N8N_NOTES_HEADER?: string;
}

interface NotesPayload {
  ta_weekly_notes: unknown[];
  ts_weekly: unknown[];
  generated_at?: string;
}

// Debounce so a room full of people clicking at once produces one sheet read.
const CACHE_TTL_MS = 60000;
let cached: { at: number; payload: NotesPayload } | null = null;
let inFlight: Promise<NotesPayload> | null = null;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

async function fetchNotes(env: Env): Promise<NotesPayload> {
  const headerName = env.N8N_NOTES_HEADER || "x-wbr-secret";
  const resp = await fetch(env.N8N_NOTES_URL, {
    method: "POST",
    headers: { [headerName]: env.N8N_NOTES_SECRET, "content-type": "application/json" },
    body: "{}",
  });
  if (!resp.ok) throw new Error("n8n returned " + resp.status);

  const data = (await resp.json()) as NotesPayload;
  if (!data || !Array.isArray(data.ta_weekly_notes) || !Array.isArray(data.ts_weekly)) {
    throw new Error("n8n returned an unexpected shape");
  }
  // A zero-length read means the sheet call failed silently upstream. Refusing it
  // matters: an empty ts_weekly would blank the sourcer roster, and an empty
  // ta_weekly_notes would hide every TA who has no activity this week.
  if (data.ta_weekly_notes.length === 0 || data.ts_weekly.length === 0) {
    throw new Error("n8n returned empty note arrays");
  }
  return data;
}

export const onRequestPost: PagesFunction<Env> = async (ctx) => {
  const { env, request } = ctx;

  if (!env.SESSION_SECRET) return json({ error: "SESSION_SECRET not configured" }, 500);
  if (!env.N8N_NOTES_URL || !env.N8N_NOTES_SECRET) {
    return json({ error: "Refresh is not configured on this deployment" }, 503);
  }

  const cookies = parseCookies(request.headers.get("cookie"));
  const session = await verifySession(cookies[SESSION_COOKIE], env.SESSION_SECRET);
  if (!session) return json({ error: "Not signed in" }, 401);
  if (!LEADERSHIP_EMAILS.has(session.email.toLowerCase())) {
    return json({ error: "Leadership access required" }, 403);
  }

  const now = Date.now();
  if (cached && now - cached.at < CACHE_TTL_MS) {
    return json({ ...cached.payload, cached: true });
  }

  try {
    if (!inFlight) {
      inFlight = fetchNotes(env).finally(() => { inFlight = null; });
    }
    const payload = await inFlight;
    cached = { at: Date.now(), payload };
    return json({ ...payload, cached: false });
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    // Serve a stale copy rather than nothing if we have one.
    if (cached) return json({ ...cached.payload, cached: true, stale: true, error: message });
    return json({ error: message }, 502);
  }
};

// POST /api/login — accepts form-encoded { email, password }, calls Bubble's
// chromelogin endpoint to verify, then issues `tribe_session` (HttpOnly,
// signed) and `tribe_role` (readable by client JS) cookies.

import {
  LEADERSHIP_EMAILS,
  SESSION_MAX_AGE_MS,
  roleCookie,
  sessionCookie,
  signSession,
} from "../_lib/session";

interface Env {
  SESSION_SECRET: string;
}

const BUBBLE_LOGIN_URL = "https://overview.tribe.xyz/api/1.1/wf/chromelogin";

export const onRequestPost: PagesFunction<Env> = async (ctx) => {
  const secret = ctx.env.SESSION_SECRET;
  if (!secret) {
    return new Response("SESSION_SECRET not configured", { status: 500 });
  }

  let email = "";
  let password = "";
  try {
    const form = await ctx.request.formData();
    email = String(form.get("email") ?? "").trim().toLowerCase();
    password = String(form.get("password") ?? "");
  } catch (_e) {
    return redirectTo("/?error=1");
  }

  if (!email || !password) {
    return redirectTo("/?error=1");
  }

  // Call Bubble. Any non-2xx, network error, or absent token => bad creds.
  let bubbleOk = false;
  try {
    const resp = await fetch(BUBBLE_LOGIN_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    if (resp.ok) {
      const text = await resp.text();
      try {
        const json = JSON.parse(text) as { response?: { token?: unknown }; token?: unknown };
        const token = (json && (json.response?.token ?? json.token)) as unknown;
        if (typeof token === "string" && token.length > 0) {
          bubbleOk = true;
        }
      } catch (_e) {
        bubbleOk = false;
      }
    }
  } catch (_e) {
    bubbleOk = false;
  }

  if (!bubbleOk) {
    return redirectTo("/?error=1");
  }

  const isLeadership = LEADERSHIP_EMAILS.has(email);
  const exp = Date.now() + SESSION_MAX_AGE_MS;
  const signed = await signSession({ email, isLeadership, exp }, secret);

  const headers = new Headers();
  headers.append("location", "/");
  headers.append("set-cookie", sessionCookie(signed));
  headers.append("set-cookie", roleCookie(isLeadership ? "leadership" : "member"));
  headers.set("cache-control", "no-store");
  return new Response(null, { status: 302, headers });
};

// Some browsers send a preflight or a GET hit to /api/login from form
// resubmission — just bounce them home.
export const onRequestGet: PagesFunction<Env> = async () => redirectTo("/");

function redirectTo(location: string): Response {
  return new Response(null, {
    status: 302,
    headers: { location, "cache-control": "no-store" },
  });
}

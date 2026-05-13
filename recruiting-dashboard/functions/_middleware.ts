// Cloudflare Pages Functions middleware: gates every request behind a
// Bubble-backed login. On a valid `tribe_session` cookie, requests pass
// through to the static dashboard; otherwise the user sees an inline
// login page (HTML below) that POSTs to /api/login.
//
// SameSite=None + Secure cookies are required so the dashboard works
// when iframed inside Bubble (overview.tribe.xyz).

import { SESSION_COOKIE, parseCookies, verifySession } from "./_lib/session";

interface Env {
  SESSION_SECRET: string;
}

type Ctx = EventContext<Env, string, Record<string, unknown>>;

// Paths that must NOT be gated (the login form itself, logout, favicon, etc.)
const OPEN_PATH_PREFIXES = ["/api/login", "/api/logout", "/favicon"];

function isOpenPath(pathname: string): boolean {
  for (const p of OPEN_PATH_PREFIXES) {
    if (pathname === p || pathname.startsWith(p + "/") || pathname.startsWith(p)) return true;
  }
  return false;
}

export const onRequest: PagesFunction<Env> = async (ctx: Ctx) => {
  const url = new URL(ctx.request.url);

  if (isOpenPath(url.pathname)) {
    return ctx.next();
  }

  const secret = ctx.env.SESSION_SECRET;
  if (!secret) {
    // Misconfigured deploy — surface a clear error rather than silently
    // 500'ing on every static asset.
    return new Response(
      "Auth misconfigured: SESSION_SECRET env var not set on this Cloudflare Pages project.",
      { status: 500, headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  }

  const cookies = parseCookies(ctx.request.headers.get("cookie"));
  const token = cookies[SESSION_COOKIE];
  const session = await verifySession(token, secret);

  if (session) {
    return ctx.next();
  }

  // No valid session — return the login page (200 OK) for ALL gated paths.
  // This means the SPA itself, JS bundles, and JSON data are blocked until
  // login; the user only ever sees the form until they auth.
  const showError = url.searchParams.get("error") === "1";
  return new Response(loginPageHtml(showError), {
    status: 200,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    },
  });
};

function loginPageHtml(showError: boolean): string {
  const errorBanner = showError
    ? `<div class="err">Invalid email or password. Please try again.</div>`
    : "";
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Tribe Recruiting Dashboard</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg" />
<style>
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; height: 100%; }
  body {
    background: #111827;
    color: #f9fafb;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
  }
  .card {
    background: #1f2937;
    border: 1px solid #374151;
    border-radius: 12px;
    padding: 32px 32px 28px;
    width: 100%;
    max-width: 380px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.4);
  }
  h1 {
    margin: 0 0 4px;
    font-size: 20px;
    font-weight: 600;
    color: #ffffff;
  }
  p.sub {
    margin: 0 0 22px;
    font-size: 13px;
    color: #9ca3af;
  }
  label {
    display: block;
    font-size: 12px;
    color: #9ca3af;
    margin-bottom: 6px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  input[type="email"], input[type="password"] {
    width: 100%;
    background: #111827;
    border: 1px solid #374151;
    border-radius: 6px;
    color: #f9fafb;
    padding: 10px 12px;
    font-size: 14px;
    margin-bottom: 16px;
    outline: none;
  }
  input[type="email"]:focus, input[type="password"]:focus {
    border-color: #3b82f6;
  }
  button {
    width: 100%;
    background: #2563eb;
    color: #fff;
    border: 0;
    border-radius: 6px;
    padding: 11px 14px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    margin-top: 4px;
  }
  button:hover { background: #1d4ed8; }
  .err {
    background: #7f1d1d;
    border: 1px solid #b91c1c;
    color: #fecaca;
    border-radius: 6px;
    padding: 9px 11px;
    font-size: 13px;
    margin-bottom: 16px;
  }
  .foot {
    margin-top: 18px;
    font-size: 11px;
    color: #6b7280;
    text-align: center;
  }
</style>
</head>
<body>
  <form class="card" method="POST" action="/api/login" autocomplete="on">
    <h1>Tribe Recruiting Dashboard</h1>
    <p class="sub">Sign in with your Tribe credentials</p>
    ${errorBanner}
    <label for="email">Email</label>
    <input id="email" name="email" type="email" required autocomplete="username" autofocus />
    <label for="password">Password</label>
    <input id="password" name="password" type="password" required autocomplete="current-password" />
    <button type="submit">Sign in</button>
    <div class="foot">Tribe.xyz internal</div>
  </form>
</body>
</html>`;
}

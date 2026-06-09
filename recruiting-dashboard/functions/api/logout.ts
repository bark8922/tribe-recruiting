// Clears the session + role cookies and bounces back to /.

import { PROJECT_HEALTH_COOKIE, ROLE_COOKIE, SESSION_COOKIE, clearCookie } from "../_lib/session";

export const onRequest: PagesFunction = async () => {
  const headers = new Headers();
  headers.append("location", "/");
  headers.append("set-cookie", clearCookie(SESSION_COOKIE));
  headers.append("set-cookie", clearCookie(ROLE_COOKIE));
  headers.append("set-cookie", clearCookie(PROJECT_HEALTH_COOKIE));
  headers.set("cache-control", "no-store");
  return new Response(null, { status: 302, headers });
};

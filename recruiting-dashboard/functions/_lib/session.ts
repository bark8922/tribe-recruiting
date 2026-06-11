// Shared session helpers for the Pages Functions auth gate.
//
// Tokens are tiny signed payloads, NOT JWTs. Format:
//   base64url(JSON.stringify({ email, isLeadership, exp })) + "." + base64url(HMAC-SHA256(payload, SESSION_SECRET))
//
// All cookie attributes are tuned for iframe embedding under
// overview.tribe.xyz (Bubble), so `SameSite=None; Secure` is mandatory.

export const SESSION_COOKIE = "tribe_session";
export const ROLE_COOKIE = "tribe_role";
export const PROJECT_HEALTH_COOKIE = "tribe_ph";
export const SESSION_MAX_AGE_SECONDS = 2592000; // 30 days
export const SESSION_MAX_AGE_MS = SESSION_MAX_AGE_SECONDS * 1000;

// 24 leadership emails — keep lowercase.
export const LEADERSHIP_EMAILS: Set<string> = new Set([
  "anastasija@tribe.xyz",
  "andrea@tribe.xyz",
  "blake@tribe.xyz",
  "carolinemurphy@tribe.xyz",
  "chene@tribe.xyz",
  "ella@tribe.xyz",
  "gustavo@tribe.xyz",
  "jacopo@tribe.xyz",
  "katarina@tribe.xyz",
  "kristina@tribe.xyz",
  "kristinaxnikolic@gmail.com", // Kristina Colovic's Bubble account email
  "kristjana@tribe.xyz",
  "lejla@tribe.xyz",
  "maria.gerbore@tribe.xyz",
  "martin@tribe.xyz",
  "meho@tribe.xyz",
  "niki@tribe.xyz",
  "rossella@tribe.xyz",
  "salem@tribe.xyz",
  "sanja@tribe.xyz",
  "simon@tribe.xyz",
  "tijana@tribe.xyz",
  "vladimir@tribe.xyz",
  "wladyslaw@tribe.xyz",
]);

// 6 director emails — strict subset of LEADERSHIP_EMAILS. Lowercase only.
// Used to gate the Profitability tab (tribe_role=director cookie).
export const DIRECTOR_EMAILS: Set<string> = new Set([
  "blake@tribe.xyz",
  "ella@tribe.xyz",
  "jacopo@tribe.xyz",
  "kristjana@tribe.xyz",
  "martin@tribe.xyz",
  "salem@tribe.xyz",
]);

// New Project Health tab (2026-06-09) — strictly Blake + Jacopo for now.
// Gates the gated NPH tab via a readable tribe_ph=1 cookie. Lowercase only.
export const PROJECT_HEALTH_EMAILS: Set<string> = new Set([
  "blake@tribe.xyz",
  "jacopo@tribe.xyz",
]);

export interface SessionPayload {
  email: string;
  isLeadership: boolean;
  exp: number; // ms epoch
}

// --- base64url helpers -----------------------------------------------------

function bytesToBase64Url(bytes: Uint8Array): string {
  let str = "";
  for (let i = 0; i < bytes.length; i++) str += String.fromCharCode(bytes[i]);
  return btoa(str).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlToBytes(b64url: string): Uint8Array {
  const pad = b64url.length % 4 === 0 ? "" : "=".repeat(4 - (b64url.length % 4));
  const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/") + pad;
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function strToBytes(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function bytesToStr(b: Uint8Array): string {
  return new TextDecoder().decode(b);
}

// --- HMAC ------------------------------------------------------------------

async function importHmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    strToBytes(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

async function hmacSign(secret: string, data: string): Promise<string> {
  const key = await importHmacKey(secret);
  const sig = await crypto.subtle.sign("HMAC", key, strToBytes(data));
  return bytesToBase64Url(new Uint8Array(sig));
}

async function hmacVerify(secret: string, data: string, sigB64url: string): Promise<boolean> {
  try {
    const key = await importHmacKey(secret);
    return crypto.subtle.verify("HMAC", key, base64UrlToBytes(sigB64url), strToBytes(data));
  } catch (_e) {
    return false;
  }
}

// --- session sign / verify -------------------------------------------------

export async function signSession(payload: SessionPayload, secret: string): Promise<string> {
  const body = bytesToBase64Url(strToBytes(JSON.stringify(payload)));
  const sig = await hmacSign(secret, body);
  return `${body}.${sig}`;
}

export async function verifySession(
  token: string | null | undefined,
  secret: string,
): Promise<SessionPayload | null> {
  if (!token || typeof token !== "string") return null;
  const dot = token.indexOf(".");
  if (dot <= 0 || dot === token.length - 1) return null;
  const body = token.slice(0, dot);
  const sig = token.slice(dot + 1);
  const ok = await hmacVerify(secret, body, sig);
  if (!ok) return null;
  let parsed: SessionPayload;
  try {
    parsed = JSON.parse(bytesToStr(base64UrlToBytes(body))) as SessionPayload;
  } catch (_e) {
    return null;
  }
  if (!parsed || typeof parsed.email !== "string" || typeof parsed.exp !== "number") return null;
  if (Date.now() > parsed.exp) return null;
  return parsed;
}

// --- cookie helpers --------------------------------------------------------

export function parseCookies(header: string | null): Record<string, string> {
  const out: Record<string, string> = {};
  if (!header) return out;
  for (const part of header.split(";")) {
    const eq = part.indexOf("=");
    if (eq < 0) continue;
    const k = part.slice(0, eq).trim();
    const v = part.slice(eq + 1).trim();
    if (k) out[k] = decodeURIComponent(v);
  }
  return out;
}

export function sessionCookie(value: string, maxAgeSeconds = SESSION_MAX_AGE_SECONDS): string {
  return `${SESSION_COOKIE}=${encodeURIComponent(value)}; Path=/; Max-Age=${maxAgeSeconds}; HttpOnly; Secure; SameSite=None`;
}

export function roleCookie(role: "director" | "leadership" | "member", maxAgeSeconds = SESSION_MAX_AGE_SECONDS): string {
  return `${ROLE_COOKIE}=${role}; Path=/; Max-Age=${maxAgeSeconds}; Secure; SameSite=None`;
}

export function projHealthCookie(allowed: boolean, maxAgeSeconds = SESSION_MAX_AGE_SECONDS): string {
  return `${PROJECT_HEALTH_COOKIE}=${allowed ? "1" : "0"}; Path=/; Max-Age=${maxAgeSeconds}; Secure; SameSite=None`;
}

export function clearCookie(name: string): string {
  return `${name}=; Path=/; Max-Age=0; Secure; SameSite=None${name === SESSION_COOKIE ? "; HttpOnly" : ""}`;
}

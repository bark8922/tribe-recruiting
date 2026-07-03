"""
Kai chat integration — the ONLY part that needs a Keboola master token.
=======================================================================
The app's auto-provisioned Storage token is NOT enough for the Kai client; Kai
currently requires a MASTER token. Provide it as a secret env var STORAGE_API_TOKEN
in the data app configuration. STORAGE_API_URL defaults to the EU stack.

SECURITY: a master token = full admin access to project 855. If it leaks, that is
full-project compromise, and it can only be revoked by removing the user from the
project. Recommended: create a dedicated service admin user (e.g. kai-app@tribe.xyz)
and use ITS master token here, never a personal one. Never log this value.

This module is intentionally defensive: if the token or the kai_client package is
missing, the app still runs and the chat box reports "not configured".
"""

import os

# Keboola injects a `#`-prefixed secret decrypted at runtime; accept either env var name.
STORAGE_API_TOKEN = os.environ.get("STORAGE_API_TOKEN") or os.environ.get("#STORAGE_API_TOKEN")  # MASTER token (secret)
STORAGE_API_URL = os.environ.get("STORAGE_API_URL", "https://connection.eu-central-1.keboola.com")


def is_configured():
    """True only if a token is present AND the kai client library is importable."""
    if not STORAGE_API_TOKEN:
        return False
    try:
        import kai_client  # noqa: F401
        return True
    except ImportError:
        return False


def ask(question: str) -> str:
    """Send one question to Kai and return the full text answer (blocking)."""
    import asyncio, traceback

    token_names = [k for k in os.environ if "TOKEN" in k.upper() or "STORAGE" in k.upper()]
    print(f"[kai] token env names seen={token_names} configured={is_configured()}", flush=True)

    if not is_configured():
        return "Kai is not configured. Set the STORAGE_API_TOKEN (master token) secret."

    from kai_client import KaiClient

    async def _run():
        client = await KaiClient.from_storage_api(
            storage_api_token=STORAGE_API_TOKEN,
            storage_api_url=STORAGE_API_URL,
        )
        async with client:
            # High-level call: runs the FULL agent loop — it executes Kai's tool/SQL
            # calls and returns (chat_id, final_answer). The low-level send_message
            # stream only emits the tool *requests* and waits for the caller to service
            # them, which is why hand-iterating it hung with no answer.
            chat_id, answer = await client.chat(question)
            return (answer or "").strip() or "(no answer)"

    try:
        ans = asyncio.run(_run())
        print(f"[kai] final answer_len={len(ans)} preview={ans[:160]!r}", flush=True)
        return ans
    except Exception as e:  # keep the PoC resilient; surface a short message
        print("[kai] EXCEPTION:\n" + traceback.format_exc(), flush=True)
        return f"Kai error: {type(e).__name__}: {e}"

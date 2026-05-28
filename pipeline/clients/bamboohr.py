"""
BambooHR HTTP client — drop-in replacement for the MCP BambooHR tools.

The methods here mirror the MCP tool names (`run_custom_report`,
`get_employee_directory`, `get_employee`, `get_time_off_requests`,
`get_whos_out`) and return the same shapes the current refresh pipeline
expects, so the only change when we migrate off Cowork is swapping
`mcp_call('run_custom_report', ...)` for `client.run_custom_report(...)`.

Auth
----
BambooHR uses HTTP Basic with the API key as username and any placeholder
(we use 'x') as password. Generate a key at:
    https://tribe.bamboohr.com/settings/permissions/api.php

Environment variables
---------------------
    BAMBOOHR_SUBDOMAIN   e.g. "tribe"  (required)
    BAMBOOHR_API_KEY     the API key    (required)
    BAMBOOHR_DEBUG       set to "1" to log every request URL (optional)

CLI smoke-test
--------------
    # pull salary history report as CSV to stdout
    python -m pipeline.clients.bamboohr report 327

    # pull active employee directory as JSON
    python -m pipeline.clients.bamboohr directory

    # pull one employee with specific fields
    python -m pipeline.clients.bamboohr employee 30 --fields employmentHistoryStatus,customMonthlyEmployerTrueCost

    # approved time off in a window
    python -m pipeline.clients.bamboohr timeoff 2024-01-01 2026-12-31 --status approved

    # who is out today
    python -m pipeline.clients.bamboohr whosout
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.auth import HTTPBasicAuth


class BambooHRError(RuntimeError):
    """Raised when BambooHR returns a non-retryable error."""


class BambooHRClient:
    """Thin HTTP wrapper over the BambooHR REST API."""

    DEFAULT_TIMEOUT = 60  # seconds; custom reports can be slow
    MAX_RETRIES = 4
    BACKOFF_BASE = 1.5  # seconds; doubles each retry

    def __init__(
        self,
        subdomain: Optional[str] = None,
        api_key: Optional[str] = None,
        *,
        timeout: int = DEFAULT_TIMEOUT,
        debug: Optional[bool] = None,
    ):
        self.subdomain = subdomain or os.environ.get("BAMBOOHR_SUBDOMAIN")
        self.api_key = api_key or os.environ.get("BAMBOOHR_API_KEY")
        if not self.subdomain:
            raise BambooHRError(
                "BAMBOOHR_SUBDOMAIN not set (pass explicitly or export env var)"
            )
        if not self.api_key:
            raise BambooHRError(
                "BAMBOOHR_API_KEY not set (pass explicitly or export env var)"
            )
        self.timeout = timeout
        self.debug = debug if debug is not None else os.environ.get("BAMBOOHR_DEBUG") == "1"
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(self.api_key, "x")

    # ------------------------------------------------------------------
    # Internal plumbing
    # ------------------------------------------------------------------

    @property
    def _base_url(self) -> str:
        return f"https://api.bamboohr.com/api/gateway.php/{self.subdomain}/v1"

    def _request(
        self,
        path: str,
        *,
        params: Optional[dict] = None,
        accept: str = "application/json",
    ) -> requests.Response:
        url = f"{self._base_url}{path}"
        headers = {"Accept": accept}
        if self.debug:
            q = f"?{urlencode(params)}" if params else ""
            print(f"[bamboohr] GET {url}{q}", file=sys.stderr)

        last_exc: Optional[Exception] = None
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = self._session.get(
                    url, params=params, headers=headers, timeout=self.timeout
                )
            except requests.RequestException as exc:
                last_exc = exc
                self._sleep_backoff(attempt)
                continue

            # 429 and 5xx are transient — retry with backoff
            if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                if self.debug:
                    print(
                        f"[bamboohr] transient {resp.status_code} on attempt "
                        f"{attempt + 1}/{self.MAX_RETRIES}",
                        file=sys.stderr,
                    )
                self._sleep_backoff(attempt)
                last_exc = BambooHRError(
                    f"HTTP {resp.status_code}: {resp.text[:200]}"
                )
                continue

            if not resp.ok:
                raise BambooHRError(
                    f"HTTP {resp.status_code} from {url}: {resp.text[:500]}"
                )
            return resp

        # exhausted retries
        raise BambooHRError(
            f"Giving up after {self.MAX_RETRIES} attempts: {last_exc}"
        )

    def _sleep_backoff(self, attempt: int) -> None:
        time.sleep(self.BACKOFF_BASE * (2 ** attempt))

    # ------------------------------------------------------------------
    # Public API — matches MCP tool names 1:1
    # ------------------------------------------------------------------

    def run_custom_report(self, report_id: str | int, format: str = "csv") -> str:
        """
        Pull a pre-configured custom report. Returns the raw CSV body as a
        string when format='csv' (the format the pipeline expects), or a
        JSON-decoded dict when format='json'.

        Reports the pipeline depends on:
            149 — Full employee list (active + terminated)
            228 — Division History
            327 — Salary History (full)
            328 — Bonus History (full)
        """
        fmt = format.lower()
        accept = "text/csv" if fmt == "csv" else "application/json"
        resp = self._request(
            f"/reports/{report_id}",
            params={"format": "CSV" if fmt == "csv" else "JSON", "fd": "yes"},
            accept=accept,
        )
        if fmt == "csv":
            return resp.text
        return resp.json()

    def get_employee_directory(self) -> dict:
        """
        Returns BambooHR's employee directory payload:
            {"fields": [...], "employees": [{...}, ...]}
        Caller extracts .employees for the active roster.
        """
        return self._request("/employees/directory").json()

    def get_employee(
        self, employee_id: str | int, fields: Optional[str | list[str]] = None
    ) -> dict:
        """
        Fetch a single employee. If `fields` is provided (comma-separated
        string or list), only those fields are returned. Otherwise the
        default BambooHR subset comes back.

        Commonly used in the refresh:
            fields='employmentHistoryStatus,customMonthlyEmployerTrueCost'
            fields='terminationDate'
        """
        if isinstance(fields, (list, tuple)):
            fields = ",".join(fields)
        params = {"fields": fields} if fields else None
        return self._request(f"/employees/{employee_id}", params=params).json()

    def get_time_off_requests(
        self,
        start: str,
        end: str,
        *,
        status: Optional[str] = None,
        employee_id: Optional[str | int] = None,
        type: Optional[str] = None,
    ) -> list[dict]:
        """
        List time-off requests in a date window.
            start, end   — 'YYYY-MM-DD'
            status       — 'approved' | 'denied' | 'superseded' | 'requested' | 'canceled'
            employee_id  — restrict to one person
            type         — time-off type id
        """
        params: dict[str, Any] = {"start": start, "end": end}
        if status:
            params["status"] = status
        if employee_id is not None:
            params["employeeId"] = str(employee_id)
        if type is not None:
            params["type"] = str(type)
        return self._request("/time_off/requests/", params=params).json()

    def get_whos_out(
        self, start: Optional[str] = None, end: Optional[str] = None
    ) -> list[dict]:
        """
        List employees currently (or within the window) out of office.
        Defaults to "today through 14 days" when no range given, matching
        BambooHR's default.
        """
        params: dict[str, Any] = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        return self._request(
            "/time_off/whos_out/", params=params or None
        ).json()


# ----------------------------------------------------------------------
# CLI — smoke-test from the shell without writing any glue code
# ----------------------------------------------------------------------

def _main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.clients.bamboohr",
        description="Smoke-test the BambooHR HTTP client.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_report = sub.add_parser("report", help="pull a custom report")
    p_report.add_argument("id", help="report id, e.g. 327")
    p_report.add_argument(
        "--format", default="csv", choices=["csv", "json"], help="output format"
    )

    sub.add_parser("directory", help="pull active employee directory")

    p_emp = sub.add_parser("employee", help="pull one employee record")
    p_emp.add_argument("id", help="employee id (employee #)")
    p_emp.add_argument("--fields", default=None, help="comma-separated field list")

    p_to = sub.add_parser("timeoff", help="list time-off requests")
    p_to.add_argument("start", help="YYYY-MM-DD")
    p_to.add_argument("end", help="YYYY-MM-DD")
    p_to.add_argument("--status", default=None)

    p_who = sub.add_parser("whosout", help="list current/upcoming out-of-office")
    p_who.add_argument("--start", default=None)
    p_who.add_argument("--end", default=None)

    args = parser.parse_args(argv)
    client = BambooHRClient()

    if args.cmd == "report":
        out = client.run_custom_report(args.id, format=args.format)
        if isinstance(out, str):
            sys.stdout.write(out)
        else:
            json.dump(out, sys.stdout, indent=2)
    elif args.cmd == "directory":
        json.dump(client.get_employee_directory(), sys.stdout, indent=2)
    elif args.cmd == "employee":
        json.dump(
            client.get_employee(args.id, fields=args.fields),
            sys.stdout,
            indent=2,
        )
    elif args.cmd == "timeoff":
        json.dump(
            client.get_time_off_requests(args.start, args.end, status=args.status),
            sys.stdout,
            indent=2,
        )
    elif args.cmd == "whosout":
        json.dump(
            client.get_whos_out(start=args.start, end=args.end),
            sys.stdout,
            indent=2,
        )
    else:  # pragma: no cover — argparse enforces this
        parser.error(f"unknown cmd: {args.cmd}")

    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

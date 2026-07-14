#!/usr/bin/env python3
"""Diagnostic tool: check if an HTTP proxy keeps the same egress across repeated requests.

Uses the same ``--session-id`` tag on every request, but this is ONLY a correlation
tag for the test run. It does NOT create a sticky session or influence proxy behavior.
Public free proxies cannot be made sticky by any client-side header.
"""
import argparse
import ipaddress
import json
import re
import urllib.request

TARGET = "http://ip-api.com/json/?fields=status,query,countryCode"
COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


def _validate_ip(value):
    """Return a valid IPv4/IPv6 string or None."""
    if not value or not isinstance(value, str):
        return None
    try:
        ipaddress.ip_address(value)
        return str(ipaddress.ip_address(value))
    except ValueError:
        return None


def _validate_country(value):
    """Return an uppercase 2-letter country code or None."""
    if not value or not isinstance(value, str):
        return None
    upper = value.upper()
    return upper if COUNTRY_RE.match(upper) else None


def request_egress(proxy, session_id, target=TARGET, timeout=10):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": f"http://{proxy}", "https": f"http://{proxy}"}))
    request = urllib.request.Request(target, headers={"User-Agent": "ProxyScraper/5.0", "X-Session-ID": session_id})
    try:
        with opener.open(request, timeout=timeout) as response:
            data = json.loads(response.read(8192).decode("utf-8"))
        if data.get("status") != "success":
            return {"error": "invalid geo response"}
        ip = _validate_ip(data.get("query"))
        cc = _validate_country(data.get("countryCode"))
        if not ip or not cc:
            return {"error": "malformed geo response"}
        return {"ip": ip, "country_code": cc}
    except Exception as error:
        return {"error": type(error).__name__}


def summarize_samples(samples, requested):
    successful = [sample for sample in samples if sample.get("ip") and sample.get("country_code")]
    ips = sorted({sample["ip"] for sample in successful})
    countries = sorted({sample["country_code"] for sample in successful})
    return {
        "requested": requested, "successes": len(successful),
        "stable": len(successful) == requested and len(ips) == len(countries) == 1,
        "unique_ips": ips, "unique_countries": countries, "samples": samples,
    }


def check_session(proxy, session_id, samples=3, target=TARGET, timeout=10):
    return summarize_samples([request_egress(proxy, session_id, target, timeout) for _ in range(samples)], samples)


def main():
    parser = argparse.ArgumentParser(description="Diagnostic: check if HTTP proxy keeps the same egress across repeated requests. Session ID is a correlation tag only — it does not create sticky sessions.")
    parser.add_argument("--proxy", required=True, help="HTTP proxy as host:port")
    parser.add_argument("--session-id", required=True, help="Correlation tag for this test run (not a sticky-session token)")
    parser.add_argument("--samples", type=int, default=3, choices=range(2, 11))
    parser.add_argument("--target", default=TARGET, help="JSON endpoint returning status, query/IP, and countryCode")
    parser.add_argument("--timeout", type=int, default=10, choices=range(1, 61))
    args = parser.parse_args()
    result = check_session(args.proxy, args.session_id, args.samples, args.target, args.timeout)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["stable"] else 1)


if __name__ == "__main__":
    main()

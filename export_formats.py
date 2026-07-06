#!/usr/bin/env python3
"""
export_formats.py — Export proxy list in multiple formats.

Formats: txt, json, csv, yaml, env, toml
"""
import csv
import io
import json
import os
from typing import List, Dict


def export_txt(proxies: List[Dict], output: str):
    """Export as plain text (host:port)."""
    lines = sorted(set(f"{p['ip']}:{p['port']}" for p in proxies))
    with open(output, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"✅ TXT → {output} ({len(lines)} proxies)")


def export_json(proxies: List[Dict], output: str):
    """Export as JSON."""
    with open(output, "w") as f:
        json.dump(proxies, f, indent=2)
    print(f"✅ JSON → {output} ({len(proxies)} proxies)")


def export_csv(proxies: List[Dict], output: str):
    """Export as CSV."""
    if not proxies:
        return
    fields = ["ip", "port", "protocol", "score", "anonymity", "country_code", "city", "isp", "response_time_ms"]
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(proxies)
    print(f"✅ CSV → {output} ({len(proxies)} proxies)")


def export_yaml(proxies: List[Dict], output: str):
    """Export as YAML (manual, no dependency)."""
    lines = ["proxies:"]
    for p in proxies:
        lines.append(f"  - ip: \"{p.get('ip', '')}\"")
        lines.append(f"    port: {p.get('port', 0)}")
        lines.append(f"    protocol: \"{p.get('protocol', 'http')}\"")
        lines.append(f"    score: {p.get('score', 0)}")
        lines.append(f"    country: \"{p.get('country_code', '')}\"")
        lines.append(f"    anonymity: \"{p.get('anonymity', 'unknown')}\"")
    with open(output, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"✅ YAML → {output} ({len(proxies)} proxies)")


def export_env(proxies: List[Dict], output: str):
    """Export as environment variable format."""
    lines = ["# Proxy Pool Environment Variables"]
    lines.append("# Usage: source proxy-env.sh")
    lines.append("")
    best_http = next((p for p in proxies if p.get("protocol") == "http"), proxies[0] if proxies else None)
    best_socks = next((p for p in proxies if p.get("protocol") == "socks5"), None)
    if best_http:
        lines.append(f'export HTTP_PROXY="http://{best_http["ip"]}:{best_http["port"]}"')
        lines.append(f'export http_proxy="$HTTP_PROXY"')
    if best_socks:
        lines.append(f'export ALL_PROXY="socks5://{best_socks["ip"]}:{best_socks["port"]}"')
        lines.append(f'export all_proxy="$ALL_PROXY"')
    lines.append("")
    lines.append("# All proxies (JSON)")
    lines.append(f'export PROXY_POOL=\'{json.dumps([f"{p["ip"]}:{p["port"]}" for p in proxies[:20]])}\'')
    with open(output, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"✅ ENV → {output}")


def export_all(proxies: List[Dict], base_name: str = "proxies", formats: List[str] = None):
    """Export in all requested formats."""
    if formats is None:
        formats = ["txt", "json", "csv", "yaml", "env"]

    exporters = {
        "txt": lambda p, o: export_txt(p, o),
        "json": lambda p, o: export_json(p, o),
        "csv": lambda p, o: export_csv(p, o),
        "yaml": lambda p, o: export_yaml(p, o),
        "env": lambda p, o: export_env(p, o),
    }

    exts = {"txt": ".txt", "json": ".json", "csv": ".csv", "yaml": ".yaml", "env": ".sh"}

    for fmt in formats:
        if fmt in exporters:
            ext = exts.get(fmt, f".{fmt}")
            exporters[fmt](proxies, f"{base_name}{ext}")


if __name__ == "__main__":
    # Test with sample data
    sample = [
        {"ip": "1.2.3.4", "port": 8080, "protocol": "http", "score": 85, "anonymity": "elite", "country_code": "US", "city": "NYC", "isp": "Amazon", "response_time_ms": 120},
        {"ip": "5.6.7.8", "port": 3128, "protocol": "socks5", "score": 72, "anonymity": "transparent", "country_code": "ID", "city": "Jakarta", "isp": "Telkomsel", "response_time_ms": 200},
    ]
    export_all(sample, "test_export", ["txt", "json", "csv", "yaml", "env"])

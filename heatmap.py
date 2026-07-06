#!/usr/bin/env python3
"""
heatmap.py — Geo-distribution heatmap visualization.

Generates HTML page with proxy distribution by country.
"""
import json
from typing import Dict, List

COUNTRY_NAMES = {
    "US": "United States", "ID": "Indonesia", "JP": "Japan", "DE": "Germany",
    "GB": "United Kingdom", "FR": "France", "BR": "Brazil", "IN": "India",
    "RU": "Russia", "CN": "China", "KR": "South Korea", "NL": "Netherlands",
    "SG": "Singapore", "AU": "Australia", "CA": "Canada", "IT": "Italy",
    "ES": "Spain", "TR": "Turkey", "TH": "Thailand", "VN": "Vietnam",
    "PH": "Philippines", "MY": "Malaysia", "HK": "Hong Kong", "TW": "Taiwan",
}


def generate_heatmap(proxies: List[Dict], output: str = "heatmap.html"):
    """Generate geo-distribution heatmap HTML."""
    # Count by country
    by_country: Dict[str, int] = {}
    for p in proxies:
        cc = p.get("country_code", "XX") or "XX"
        by_country[cc] = by_country.get(cc, 0) + 1

    # Sort by count
    sorted_countries = sorted(by_country.items(), key=lambda x: x[1], reverse=True)
    total = len(proxies)
    max_count = max(by_country.values()) if by_country else 1

    # Build bars
    bars_html = ""
    colors = ["#00ff88", "#00ccff", "#ffcc00", "#ff8844", "#ff4444", "#cc44ff"]
    for i, (cc, count) in enumerate(sorted_countries[:20]):
        pct = round(100 * count / total, 1) if total else 0
        bar_width = round(100 * count / max_count, 1) if max_count else 0
        color = colors[i % len(colors)]
        name = COUNTRY_NAMES.get(cc, cc)
        bars_html += f"""
        <div class="bar-row">
            <span class="bar-label">{cc} {name}</span>
            <div class="bar-track">
                <div class="bar-fill" style="width:{bar_width}%;background:{color}"></div>
            </div>
            <span class="bar-value">{count} ({pct}%)</span>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Proxy Geo-Distribution</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui;background:#0a0a0a;color:#e0e0e0;padding:20px}}
h1{{font-size:1.5em;margin-bottom:16px;color:#00ff88}}
.stats{{display:flex;gap:20px;margin-bottom:20px;flex-wrap:wrap}}
.stat{{background:#1a1a2e;padding:16px 24px;border-radius:10px;border:1px solid #2a2a4a}}
.stat .num{{font-size:1.8em;font-weight:700;color:#00ff88}}
.stat .label{{font-size:.85em;color:#888}}
.bar-row{{display:flex;align-items:center;margin:6px 0;gap:10px}}
.bar-label{{width:180px;font-size:.85em;color:#ccc;text-align:right}}
.bar-track{{flex:1;height:24px;background:#16213e;border-radius:6px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:6px;transition:width .3s}}
.bar-value{{width:100px;font-size:.85em;color:#aaa}}
</style>
</head>
<body>
<h1>🌍 Proxy Geo-Distribution</h1>
<div class="stats">
    <div class="stat"><div class="num">{total}</div><div class="label">Total Proxies</div></div>
    <div class="stat"><div class="num">{len(by_country)}</div><div class="label">Countries</div></div>
    <div class="stat"><div class="num">{sorted_countries[0][0] if sorted_countries else 'N/A'}</div><div class="label">Top Country</div></div>
</div>
<h2>Distribution</h2>
{bars_html}
</body>
</html>"""

    with open(output, "w") as f:
        f.write(html)
    print(f"✅ Heatmap → {output} ({len(by_country)} countries, {total} proxies)")
    return by_country


if __name__ == "__main__":
    # Test
    sample = [
        {"country_code": "US"}, {"country_code": "US"}, {"country_code": "US"},
        {"country_code": "ID"}, {"country_code": "ID"},
        {"country_code": "JP"}, {"country_code": "DE"}, {"country_code": "XX"},
    ]
    generate_heatmap(sample, "/tmp/test_heatmap.html")

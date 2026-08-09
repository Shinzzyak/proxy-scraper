#!/usr/bin/env python3
"""config_loader.py — Load config.yaml with CLI/env precedence.

Merge order (lowest → highest priority):
  1. config.yaml defaults
  2. environment variables (CONFIG_KEY style)
  3. explicit CLI args (passed in by caller)

Supports a YAML subset (indentation + key: value + lists). No PyYAML
dependency — keeps the repo stdlib-only.
"""
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = ROOT / "config.yaml"


def parse_yaml_subset(text: str) -> dict:
    """Parse a small YAML subset: nested maps (2-space indent) + scalars + lists.

    Returns {} on any parse issue — config is optional, never fatal.
    """
    result: dict = {}
    stack: list[tuple[int, dict]] = [(-1, result)]
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if not stack:
            stack.append((-1, result))
        parent = stack[-1][1]
        if line.startswith("- "):
            parent.setdefault("_list", []).append(_scalar(line[2:]))
            continue
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()
        if val == "":
            child = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            # strip inline comments (e.g. "0.5  # alive rate below this")
            val = re.split(r"\s+#", val, maxsplit=1)[0].strip()
            parent[key] = _scalar(val)
    return result


def _scalar(val: str):
    val = val.strip()
    if val.lower() == "true":
        return True
    if val.lower() == "false":
        return False
    if re.fullmatch(r"-?\d+", val):
        return int(val)
    if re.fullmatch(r"-?\d+\.\d+", val):
        return float(val)
    return val.strip("\"'") if val else ""


def load_config(path: str | os.PathLike = DEFAULT_CONFIG_PATH) -> dict:
    """Load config.yaml into a flat dict with dotted keys, e.g. {'max_validate': 2000}."""
    cfg: dict = {}
    p = Path(path)
    if not p.exists():
        return cfg
    try:
        data = parse_yaml_subset(p.read_text(encoding="utf-8"))
    except Exception:
        return cfg

    def flatten(node: dict, prefix: str = ""):
        for k, v in node.items():
            key = f"{prefix}{k}" if prefix else k
            if isinstance(v, dict):
                if "_list" in v:
                    cfg[key] = v["_list"]
                else:
                    flatten(v, key + ".")
            else:
                cfg[key] = v

    flatten(data)
    return cfg


def env_override(key: str, value, env_prefix: str = "PROXY_CFG_"):
    """Apply environment override for one config key, if present."""
    env_key = env_prefix + key.upper().replace(".", "_")
    raw = os.environ.get(env_key)
    if raw is None:
        return value
    if isinstance(value, bool):
        return raw.lower() in {"1", "true", "yes"}
    if isinstance(value, int):
        try:
            return int(raw)
        except ValueError:
            return value
    if isinstance(value, float):
        try:
            return float(raw)
        except ValueError:
            return value
    return raw


def merged_config(cli_defaults: dict | None = None, cli_args: dict | None = None) -> dict:
    """Merge config.yaml + env + CLI. cli_defaults: argparse defaults; cli_args: parsed values."""
    cfg = load_config()
    for key in list(cfg):
        cfg[key] = env_override(key, cfg[key])

    if cli_defaults:
        for key, default in cli_defaults.items():
            if key not in cfg:
                cfg[key] = default
    if cli_args:
        for key, value in cli_args.items():
            if value is not None:
                cfg[key] = value
    return cfg


if __name__ == "__main__":
    import json
    print(json.dumps(load_config(), indent=2))

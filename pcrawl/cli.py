from __future__ import annotations

import argparse
import ast
import json
import logging
import sys
from typing import Any

from . import __version__
from .engine import CrawlerEngine
from .utils import load_object


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pcrawl", description="Run a pcrawl spider")
    parser.add_argument("--version", action="version", version=f"pcrawl {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run a spider")
    run.add_argument("spider", help="import path such as 'examples.simple_http:SimpleSpider'")
    run.add_argument("--workers", type=int)
    run.add_argument("--concurrency", type=int)
    run.add_argument("--queue", choices=("memory", "redis"))
    run.add_argument("--redis-url")
    run.add_argument("--purser", choices=("auto", "off", "embedded", "local"))
    run.add_argument("--keep-alive", action="store_true", default=None)
    run.add_argument("--log-level")
    run.add_argument(
        "--settings-module",
        help="project settings module, e.g. 'myproject.settings' "
        "(defaults to PCRAWL_SETTINGS_MODULE or pcrawl.cfg)",
    )
    run.add_argument(
        "-s",
        "--set",
        dest="settings",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override a setting, repeatable; values accept Python literals",
    )
    run.add_argument("--json", action="store_true", help="print final stats as JSON")
    return parser


def _coerce_setting(value: str) -> Any:
    stripped = value.strip()
    lowered = stripped.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return ast.literal_eval(stripped)
    except (ValueError, SyntaxError):
        return stripped


def _setting_overrides(args: argparse.Namespace, parser: argparse.ArgumentParser) -> dict[str, Any]:
    changes: dict[str, Any] = {}
    for field in ("workers", "concurrency", "redis_url", "keep_alive", "log_level"):
        value = getattr(args, field, None)
        if value is not None:
            changes[field] = value
    if args.queue is not None:
        changes["queue_backend"] = args.queue
    if args.purser is not None:
        changes["purser_mode"] = args.purser
    for item in args.settings:
        if "=" not in item:
            parser.error(f"-s/--set expects KEY=VALUE, got {item!r}")
        key, raw_value = item.split("=", 1)
        key = key.strip()
        if not key:
            parser.error(f"-s/--set expects a non-empty key, got {item!r}")
        changes[key] = _coerce_setting(raw_value)
    return changes


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command != "run":
        return 2

    spider_cls = load_object(args.spider)
    changes = _setting_overrides(args, parser)
    engine = CrawlerEngine(
        spider_cls,
        changes,
        settings_module=args.settings_module,
    )

    logging.basicConfig(
        level=getattr(logging, str(engine.settings.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(process)d] %(name)s: %(message)s",
    )
    stats = engine.run()
    if args.json:
        print(json.dumps(stats, indent=2, sort_keys=True))
    else:
        print(f"pcrawl finished: {stats}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

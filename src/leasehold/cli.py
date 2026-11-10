"""The ``leasehold`` command line."""

from __future__ import annotations

import argparse
import json
import sys

from leasehold import __version__, naming
from leasehold.spec import (
    ServiceSpec,
    SpecError,
    check_registry,
    find_specs,
    load_spec,
    resource_names,
)


def _cmd_validate(args: argparse.Namespace) -> int:
    paths = find_specs(args.paths)
    if not paths:
        print("no service.yml files found", file=sys.stderr)
        return 1
    specs: list[ServiceSpec] = []
    failed = False
    for path in paths:
        try:
            spec = load_spec(path)
        except SpecError as e:
            failed = True
            print(f"FAIL {path}")
            for msg in e.messages:
                print(f"     {msg}")
            continue
        specs.append(spec)
        print(f"ok   {path} ({spec.name})")
    registry_errors = check_registry(specs, partial=args.partial)
    for msg in registry_errors:
        print(f"FAIL registry: {msg}")
    return 1 if failed or registry_errors else 0


def _cmd_names(args: argparse.Namespace) -> int:
    try:
        naming.check_env(args.env)
        spec = load_spec(args.path)
    except (SpecError, naming.NamingError) as e:
        print(e, file=sys.stderr)
        return 1
    rows = resource_names(spec, args.env)
    widths = [max(len(r[i]) for r in rows) for i in range(2)]
    for kind, declared, actual in rows:
        print(f"{kind:<{widths[0]}}  {declared:<{widths[1]}}  {actual}")
    return 0


def _cmd_schema(args: argparse.Namespace) -> int:
    schema = ServiceSpec.model_json_schema(by_alias=True)
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = "leasehold service.yml"
    print(json.dumps(schema, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="leasehold", description="leasehold platform tooling")
    p.add_argument("--version", action="version", version=f"leasehold {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    v = sub.add_parser("validate", help="validate service.yml files and how they fit together")
    v.add_argument("paths", nargs="+", help="service.yml files or directories to search")
    v.add_argument(
        "--partial",
        action="store_true",
        help="accept references to services that aren't among the given files",
    )
    v.set_defaults(func=_cmd_validate)

    n = sub.add_parser("names", help="show the resource names a service gets in an environment")
    n.add_argument("path", help="a service.yml file")
    n.add_argument("--env", default=naming.PROD, help="environment (default: prod)")
    n.set_defaults(func=_cmd_names)

    s = sub.add_parser("schema", help="print the JSON Schema of service.yml (for editors)")
    s.set_defaults(func=_cmd_schema)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

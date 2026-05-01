#!/usr/bin/env python3
"""Milestone B: dynamic CSV analysis and optional cross-file comparison."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

ROOT_PKG = Path(__file__).resolve().parents[1]  # Task_03_Descriptive_Stats
if str(ROOT_PKG) not in sys.path:
    sys.path.insert(0, str(ROOT_PKG))

from election_analysis_b.compare import compare_all_group_dimensions, compare_shared_payloads  # noqa: E402
from election_analysis_b.pathing import ensure_task02_on_path  # noqa: E402
from election_analysis_b.registry import default_registry_path, get_named, load_registry  # noqa: E402
from election_analysis_b.schema import validate_group_specs  # noqa: E402


def _json_default(o: object) -> object:
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    raise TypeError(f"not JSON serializable: {type(o)}")


def _parse_group_args(parts: list[str]) -> tuple[tuple[str, ...], ...]:
    out: list[tuple[str, ...]] = []
    for p in parts:
        cols = tuple(c.strip() for c in p.split(",") if c.strip())
        if cols:
            out.append(cols)
    return tuple(out)


def _emit(payload: dict[str, object], out_path: Path | None) -> None:
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2, default=_json_default)
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


def main() -> int:
    ensure_task02_on_path()

    from election_analysis.contract import normalize_for_compare  # noqa: WPS433
    from election_analysis.stdlib_impl.csv_loader import load_csv_rows  # noqa: WPS433
    from election_analysis.stdlib_impl.pipeline import analyze_table  # noqa: WPS433

    p = argparse.ArgumentParser(
        description="Analyze any CSV with dynamic schema inference and configurable grouping.",
    )
    p.add_argument("csv_path", nargs="?", type=Path, help="Input CSV.")
    p.add_argument(
        "--groups",
        "-g",
        action="append",
        default=None,
        help='Group spec(s), comma-separated columns, e.g. "page_id" or "page_id,ad_id". Repeatable.',
    )
    p.add_argument(
        "--dataset",
        "-d",
        default=None,
        help="Named entry from datasets/registry.json (overrides positional csv/groups unless groups given).",
    )
    p.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="Alternate registry JSON (default: Task_03/datasets/registry.json).",
    )
    p.add_argument("--out", "-o", type=Path, default=None, help="Write JSON results here.")
    p.add_argument(
        "--compare",
        "--with",
        dest="compare_path",
        type=Path,
        default=None,
        metavar="OTHER_CSV",
        help="Second CSV: emit shared-column dataset + column stats vs first.",
    )
    p.add_argument(
        "--list-datasets",
        action="store_true",
        help="Print dataset names from the registry and exit.",
    )

    args = p.parse_args()
    reg_path = args.registry if args.registry is not None else default_registry_path()

    if args.list_datasets:
        reg = load_registry(reg_path)
        for name, spec in sorted(reg.items()):
            path_s = spec.csv_path if spec.csv_path else "(no path)"
            print(f"{name}: {path_s}")
        return 0

    csv_path = args.csv_path
    group_specs = _parse_group_args(list(args.groups or []))

    if args.dataset:
        spec = get_named(reg_path, args.dataset)
        if csv_path is None:
            csv_path = spec.csv_path
        elif csv_path.resolve() != spec.csv_path.resolve():
            pass  # honor explicit positional over registry path
        if not group_specs:
            group_specs = spec.groups

    if csv_path is None:
        print("Provide a CSV path, or use --dataset with a registry entry.", file=sys.stderr)
        return 2

    if not csv_path.is_file():
        print(f"Missing CSV: {csv_path}", file=sys.stderr)
        return 2

    column_order, rows = load_csv_rows(csv_path.resolve())
    if not group_specs:
        print("Specify --groups ..., or choose --dataset ... with preset groups.", file=sys.stderr)
        return 2

    validate_group_specs(columns=list(column_order), groups=group_specs)

    payload_a = analyze_table(rows=rows, column_order=column_order, group_specs=group_specs)
    payload_a_norm = normalize_for_compare(payload_a)

    if args.compare_path:
        csv_b = args.compare_path.resolve()
        if not csv_b.is_file():
            print(f"Missing comparison CSV: {csv_b}", file=sys.stderr)
            return 2
        col_b, rows_b = load_csv_rows(csv_b.resolve())
        validate_group_specs(columns=list(col_b), groups=group_specs)
        payload_b = analyze_table(rows=rows_b, column_order=col_b, group_specs=group_specs)
        payload_b_norm = normalize_for_compare(payload_b)

        full = {
            "first_csv": str(csv_path.resolve()),
            "second_csv": str(csv_b.resolve()),
            "group_specs": group_specs,
            "global_comparison": compare_shared_payloads(payload_a_norm, payload_b_norm),
            "groups_comparison": compare_all_group_dimensions(
                payload_a_norm["groups"],  # type: ignore[arg-type]
                payload_b_norm["groups"],  # type: ignore[arg-type]
                [",".join(s) for s in group_specs],
            ),
            "individual": {"first": payload_a_norm, "second": payload_b_norm},
        }
        _emit(full, args.out)
        return 0

    _emit(dict(payload_a_norm), args.out)  # type: ignore[arg-type]
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

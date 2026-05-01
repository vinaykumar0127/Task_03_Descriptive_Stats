"""Named dataset presets: CSV path (optional) + default grouping specs.

Paths in JSON may be absolute or relative to the registry file directory.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    csv_path: Path
    groups: tuple[tuple[str, ...], ...]
    meta: dict[str, Any]


def _parse_groups(raw: Any) -> tuple[tuple[str, ...], ...]:
    out: list[tuple[str, ...]] = []
    for spec in raw:
        if not isinstance(spec, list):
            raise TypeError("each group must be a list of column names")
        out.append(tuple(str(c).strip() for c in spec))
    return tuple(out)


def load_registry(registry_path: Path) -> dict[str, DatasetSpec]:
    data = json.loads(registry_path.read_text(encoding="utf-8"))
    base = registry_path.parent
    datasets = data.get("datasets", {})
    out: dict[str, DatasetSpec] = {}
    for name, blob in datasets.items():
        raw_path = blob.get("csv_path")
        if raw_path:
            path = Path(raw_path)
            if not path.is_absolute():
                path = (base / path).resolve()
        else:
            path = Path()
        gs = blob.get("groups", [])
        groups = _parse_groups(gs)
        meta = {k: v for k, v in blob.items() if k not in ("csv_path", "groups")}
        out[name] = DatasetSpec(name=name, csv_path=path, groups=groups, meta=meta)
    return out


def get_named(registry_path: Path, name: str) -> DatasetSpec:
    reg = load_registry(registry_path)
    if name not in reg:
        raise KeyError(f"unknown dataset {name!r}; known: {sorted(reg)!r}")
    spec = reg[name]
    if not spec.csv_path or not spec.csv_path.is_file():
        raise FileNotFoundError(
            f"dataset {name!r} csv_path missing or not a file: {spec.csv_path}",
        )
    return spec


def default_registry_path() -> Path:
    here = Path(__file__).resolve().parent.parent / "datasets" / "registry.json"
    return here

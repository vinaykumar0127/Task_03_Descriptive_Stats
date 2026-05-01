"""Schema helpers: derive column order and validate group specs against observed headers."""

from __future__ import annotations

from typing import Sequence


def validate_group_specs(
    *,
    columns: Sequence[str],
    groups: Sequence[tuple[str, ...]],
) -> None:
    colset = set(columns)
    for spec in groups:
        for g in spec:
            if g not in colset:
                raise ValueError(f"group column {g!r} not in schema {list(columns)!r}")

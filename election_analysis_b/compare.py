"""Cross-dataset comparison on shared columns (dataset- and column-level payloads)."""

from __future__ import annotations

from typing import Any, Mapping


def _sorted_intersection(columns_a: list[str], columns_b: list[str]) -> list[str]:
    sa, sb = set(columns_a), set(columns_b)
    return sorted(sa & sb)


def compare_shared_payloads(payload_a: dict[str, Any], payload_b: dict[str, Any]) -> dict[str, Any]:
    """Given two analyze_table-style payloads, align on intersecting column names."""
    cols_a = list(payload_a["dataset"]["columns"])
    cols_b = list(payload_b["dataset"]["columns"])
    shared = _sorted_intersection(cols_a, cols_b)

    cols_only_a = sorted(set(cols_a) - set(shared))
    cols_only_b = sorted(set(cols_b) - set(shared))

    return {
        "shared_columns": shared,
        "columns_only_in_first": cols_only_a,
        "columns_only_in_second": cols_only_b,
        "dataset_level": {
            "first": payload_a["dataset"],
            "second": payload_b["dataset"],
            "missing_per_column_aligned": {
                c: (
                    payload_a["dataset"]["missing_per_column"].get(c),
                    payload_b["dataset"]["missing_per_column"].get(c),
                )
                for c in shared
            },
        },
        "column_stats": {
            c: {"first": payload_a["columns"][c], "second": payload_b["columns"][c]} for c in shared
        },
    }


def compare_group_dimension(
    grouped_a: Mapping[str, Any],
    grouped_b: Mapping[str, Any],
    *,
    group_dimension_label: str,
) -> dict[str, Any]:
    """Compare one grouping dimension (e.g. ``'page_id'``) between two Milestone-A ``groups`` payloads.

    Structure is::

        grouped['page_id']['["p1"]'] -> {'row_count', 'dataset', 'columns'}
    """
    ta = grouped_a.get(group_dimension_label)
    tb = grouped_b.get(group_dimension_label)
    if ta is None or tb is None:
        return {"present_in_first": ta is not None, "present_in_second": tb is not None}

    if not isinstance(ta, Mapping) or not isinstance(tb, Mapping):
        return {"error": "expected mapping of subgroup-key -> metrics"}

    keys_a = set(str(k) for k in ta.keys())
    keys_b = set(str(k) for k in tb.keys())
    shared = sorted(keys_a & keys_b)
    out: dict[str, Any] = {
        "subgroup_keys_only_in_first": sorted(keys_a - keys_b),
        "subgroup_keys_only_in_second": sorted(keys_b - keys_a),
        "paired_by_subgroup": {},
    }
    for k in shared:
        ga = ta[k]  # type: ignore[index]
        gb = tb[k]  # type: ignore[index]
        out["paired_by_subgroup"][k] = compare_shared_payloads(
            {"dataset": ga["dataset"], "columns": ga["columns"]},
            {"dataset": gb["dataset"], "columns": gb["columns"]},
        )
    return out


def compare_all_group_dimensions(
    grouped_a: Mapping[str, Any],
    grouped_b: Mapping[str, Any],
    labels: list[str],
) -> dict[str, Any]:
    return {label: compare_group_dimension(grouped_a, grouped_b, group_dimension_label=label) for label in labels}

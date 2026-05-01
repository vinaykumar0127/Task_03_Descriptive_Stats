"""Resolve Task 02 package on sys.path regardless of cwd."""

from __future__ import annotations

import sys
from pathlib import Path


def repo_root_from_here(here: Path) -> Path:
    """`here` should be ``__file__`` resolved."""
    # Task_03_Descriptive_Stats/election_analysis_b/*.py → parents[2] = repo root
    return Path(here).resolve().parents[2]


def ensure_task02_on_path() -> Path:
    """Insert ``Task_02_Descriptive_Stats`` so ``election_analysis`` imports work."""
    root = repo_root_from_here(Path(__file__))
    task02 = root / "Task_02_Descriptive_Stats"
    tp = str(task02)
    if tp not in sys.path:
        sys.path.insert(0, tp)
    return task02

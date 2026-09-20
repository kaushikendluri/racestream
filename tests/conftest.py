"""Shared test configuration.

Each service directory is put on the path so the test suite can import any of
them. The packages are named ``racestream_<service>`` rather than ``app``
precisely so that this works: four services each exporting a top-level ``app``
would shadow one another on a shared path, and only the first would be
importable.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

for service in ("ingestion", "api", "processor", "replay"):
    path = REPO_ROOT / "services" / service
    if path.is_dir() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

"""
conftest.py — pytest configuration for RakuFlow test suite.

Adds project source directories to sys.path so test modules can import
ingestion and processing modules without installing as packages.
"""

from __future__ import annotations

import sys
from pathlib import Path

# ── Project root (one level up from tests/) ───────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent

# ── Add source directories to sys.path ────────────────────────────────────────
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))
sys.path.insert(0, str(PROJECT_ROOT / "processing"))
sys.path.insert(0, str(PROJECT_ROOT / "quality"))

# streamlit_app.py — Streamlit Cloud entry point for RakuFlow Analytics Dashboard.
#
# Streamlit Community Cloud looks for the main app file in the repo root.
# This file imports and runs the dashboard from the dashboard/ subdirectory.

import sys
from pathlib import Path

# Make dashboard/ importable
sys.path.insert(0, str(Path(__file__).parent / "dashboard"))

# Import and execute dashboard (set_page_config called inside app.py)
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "dashboard_app",
    Path(__file__).parent / "dashboard" / "app.py",
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["dashboard_app"] = _mod
_spec.loader.exec_module(_mod)
_mod.main()


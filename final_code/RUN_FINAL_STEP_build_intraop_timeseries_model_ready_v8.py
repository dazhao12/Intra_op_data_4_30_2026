#!/usr/bin/env python3
"""Convenience entrypoint for the final model-ready build script.

Delegates execution to:
  final_code/06_final_model_ready/build_intraop_timeseries_model_ready_v8.py
"""
from pathlib import Path
import runpy

SCRIPT = Path(__file__).resolve().parent / '06_final_model_ready' / 'build_intraop_timeseries_model_ready_v8.py'
runpy.run_path(str(SCRIPT), run_name='__main__')

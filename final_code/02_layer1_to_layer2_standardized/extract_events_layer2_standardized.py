"""
MOVER 手术事件标准化脚本 - Layer 2 (Events Aligned)
==================================================
1. 对齐手术事件到 1 分钟网格。
2. 产出：intraop_events_layer2_standardized.parquet
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
OUTPUT_FILE = BASE_DIR / "intraop_events_layer2_standardized.parquet"
INPUT_CANDIDATES = [
    BASE_DIR / "intraop_events_checklist_layer1.parquet",
    BASE_DIR / "archive_data/layer1/intraop_events_checklist_layer1.parquet",
    BASE_DIR / "intraop_procedure_events_layer1_raw.parquet",
    BASE_DIR / "archive_data/layer1/intraop_procedure_events_layer1_raw.parquet",
]

def resolve_input_file(candidates):
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Layer1 input not found in: {candidates}")

def standardize_events():
    input_file = resolve_input_file(INPUT_CANDIDATES)
    print(f"Loading Events data: {input_file}")
    df = pd.read_parquet(input_file)

    # 兼容两种 layer1 结构：
    # 1) procedure events raw: EVENT_TIME + EVENT_DISPLAY_NAME
    # 2) events_checklist flowsheet: RECORDED_TIME + FLO_MEAS_NAME (+ MEAS_VALUE)
    if {"EVENT_TIME", "EVENT_DISPLAY_NAME"}.issubset(df.columns):
        df["RECORDED_TIME"] = pd.to_datetime(df["EVENT_TIME"], errors="coerce").dt.floor("1min")
        df_out = df[["LOG_ID", "RECORDED_TIME", "EVENT_DISPLAY_NAME"]].copy()
        df_out.columns = ["LOG_ID", "RECORDED_TIME", "EVENT_NAME"]
    elif {"RECORDED_TIME", "FLO_MEAS_NAME"}.issubset(df.columns):
        df["RECORDED_TIME"] = pd.to_datetime(df["RECORDED_TIME"], errors="coerce").dt.floor("1min")
        meas_value = df["MEAS_VALUE"].astype(str).str.strip() if "MEAS_VALUE" in df.columns else ""
        value_missing = meas_value.isin(["", "nan", "None"]) if hasattr(meas_value, "isin") else True
        event_name = np.where(
            value_missing,
            df["FLO_MEAS_NAME"].astype(str).str.strip(),
            df["FLO_MEAS_NAME"].astype(str).str.strip() + "::" + meas_value,
        )
        df_out = pd.DataFrame(
            {
                "LOG_ID": df["LOG_ID"],
                "RECORDED_TIME": df["RECORDED_TIME"],
                "EVENT_NAME": event_name,
            }
        )
    else:
        raise ValueError(f"Unsupported events schema in {input_file}")

    df_out = df_out.dropna(subset=["LOG_ID", "RECORDED_TIME", "EVENT_NAME"])
    df_out = df_out.drop_duplicates()
    
    df_out.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved: {OUTPUT_FILE.name} ({len(df_out):,} events)")

if __name__ == "__main__":
    standardize_events()

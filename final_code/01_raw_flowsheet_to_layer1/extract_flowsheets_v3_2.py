"""
MOVER 术中 Flowsheet 全量提取 - V3.2 (终极稳定版)
1. 强制使用 Python 引擎读取 CSV，防止 C 引擎在处理脏数据时 Segmentation Fault
2. 对日期解析增加 try-except 保护
"""
import os
import sys
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import time
from datetime import datetime

FLOWSHEET_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/raw/srv/EPIC_flowsheets")
COHORT_FILE   = Path("/N/project/analgesia_perioperation/data/MOVER/processed/final_single_mrn_single_login_with_definable_intraop_time.csv")
OUTPUT_DIR    = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
PARTS_DIR     = OUTPUT_DIR / "parts_v3"
PARTS_DIR.mkdir(parents=True, exist_ok=True)

MODULE_MAPPING = {
    "vitals": ["Devices Testing Template", "Anesthesia Monitoring", "Invasive", "Non-Invasive", "Vitals", "Vital Signs", "BP/Pulse", "Quick Vitals", "Trauma Arrival/Mode/Vitals", "Code Vital Signs", "Code Vitals"],
    "respiratory": ["Anesthesia Agents", "Respiratory", "General Respiratory", "O2 Device/Airway", "Extubation Assessment", "Resp Review", "NICU RT", "General Resp"],
    "io": ["OR Intake and Output", "Intake/Output", "Intake/Output - UCI", "Renal/GU", "OB Intake/Output", "Intake", "Vital Signs/Intake"],
    "neuro": ["Neuro", "Neuro Assess", "Neuro - ICU", "BH Daily Observations"],
    "labs_poct": ["Glycemic Control - POCT", "POCT"],
    "events_checklist": ["Anesthesia Checklist", "Verification", "Pre Proc Verification and Time Out", "Debriefing", "Pre Procedure Documentation", "Patient Position", "Complication/Disposition"]
}

def get_module(flo_name):
    if pd.isna(flo_name): return "other"
    name = str(flo_name).strip()
    for mod, keys in MODULE_MAPPING.items():
        if name in keys: return mod
    return "other"

def safe_to_datetime(series):
    """最稳健的日期转换：如果向量化转换崩了，就回退到循环转换"""
    try:
        return pd.to_datetime(series, errors='coerce')
    except:
        return series.apply(lambda x: pd.to_datetime(x, errors='coerce'))

def load_cohort():
    df = pd.read_csv(COHORT_FILE, low_memory=False)
    df["win_start"] = pd.to_datetime(df["anesthesia_start_time"], errors="coerce")
    df["win_end"]   = pd.to_datetime(df["anesthesia_stop_time"],  errors="coerce")
    df["win_start"] = df["win_start"].fillna(pd.to_datetime(df["or_in_time"], errors="coerce"))
    df["win_end"]   = df["win_end"].fillna(pd.to_datetime(df["or_out_time"], errors="coerce"))
    df = df[df["win_start"].notna() & df["win_end"].notna()].copy()
    return df.set_index("LOG_ID")[["win_start", "win_end"]].to_dict("index")

def process_one_file(filepath, cohort_dict):
    fname = filepath.name
    print(f"\n[Processing V3.2] {fname}")
    t0 = time.time()
    chunk_size = 100_000 
    total_rows = kept_rows = 0
    sub_idx = 0

    # 这里的改进：engine='python' 慢但稳，绝不崩
    reader = pd.read_csv(filepath, chunksize=chunk_size, on_bad_lines='skip', engine='python')

    for i, chunk in enumerate(reader):
        try:
            total_rows += len(chunk)
            if "LOG_ID" not in chunk.columns: break
            chunk = chunk[chunk["LOG_ID"].isin(cohort_dict)].copy()
            if chunk.empty: continue

            time_col = "RECORDED_TIME"
            if time_col not in chunk.columns:
                for alt in ["recorded_time", "AN_START_DATETIME"]:
                    if alt in chunk.columns: time_col = alt; break
            
            # 使用最稳健的日期解析
            chunk["_dt"] = safe_to_datetime(chunk[time_col])
            chunk = chunk[chunk["_dt"].notna()].copy()
            if chunk.empty: continue
            
            chunk["_ws"] = chunk["LOG_ID"].map(lambda x: cohort_dict[x]["win_start"])
            chunk["_we"] = chunk["LOG_ID"].map(lambda x: cohort_dict[x]["win_end"])
            in_win = (chunk["_dt"] >= chunk["_ws"]) & (chunk["_dt"] <= chunk["_we"])
            chunk = chunk[in_win].copy()
            if chunk.empty: continue

            chunk["_mod"] = chunk["FLO_NAME"].apply(get_module)
            for mod, grp in chunk.groupby("_mod"):
                if grp.empty: continue
                out_name = f"{filepath.stem}__{mod}__part{sub_idx}.parquet"
                grp.drop(columns=["_dt", "_ws", "_we", "_mod"], errors='ignore').to_parquet(PARTS_DIR / out_name, index=False)
                kept_rows += len(grp)
            
            sub_idx += 1
            if (i + 1) % 50 == 0:
                print(f"  chunk {i+1}: scanned={total_rows:,} kept={kept_rows:,} elapsed={time.time()-t0:.0f}s")

        except Exception as e:
            print(f"  CRITICAL chunk {i}: {e}"); continue

    print(f"  Done V3.2: {fname} kept={kept_rows:,}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_index", type=str, required=True)
    args = parser.parse_args()
    flowsheet_files = sorted(FLOWSHEET_DIR.glob("*.csv"))
    filepath = flowsheet_files[int(args.file_index)]
    process_one_file(filepath, load_cohort())

if __name__ == "__main__": main()

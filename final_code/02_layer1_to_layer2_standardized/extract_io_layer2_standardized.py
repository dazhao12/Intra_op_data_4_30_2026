"""
MOVER IO 全量可用提取脚本 - Layer 2
===================================
策略：保留全部可解析数值，按 1 分钟求和聚合（IO 事件通常是加和）。
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
OUTPUT_FILE = BASE_DIR / "intraop_io_layer2_standardized.parquet"
INPUT_CANDIDATES = [
    BASE_DIR / "intraop_io_layer1.parquet",
    BASE_DIR / "archive_data/layer1/intraop_io_layer1.parquet",
]

ALIAS_RULES = [
    ("MAINTENANCE FLUID", "Intake_Fluid"),
    ("ADDITIONAL INTAKE", "Intake_Fluid"),
    ("URINE OUTPUT", "Urine_Output"),
    ("URINARY DRAIN", "Urine_Output"),
    ("BLOOD LOSS", "EBL"),
    ("PRBC", "Blood_Products"),
    ("FFP", "Blood_Products"),
    ("PLATELETS", "Blood_Products"),
]


def resolve_input_file(candidates):
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Layer1 input not found in: {candidates}")


def normalize_var_name(name, prefix="IO"):
    raw = str(name).strip().upper()
    raw = re.sub(r"[^A-Z0-9]+", "_", raw).strip("_")
    if not raw:
        raw = "UNKNOWN"
    if raw[0].isdigit():
        raw = f"N_{raw}"
    return f"{prefix}_{raw}"


def alias_io_name(name):
    n = str(name).upper()
    for pattern, target in ALIAS_RULES:
        if pattern in n:
            return target
    return normalize_var_name(name, prefix="IO")


def standardize_io():
    input_file = resolve_input_file(INPUT_CANDIDATES)
    print(f"Loading IO data: {input_file}")
    df = pd.read_parquet(
        input_file,
        columns=["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME", "MEAS_VALUE"],
    )
    df = df.dropna(subset=["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME"]).copy()
    df["RECORDED_TIME"] = pd.to_datetime(df["RECORDED_TIME"], errors="coerce")
    df = df[df["RECORDED_TIME"].notna()].copy()

    vals = pd.to_numeric(df["MEAS_VALUE"], errors="coerce")
    df = df[vals.notna()].copy()
    df["MEAS_VALUE"] = vals[vals.notna()].astype(float)
    df = df[np.isfinite(df["MEAS_VALUE"])].copy()

    df["IO_NAME"] = df["FLO_MEAS_NAME"].apply(alias_io_name)
    df["RECORDED_TIME"] = df["RECORDED_TIME"].dt.floor("1min")

    print("Aligning to 1-minute grid (Sum Aggregation)...")
    df_aligned = (
        df.groupby(["LOG_ID", "RECORDED_TIME", "IO_NAME"], as_index=False)["MEAS_VALUE"]
        .sum()
        .rename(columns={"MEAS_VALUE": "IO_VALUE"})
    )
    df_aligned.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved: {OUTPUT_FILE.name} ({len(df_aligned):,} rows)")


if __name__ == "__main__":
    standardize_io()

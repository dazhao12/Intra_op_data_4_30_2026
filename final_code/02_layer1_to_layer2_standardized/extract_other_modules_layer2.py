"""
MOVER 呼吸/神经全量可用提取脚本 - Layer 2
=======================================
策略：保留全部可解析数值，默认不做范围硬筛，按 1 分钟中位数聚合。
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")

RESP_ALIAS = {
    "UC ANE R AGENTS SEVOFLURANE": "Sevoflurane_agent",
    "UC ANE R AGENTS DESFLURANE": "Desflurane_agent",
    "UC ANE R AGENTS ISOFLURANE": "Isoflurane_agent",
    "UC ANE R VENT PEEP": "PEEP",
    "UC ANE R VENT TIDAL VOLUME OBSERVED": "Tidal_Volume",
    "UC ANE R RESPIRATIONS": "Resp_Rate",
    "ETCO2": "EtCO2",
}

NEURO_ALIAS = {
    "UC ANE R BIS": "BIS",
}


def normalize_var_name(name, prefix):
    raw = str(name).strip().upper()
    raw = re.sub(r"[^A-Z0-9]+", "_", raw).strip("_")
    if not raw:
        raw = "UNKNOWN"
    if raw[0].isdigit():
        raw = f"N_{raw}"
    return f"{prefix}_{raw}"


def resolve_layer1_file(module_name):
    candidates = [
        BASE_DIR / f"intraop_{module_name}_layer1.parquet",
        BASE_DIR / f"archive_data/layer1/intraop_{module_name}_layer1.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def get_alias(module_name, flo_meas_name):
    name = str(flo_meas_name).strip().upper()
    if module_name == "respiratory":
        if name in RESP_ALIAS:
            return RESP_ALIAS[name]
        return normalize_var_name(name, prefix="RESP")
    if module_name == "neuro":
        if name in NEURO_ALIAS:
            return NEURO_ALIAS[name]
        return normalize_var_name(name, prefix="NEURO")
    return normalize_var_name(name, prefix=module_name.upper())


def standardize_module(module_name):
    input_file = resolve_layer1_file(module_name)
    output_file = BASE_DIR / f"intraop_{module_name}_layer2_standardized.parquet"

    if input_file is None:
        print(f"Skipping {module_name}: Input file not found.")
        return

    print(f"Processing {module_name}: {input_file}")
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

    df["VITAL_NAME"] = df["FLO_MEAS_NAME"].apply(lambda x: get_alias(module_name, x))
    df["RECORDED_TIME"] = df["RECORDED_TIME"].dt.floor("1min")

    df_aligned = (
        df.groupby(["LOG_ID", "RECORDED_TIME", "VITAL_NAME"], as_index=False)["MEAS_VALUE"]
        .median()
        .rename(columns={"MEAS_VALUE": "VITAL_VALUE"})
    )
    df_aligned.to_parquet(output_file, index=False)
    print(f"  Saved {output_file.name}: {len(df_aligned):,} rows")


if __name__ == "__main__":
    standardize_module("respiratory")
    standardize_module("neuro")

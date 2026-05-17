"""
MOVER 术中 Vitals 全量可用提取脚本 - Layer 2 (Wide-friendly)
==============================================================
策略：
1. 优先保留所有可解析数值型监测值（不做临床范围硬筛）。
2. 同时解析血压字符串，补充 SBP/DBP/MAP（IBP/NBP/BP）。
3. 对齐到 1 分钟网格，按中位数聚合。
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
OUTPUT_FILE = BASE_DIR / "intraop_vitals_layer2_standardized.parquet"
INPUT_CANDIDATES = [
    BASE_DIR / "intraop_vitals_layer1.parquet",
    BASE_DIR / "archive_data/layer1/intraop_vitals_layer1.parquet",
]

# 保留历史常用命名，未命中的变量走自动标准化名称。
ALIAS_MAP = {
    "UC ANE HEART RATE": "HR_combined",
    "PULSE": "HR_combined",
    "R HEART RATE": "HR_combined",
    "UC ANE R PULSE OXIMETRY": "SpO2_cleaned",
    "PULSE OXIMETRY": "SpO2_cleaned",
    "RESPIRATIONS": "RR",
    "UC ANE R RESPIRATION": "RR",
    "UC ANE R VENT ETCO2": "EtCO2",
    "UCI ANE R ETCO2RR": "EtCO2",
    "ETCO2": "EtCO2",
    "UC ANE R TEMPERATURE": "Temp_C",
    "TEMPERATURE": "Temp_C",
    "UC ANE R BIS": "BIS",
    "UC ANE R CARDIAC INDEX": "CI",
    "UC ANE R CARDIAC OUTPUT": "CO",
    "UC ANE R SV": "SV",
    "UC ANE R SVR": "SVR",
    "UC ANE R FIO2": "FiO2",
}

BP_REGEX = re.compile(r"(\d+)/(\d+)(?:\s*\((\d+)\))?")


def resolve_input_file(candidates):
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Layer1 input not found in: {candidates}")


def normalize_var_name(name, prefix="VITAL"):
    raw = str(name).strip().upper()
    raw = re.sub(r"[^A-Z0-9]+", "_", raw).strip("_")
    if not raw:
        raw = "UNKNOWN"
    if raw[0].isdigit():
        raw = f"N_{raw}"
    return f"{prefix}_{raw}"


def parse_bp_string(val):
    if pd.isna(val):
        return None, None, None
    s = str(val)
    m = BP_REGEX.search(s)
    if not m:
        return None, None, None
    sbp = float(m.group(1))
    dbp = float(m.group(2))
    map_val = float(m.group(3)) if m.group(3) else (sbp + 2 * dbp) / 3
    return sbp, dbp, map_val


def classify_bp_prefix(flo_meas_name):
    n = str(flo_meas_name).upper()
    if "ARTERIAL LINE" in n or "A-LINE" in n or "ARTERIAL" in n:
        return "IBP"
    if "BLOOD PRESSURE" in n or "CUFF" in n or "MAP" in n:
        return "NBP"
    return "BP"


def standardize_and_align():
    input_file = resolve_input_file(INPUT_CANDIDATES)
    print(f"Loading Layer 1 vitals: {input_file}")
    df = pd.read_parquet(
        input_file,
        columns=["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME", "MEAS_VALUE"],
    )

    df = df.dropna(subset=["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME"]).copy()
    df["RECORDED_TIME"] = pd.to_datetime(df["RECORDED_TIME"], errors="coerce")
    df = df[df["RECORDED_TIME"].notna()].copy()
    df["FLO_MEAS_NAME"] = df["FLO_MEAS_NAME"].astype(str).str.strip()

    # A. 全量数值提取（不过滤临床范围）
    num_vals = pd.to_numeric(df["MEAS_VALUE"], errors="coerce")
    df_num = df[num_vals.notna()].copy()
    df_num["MEAS_VALUE"] = num_vals[num_vals.notna()].astype(float)
    df_num["VITAL_NAME"] = df_num["FLO_MEAS_NAME"].map(ALIAS_MAP).fillna(
        df_num["FLO_MEAS_NAME"].apply(lambda x: normalize_var_name(x, prefix="VITAL"))
    )
    out_numeric = df_num[["LOG_ID", "RECORDED_TIME", "VITAL_NAME", "MEAS_VALUE"]]
    print(f"Numeric rows kept: {len(out_numeric):,}")

    # B. 血压字符串解析，补充 SBP/DBP/MAP（不替代数值提取）
    bp_src = df[df["MEAS_VALUE"].astype(str).str.contains("/", na=False)].copy()
    parsed = bp_src["MEAS_VALUE"].apply(parse_bp_string)
    bp_prefix = bp_src["FLO_MEAS_NAME"].apply(classify_bp_prefix)

    bp_frames = []
    for i, suffix in enumerate(["SBP", "DBP", "MAP"]):
        sub = bp_src.copy()
        sub["MEAS_VALUE"] = parsed.apply(lambda x: x[i] if x else np.nan).astype(float)
        sub = sub[sub["MEAS_VALUE"].notna()].copy()
        if sub.empty:
            continue
        sub["VITAL_NAME"] = bp_prefix.loc[sub.index] + "_" + suffix
        bp_frames.append(sub[["LOG_ID", "RECORDED_TIME", "VITAL_NAME", "MEAS_VALUE"]])

    out_bp = (
        pd.concat(bp_frames, ignore_index=True)
        if bp_frames
        else pd.DataFrame(columns=["LOG_ID", "RECORDED_TIME", "VITAL_NAME", "MEAS_VALUE"])
    )
    print(f"BP parsed rows added: {len(out_bp):,}")

    df_std = pd.concat([out_numeric, out_bp], ignore_index=True)
    df_std = df_std[np.isfinite(df_std["MEAS_VALUE"])].copy()
    df_std["RECORDED_TIME"] = df_std["RECORDED_TIME"].dt.floor("1min")

    print("Aligning to 1-minute grid (Median Aggregation)...")
    df_aligned = (
        df_std.groupby(["LOG_ID", "RECORDED_TIME", "VITAL_NAME"], as_index=False)["MEAS_VALUE"]
        .median()
        .rename(columns={"MEAS_VALUE": "VITAL_VALUE"})
    )

    print(f"Saving: {OUTPUT_FILE.name} ({len(df_aligned):,} rows)")
    df_aligned.to_parquet(OUTPUT_FILE, index=False)
    print("ALL DONE!")


if __name__ == "__main__":
    standardize_and_align()

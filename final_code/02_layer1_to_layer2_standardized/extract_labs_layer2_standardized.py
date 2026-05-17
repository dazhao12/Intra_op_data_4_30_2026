"""
MOVER Labs 标准化提取脚本 - Layer 2
==================================
策略：
1) 仅保留可解析数值化验
2) 使用严格别名匹配，避免 PH 等宽松子串误归类
3) 先剔除明显哨兵值，再按 1 分钟中位数聚合
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
OUTPUT_FILE = BASE_DIR / "intraop_labs_layer2_standardized.parquet"
INPUT_CANDIDATES = [
    BASE_DIR / "intraop_labs_layer1_raw.parquet",
    BASE_DIR / "archive_data/layer1/intraop_labs_layer1_raw.parquet",
]

ALIAS_RULES = [
    (r"^\s*(LAB[_\s]+)?PH\s*$", "pH"),
    (r"^\s*(LAB[_\s]+)?PCO2\s*$", "pCO2"),
    (r"^\s*(LAB[_\s]+)?PO2\s*$", "pO2"),
    (r"^\s*(LAB[_\s]+)?LACTATE\s*$", "Lactate"),
    (r"^\s*(LAB[_\s]+)?HEMOGLOBIN\s*$", "Hemoglobin"),
    (r"^\s*(LAB[_\s]+)?GLUCOSE\s*$", "Glucose"),
    (r"^\s*(LAB[_\s]+)?POTASSIUM\s*$", "Potassium"),
    (r"^\s*(LAB[_\s]+)?SODIUM\s*$", "Sodium"),
    (r"^\s*(LAB[_\s]+)?CALCIUM[\s,]+IONIZED\s*$", "Ionized calcium"),
]

SENTINEL_VALUES = {9999999, 999999, 99999, 9999, -9999999, -999999, -99999, -9999}

STANDARD_NAME_MAP = {
    "ph": "pH",
    "pco2": "pCO2",
    "po2": "pO2",
    "lactate": "Lactate",
    "hemoglobin": "Hemoglobin",
    "glucose": "Glucose",
    "potassium": "Potassium",
    "sodium": "Sodium",
    "calcium ionized": "Ionized calcium",
    "bicarbonate": "Bicarbonate",
    "oxygen": "Oxygen",
    "oxygen saturation": "Oxygen saturation",
    "hematocrit": "Hematocrit",
    "platelets": "Platelets",
    "carbon dioxide": "Carbon dioxide",
    "creatinine": "Creatinine",
    "chloride": "Chloride",
    "urea nitrogen": "Urea nitrogen",
    "base excess standard": "Base excess",
}

ACRONYM_MAP = {
    "abo": "ABO",
    "rh": "Rh",
    "dna": "DNA",
    "rna": "RNA",
    "igg": "IgG",
    "igm": "IgM",
    "iv": "IV",
}

def prettify_lab_name(name: str) -> str:
    s = str(name).strip()
    if s.upper().startswith("LAB_"):
        s = s[4:]
    elif s.lower().startswith("lab_"):
        s = s[4:]
    s = s.replace("_", " ").strip().lower()
    if not s:
        return "Unknown"
    if s in STANDARD_NAME_MAP:
        return STANDARD_NAME_MAP[s]

    parts = []
    for tok in s.split():
        if tok in ACRONYM_MAP:
            parts.append(ACRONYM_MAP[tok])
        else:
            parts.append(tok.capitalize())
    return " ".join(parts)


def resolve_input_file(candidates):
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Layer1 input not found in: {candidates}")


def normalize_var_name(name, prefix="LAB"):
    raw = str(name).strip().upper()
    raw = re.sub(r"[^A-Z0-9]+", "_", raw).strip("_")
    if not raw:
        raw = "UNKNOWN"
    if raw[0].isdigit():
        raw = f"N_{raw}"
    return f"{prefix}_{raw}"


def alias_lab_name(name):
    n = str(name).strip().upper()
    for pattern, target in ALIAS_RULES:
        if re.match(pattern, n):
            return prettify_lab_name(target)
    return prettify_lab_name(normalize_var_name(name, prefix="LAB"))


def standardize_labs():
    input_file = resolve_input_file(INPUT_CANDIDATES)
    print(f"Loading Labs data: {input_file}")
    df = pd.read_parquet(
        input_file,
        columns=["LOG_ID", "Collection Datetime", "Lab Name", "Observation Value"],
    )
    df = df.dropna(subset=["LOG_ID", "Collection Datetime", "Lab Name"]).copy()
    df["RECORDED_TIME"] = pd.to_datetime(df["Collection Datetime"], errors="coerce")
    df = df[df["RECORDED_TIME"].notna()].copy()

    vals = pd.to_numeric(df["Observation Value"], errors="coerce")
    df = df[vals.notna()].copy()
    df["MEAS_VALUE"] = vals[vals.notna()].astype(float)
    df = df[np.isfinite(df["MEAS_VALUE"])].copy()
    df = df[~df["MEAS_VALUE"].isin(SENTINEL_VALUES)].copy()
    df = df[df["MEAS_VALUE"].abs() < 1e6].copy()

    df["LAB_NAME"] = df["Lab Name"].apply(alias_lab_name)
    df["RECORDED_TIME"] = df["RECORDED_TIME"].dt.floor("1min")

    print("Aligning to 1-minute grid (Median)...")
    df_aligned = (
        df.groupby(["LOG_ID", "RECORDED_TIME", "LAB_NAME"], as_index=False)["MEAS_VALUE"]
        .median()
        .rename(columns={"MEAS_VALUE": "LAB_VALUE"})
    )

    df_aligned.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved: {OUTPUT_FILE.name} ({len(df_aligned):,} rows)")


if __name__ == "__main__":
    standardize_labs()

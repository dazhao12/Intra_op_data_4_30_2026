#!/usr/bin/env python3
"""
Generate variable dictionaries for intraop long tables with distribution and missingness.

Outputs:
- variable_dictionary_numeric.csv
- variable_dictionary_events.csv
- variable_dictionary_summary.md
"""

import math
import random
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
COHORT_FILE = Path(
    "/N/project/analgesia_perioperation/data/MOVER/processed/final_single_mrn_single_login_with_definable_intraop_time.csv"
)
OUT_DIR = BASE_DIR / "variable_dictionary"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 500_000
SAMPLE_SIZE = 20_000

NUMERIC_TABLES = [
    ("vitals", BASE_DIR / "intraop_vitals_layer2_standardized.parquet", "VITAL_NAME", "VITAL_VALUE"),
    ("respiratory", BASE_DIR / "intraop_respiratory_layer2_standardized.parquet", "VITAL_NAME", "VITAL_VALUE"),
    ("neuro", BASE_DIR / "intraop_neuro_layer2_standardized.parquet", "VITAL_NAME", "VITAL_VALUE"),
    ("io", BASE_DIR / "intraop_io_layer2_standardized.parquet", "IO_NAME", "IO_VALUE"),
    ("labs", BASE_DIR / "intraop_labs_layer2_standardized.parquet", "LAB_NAME", "LAB_VALUE"),
]

EVENT_TABLE = ("events", BASE_DIR / "intraop_events_layer2_standardized.parquet", "EVENT_NAME")

EXACT_ZH = {
    "HR_combined": "心率",
    "SpO2_cleaned": "血氧饱和度",
    "RR": "呼吸频率",
    "EtCO2": "呼气末二氧化碳",
    "Temp_C": "体温(摄氏度)",
    "BIS": "脑电双频指数(BIS)",
    "CI": "心脏指数",
    "CO": "心输出量",
    "SV": "每搏输出量",
    "SVR": "外周血管阻力",
    "FiO2": "吸入氧浓度",
    "IBP_SBP": "有创血压收缩压",
    "IBP_DBP": "有创血压舒张压",
    "IBP_MAP": "有创平均动脉压",
    "NBP_SBP": "无创血压收缩压",
    "NBP_DBP": "无创血压舒张压",
    "NBP_MAP": "无创平均动脉压",
    "BP_SBP": "血压收缩压",
    "BP_DBP": "血压舒张压",
    "BP_MAP": "平均动脉压",
    "Agent_Sevo": "七氟烷",
    "Agent_Des": "地氟烷",
    "Agent_Iso": "异氟烷",
    "Resp_Rate": "呼吸频率",
    "Tidal_Volume": "潮气量",
    "PEEP": "呼气末正压",
    "Intake_Fluid": "液体入量",
    "Urine_Output": "尿量",
    "EBL": "估计失血量",
    "Blood_Products": "血制品输入",
    "pH": "血气pH",
    "pCO2": "血气二氧化碳分压",
    "pO2": "血气氧分压",
    "Lactate": "乳酸",
    "Hemoglobin": "血红蛋白",
    "Glucose": "血糖",
    "Potassium": "血钾",
    "Sodium": "血钠",
    "Ionized calcium": "离子钙",
    "Bicarbonate": "碳酸氢根",
    "Oxygen": "氧分压/氧相关",
    "Oxygen saturation": "氧饱和度",
    "Hematocrit": "红细胞压积",
    "Platelets": "血小板",
}

TOKEN_ZH = {
    "START": "开始",
    "STOP": "结束",
    "END": "结束",
    "INTUBATION": "插管",
    "EXTUBATION": "拔管",
    "ANESTHESIA": "麻醉",
    "PATIENT": "患者",
    "POSITION": "体位",
    "POSITIONING": "体位摆放",
    "TABLE": "手术台",
    "HEAD": "头部",
    "NECK": "颈部",
    "EYES": "眼部",
    "ARMS": "上肢",
    "PRESSURE": "压力",
    "PADDING": "垫护",
    "CHECK": "检查",
    "CHECKS": "检查",
    "SKIN": "皮肤",
    "INCISION": "切开",
    "DELIVERY": "分娩",
    "COOLING": "降温",
    "WARMING": "复温",
    "BLOOD": "血液",
    "LOSS": "丢失",
    "URINE": "尿液",
    "OUTPUT": "输出",
    "FLUID": "液体",
    "GLUCOSE": "葡萄糖",
    "OXYGEN": "氧",
    "RESP": "呼吸",
    "RESPIRATIONS": "呼吸",
    "RATE": "频率",
    "VOLUME": "容量",
    "MAP": "平均动脉压",
    "SBP": "收缩压",
    "DBP": "舒张压",
    "ARTERIAL": "动脉",
    "LINE": "导管",
    "TEMPERATURE": "温度",
    "HEART": "心脏",
    "PAIN": "疼痛",
    "SCORE": "评分",
    "SEVOFLURANE": "七氟烷",
    "DESFLURANE": "地氟烷",
    "ISOFLURANE": "异氟烷",
    "SUPINE": "仰卧位",
    "LITHOTOMY": "截石位",
    "TRENDelenberg".upper(): "头低位",
    "PACU": "麻醉恢复室",
}


def load_cohort_n():
    df = pd.read_csv(COHORT_FILE, low_memory=False)
    ws = pd.to_datetime(df["anesthesia_start_time"], errors="coerce")
    we = pd.to_datetime(df["anesthesia_stop_time"], errors="coerce")
    ws = ws.fillna(pd.to_datetime(df["or_in_time"], errors="coerce"))
    we = we.fillna(pd.to_datetime(df["or_out_time"], errors="coerce"))
    return int(df[ws.notna() & we.notna()]["LOG_ID"].nunique())


def merge_sample(existing_sample, new_vals):
    if len(new_vals) == 0:
        return existing_sample

    if len(new_vals) > SAMPLE_SIZE:
        idx = np.random.choice(len(new_vals), SAMPLE_SIZE, replace=False)
        new_pick = new_vals[idx]
    else:
        new_pick = new_vals

    if not existing_sample:
        merged = new_pick
    else:
        merged = np.concatenate([np.array(existing_sample, dtype=float), new_pick])

    if len(merged) > SAMPLE_SIZE:
        idx = np.random.choice(len(merged), SAMPLE_SIZE, replace=False)
        merged = merged[idx]

    return merged.tolist()


def _translate_tokenized_text(text):
    if text is None:
        return ""
    s = str(text).replace("::", " :: ").replace("/", " / ").replace("-", " ").replace(";", " ; ")
    parts = s.split()
    zh_parts = []
    for p in parts:
        key = p.upper()
        zh_parts.append(TOKEN_ZH.get(key, p))
    out = " ".join(zh_parts)
    out = out.replace(" :: ", "：").replace(" ; ", "；").replace(" / ", "/")
    return out


def variable_to_zh(table_name, variable):
    v = str(variable)
    if v in EXACT_ZH:
        return EXACT_ZH[v]

    prefix_map = {
        "VITAL_": "生命体征",
        "RESP_": "呼吸监测",
        "LAB_": "检验",
        "IO_": "出入量",
        "NEURO_": "神经监测",
    }
    for pref, label in prefix_map.items():
        if v.startswith(pref):
            raw = v[len(pref):].replace("_", " ")
            return f"{label}:{_translate_tokenized_text(raw)}"

    if table_name == "events":
        return _translate_tokenized_text(v)

    return _translate_tokenized_text(v)


def add_zh_column(df, table_name):
    zh = df["variable"].apply(lambda x: variable_to_zh(table_name, x))
    insert_pos = df.columns.get_loc("variable") + 1
    df.insert(insert_pos, "variable_zh", zh)
    return df


def summarize_numeric_table(table_name, file_path, var_col, value_col, cohort_n):
    stats = defaultdict(
        lambda: {
            "obs_n": 0,
            "sum": 0.0,
            "sum_sq": 0.0,
            "min": math.inf,
            "max": -math.inf,
            "zero_n": 0,
            "neg_n": 0,
            "cases": set(),
            "time_min": None,
            "time_max": None,
            "sample": [],
        }
    )

    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(columns=["LOG_ID", "RECORDED_TIME", var_col, value_col], batch_size=BATCH_SIZE):
        df = batch.to_pandas()
        df = df.dropna(subset=["LOG_ID", "RECORDED_TIME", var_col, value_col])
        if df.empty:
            continue
        df[var_col] = df[var_col].astype(str)
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        df = df[np.isfinite(df[value_col])]
        if df.empty:
            continue

        for var_name, grp in df.groupby(var_col):
            s = stats[var_name]
            vals = grp[value_col].to_numpy(dtype=float)
            n = len(vals)
            s["obs_n"] += n
            s["sum"] += float(vals.sum())
            s["sum_sq"] += float(np.square(vals).sum())
            s["min"] = min(s["min"], float(vals.min()))
            s["max"] = max(s["max"], float(vals.max()))
            s["zero_n"] += int((vals == 0).sum())
            s["neg_n"] += int((vals < 0).sum())
            s["cases"].update(grp["LOG_ID"].astype(str).unique().tolist())

            t = pd.to_datetime(grp["RECORDED_TIME"], errors="coerce")
            tmin = t.min()
            tmax = t.max()
            if pd.notna(tmin):
                if s["time_min"] is None or tmin < s["time_min"]:
                    s["time_min"] = tmin
            if pd.notna(tmax):
                if s["time_max"] is None or tmax > s["time_max"]:
                    s["time_max"] = tmax

            s["sample"] = merge_sample(s["sample"], vals)

    rows = []
    for var_name, s in stats.items():
        n = s["obs_n"]
        mean = s["sum"] / n if n else np.nan
        var = (s["sum_sq"] - (s["sum"] ** 2) / n) / (n - 1) if n > 1 else np.nan
        std = math.sqrt(var) if (not np.isnan(var) and var >= 0) else np.nan
        samp = np.array(s["sample"], dtype=float) if s["sample"] else np.array([], dtype=float)
        q01 = float(np.quantile(samp, 0.01)) if len(samp) else np.nan
        q05 = float(np.quantile(samp, 0.05)) if len(samp) else np.nan
        q50 = float(np.quantile(samp, 0.50)) if len(samp) else np.nan
        q95 = float(np.quantile(samp, 0.95)) if len(samp) else np.nan
        q99 = float(np.quantile(samp, 0.99)) if len(samp) else np.nan
        case_n = len(s["cases"])
        case_cov = (case_n / cohort_n * 100.0) if cohort_n else np.nan
        case_miss = (100.0 - case_cov) if cohort_n else np.nan
        rows.append(
            {
                "table": table_name,
                "variable": var_name,
                "obs_n": n,
                "case_n": case_n,
                "case_coverage_pct": round(case_cov, 4),
                "case_missing_pct": round(case_miss, 4),
                "value_mean": mean,
                "value_std": std,
                "value_min": s["min"] if s["min"] != math.inf else np.nan,
                "value_p01_approx": q01,
                "value_p05_approx": q05,
                "value_p50_approx": q50,
                "value_p95_approx": q95,
                "value_p99_approx": q99,
                "value_max": s["max"] if s["max"] != -math.inf else np.nan,
                "zero_pct": round((s["zero_n"] / n * 100.0), 4) if n else np.nan,
                "neg_pct": round((s["neg_n"] / n * 100.0), 4) if n else np.nan,
                "time_min": s["time_min"],
                "time_max": s["time_max"],
                "quantile_method": f"reservoir_sample_{SAMPLE_SIZE}",
            }
        )
    return pd.DataFrame(rows)


def summarize_events_table(table_name, file_path, var_col, cohort_n):
    stats = defaultdict(
        lambda: {"obs_n": 0, "cases": set(), "time_min": None, "time_max": None}
    )
    pf = pq.ParquetFile(file_path)
    for batch in pf.iter_batches(columns=["LOG_ID", "RECORDED_TIME", var_col], batch_size=BATCH_SIZE):
        df = batch.to_pandas()
        df = df.dropna(subset=["LOG_ID", "RECORDED_TIME", var_col])
        if df.empty:
            continue
        df[var_col] = df[var_col].astype(str)
        t = pd.to_datetime(df["RECORDED_TIME"], errors="coerce")
        df = df[t.notna()].copy()
        if df.empty:
            continue
        df["RECORDED_TIME"] = pd.to_datetime(df["RECORDED_TIME"], errors="coerce")

        for var_name, grp in df.groupby(var_col):
            s = stats[var_name]
            s["obs_n"] += len(grp)
            s["cases"].update(grp["LOG_ID"].astype(str).unique().tolist())
            tmin = grp["RECORDED_TIME"].min()
            tmax = grp["RECORDED_TIME"].max()
            if s["time_min"] is None or tmin < s["time_min"]:
                s["time_min"] = tmin
            if s["time_max"] is None or tmax > s["time_max"]:
                s["time_max"] = tmax

    rows = []
    for var_name, s in stats.items():
        case_n = len(s["cases"])
        case_cov = (case_n / cohort_n * 100.0) if cohort_n else np.nan
        case_miss = (100.0 - case_cov) if cohort_n else np.nan
        rows.append(
            {
                "table": table_name,
                "variable": var_name,
                "obs_n": s["obs_n"],
                "case_n": case_n,
                "case_coverage_pct": round(case_cov, 4),
                "case_missing_pct": round(case_miss, 4),
                "time_min": s["time_min"],
                "time_max": s["time_max"],
            }
        )
    return pd.DataFrame(rows)


def write_summary_md(numeric_df, events_df, cohort_n):
    out_md = OUT_DIR / "variable_dictionary_summary.md"
    lines = []
    lines.append("# Intraop Variable Dictionary Summary")
    lines.append("")
    lines.append(f"- Cohort cases (with valid intraop window): {cohort_n}")
    lines.append(f"- Numeric variables total: {len(numeric_df):,}")
    lines.append(f"- Event variables total: {len(events_df):,}")
    lines.append("")
    lines.append("## Table-level counts")
    for table_name, grp in numeric_df.groupby("table"):
        lines.append(
            f"- {table_name}: {len(grp):,} variables, obs_n={int(grp['obs_n'].sum()):,}"
        )
    lines.append(
        f"- events: {len(events_df):,} variables, obs_n={int(events_df['obs_n'].sum()):,}"
    )
    lines.append("")
    lines.append("## Notes")
    lines.append("- Missingness is case-level missingness: 1 - (case_n / cohort_cases).")
    lines.append(
        f"- Quantiles in numeric dictionary are approximate from reservoir sampling (n={SAMPLE_SIZE})."
    )

    out_md.write_text("\n".join(lines))
    return out_md


def main():
    random.seed(20260512)
    cohort_n = load_cohort_n()
    print(f"Cohort cases: {cohort_n}")

    numeric_parts = []
    for table_name, file_path, var_col, value_col in NUMERIC_TABLES:
        print(f"Summarizing numeric table: {table_name} -> {file_path.name}")
        numeric_parts.append(
            summarize_numeric_table(table_name, file_path, var_col, value_col, cohort_n)
        )
    numeric_df = pd.concat(numeric_parts, ignore_index=True)
    numeric_df = add_zh_column(numeric_df, table_name="numeric")
    numeric_df = numeric_df.sort_values(["table", "obs_n"], ascending=[True, False])

    print(f"Summarizing event table: {EVENT_TABLE[0]} -> {EVENT_TABLE[1].name}")
    events_df = summarize_events_table(EVENT_TABLE[0], EVENT_TABLE[1], EVENT_TABLE[2], cohort_n)
    events_df = add_zh_column(events_df, table_name="events")
    events_df = events_df.sort_values(["obs_n"], ascending=[False])

    out_num = OUT_DIR / "variable_dictionary_numeric.csv"
    out_evt = OUT_DIR / "variable_dictionary_events.csv"
    numeric_df.to_csv(out_num, index=False)
    events_df.to_csv(out_evt, index=False)
    out_md = write_summary_md(numeric_df, events_df, cohort_n)

    print(f"Saved: {out_num}")
    print(f"Saved: {out_evt}")
    print(f"Saved: {out_md}")


if __name__ == "__main__":
    main()

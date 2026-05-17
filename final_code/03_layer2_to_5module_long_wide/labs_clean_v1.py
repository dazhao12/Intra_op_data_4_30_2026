#!/usr/bin/env python3
"""
labs_clean_v1:
1) keep clinically useful intraop lab variables
2) remove sentinel values and out-of-range outliers
3) export pre/post comparison and excluded-category frequencies
"""

from pathlib import Path
import numpy as np
import pandas as pd
import re

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
IN_FILE = BASE_DIR / "intraop_labs_layer2_standardized.parquet"
OUT_DIR = BASE_DIR / "labs_clean_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEEP_RULES = {
    "pH": (6.8, 7.8),
    "pCO2": (10, 150),
    "pO2": (20, 700),
    "Lactate": (0.1, 30),
    "Hemoglobin": (2, 25),
    "Glucose": (20, 1200),
    "Potassium": (1.5, 9.0),
    "Sodium": (100, 180),
    "Ionized calcium": (0.3, 3.0),
    "Bicarbonate": (5, 60),
    "Oxygen": (20, 700),
    "Oxygen saturation": (0, 100),
    "Hematocrit": (5, 70),
    "Platelets": (5, 2000),
}

SENTINEL_VALUES = {9999999, 999999, 99999, 9999, -9999999, -999999, -99999, -9999}

# 主时序层默认仅纳入这些术中关键实验室指标
MAIN_TIMESERIES_LABS = {
    "pH",
    "pCO2",
    "pO2",
    "Lactate",
    "Hemoglobin",
    "Glucose",
    "Potassium",
    "Sodium",
    "Ionized calcium",
    "Bicarbonate",
    "Oxygen",
    "Oxygen saturation",
}

EXCLUDE_CATEGORY_RULES = {
    "bloodbank_match": r"ABO[ _]?RH|BLOOD[ _]?GROUP|CROSSMATCH|ANTIBODY|COOMBS|BLOOD[ _]?PRODUCT",
    "microbiology": r"MICROORGANISM|FUNGUS|CULTURE|DNA|PATHOGEN",
    "method_or_specimen_source": r"METHOD|SPECIMEN[ _]?SOURCE|SPECIMEN[ _]?COLLECTION|PANEL|SOURCE",
}


def prettify_lab_name(name: str) -> str:
    s = str(name).strip()
    if s.upper().startswith("LAB_"):
        s = s[4:]
    elif s.startswith("Lab_"):
        s = s[4:]
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    if not s:
        return "Unknown"
    # clinical standard aliases
    if s == "ph":
        return "pH"
    if s == "pco2":
        return "pCO2"
    if s == "po2":
        return "pO2"
    if s == "hgb":
        return "Hemoglobin"
    if s == "na":
        return "Sodium"
    if s == "k":
        return "Potassium"
    if s in {"ca ion", "calcium ionized"}:
        return "Ionized calcium"
    return s[0].upper() + s[1:]


def category_of_lab(name: str) -> str:
    s = str(name).upper()
    for cat, pattern in EXCLUDE_CATEGORY_RULES.items():
        if pd.Series([s]).str.contains(pattern, regex=True, na=False).iloc[0]:
            return cat
    return "other"


def summarize_by_var(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    g = df.groupby("LAB_NAME", dropna=False)
    out = g.agg(
        **{
            f"{prefix}_obs_n": ("LOG_ID", "size"),
            f"{prefix}_case_n": ("LOG_ID", "nunique"),
            f"{prefix}_mean": ("LAB_VALUE", "mean"),
            f"{prefix}_p01": ("LAB_VALUE", lambda x: np.nan if len(x) == 0 else x.quantile(0.01)),
            f"{prefix}_p50": ("LAB_VALUE", lambda x: np.nan if len(x) == 0 else x.quantile(0.50)),
            f"{prefix}_p99": ("LAB_VALUE", lambda x: np.nan if len(x) == 0 else x.quantile(0.99)),
            f"{prefix}_max": ("LAB_VALUE", "max"),
        }
    ).reset_index()
    return out


def run_strict_v1(df: pd.DataFrame):
    keep_vars = set(KEEP_RULES.keys())
    keep_df = df[df["LAB_NAME"].isin(keep_vars)].copy()
    keep_df["is_sentinel"] = keep_df["LAB_VALUE"].isin(SENTINEL_VALUES) | (keep_df["LAB_VALUE"].abs() >= 1e6)
    keep_df = keep_df[~keep_df["is_sentinel"]].copy()

    keep_df["vmin"] = keep_df["LAB_NAME"].map(lambda x: KEEP_RULES[x][0])
    keep_df["vmax"] = keep_df["LAB_NAME"].map(lambda x: KEEP_RULES[x][1])
    keep_df["in_range"] = (keep_df["LAB_VALUE"] >= keep_df["vmin"]) & (keep_df["LAB_VALUE"] <= keep_df["vmax"])

    pre = summarize_by_var(keep_df, "pre")
    post_df = keep_df[keep_df["in_range"]].copy()
    post = summarize_by_var(post_df, "post")

    cmp_df = pre.merge(post, on="LAB_NAME", how="left")
    cmp_df["dropped_obs_n"] = cmp_df["pre_obs_n"] - cmp_df["post_obs_n"].fillna(0)
    cmp_df["drop_pct"] = np.where(
        cmp_df["pre_obs_n"] > 0, cmp_df["dropped_obs_n"] / cmp_df["pre_obs_n"] * 100.0, np.nan
    )
    cmp_df = cmp_df.sort_values("pre_obs_n", ascending=False)
    cmp_df.to_csv(OUT_DIR / "labs_clean_v1_prepost_stats.csv", index=False)

    clean_df = post_df[["LOG_ID", "RECORDED_TIME", "LAB_NAME", "LAB_VALUE"]].copy()
    clean_df.to_parquet(OUT_DIR / "intraop_labs_layer2_clean_v1.parquet", index=False)

    # 主时序层 labs（默认不包含全部 lab）
    main_ts_df = clean_df[clean_df["LAB_NAME"].isin(MAIN_TIMESERIES_LABS)].copy()
    main_ts_df.to_parquet(OUT_DIR / "intraop_labs_layer2_main_timeseries_candidates.parquet", index=False)

    pd.DataFrame(
        [{"LAB_NAME": k, "value_min": v[0], "value_max": v[1]} for k, v in KEEP_RULES.items()]
    ).to_csv(OUT_DIR / "labs_clean_v1_keep_variables_and_rules.csv", index=False)
    return keep_df, clean_df, main_ts_df


def run_broad_v1b(df: pd.DataFrame):
    # keep all non-excluded categories, then clean obvious anomalies
    broad = df[df["exclude_category"] == "other"].copy()
    broad["is_sentinel"] = broad["LAB_VALUE"].isin(SENTINEL_VALUES) | (broad["LAB_VALUE"].abs() >= 1e6)
    broad = broad[~broad["is_sentinel"]].copy()

    # robust per-variable range by quantiles
    q = (
        broad.groupby("LAB_NAME")["LAB_VALUE"]
        .quantile([0.001, 0.999])
        .unstack()
        .rename(columns={0.001: "q001", 0.999: "q999"})
        .reset_index()
    )
    n = broad.groupby("LAB_NAME").size().reset_index(name="obs_n")
    q = q.merge(n, on="LAB_NAME", how="left")
    broad = broad.merge(q, on="LAB_NAME", how="left")
    # if variable very sparse, skip quantile trimming
    broad["in_range"] = np.where(
        broad["obs_n"] >= 200,
        (broad["LAB_VALUE"] >= broad["q001"]) & (broad["LAB_VALUE"] <= broad["q999"]),
        True,
    )

    pre = summarize_by_var(broad, "pre")
    post_df = broad[broad["in_range"]].copy()
    post = summarize_by_var(post_df, "post")
    cmp_df = pre.merge(post, on="LAB_NAME", how="left")
    cmp_df["dropped_obs_n"] = cmp_df["pre_obs_n"] - cmp_df["post_obs_n"].fillna(0)
    cmp_df["drop_pct"] = np.where(
        cmp_df["pre_obs_n"] > 0, cmp_df["dropped_obs_n"] / cmp_df["pre_obs_n"] * 100.0, np.nan
    )
    cmp_df = cmp_df.sort_values("pre_obs_n", ascending=False)
    cmp_df.to_csv(OUT_DIR / "labs_clean_v1b_prepost_stats.csv", index=False)

    rules = q.sort_values("obs_n", ascending=False)
    rules.to_csv(OUT_DIR / "labs_clean_v1b_quantile_rules.csv", index=False)

    clean_df = post_df[["LOG_ID", "RECORDED_TIME", "LAB_NAME", "LAB_VALUE"]].copy()
    clean_df.to_parquet(OUT_DIR / "intraop_labs_layer2_clean_v1b_broad.parquet", index=False)
    return broad, clean_df


def main():
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input: {IN_FILE}")

    df = pd.read_parquet(IN_FILE)
    df = df.rename(columns={"LAB_NAME": "LAB_NAME", "LAB_VALUE": "LAB_VALUE"})
    df = df.dropna(subset=["LOG_ID", "RECORDED_TIME", "LAB_NAME", "LAB_VALUE"]).copy()
    df["LAB_NAME"] = df["LAB_NAME"].astype(str).map(prettify_lab_name)
    df["LAB_VALUE"] = pd.to_numeric(df["LAB_VALUE"], errors="coerce")
    df = df[np.isfinite(df["LAB_VALUE"])].copy()

    # frequency of excluded categories on full labs layer2
    df["exclude_category"] = df["LAB_NAME"].apply(category_of_lab)
    cat_freq = (
        df.groupby("exclude_category")
        .agg(obs_n=("LOG_ID", "size"), case_n=("LOG_ID", "nunique"))
        .reset_index()
        .sort_values("obs_n", ascending=False)
    )
    cat_freq.to_csv(OUT_DIR / "labs_excluded_category_frequency.csv", index=False)

    top_vars = (
        df.groupby(["exclude_category", "LAB_NAME"])
        .agg(obs_n=("LOG_ID", "size"), case_n=("LOG_ID", "nunique"))
        .reset_index()
        .sort_values(["exclude_category", "obs_n"], ascending=[True, False])
    )
    top_vars.to_csv(OUT_DIR / "labs_excluded_category_topvars.csv", index=False)

    keep_df, clean_df, main_ts_df = run_strict_v1(df)
    broad_pre, broad_clean = run_broad_v1b(df)

    summary_lines = [
        "# labs_clean_v1 summary",
        "",
        f"- input rows: {len(df):,}",
        f"- keep-list rows before range filter: {len(keep_df):,}",
        f"- output rows after range filter: {len(clean_df):,}",
        f"- output variables: {clean_df['LAB_NAME'].nunique():,}",
        f"- main-timeseries candidate rows: {len(main_ts_df):,}",
        f"- main-timeseries candidate variables: {main_ts_df['LAB_NAME'].nunique():,}",
        "",
        "# labs_clean_v1b (broad) summary",
        f"- broad pre rows (non-excluded categories): {len(broad_pre):,}",
        f"- broad output rows after quantile filter: {len(broad_clean):,}",
        f"- broad output variables: {broad_clean['LAB_NAME'].nunique():,}",
        "",
        "Outputs:",
        "- intraop_labs_layer2_clean_v1.parquet",
        "- intraop_labs_layer2_main_timeseries_candidates.parquet",
        "- intraop_labs_layer2_clean_v1b_broad.parquet",
        "- labs_clean_v1_keep_variables_and_rules.csv",
        "- labs_clean_v1_prepost_stats.csv",
        "- labs_clean_v1b_quantile_rules.csv",
        "- labs_clean_v1b_prepost_stats.csv",
        "- labs_excluded_category_frequency.csv",
        "- labs_excluded_category_topvars.csv",
    ]
    (OUT_DIR / "README_labs_clean_v1.md").write_text("\n".join(summary_lines))

    print(f"Saved clean labs to: {OUT_DIR}")


if __name__ == "__main__":
    main()

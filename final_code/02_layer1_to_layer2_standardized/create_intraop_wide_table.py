"""
MOVER 术中全量宽表组装脚本 - Layer 3
==================================
1. 合并 Vitals, Respiratory, Labs, IO, Events 所有的 1-min 对齐数据。
2. 产出：intraop_full_wide_layer2.parquet
3. 生成全量数据统计描述。
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
OUTPUT_FILE = BASE_DIR / "intraop_full_wide_layer2.parquet"

FILES = {
    "vitals": BASE_DIR / "intraop_vitals_layer2_standardized.parquet",
    "resp": BASE_DIR / "intraop_respiratory_layer2_standardized.parquet",
    "neuro": BASE_DIR / "intraop_neuro_layer2_standardized.parquet",
    "labs": BASE_DIR / "intraop_labs_layer2_standardized.parquet",
    "io": BASE_DIR / "intraop_io_layer2_standardized.parquet",
    "events": BASE_DIR / "intraop_events_layer2_standardized.parquet"
}

def create_wide_table():
    full_df = None
    
    for name, path in FILES.items():
        if not path.exists():
            print(f"Warning: {path.name} not found, skipping.")
            continue
            
        print(f"Processing {name}...")
        df = pd.read_parquet(path)
        
        # 统一列名以方便 pivot: LOG_ID, RECORDED_TIME, NAME, VALUE
        if name == "events":
            df = df[["LOG_ID", "RECORDED_TIME", "EVENT_NAME"]].copy()
            df["VALUE"] = 1
            df.columns = ["LOG_ID", "RECORDED_TIME", "NAME", "VALUE"]
        else:
            non_id = [c for c in df.columns if c not in ["LOG_ID", "RECORDED_TIME"]]
            if len(non_id) < 2:
                print(f"Warning: {path.name} schema unexpected, skipping.")
                continue
            name_col, value_col = non_id[0], non_id[1]
            df = df[["LOG_ID", "RECORDED_TIME", name_col, value_col]].copy()
            df.columns = ["LOG_ID", "RECORDED_TIME", "NAME", "VALUE"]
            
        # Pivot to Wide
        df_wide = df.pivot_table(
            index=["LOG_ID", "RECORDED_TIME"], 
            columns="NAME", 
            values="VALUE", 
            aggfunc="median" if name != "io" else "sum"
        ).reset_index()
        
        if full_df is None:
            full_df = df_wide
        else:
            full_df = pd.merge(full_df, df_wide, on=["LOG_ID", "RECORDED_TIME"], how="outer")
            
    if full_df is not None:
        print(f"Final Wide Table Shape: {full_df.shape}")
        
        # 统计描述
        print("\nGenerating Statistical Description...")
        stats = full_df.describe(percentiles=[0.01, 0.5, 0.99]).T
        stats["missing_pct"] = (1 - stats["count"] / len(full_df)) * 100
        
        stats_path = BASE_DIR / "intraop_full_wide_stats.csv"
        stats.to_csv(stats_path)
        
        print(f"Saving wide table to {OUTPUT_FILE.name}...")
        full_df.to_parquet(OUTPUT_FILE, index=False)
        
        # 打印关键统计到屏幕
        print("\n=== Key Variables Missingness & Ranges ===")
        preview = stats[["count", "mean", "1%", "99%", "missing_pct"]].head(30)
        try:
            print(preview.to_markdown())
        except Exception:
            print(preview.to_string())
        
if __name__ == "__main__":
    create_wide_table()

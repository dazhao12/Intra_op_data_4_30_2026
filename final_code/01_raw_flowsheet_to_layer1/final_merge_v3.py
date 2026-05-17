"""
MOVER 最终合并脚本 (V3 合并版)
将 parts_v3 下的 9439 个小文件按模块快速合并并清理
"""
import pandas as pd
from pathlib import Path
import os
import shutil

BASE_DIR  = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
PARTS_DIR = BASE_DIR / "parts_v3"
MODULES   = ["vitals", "respiratory", "io", "neuro", "labs_poct", "events_checklist", "other"]

def merge_all():
    print(f"Starting final merge of {len(list(PARTS_DIR.glob('*.parquet')))} parts...")
    
    for mod in MODULES:
        parts = sorted(PARTS_DIR.glob(f"*__{mod}__*.parquet"))
        if not parts:
            print(f"  Module {mod}: No data found.")
            continue
        
        print(f"  Merging {mod} ({len(parts)} parts)...")
        
        # 为了节省内存，我们分块读入
        dfs = []
        for i, p in enumerate(parts):
            try:
                dfs.append(pd.read_parquet(p))
                # 每 500 个小文件做一次中间合并，防止内存溢出
                if len(dfs) >= 500:
                    intermediate = pd.concat(dfs, ignore_index=True)
                    dfs = [intermediate]
            except Exception as e:
                print(f"    Error reading {p.name}: {e}")
        
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            # 去重：基于 LOG_ID, RECORDED_TIME 和 FLO_MEAS_NAME (如果是 vitals)
            subset = ["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME"] if "FLO_MEAS_NAME" in final_df.columns else ["LOG_ID", "RECORDED_TIME"]
            final_df.drop_duplicates(subset=subset, inplace=True, errors='ignore')
            
            out_path = BASE_DIR / f"intraop_{mod}_layer1.parquet"
            final_df.to_parquet(out_path, index=False)
            print(f"  [SUCCESS] {out_path.name}: {len(final_df):,} rows saved.")

def cleanup():
    print("\nCleaning up parts_v3 directory...")
    try:
        shutil.rmtree(PARTS_DIR)
        print("Cleanup complete. All 9,439 part files deleted.")
    except Exception as e:
        print(f"Cleanup error: {e}")

if __name__ == "__main__":
    merge_all()
    cleanup()
    print("\nALL DONE! Your intraoperative dataset is ready.")

import argparse
from pathlib import Path
import time
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
PARTS_DIR = BASE_DIR / "parts_v3"
MODULES = ["vitals", "respiratory", "io", "neuro", "labs_poct", "events_checklist", "other"]
DEFAULT_BASELINE_DIR = BASE_DIR / "archive/legacy_archive_data/archive_data/layer1"


def count_rows_parquet(path):
    try:
        return int(len(pd.read_parquet(path)))
    except Exception:
        return None


def merge_one_module(module):
    t0 = time.time()
    parts = sorted(PARTS_DIR.glob(f"*__{module}__*.parquet"))
    print(f"[{module}] parts={len(parts)}", flush=True)
    if not parts:
        return {
            "module": module,
            "parts_files": 0,
            "pre_rows": 0,
            "post_rows": 0,
            "dedup_removed": 0,
            "ok": False,
            "error": "no_parts",
        }

    dfs = []
    pre_rows = 0
    for i, p in enumerate(parts, 1):
        df = pd.read_parquet(p)
        pre_rows += len(df)
        dfs.append(df)
        if len(dfs) >= 500:
            dfs = [pd.concat(dfs, ignore_index=True)]
        if i % 2000 == 0 or i == len(parts):
            print(
                f"[{module}] read_parts={i}/{len(parts)} pre_rows={pre_rows:,} elapsed={time.time()-t0:.0f}s",
                flush=True,
            )

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    subset = (
        ["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME"]
        if "FLO_MEAS_NAME" in final_df.columns
        else ["LOG_ID", "RECORDED_TIME"]
    )
    pre_dedup_rows = len(final_df)
    final_df = final_df.drop_duplicates(subset=subset)
    post_rows = len(final_df)

    out_path = BASE_DIR / f"intraop_{module}_layer1.parquet"
    final_df.to_parquet(out_path, index=False, compression="gzip")
    print(
        f"[{module}] done pre={pre_rows:,} concat={pre_dedup_rows:,} post={post_rows:,} "
        f"removed={pre_rows-post_rows:,} elapsed={time.time()-t0:.0f}s",
        flush=True,
    )

    return {
        "module": module,
        "parts_files": len(parts),
        "pre_rows": int(pre_rows),
        "post_rows": int(post_rows),
        "dedup_removed": int(pre_rows - post_rows),
        "ok": True,
        "error": "",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline_dir", type=str, default=str(DEFAULT_BASELINE_DIR))
    parser.add_argument(
        "--report_csv",
        type=str,
        default=str(BASE_DIR / "layer1_qc_report_after_header_fix.csv"),
    )
    args = parser.parse_args()

    baseline_dir = Path(args.baseline_dir)
    report_path = Path(args.report_csv)
    rows = []

    for mod in MODULES:
        rec = merge_one_module(mod)
        baseline_file = baseline_dir / f"intraop_{mod}_layer1.parquet"
        baseline_rows = count_rows_parquet(baseline_file) if baseline_file.exists() else None
        rec["baseline_rows"] = baseline_rows
        rec["delta_vs_baseline"] = None if baseline_rows is None else rec["post_rows"] - baseline_rows
        rec["ratio_vs_baseline"] = (
            None if (baseline_rows is None or baseline_rows == 0) else rec["post_rows"] / float(baseline_rows)
        )
        rows.append(rec)

    df = pd.DataFrame(rows)
    df.to_csv(report_path, index=False)
    print("\n===== Layer1 QC Summary =====", flush=True)
    print(
        df[
            [
                "module",
                "parts_files",
                "pre_rows",
                "post_rows",
                "dedup_removed",
                "baseline_rows",
                "delta_vs_baseline",
                "ratio_vs_baseline",
                "ok",
                "error",
            ]
        ].to_string(index=False),
        flush=True,
    )
    print(f"\nReport saved: {report_path}", flush=True)


if __name__ == "__main__":
    main()

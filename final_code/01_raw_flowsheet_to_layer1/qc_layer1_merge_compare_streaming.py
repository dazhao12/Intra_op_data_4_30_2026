"""
Streaming layer1 merge + QC report.

This version avoids loading a whole module into memory. It uses a SQLite key
index for exact cross-part deduplication and appends unique rows to parquet.
"""
import argparse
import sqlite3
import time
from pathlib import Path

import fastparquet
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
PARTS_DIR = BASE_DIR / "parts_v3"
MODULES = ["vitals", "respiratory", "io", "neuro", "labs_poct", "events_checklist", "other"]
DEFAULT_BASELINE_DIR = BASE_DIR / "archive/legacy_archive_data/archive_data/layer1"
SEP = "\x1f"
NA_SENTINEL = "<NA>"


def count_rows_parquet(path):
    try:
        return int(len(pd.read_parquet(path)))
    except Exception:
        return None


def make_keys(df, key_cols):
    filled = [df[col].where(df[col].notna(), NA_SENTINEL).astype(str) for col in key_cols]
    key = filled[0]
    for series in filled[1:]:
        key = key + SEP + series
    return key


def open_seen_db(path):
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    conn.execute("CREATE TABLE seen (key TEXT PRIMARY KEY, batch INTEGER, row_idx INTEGER) WITHOUT ROWID")
    conn.execute("CREATE INDEX seen_batch_idx ON seen(batch)")
    return conn


def append_parquet(out_path, df, first_write):
    fastparquet.write(
        str(out_path),
        df,
        compression="GZIP",
        append=not first_write,
        write_index=False,
    )


def merge_one_module(module):
    t0 = time.time()
    parts = sorted(PARTS_DIR.glob(f"*__{module}__*.parquet"))
    out_path = BASE_DIR / f"intraop_{module}_layer1.parquet"
    db_path = BASE_DIR / f".dedup_seen_{module}.sqlite"
    if out_path.exists():
        out_path.unlink()

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

    conn = open_seen_db(db_path)
    cur = conn.cursor()
    pre_rows = 0
    post_rows = 0
    first_write = True

    try:
        for batch_id, part in enumerate(parts, 1):
            df = pd.read_parquet(part)
            pre_rows += len(df)
            key_cols = (
                ["LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME"]
                if "FLO_MEAS_NAME" in df.columns
                else ["LOG_ID", "RECORDED_TIME"]
            )

            df = df.drop_duplicates(subset=key_cols).reset_index(drop=True)
            if not df.empty:
                keys = make_keys(df, key_cols)
                records = zip(keys.tolist(), [batch_id] * len(df), range(len(df)))
                cur.executemany(
                    "INSERT OR IGNORE INTO seen(key, batch, row_idx) VALUES (?, ?, ?)",
                    records,
                )
                inserted_idx = [r[0] for r in cur.execute("SELECT row_idx FROM seen WHERE batch=?", (batch_id,))]
                if inserted_idx:
                    unique_df = df.iloc[inserted_idx]
                    append_parquet(out_path, unique_df, first_write)
                    first_write = False
                    post_rows += len(unique_df)

            if batch_id % 500 == 0 or batch_id == len(parts):
                conn.commit()
                print(
                    f"[{module}] parts={batch_id}/{len(parts)} pre={pre_rows:,} "
                    f"post={post_rows:,} removed={pre_rows-post_rows:,} elapsed={time.time()-t0:.0f}s",
                    flush=True,
                )
    finally:
        conn.commit()
        conn.close()

    print(
        f"[{module}] done pre={pre_rows:,} post={post_rows:,} "
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
    parser.add_argument("--report_csv", type=str, default=str(BASE_DIR / "layer1_qc_report_after_header_fix.csv"))
    args = parser.parse_args()

    baseline_dir = Path(args.baseline_dir)
    rows = []
    for module in MODULES:
        rec = merge_one_module(module)
        baseline_file = baseline_dir / f"intraop_{module}_layer1.parquet"
        baseline_rows = count_rows_parquet(baseline_file) if baseline_file.exists() else None
        rec["baseline_rows"] = baseline_rows
        rec["delta_vs_baseline"] = None if baseline_rows is None else rec["post_rows"] - baseline_rows
        rec["ratio_vs_baseline"] = (
            None if baseline_rows in (None, 0) else rec["post_rows"] / float(baseline_rows)
        )
        rows.append(rec)

    report = pd.DataFrame(rows)
    report.to_csv(args.report_csv, index=False)
    cols = [
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
    print("\n===== Layer1 QC Summary =====", flush=True)
    print(report[cols].to_string(index=False), flush=True)
    print(f"\nReport saved: {args.report_csv}", flush=True)


if __name__ == "__main__":
    main()

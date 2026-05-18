import argparse
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
PARTS_DIR = BASE_DIR / "parts_v3"
MODULES = ["vitals", "respiratory", "io", "neuro", "labs_poct", "events_checklist", "other"]
DEFAULT_BASELINE_DIR = BASE_DIR / "archive/legacy_archive_data/archive_data/layer1"


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def count_baseline_rows(con, path):
    if not path.exists():
        return None
    sql = f"SELECT COUNT(*) FROM read_parquet({q(path)})"
    return int(con.execute(sql).fetchone()[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline_dir", type=str, default=str(DEFAULT_BASELINE_DIR))
    parser.add_argument(
        "--report_csv",
        type=str,
        default=str(BASE_DIR / "layer1_qc_report_after_header_fix.csv"),
    )
    parser.add_argument(
        "--tmp_dir",
        type=str,
        default=str(BASE_DIR / "duckdb_tmp"),
    )
    parser.add_argument("--modules", nargs="+", default=MODULES)
    args = parser.parse_args()

    baseline_dir = Path(args.baseline_dir)
    report_path = Path(args.report_csv)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA temp_directory={q(tmp_dir)}")
    con.execute("PRAGMA memory_limit='200GB'")
    con.execute("SET preserve_insertion_order=false")

    rows = []
    for mod in args.modules:
        pattern = str(PARTS_DIR / f"*__{mod}__*.parquet")
        files = sorted(PARTS_DIR.glob(f"*__{mod}__*.parquet"))
        out_path = BASE_DIR / f"intraop_{mod}_layer1.parquet"

        rec = {
            "module": mod,
            "parts_files": len(files),
            "pre_rows": 0,
            "post_rows": 0,
            "dedup_removed": 0,
            "output_file": str(out_path),
            "ok": True,
            "error": "",
        }

        if not files:
            rec["ok"] = False
            rec["error"] = "no_parts"
        else:
            try:
                read_expr = f"read_parquet({q(pattern)}, union_by_name=True)"
                pre_rows = int(con.execute(f"SELECT COUNT(*) FROM {read_expr}").fetchone()[0])
                rec["pre_rows"] = pre_rows

                cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()]
                has_flo_meas_name = "FLO_MEAS_NAME" in cols
                if has_flo_meas_name:
                    key = "LOG_ID, RECORDED_TIME, FLO_MEAS_NAME"
                else:
                    key = "LOG_ID, RECORDED_TIME"

                dedup_sql = f"""
                COPY (
                    SELECT * EXCLUDE (rn)
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY {key}) AS rn
                        FROM {read_expr}
                    ) t
                    WHERE rn = 1
                )
                TO {q(out_path)}
                (FORMAT PARQUET, COMPRESSION GZIP)
                """
                con.execute(dedup_sql)

                post_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet({q(out_path)})").fetchone()[0])
                rec["post_rows"] = post_rows
                rec["dedup_removed"] = pre_rows - post_rows
            except Exception as e:
                rec["ok"] = False
                rec["error"] = str(e)

        baseline_file = baseline_dir / f"intraop_{mod}_layer1.parquet"
        baseline_rows = count_baseline_rows(con, baseline_file)
        rec["baseline_file"] = str(baseline_file)
        rec["baseline_rows"] = baseline_rows
        rec["delta_vs_baseline"] = None if baseline_rows is None else rec["post_rows"] - baseline_rows
        rec["ratio_vs_baseline"] = (
            None if (baseline_rows is None or baseline_rows == 0) else rec["post_rows"] / float(baseline_rows)
        )
        rows.append(rec)

    report_df = pd.DataFrame(rows)
    report_df.to_csv(report_path, index=False)

    show_cols = [
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
    print("\n===== Layer1 QC Summary (DuckDB) =====")
    print(report_df[show_cols].to_string(index=False))
    print(f"\nReport saved: {report_path}")


if __name__ == "__main__":
    main()

"""
Final Layer1 merge with conservative content-level deduplication.

This version rebuilds Layer1 directly from parts_v3, instead of repairing the
already-merged files. It fixes two issues:
  1. Do not deduplicate on only LOG_ID + RECORDED_TIME + FLO_MEAS_NAME.
     That can drop clinically distinct same-minute rows with different values.
  2. Move obvious "extra" rows out of other:
       vitals_extra -> vitals
       io_extra -> io
       neuro_or_pain -> neuro

Outputs are written to a versioned directory by default and do not overwrite
the existing intraop_*_layer1.parquet files.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
PARTS_DIR = BASE_DIR / "parts_v3"
OUT_DIR = BASE_DIR / "layer1_v3_3_content_dedup_reclassified"
TMP_DIR = BASE_DIR / "duckdb_tmp"

MODULES = [
    "vitals",
    "respiratory",
    "io",
    "neuro",
    "labs_poct",
    "events_checklist",
    "other",
]

EXPECTED_COLUMNS = [
    "OR_CASE_ID",
    "LOG_ID",
    "PAT_ID",
    "MRN",
    "HSP_ACCOUNT_ID",
    "OR_LINK_CSN",
    "PAT_ENC_CSN_ID",
    "ENC_TYPE_C",
    "ENC_TYPE_NM",
    "SURGERY_DATE",
    "IN_OR_DTTM",
    "OUT_OR_DTTM",
    "AN_START_DATETIME",
    "AN_STOP_DATETIME",
    "INPATIENT_DATA_ID",
    "FSD_ID",
    "FLO_MEAS_ID",
    "FLO_TEMPLATE_NAME",
    "FLO_NAME",
    "FLO_MEAS_NAME",
    "FLO_DISPLAY_NAME",
    "RECORD_TYPE",
    "RECORDED_TIME",
    "MEAS_VALUE",
    "UNITS",
    "MEAS_COMMENT",
    "LINE",
]

CONTENT_DEDUP_COLUMNS = [c for c in EXPECTED_COLUMNS if c != "LINE"]


def q(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def part_glob(module: str) -> str:
    return str(PARTS_DIR / f"*__{module}__*.parquet")


def parquet_exists(module: str) -> bool:
    return any(PARTS_DIR.glob(f"*__{module}__*.parquet"))


def col_list(cols: list[str]) -> str:
    return ",\n        ".join(cols)


def grouped_select(input_sql: str) -> str:
    group_cols = col_list(CONTENT_DEDUP_COLUMNS)
    select_cols = col_list(CONTENT_DEDUP_COLUMNS)
    return f"""
    SELECT
        {select_cols},
        min(LINE) AS LINE
    FROM (
        {input_sql}
    )
    GROUP BY
        {group_cols}
    """


TARGET_CASE = """
CASE
  WHEN upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%PAIN%'
       OR trim(coalesce(cast(FLO_NAME as varchar), '')) IN ('Pain Mgmt', 'Pain Screening')
    THEN 'neuro'
  WHEN trim(coalesce(cast(FLO_NAME as varchar), '')) IN ('CCP Intake/Output', 'OR Input/Output')
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%URINE OUTPUT%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%URINARY DRAIN OUTPUT%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%ESTIMATED BLOOD LOSS%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%MAINTENANCE FLUID VOLUME%'
    THEN 'io'
  WHEN trim(coalesce(cast(FLO_NAME as varchar), '')) IN (
       'Custom Formula Data',
       'CCP Vital Signs',
       'ED Vitals',
       'Vitals/Screening',
       'Code Quick Vitals',
       'Height/Weight',
       'IP Nutrition'
       )
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%BLOOD PRESSURE%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%SYSTOLIC BP%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%DIASTOLIC BP%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%PULSE%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%RESPIRATIONS%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%TEMPERATURE%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%TEMP SOURCE%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%WEIGHT%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%HEIGHT%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%BMI%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%BSA%'
       OR upper(coalesce(cast(FLO_MEAS_NAME as varchar), '') || ' ' || coalesce(cast(FLO_DISPLAY_NAME as varchar), '')) LIKE '%CARDIAC RHYTHM%'
    THEN 'vitals'
  ELSE 'other'
END
"""


def source_sql(module: str) -> str:
    return f"""
    SELECT {col_list(EXPECTED_COLUMNS)}
    FROM read_parquet({q(part_glob(module))}, union_by_name=true)
    """


def moved_from_other_sql(target_module: str) -> str:
    return f"""
    SELECT {col_list(EXPECTED_COLUMNS)}
    FROM (
        SELECT
            {col_list(EXPECTED_COLUMNS)},
            {TARGET_CASE} AS target_module
        FROM read_parquet({q(part_glob("other"))}, union_by_name=true)
    )
    WHERE target_module = '{target_module}'
    """


def input_sql_for_target(module: str) -> str:
    if module in {"vitals", "io", "neuro"}:
        return f"""
        {source_sql(module)}
        UNION ALL BY NAME
        {moved_from_other_sql(module)}
        """
    if module == "other":
        return moved_from_other_sql("other")
    return source_sql(module)


def count_query(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]


def count_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> int | None:
    if not path.exists():
        return None
    return con.execute(
        f"SELECT COUNT(*) FROM read_parquet({q(path)}, union_by_name=true)"
    ).fetchone()[0]


def write_module(con: duckdb.DuckDBPyConnection, module: str, out_dir: Path) -> dict:
    if not parquet_exists(module) and module != "other":
        return {
            "module": module,
            "pre_rows": 0,
            "post_rows": 0,
            "dedup_removed": 0,
            "current_official_rows": count_parquet(con, BASE_DIR / f"intraop_{module}_layer1.parquet"),
            "baseline_rows": count_parquet(
                con,
                BASE_DIR
                / "archive/legacy_archive_data/archive_data/layer1"
                / f"intraop_{module}_layer1.parquet",
            ),
            "delta_vs_current_official": None,
            "delta_vs_baseline": None,
            "output_file": None,
        }

    input_sql = input_sql_for_target(module)
    pre_rows = count_query(con, input_sql)

    out_path = out_dir / f"intraop_{module}_layer1.parquet"
    tmp_path = out_dir / f"intraop_{module}_layer1.parquet.tmp"
    if tmp_path.exists():
        tmp_path.unlink()
    if out_path.exists():
        out_path.unlink()

    con.execute(
        f"""
        COPY (
            {grouped_select(input_sql)}
        ) TO {q(tmp_path)} (FORMAT PARQUET, COMPRESSION GZIP)
        """
    )
    tmp_path.rename(out_path)

    post_rows = count_parquet(con, out_path)
    current_rows = count_parquet(con, BASE_DIR / f"intraop_{module}_layer1.parquet")
    baseline_rows = count_parquet(
        con,
        BASE_DIR
        / "archive/legacy_archive_data/archive_data/layer1"
        / f"intraop_{module}_layer1.parquet",
    )

    return {
        "module": module,
        "pre_rows": pre_rows,
        "post_rows": post_rows,
        "dedup_removed": pre_rows - post_rows,
        "current_official_rows": current_rows,
        "baseline_rows": baseline_rows,
        "delta_vs_current_official": None if current_rows is None else post_rows - current_rows,
        "delta_vs_baseline": None if baseline_rows is None else post_rows - baseline_rows,
        "output_file": str(out_path),
    }


def write_moved_detail(con: duckdb.DuckDBPyConnection, out_dir: Path) -> Path:
    out_path = out_dir / "layer1_v3_3_moved_from_other_by_flo.csv"
    moved = con.execute(
        f"""
        SELECT
            target_module,
            FLO_NAME,
            FLO_MEAS_NAME,
            FLO_DISPLAY_NAME,
            COUNT(*) AS rows
        FROM (
            SELECT
                FLO_NAME,
                FLO_MEAS_NAME,
                FLO_DISPLAY_NAME,
                {TARGET_CASE} AS target_module
            FROM read_parquet({q(part_glob("other"))}, union_by_name=true)
        )
        WHERE target_module <> 'other'
        GROUP BY 1,2,3,4
        ORDER BY target_module, rows DESC
        """
    ).df()
    moved.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=str(OUT_DIR))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='200GB'")
    con.execute(f"PRAGMA temp_directory={q(TMP_DIR)}")
    con.execute("SET preserve_insertion_order=false")

    rows = []
    for module in MODULES:
        print(f"[merge] {module}", flush=True)
        row = write_module(con, module, out_dir)
        rows.append(row)
        print(
            f"  pre={row['pre_rows']:,} post={row['post_rows']:,} "
            f"removed={row['dedup_removed']:,}",
            flush=True,
        )

    moved_path = write_moved_detail(con, out_dir)
    report = pd.DataFrame(rows)
    report_path = out_dir / "layer1_v3_3_qc_report.csv"
    report.to_csv(report_path, index=False)

    print("\nQC report")
    print(report.to_string(index=False))
    print(f"\nSaved: {report_path}")
    print(f"Saved: {moved_path}")


if __name__ == "__main__":
    main()

"""
Post-hoc reclassification of obvious extra rows currently sitting in layer1 other.

The raw extraction originally routes by FLO_NAME. Some rows under broad FLO_NAME
values such as Data, Custom Formula Data, and CCP Vital Signs are better treated
as vitals/io/neuro rows. This script creates *_reclassified.parquet outputs
without overwriting the current layer1 files.
"""
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
REPORT = BASE_DIR / "layer1_reclassification_report.csv"

MODULES = ["vitals", "io", "neuro", "other"]


def q(path):
    return "'" + str(path).replace("'", "''") + "'"


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


def main():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='200GB'")
    con.execute("SET preserve_insertion_order=false")

    other = BASE_DIR / "intraop_other_layer1.parquet"
    con.execute(
        f"""
        CREATE TEMP VIEW other_tagged AS
        SELECT *, {TARGET_CASE} AS target_module
        FROM read_parquet({q(other)}, union_by_name=True)
        """
    )

    rows = []
    for module in MODULES:
        out_path = BASE_DIR / f"intraop_{module}_layer1_reclassified.parquet"
        if out_path.exists():
            out_path.unlink()

        if module == "other":
            sql = f"""
            COPY (
              SELECT * EXCLUDE (target_module)
              FROM other_tagged
              WHERE target_module = 'other'
            ) TO {q(out_path)} (FORMAT PARQUET, COMPRESSION GZIP)
            """
        else:
            current = BASE_DIR / f"intraop_{module}_layer1.parquet"
            sql = f"""
            COPY (
              SELECT * FROM read_parquet({q(current)}, union_by_name=True)
              UNION ALL BY NAME
              SELECT * EXCLUDE (target_module)
              FROM other_tagged
              WHERE target_module = '{module}'
            ) TO {q(out_path)} (FORMAT PARQUET, COMPRESSION GZIP)
            """
        con.execute(sql)
        rows.append(
            {
                "module": module,
                "current_rows": con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({q(BASE_DIR / f'intraop_{module}_layer1.parquet')}, union_by_name=True)"
                ).fetchone()[0],
                "reclassified_rows": con.execute(
                    f"SELECT COUNT(*) FROM read_parquet({q(out_path)}, union_by_name=True)"
                ).fetchone()[0],
                "output_file": str(out_path),
            }
        )

    moved = con.execute(
        """
        SELECT target_module, FLO_NAME, FLO_MEAS_NAME, FLO_DISPLAY_NAME, COUNT(*) AS rows
        FROM other_tagged
        WHERE target_module <> 'other'
        GROUP BY 1,2,3,4
        ORDER BY target_module, rows DESC
        """
    ).df()
    moved_path = BASE_DIR / "layer1_reclassification_moved_rows_by_flo.csv"
    moved.to_csv(moved_path, index=False)

    report = pd.DataFrame(rows)
    report["delta"] = report["reclassified_rows"] - report["current_rows"]
    report.to_csv(REPORT, index=False)
    print(report.to_string(index=False))
    print(f"Report saved: {REPORT}")
    print(f"Moved-row detail saved: {moved_path}")


if __name__ == "__main__":
    main()

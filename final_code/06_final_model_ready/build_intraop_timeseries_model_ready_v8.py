#!/usr/bin/env python3
"""
Build the final MOVER intraoperative time-series model-ready table.

Final output:
  final_data/MODEL_INPUT_intraop_timeseries_model_ready_v8.parquet

Intermediate input:
  archive/intermediate_data/v2_module_outputs/core_wide_v1_fix1_imputed_nolabs_v6_grid/core_wide_v1_fix1_imputed_nolabs_v6_grid.parquet

This is the Python version of the prior v8 final R build script. The final release keeps
only this script plus the single model-input parquet in final_data.
"""
from pathlib import Path
import argparse
import json
import numpy as np
import pandas as pd

ROOT = Path('/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026')
INTERMEDIATE_DIR = ROOT / 'archive/intermediate_data'
FINAL_DATA_DIR = ROOT / 'final_data'
INPUT_FILE = INTERMEDIATE_DIR / 'v2_module_outputs/core_wide_v1_fix1_imputed_nolabs_v6_grid/core_wide_v1_fix1_imputed_nolabs_v6_grid.parquet'
META_FILE = Path('/N/project/analgesia_perioperation/data/MOVER/processed/anesthesia_surgery_info_final_cohort_latest/mover_anesthesia_surgery_info_corrected_with_esc_risk.csv')
OUTPUT_FILE = FINAL_DATA_DIR / 'MODEL_INPUT_intraop_timeseries_model_ready_v8.parquet'
QC_FILE = ROOT / 'archive/metadata/final_model_ready_v8_python_qc.json'
RAW_FLOWSHEET_DIR = Path('/N/project/analgesia_perioperation/data/MOVER/raw/srv/EPIC_flowsheets')
COHORT_FILE = ROOT / 'archive/intermediate_data/reference_inputs/final_single_mrn_single_login_with_definable_intraop_time.csv'
ARCHIVED_CODE_DIR = ROOT / 'archive/intermediate_code/scripts'

# This manifest documents the full historical raw-to-final chain.
# Only the final build step is implemented directly in this Python script.
# Earlier steps are archived for reproducibility and were historically a Python/R mixed pipeline.
PIPELINE_STEPS = [
    {
        'stage': '01_raw_flowsheet_to_layer1',
        'status': 'archived_historical_code_python',
        'code': ARCHIVED_CODE_DIR / 'extract_flowsheets_v3_2.py',
        'inputs': [RAW_FLOWSHEET_DIR, COHORT_FILE],
        'outputs': [ROOT / 'archive/legacy_archive_data/archive_data/layer1'],
    },
    {
        'stage': '02_layer1_to_layer2_standardized',
        'status': 'archived_historical_code_python',
        'code': ARCHIVED_CODE_DIR,
        'inputs': [ROOT / 'archive/legacy_archive_data/archive_data/layer1'],
        'outputs': [ROOT / 'archive/intermediate_data/layer2_parts_original'],
    },
    {
        'stage': '03_layer2_to_5module_long_wide',
        'status': 'archived_historical_code_mixed_R_python',
        'code': ARCHIVED_CODE_DIR / 'build_module_wide_v2.R',
        'inputs': [ROOT / 'archive/intermediate_data/layer2_parts_original'],
        'outputs': [ROOT / 'archive/intermediate_data/v2_module_outputs/long', ROOT / 'archive/intermediate_data/v2_module_outputs/wide'],
    },
    {
        'stage': '04_merge_5modules',
        'status': 'archived_historical_code_R',
        'code': ARCHIVED_CODE_DIR / 'build_merged_wide_5modules_v3.R',
        'inputs': [ROOT / 'archive/intermediate_data/v2_module_outputs/long', ROOT / 'archive/intermediate_data/v2_module_outputs/wide'],
        'outputs': [ROOT / 'archive/intermediate_data/v2_module_outputs/merged_5modules_v3/intraop_merged_5modules_wide_v3.parquet'],
    },
    {
        'stage': '05_minute_grid_and_imputation',
        'status': 'archived_historical_code_R',
        'code': ARCHIVED_CODE_DIR / 'build_core_wide_v1_fix1_imputed_nolabs_v6_grid.R',
        'inputs': [ROOT / 'archive/intermediate_data/v2_module_outputs/core_wide_v1_fix1'],
        'outputs': [INPUT_FILE],
    },
    {
        'stage': '06_final_model_ready',
        'status': 'current_final_code_python',
        'code': Path(__file__),
        'inputs': [INPUT_FILE, META_FILE],
        'outputs': [OUTPUT_FILE],
    },
]

VAR_MAP = {
    'HR_core': 'HR',
    'SpO2_core': 'SpO2',
    'RR_core': 'RR',
    'EtCO2_core': 'EtCO2',
    'FiO2_core': 'FiO2',
    'PIP_core': 'PIP',
    'PEEP_core': 'PEEP',
    'Tidal_volume_core': 'TV',
    'Minute_volume_core': 'MV',
    'Temp_C_core': 'Temp',
    'Nitric_oxide_core': 'NO',
    'BP_SBP': 'BP_SBP',
    'BP_MAP': 'BP_MAP',
    'BP_DBP': 'BP_DBP',
    'IBP_SBP': 'IBP_SBP',
    'IBP_MAP': 'IBP_MAP',
    'IBP_DBP': 'IBP_DBP',
    'NBP_SBP': 'NBP_SBP',
    'NBP_MAP': 'NBP_MAP',
    'NBP_DBP': 'NBP_DBP',
}

GAS_MAP = {
    'Sevoflurane_core': 'Sevo',
    'Isoflurane_core': 'Iso',
    'Desflurane_core': 'Des',
}

IO_VAR_MAP = {
    'Intake_fluid_core': 'Intake_fluid',
    'Urine_output_core': 'Urine_output',
    'EBL_core': 'EBL',
    'Blood_products_core': 'Blood_products',
    'Other_output_core': 'Other_output',
}

KEY_COLS = ['OR_CASE_ID', 'LOG_ID', 'minute_index', 'RECORDED_TIME', 'time_boundary_source']
QC_COL_CANDIDATES = [
    'BP_SBP_source', 'BP_MAP_source', 'BP_DBP_source',
    'BP_qc_flag',
    'IBP_triplet_corrected_flag', 'BP_triplet_corrected_flag',
    'io_any_recorded_flag',
]


def require_paths() -> None:
    missing = [p for p in [INPUT_FILE, META_FILE] if not p.exists()]
    if missing:
        raise FileNotFoundError('Missing required input path(s):\n' + '\n'.join(map(str, missing)))


def int_flag(series: pd.Series, default: int = 0) -> pd.Series:
    return series.fillna(default).astype('int8')


def build_model_ready() -> pd.DataFrame:
    require_paths()
    print(f'[INFO] loading v6 grid: {INPUT_FILE}')
    dt = pd.read_parquet(INPUT_FILE)
    print(f'[INFO] v6 rows={len(dt):,} cols={len(dt.columns):,}')

    meta = pd.read_csv(META_FILE, usecols=['LOG_ID', 'OR_CASE_ID'])
    if 'OR_CASE_ID' in dt.columns:
        dt = dt.drop(columns=['OR_CASE_ID'])
    dt = dt.merge(meta, on='LOG_ID', how='left')

    keep_cols = [c for c in KEY_COLS if c in dt.columns]
    keep_cols += [c for c in QC_COL_CANDIDATES if c in dt.columns]

    # Continuous variables: rename value columns and preserve imputation semantics.
    for old_v, new_v in VAR_MAP.items():
        if old_v not in dt.columns:
            continue
        miss_col = f'{old_v}_missing_flag'
        locf_col = f'{old_v}_filled_by_locf_flag'
        lead_col = f'{old_v}_leading_na_flag'
        age_col = f'{old_v}_time_since_last_obs_min'

        dt = dt.rename(columns={old_v: new_v})
        obs_col = f'{new_v}_observed'
        new_locf = f'{new_v}_is_locf'
        new_lead = f'{new_v}_is_leading_default'
        gap_col = f'{new_v}_gap_min'

        dt[obs_col] = (1 - int_flag(dt.get(miss_col, pd.Series(1, index=dt.index)))).astype('int8')
        dt[new_locf] = int_flag(dt.get(locf_col, pd.Series(0, index=dt.index)))
        dt[new_lead] = int_flag(dt.get(lead_col, pd.Series(0, index=dt.index)))
        dt[gap_col] = dt[age_col] if age_col in dt.columns else np.nan

        if new_v in {'IBP_SBP','IBP_MAP','IBP_DBP','BP_SBP','BP_MAP','BP_DBP'}:
            missing_value = dt[new_v].isna()
            dt.loc[missing_value, [obs_col, new_locf, new_lead]] = [0, 0, 0]
            dt.loc[missing_value, gap_col] = -1.0
            global_med = dt[new_v].median(skipna=True)
            dt[new_v] = dt[new_v].fillna(global_med)

        dt[gap_col] = dt[gap_col].fillna(-1.0)
        keep_cols += [new_v, obs_col, new_locf, new_lead, gap_col]

    # Volatile anesthetic gases: mutually exclusive state machine.
    for old_v, new_v in GAS_MAP.items():
        miss_col = f'{old_v}_missing_flag'
        raw_col = f'raw_{new_v}'
        if old_v in dt.columns:
            missing = int_flag(dt.get(miss_col, pd.Series(1, index=dt.index)))
            dt[raw_col] = dt[old_v].where(missing == 0, np.nan)
        else:
            dt[raw_col] = np.nan

    raw_cols = ['raw_Sevo', 'raw_Iso', 'raw_Des']
    raw = dt[raw_cols]
    gas_obs_mask = raw.notna().any(axis=1)
    positive = raw.gt(0)
    n_pos = positive.sum(axis=1)
    max_name = raw.where(positive).idxmax(axis=1).map({'raw_Sevo': 'sevo', 'raw_Iso': 'iso', 'raw_Des': 'des'})

    current_event = pd.Series(pd.NA, index=dt.index, dtype='object')
    conflict = (gas_obs_mask & (n_pos > 1))
    current_event.loc[conflict] = max_name.loc[conflict].fillna('unknown')
    single = gas_obs_mask & (n_pos == 1)
    current_event.loc[single] = max_name.loc[single].fillna('unknown')
    none = gas_obs_mask & (n_pos == 0)
    current_event.loc[none] = 'none'

    event_to_id = {'none': 0, 'sevo': 1, 'iso': 2, 'des': 3, 'unknown': 4}
    agent_id = current_event.map(event_to_id)
    if 'LOG_ID' in dt.columns:
        agent_id = agent_id.groupby(dt['LOG_ID'], sort=False).ffill()
    agent_id = agent_id.fillna(0).astype('int8')
    id_to_event = {0: 'none', 1: 'sevo', 2: 'iso', 3: 'des', 4: 'unknown'}
    dt['volatile_agent'] = agent_id.map(id_to_event)
    dt['volatile_mixed_or_conflict_flag'] = conflict.astype('int8')

    for raw_col, new_v in [('raw_Sevo', 'Sevo'), ('raw_Iso', 'Iso'), ('raw_Des', 'Des')]:
        locf = dt.groupby('LOG_ID', sort=False)[raw_col].ffill() if 'LOG_ID' in dt.columns else dt[raw_col].ffill()
        agent_str = new_v.lower()
        dt[new_v] = np.where((dt['volatile_agent'] == agent_str) & locf.notna(), locf, 0)
        obs_col = f'{new_v}_observed'
        locf_col = f'{new_v}_is_locf'
        lead_col = f'{new_v}_is_leading_default'
        gap_col = f'{new_v}_gap_min'
        old_v = {v: k for k, v in GAS_MAP.items()}[new_v]
        age_col = f'{old_v}_time_since_last_obs_min'
        dt[obs_col] = dt[raw_col].notna().astype('int8')
        dt[locf_col] = ((dt[obs_col] == 0) & (dt['volatile_agent'] == agent_str)).astype('int8')
        dt[lead_col] = ((dt[obs_col] == 0) & (dt[locf_col] == 0) & locf.isna()).astype('int8')
        dt[gap_col] = dt[age_col] if age_col in dt.columns else np.nan
        dt[gap_col] = dt[gap_col].fillna(-1.0)
        keep_cols += [new_v, obs_col, locf_col, lead_col, gap_col]

    keep_cols += ['volatile_agent', 'volatile_mixed_or_conflict_flag']

    # I/O variables.
    for old_v, new_v in IO_VAR_MAP.items():
        if old_v in dt.columns:
            dt = dt.rename(columns={old_v: new_v})
            keep_cols.append(new_v)
        rec_old = f'{old_v}_recorded_flag'
        rec_new = f'{new_v}_observed'
        if rec_old in dt.columns:
            dt = dt.rename(columns={rec_old: rec_new})
            keep_cols.append(rec_new)

    keep_cols = [c for i, c in enumerate(keep_cols) if c in dt.columns and c not in keep_cols[:i]]
    out = dt.loc[:, keep_cols].copy()

    # Source/QC labels for imputed BP.
    for bp in ['BP_SBP', 'BP_MAP', 'BP_DBP']:
        source_col = f'{bp}_source'
        if source_col in out.columns and f'{bp}_observed' in out.columns:
            obs0 = out[f'{bp}_observed'] == 0
            out.loc[obs0 & (out[f'{bp}_is_locf'] == 1), source_col] = 'imputed_locf'
            out.loc[obs0 & (out[f'{bp}_is_leading_default'] == 1), source_col] = 'imputed_leading_default'
            out.loc[obs0 & (out[f'{bp}_is_locf'] == 0) & (out[f'{bp}_is_leading_default'] == 0), source_col] = 'imputed_unavailable'
    if 'BP_qc_flag' in out.columns and 'BP_MAP_observed' in out.columns:
        obs0 = out['BP_MAP_observed'] == 0
        out.loc[obs0 & (out['BP_MAP_is_locf'] == 1), 'BP_qc_flag'] = 'imputed_locf'
        out.loc[obs0 & (out['BP_MAP_is_leading_default'] == 1), 'BP_qc_flag'] = 'imputed_leading_default'
        out.loc[obs0 & (out['BP_MAP_is_locf'] == 0) & (out['BP_MAP_is_leading_default'] == 0), 'BP_qc_flag'] = 'imputed_unavailable'

    if {'LOG_ID', 'minute_index'}.issubset(out.columns):
        out = out.sort_values(['LOG_ID', 'minute_index']).reset_index(drop=True)
    print(f'[INFO] final rows={len(out):,} cols={len(out.columns):,}')
    return out


def write_outputs(df: pd.DataFrame) -> None:
    FINAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    QC_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    qc = {
        'output_file': str(OUTPUT_FILE),
        'rows': int(len(df)),
        'cols': int(len(df.columns)),
        'duplicate_LOG_ID_minute_index': int(df.duplicated(['LOG_ID', 'minute_index']).sum()) if {'LOG_ID','minute_index'}.issubset(df.columns) else None,
        'na_cells_total': int(df.isna().sum().sum()),
    }
    QC_FILE.write_text(json.dumps(qc, indent=2), encoding='utf-8')
    print(f'[DONE] wrote {OUTPUT_FILE}')
    print(f'[DONE] wrote QC {QC_FILE}')


def check_full_pipeline() -> None:
    """Check whether the archived raw-to-final pipeline artifacts still exist."""
    print('Full historical raw-to-final pipeline check')
    print('NOTE: stages 01-05 are archived historical process code; not a single pure-Python rerun pipeline.')
    any_missing = False
    for step in PIPELINE_STEPS:
        print('\n[{stage}] {status}'.format(**step))
        code = step['code']
        code_exists = code.exists()
        print('  code:', code, 'OK' if code_exists else 'MISSING')
        if not code_exists:
            any_missing = True
        for label in ['inputs', 'outputs']:
            for path in step[label]:
                exists = path.exists()
                print('  {0}: {1} {2}'.format(label[:-1], path, 'OK' if exists else 'MISSING'))
                if not exists:
                    any_missing = True
    if any_missing:
        raise SystemExit(1)
    print('\nOK: all documented code/artifact paths exist. This confirms traceability, not one-click raw rerun.')

def check_release() -> None:
    required = [INPUT_FILE, META_FILE, OUTPUT_FILE]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        print('MISSING:')
        for p in missing:
            print(p)
        raise SystemExit(1)
    print('OK: final Python code and single final model data path are aligned.')
    for p in required:
        print(p)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--check-only', action='store_true', help='Only check expected final input/output paths.')
    parser.add_argument('--check-full-pipeline', action='store_true', help='Check archived raw-to-final pipeline code and artifact paths.')
    args = parser.parse_args()
    if args.check_only:
        check_release()
        return
    if args.check_full_pipeline:
        check_full_pipeline()
        return
    df = build_model_ready()
    write_outputs(df)


if __name__ == '__main__':
    main()

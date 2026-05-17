#!/usr/bin/env python3
"""
Build labs_extended_v1 from existing labs_clean_v1b outputs.

Outputs:
- labs_extended_v1_keep_variables_and_rules.csv
- labs_extended_v1_selected_prepost_stats.csv
- README_labs_extended_v1.md
"""

from pathlib import Path
import pandas as pd
import numpy as np


BASE_DIR = Path("/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026")
IN_DIR = BASE_DIR / "labs_clean_v1"
OUT_DIR = BASE_DIR / "labs_clean_v1"

STATS_FILE = IN_DIR / "labs_clean_v1b_prepost_stats.csv"
QRULES_FILE = IN_DIR / "labs_clean_v1b_quantile_rules.csv"

OUT_RULES = OUT_DIR / "labs_extended_v1_keep_variables_and_rules.csv"
OUT_STATS = OUT_DIR / "labs_extended_v1_selected_prepost_stats.csv"
OUT_README = OUT_DIR / "README_labs_extended_v1.md"


LAB_RULES = [
    # Core blood-gas / electrolytes / perfusion
    {"LAB_NAME": "pH", "canonical_name": "pH", "clinical_group": "blood_gas", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "unitless", "value_min": 6.8, "value_max": 7.8, "notes": "arterial/venous acid-base"},
    {"LAB_NAME": "Carbon dioxide", "canonical_name": "pCO2", "clinical_group": "blood_gas", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmHg", "value_min": 10, "value_max": 150, "notes": "maps to pCO2-style measurement"},
    {"LAB_NAME": "Oxygen", "canonical_name": "pO2", "clinical_group": "blood_gas", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmHg", "value_min": 20, "value_max": 700, "notes": "maps to pO2-style measurement"},
    {"LAB_NAME": "Oxygen saturation", "canonical_name": "SaO2", "clinical_group": "blood_gas", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "%", "value_min": 0, "value_max": 100, "notes": "lab oxygen saturation"},
    {"LAB_NAME": "Bicarbonate", "canonical_name": "HCO3", "clinical_group": "blood_gas", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": 5, "value_max": 60, "notes": "blood gas bicarbonate"},
    {"LAB_NAME": "Base excess", "canonical_name": "BaseExcess", "clinical_group": "blood_gas", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": -40, "value_max": 40, "notes": "acid-base reserve"},
    {"LAB_NAME": "Lactate", "canonical_name": "Lactate", "clinical_group": "metabolic", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": 0.1, "value_max": 30, "notes": "hypoperfusion marker"},
    {"LAB_NAME": "Glucose", "canonical_name": "Glucose", "clinical_group": "metabolic", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mg/dL", "value_min": 20, "value_max": 1200, "notes": "perioperative glucose"},
    {"LAB_NAME": "Sodium", "canonical_name": "Sodium", "clinical_group": "electrolyte", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": 100, "value_max": 180, "notes": "major electrolyte"},
    {"LAB_NAME": "Potassium", "canonical_name": "Potassium", "clinical_group": "electrolyte", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": 1.5, "value_max": 9.0, "notes": "major electrolyte"},
    {"LAB_NAME": "Chloride", "canonical_name": "Chloride", "clinical_group": "electrolyte", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": 60, "value_max": 140, "notes": "major electrolyte"},
    {"LAB_NAME": "Ionized calcium", "canonical_name": "IonizedCalcium", "clinical_group": "electrolyte", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "mmol/L", "value_min": 0.3, "value_max": 3.0, "notes": "ionized calcium"},
    {"LAB_NAME": "Calcium", "canonical_name": "Calcium", "clinical_group": "electrolyte", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "mg/dL", "value_min": 3.0, "value_max": 15.0, "notes": "total calcium"},
    {"LAB_NAME": "Magnesium", "canonical_name": "Magnesium", "clinical_group": "electrolyte", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "mg/dL", "value_min": 0.5, "value_max": 10.0, "notes": "electrolyte support"},
    {"LAB_NAME": "Anion gap", "canonical_name": "AnionGap", "clinical_group": "metabolic", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "mmol/L", "value_min": -10, "value_max": 60, "notes": "metabolic status"},
    # Hematology
    {"LAB_NAME": "Hemoglobin", "canonical_name": "Hemoglobin", "clinical_group": "hematology", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "g/dL", "value_min": 2, "value_max": 25, "notes": "oxygen-carrying capacity"},
    {"LAB_NAME": "Hematocrit", "canonical_name": "Hematocrit", "clinical_group": "hematology", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "%", "value_min": 5, "value_max": 70, "notes": "blood concentration"},
    {"LAB_NAME": "Platelets", "canonical_name": "Platelets", "clinical_group": "hematology", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 1, "unit": "10^3/uL", "value_min": 5, "value_max": 2000, "notes": "platelet count"},
    {"LAB_NAME": "Erythrocytes", "canonical_name": "RBC", "clinical_group": "hematology", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "10^6/uL", "value_min": 0.5, "value_max": 8.0, "notes": "feature-only by default"},
    {"LAB_NAME": "Leukocytes corrected for nucleated erythrocytes", "canonical_name": "WBC", "clinical_group": "hematology", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "10^3/uL", "value_min": 0.1, "value_max": 200.0, "notes": "feature-only by default"},
    {"LAB_NAME": "Erythrocyte mean corpuscular volume", "canonical_name": "MCV", "clinical_group": "hematology", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "fL", "value_min": 50, "value_max": 140, "notes": "feature-only by default"},
    {"LAB_NAME": "Erythrocyte distribution width", "canonical_name": "RDW", "clinical_group": "hematology", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "%", "value_min": 5, "value_max": 40, "notes": "feature-only by default"},
    {"LAB_NAME": "Platelet mean volume", "canonical_name": "MPV", "clinical_group": "hematology", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "fL", "value_min": 3, "value_max": 30, "notes": "feature-only by default"},
    # Renal / chemistry
    {"LAB_NAME": "Creatinine", "canonical_name": "Creatinine", "clinical_group": "renal", "tier": "core", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "mg/dL", "value_min": 0.1, "value_max": 20, "notes": "renal function"},
    {"LAB_NAME": "Urea nitrogen", "canonical_name": "BUN", "clinical_group": "renal", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "mg/dL", "value_min": 1, "value_max": 250, "notes": "renal function"},
    {"LAB_NAME": "Albumin", "canonical_name": "Albumin", "clinical_group": "chemistry", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "g/dL", "value_min": 0.5, "value_max": 7.0, "notes": "feature-only by default"},
    {"LAB_NAME": "Bilirubin", "canonical_name": "Bilirubin", "clinical_group": "chemistry", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "mg/dL", "value_min": 0.1, "value_max": 50, "notes": "feature-only by default"},
    {"LAB_NAME": "Alanine aminotransferase", "canonical_name": "ALT", "clinical_group": "chemistry", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "U/L", "value_min": 1, "value_max": 5000, "notes": "feature-only by default"},
    {"LAB_NAME": "Aspartate aminotransferase", "canonical_name": "AST", "clinical_group": "chemistry", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "U/L", "value_min": 1, "value_max": 5000, "notes": "feature-only by default"},
    {"LAB_NAME": "Troponin i cardiac", "canonical_name": "TroponinI", "clinical_group": "cardiac_marker", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "assay_dependent", "value_min": 0, "value_max": 200, "notes": "feature-only by default"},
    {"LAB_NAME": "C reactive protein", "canonical_name": "CRP", "clinical_group": "inflammation", "tier": "extended", "include_for_main_timeseries": False, "extract_priority": 3, "unit": "mg/L", "value_min": 0, "value_max": 500, "notes": "feature-only by default"},
    # Coagulation
    {"LAB_NAME": "Activated clotting time", "canonical_name": "ACT", "clinical_group": "coagulation", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "sec", "value_min": 50, "value_max": 2000, "notes": "highly relevant in anticoagulated cases"},
    {"LAB_NAME": "Coagulation tissue factor induced", "canonical_name": "PT", "clinical_group": "coagulation", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "sec", "value_min": 5, "value_max": 150, "notes": "PT-like measure"},
    {"LAB_NAME": "Coagulation tissue factor induced inr", "canonical_name": "INR", "clinical_group": "coagulation", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "ratio", "value_min": 0.5, "value_max": 20, "notes": "INR-like measure"},
    {"LAB_NAME": "Coagulation surface induced", "canonical_name": "aPTT", "clinical_group": "coagulation", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "sec", "value_min": 10, "value_max": 200, "notes": "aPTT-like measure"},
    {"LAB_NAME": "Fibrinogen", "canonical_name": "Fibrinogen", "clinical_group": "coagulation", "tier": "extended", "include_for_main_timeseries": True, "extract_priority": 2, "unit": "mg/dL", "value_min": 20, "value_max": 1200, "notes": "hemostasis marker"},
]


def main() -> None:
    if not STATS_FILE.exists():
        raise FileNotFoundError(f"Missing input stats: {STATS_FILE}")
    if not QRULES_FILE.exists():
        raise FileNotFoundError(f"Missing input quantile rules: {QRULES_FILE}")

    stats = pd.read_csv(STATS_FILE)
    qrules = pd.read_csv(QRULES_FILE)
    rules = pd.DataFrame(LAB_RULES)

    merged = rules.merge(
        stats[
            [
                "LAB_NAME",
                "pre_obs_n",
                "pre_case_n",
                "post_obs_n",
                "post_case_n",
                "drop_pct",
                "pre_p01",
                "pre_p50",
                "pre_p99",
                "pre_max",
                "post_p01",
                "post_p50",
                "post_p99",
                "post_max",
            ]
        ],
        on="LAB_NAME",
        how="left",
    )
    merged = merged.merge(
        qrules[["LAB_NAME", "q001", "q999", "obs_n"]].rename(columns={"obs_n": "obs_n_qrule"}),
        on="LAB_NAME",
        how="left",
    )

    max_case_n = np.nanmax(stats["pre_case_n"].values)
    merged["coverage_pct_in_labs"] = np.where(
        max_case_n > 0, merged["pre_case_n"] / max_case_n * 100.0, np.nan
    )

    merged["range_rule_type"] = "hard_range"
    merged["sentinel_values"] = "9999999|999999|99999|9999|-9999999|-999999|-99999|-9999"
    merged["recommendation"] = np.where(
        merged["include_for_main_timeseries"],
        "main_timeseries_candidate",
        "feature_or_event_only",
    )

    # Order for readability
    merged = merged.sort_values(
        by=["extract_priority", "tier", "clinical_group", "pre_case_n"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)

    merged.to_csv(OUT_RULES, index=False)
    merged[
        [
            "LAB_NAME",
            "canonical_name",
            "clinical_group",
            "tier",
            "include_for_main_timeseries",
            "extract_priority",
            "pre_obs_n",
            "pre_case_n",
            "post_obs_n",
            "post_case_n",
            "coverage_pct_in_labs",
            "drop_pct",
        ]
    ].to_csv(OUT_STATS, index=False)

    missing = merged[merged["pre_case_n"].isna()]["LAB_NAME"].tolist()
    summary_lines = [
        "# labs_extended_v1 summary",
        "",
        f"- total selected variables: {len(merged)}",
        f"- core: {(merged['tier'] == 'core').sum()}",
        f"- extended: {(merged['tier'] == 'extended').sum()}",
        f"- main timeseries candidates: {merged['include_for_main_timeseries'].sum()}",
        f"- feature/event only: {(~merged['include_for_main_timeseries']).sum()}",
        "",
        "Clinical groups:",
    ]
    grp = (
        merged.groupby("clinical_group")
        .agg(var_n=("LAB_NAME", "size"), main_n=("include_for_main_timeseries", "sum"))
        .reset_index()
        .sort_values("var_n", ascending=False)
    )
    for _, r in grp.iterrows():
        summary_lines.append(
            f"- {r['clinical_group']}: {int(r['var_n'])} vars ({int(r['main_n'])} main-timeseries)"
        )

    summary_lines.extend(
        [
            "",
            "Outputs:",
            f"- {OUT_RULES.name}",
            f"- {OUT_STATS.name}",
            "",
            "Notes:",
            "- This file extends strict v1 with additional low-frequency but clinically meaningful labs.",
            "- Main-timeseries and feature-only are split to avoid overly sparse wide-table columns.",
            "- Sentinel handling is inherited from labs_clean_v1 rules.",
        ]
    )
    if missing:
        summary_lines.append("")
        summary_lines.append("Variables not found in stats (check naming):")
        for x in missing:
            summary_lines.append(f"- {x}")

    OUT_README.write_text("\n".join(summary_lines))
    print(f"Saved: {OUT_RULES}")
    print(f"Saved: {OUT_STATS}")
    print(f"Saved: {OUT_README}")


if __name__ == "__main__":
    main()

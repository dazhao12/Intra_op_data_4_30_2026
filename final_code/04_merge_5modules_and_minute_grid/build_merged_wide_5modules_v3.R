#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

root <- "/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026"
v2_dir <- file.path(root, "v2_module_outputs")
dict_file <- file.path(root, "variable_dictionary/timeseries_extraction_dictionary_v1.csv")
tier_file <- file.path(v2_dir, "dictionary_5modules/variables_drop_recommended_v2.csv")

out_dir <- file.path(v2_dir, "merged_5modules_v3")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

module_long <- list(
  vitals = file.path(v2_dir, "long/intraop_vitals_long_clean_v2.parquet"),
  labs = file.path(v2_dir, "long/intraop_labs_long_clean_extended_v2.parquet"),
  io = file.path(v2_dir, "long/intraop_io_long_clean_v2.parquet"),
  respiratory = file.path(v2_dir, "long/intraop_respiratory_long_clean_v2.parquet"),
  neuro = file.path(v2_dir, "long/intraop_neuro_long_clean_v2.parquet")
)

load_module_long <- function(module, path) {
  dt <- as.data.table(read_parquet(path))
  dt[, table := module]
  dt[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
  dt <- dt[!is.na(LOG_ID) & !is.na(RECORDED_TIME) & !is.na(variable) & is.finite(value)]
  dt
}

dict <- as.data.table(fread(dict_file))
dict <- dict[tolower(table) %in% names(module_long)]
dict[, table := tolower(table)]
dict[, merge_group := tolower(as.character(merge_group))]
dict[, source_rank := as.numeric(source_rank)]
map <- unique(dict[, .(table, variable, canonical_name, merge_group, source_rank, valid_min, valid_max)])

drop_vars <- NULL
if (file.exists(tier_file)) {
  ddrop <- as.data.table(fread(tier_file))
  drop_vars <- unique(ddrop[, .(module, variable)])
  drop_vars[, module := as.character(module)]
  # Keep these low-frequency hemodynamic variables per user request.
  keep_even_if_dropped <- data.table(
    module = rep("vitals", 4),
    variable = c("CI", "CO", "SV", "SVR")
  )
  drop_vars <- drop_vars[!keep_even_if_dropped, on = .(module, variable)]
}

exclude_temp_labs <- c(
  "Carbon dioxide adjusted to patient s actual temperature",
  "Oxygen adjusted to patient s actual temperature"
)

frames <- list()
for (m in names(module_long)) {
  f <- module_long[[m]]
  if (!file.exists(f)) next
  message("[INFO] load module long: ", m)
  frames[[length(frames) + 1]] <- load_module_long(m, f)
}
raw <- rbindlist(frames, fill = TRUE)
merged <- merge(raw, map, by = c("table", "variable"), all = FALSE)

if (!is.null(drop_vars) && nrow(drop_vars) > 0) {
  merged <- merged[!drop_vars, on = .(table = module, variable = variable)]
}
merged <- merged[!(table == "labs" & variable %in% exclude_temp_labs)]
merged <- merged[!(canonical_name == "Temperature" & (value < 30 | value > 43))]
merged <- merged[(is.na(valid_min) | value >= valid_min) & (is.na(valid_max) | value <= valid_max)]

# BP split override: avoid mixing SBP/MAP/DBP into one canonical.
bp_override <- data.table(
  variable = c(
    "NBP_SBP", "NBP_MAP", "NBP_DBP",
    "VITAL_UC_ANE_R_BLOOD_PRESSURE_MAP", "VITAL_MODEL_R_MAP_CUFF", "BP_MAP",
    "IBP_SBP", "IBP_MAP", "IBP_DBP",
    "VITAL_UC_ANE_R_ARTERIAL_LINE_MAP_ART", "VITAL_MODEL_R_MAP_A_LINE_2"
  ),
  canonical_name_bp = c(
    "NBP_SBP", "NBP_MAP", "NBP_DBP",
    "NBP_MAP", "NBP_MAP", "NBP_MAP",
    "IBP_SBP", "IBP_MAP", "IBP_DBP",
    "IBP_MAP", "IBP_MAP"
  ),
  merge_group_bp = c(
    "nbp_sbp", "nbp_map", "nbp_dbp",
    "nbp_map", "nbp_map", "nbp_map",
    "ibp_sbp", "ibp_map", "ibp_dbp",
    "ibp_map", "ibp_map"
  ),
  source_rank_bp = c(
    1, 1, 1,
    2, 3, 4,
    1, 1, 1,
    2, 3
  )
)
merged <- merge(merged, bp_override, by = "variable", all.x = TRUE)
merged[!is.na(canonical_name_bp), canonical_name := canonical_name_bp]
merged[!is.na(merge_group_bp), merge_group := merge_group_bp]
merged[!is.na(source_rank_bp), source_rank := source_rank_bp]
merged[, c("canonical_name_bp", "merge_group_bp", "source_rank_bp") := NULL]

# One value per source variable per minute.
merged <- merged[, .(value = median(value, na.rm = TRUE)), by = .(
  LOG_ID, RECORDED_TIME, table, variable, canonical_name, merge_group, source_rank
)]

# Source conflict resolution.
table_order <- c("vitals", "respiratory", "neuro", "io", "labs")
merged[, table_pref := match(table, table_order)]
setorder(merged, LOG_ID, RECORDED_TIME, merge_group, source_rank, table_pref)
final_long <- merged[, .SD[1], by = .(LOG_ID, RECORDED_TIME, merge_group)]
setnames(final_long, "table", "source_table")
setnames(final_long, "variable", "source_variable")

final_long_out <- final_long[, .(
  LOG_ID, RECORDED_TIME, merge_group, canonical_name, value, source_table, source_variable, source_rank
)]

# Drop the last three requested "Other_*" channels from final table.
drop_canonical_v3 <- c(
  "Other_Vital_Uci_Ane_Gas_Analyzer_N2O_Etn20",
  "Other_Vital_Uci_Ane_R_Fico2",
  "Other_Vital_Uci_Ane_R_Pulmonary_Artery_Wedge_Pressure"
)
final_long_out <- final_long_out[!canonical_name %in% drop_canonical_v3]

# Simplify retained Other_* names (no Backup prefix).
rename_map_v3 <- data.table(
  old = c(
    "Other_Agent_Des", "Other_Agent_Iso", "Other_Agent_Sevo",
    "Other_Blood_Products", "Other_Io_Uc_Ane_R_Other_Output", "Other_Resp_Rate",
    "Other_Resp_Uc_Ane_R_02_Flowrate", "Other_Resp_Uc_Ane_R_Agents_Air", "Other_Resp_Uc_Ane_R_Tof",
    "Other_Vital_Uc_Ane_R_02_Flowrate", "Other_Vital_Uc_Ane_R_Agents_Air", "Other_Vital_Uc_R_Pain_Intensity_Score",
    "Other_Ci", "Other_Co", "Other_Sv", "Other_Svr"
  ),
  new = c(
    "Desflurane_alt", "Isoflurane_alt", "Sevoflurane_alt",
    "Blood_products_alt", "Other_output_volume", "Respiratory_rate_alt",
    "O2_flow_respiratory_alt", "Air_signal_respiratory_alt", "Tof_alt",
    "O2_flow_vital_alt", "Air_signal_vital_alt", "Pain_score_alt",
    "CI", "CO", "SV", "SVR"
  )
)
final_long_out <- merge(final_long_out, rename_map_v3, by.x = "canonical_name", by.y = "old", all.x = TRUE)
final_long_out[!is.na(new), canonical_name := new]
final_long_out[, new := NULL]

setorder(final_long_out, LOG_ID, RECORDED_TIME, canonical_name)

# Build wide with split BP + other variables.
wide_base <- dcast(
  final_long_out[, .(LOG_ID, RECORDED_TIME, canonical_name, value)],
  LOG_ID + RECORDED_TIME ~ canonical_name,
  value.var = "value"
)
setorder(wide_base, LOG_ID, RECORDED_TIME)

# Build BP fused columns with invasive-first fallback.
for (cname in c("IBP_SBP", "IBP_MAP", "IBP_DBP", "NBP_SBP", "NBP_MAP", "NBP_DBP")) {
  if (!cname %in% names(wide_base)) wide_base[, (cname) := NA_real_]
}

# Vital_DB-aligned preclean on BP channels:
# - 0 -> NA for IBP channels
# - physiological clip ranges
wide_base[, `:=`(
  IBP_SBP = fifelse(IBP_SBP == 0, NA_real_, IBP_SBP),
  IBP_MAP = fifelse(IBP_MAP == 0, NA_real_, IBP_MAP),
  IBP_DBP = fifelse(IBP_DBP == 0, NA_real_, IBP_DBP)
)]

wide_base[!is.na(IBP_SBP) & (IBP_SBP < 30 | IBP_SBP > 280), IBP_SBP := NA_real_]
wide_base[!is.na(IBP_MAP) & (IBP_MAP < 20 | IBP_MAP > 220), IBP_MAP := NA_real_]
wide_base[!is.na(IBP_DBP) & (IBP_DBP < 10 | IBP_DBP > 180), IBP_DBP := NA_real_]

wide_base[!is.na(NBP_SBP) & (NBP_SBP < 40 | NBP_SBP > 260), NBP_SBP := NA_real_]
wide_base[!is.na(NBP_MAP) & (NBP_MAP < 30 | NBP_MAP > 200), NBP_MAP := NA_real_]
wide_base[!is.na(NBP_DBP) & (NBP_DBP < 20 | NBP_DBP > 160), NBP_DBP := NA_real_]

# IBP quality gate before "IBP-first" fusion.
hard_equal <- !is.na(wide_base$IBP_SBP) & !is.na(wide_base$IBP_MAP) & !is.na(wide_base$IBP_DBP) &
  (wide_base$IBP_SBP == wide_base$IBP_MAP) & (wide_base$IBP_MAP == wide_base$IBP_DBP)
hard_order <- (!is.na(wide_base$IBP_SBP) & !is.na(wide_base$IBP_MAP) & (wide_base$IBP_SBP <= wide_base$IBP_MAP)) |
  (!is.na(wide_base$IBP_MAP) & !is.na(wide_base$IBP_DBP) & (wide_base$IBP_MAP <= wide_base$IBP_DBP)) |
  (!is.na(wide_base$IBP_SBP) & !is.na(wide_base$IBP_DBP) & (wide_base$IBP_SBP <= wide_base$IBP_DBP))

wide_base[, diff_map := fifelse(!is.na(IBP_MAP) & !is.na(NBP_MAP), abs(IBP_MAP - NBP_MAP), NA_real_)]
wide_base[, diff_ge40 := !is.na(diff_map) & diff_map >= 40]
wide_base[, diff_ge30 := !is.na(diff_map) & diff_map >= 30]
wide_base[, grp40 := rleid(LOG_ID, diff_ge40)]
wide_base[, grp30 := rleid(LOG_ID, diff_ge30)]
wide_base[, run40 := fifelse(diff_ge40, .N, 0L), by = .(LOG_ID, grp40)]
wide_base[, run30 := fifelse(diff_ge30, .N, 0L), by = .(LOG_ID, grp30)]
suspect_diff <- (wide_base$diff_ge40 & wide_base$run40 >= 3) | (wide_base$diff_ge30 & wide_base$run30 >= 5)

wide_base[, IBP_qc_reason := fcase(
  hard_equal, "ibp_flatline_equal",
  hard_order, "ibp_triplet_order_invalid",
  diff_ge40 & run40 >= 3, "ibp_nbp_gap_ge40_run3",
  diff_ge30 & run30 >= 5, "ibp_nbp_gap_ge30_run5",
  default = "ibp_ok_or_missing"
)]
wide_base[, IBP_is_valid := !(hard_equal | hard_order | suspect_diff)]

wide_base[, BP_SBP := fifelse(IBP_is_valid & !is.na(IBP_SBP), IBP_SBP, NBP_SBP)]
wide_base[, BP_MAP := fifelse(IBP_is_valid & !is.na(IBP_MAP), IBP_MAP, NBP_MAP)]
wide_base[, BP_DBP := fifelse(IBP_is_valid & !is.na(IBP_DBP), IBP_DBP, NBP_DBP)]

wide_base[, BP_SBP_source := fifelse(IBP_is_valid & !is.na(IBP_SBP), "IBP", fifelse(!is.na(NBP_SBP), "NBP", NA_character_))]
wide_base[, BP_MAP_source := fifelse(IBP_is_valid & !is.na(IBP_MAP), "IBP", fifelse(!is.na(NBP_MAP), "NBP", NA_character_))]
wide_base[, BP_DBP_source := fifelse(IBP_is_valid & !is.na(IBP_DBP), "IBP", fifelse(!is.na(NBP_DBP), "NBP", NA_character_))]

wide_base[, BP_qc_flag := fifelse(BP_MAP_source == "IBP", "ibp_used",
                           fifelse(BP_MAP_source == "NBP", "nbp_fallback", "bp_missing"))]

# Enforce SBP >= MAP >= DBP for fused BP.
pre_triplet_n <- wide_base[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP), .N]
pre_violate_n <- wide_base[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP) & !(BP_SBP >= BP_MAP & BP_MAP >= BP_DBP), .N]

# strict triplet filter
wide_base[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP) & !(BP_SBP >= BP_MAP & BP_MAP >= BP_DBP),
          c("BP_SBP", "BP_MAP", "BP_DBP", "BP_SBP_source", "BP_MAP_source", "BP_DBP_source") := .(NA_real_, NA_real_, NA_real_, NA_character_, NA_character_, NA_character_)]

# pair-wise cleanup
wide_base[!is.na(BP_SBP) & !is.na(BP_MAP) & BP_SBP < BP_MAP, BP_MAP := NA_real_]
wide_base[!is.na(BP_MAP) & !is.na(BP_DBP) & BP_MAP < BP_DBP, BP_MAP := NA_real_]
wide_base[!is.na(BP_SBP) & !is.na(BP_DBP) & BP_SBP < BP_DBP, `:=`(BP_SBP = NA_real_, BP_DBP = NA_real_)]

post_violate_n <- wide_base[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP) & !(BP_SBP >= BP_MAP & BP_MAP >= BP_DBP), .N]

ibp_invalid_n <- wide_base[IBP_qc_reason != "ibp_ok_or_missing", .N]
ibp_fallback_n <- wide_base[BP_qc_flag == "nbp_fallback", .N]

wide_base[, c("diff_map", "diff_ge40", "diff_ge30", "grp40", "grp30", "run40", "run30", "IBP_is_valid") := NULL]

# Source wide table for canonical variables
wide_src <- dcast(
  final_long_out[, .(LOG_ID, RECORDED_TIME, canonical_name, source_table)],
  LOG_ID + RECORDED_TIME ~ canonical_name,
  value.var = "source_table"
)
setnames(wide_src, old = setdiff(names(wide_src), c("LOG_ID", "RECORDED_TIME")),
         new = paste0(setdiff(names(wide_src), c("LOG_ID", "RECORDED_TIME")), "_source"))

# Reorder columns by clinical workflow part.
key_cols <- c("LOG_ID", "RECORDED_TIME")
core_vitals <- c("HR", "SpO2", "Temperature", "Resp_rate")
vent_gas <- c("EtCO2", "FiO2", "PEEP", "PIP", "Tidal_volume", "Minute_volume")
anesthetic <- c("Sevoflurane", "Isoflurane", "Desflurane", "Nitric_oxide")
bp_block <- c("BP_SBP", "BP_MAP", "BP_DBP", "IBP_SBP", "IBP_MAP", "IBP_DBP", "NBP_SBP", "NBP_MAP", "NBP_DBP")
io_block <- c("Intake_fluid", "Urine_output", "EBL", "Blood_products_alt", "Other_output_volume")
labs_block <- c(
  "Ph", "Carbon_Dioxide", "Bicarbonate", "Oxygen", "Oxygen_Saturation",
  "Hemoglobin", "Hematocrit", "Platelets", "Erythrocytes",
  "Sodium", "Potassium", "Calcium", "Ionized_Calcium", "Chloride",
  "Glucose", "Creatinine", "Urea_Nitrogen", "Base_Excess", "Magnesium",
  "Albumin", "Bilirubin", "Lactate", "Fibrinogen"
)
aux_other <- c(
  "Desflurane_alt", "Isoflurane_alt", "Sevoflurane_alt", "Respiratory_rate_alt",
  "O2_flow_respiratory_alt", "Air_signal_respiratory_alt", "Tof_alt",
  "O2_flow_vital_alt", "Air_signal_vital_alt", "Pain_score_alt",
  "CI", "CO", "SV", "SVR"
)

preferred_cols <- unique(c(key_cols, core_vitals, vent_gas, anesthetic, bp_block, io_block, labs_block, aux_other))
preferred_cols <- preferred_cols[preferred_cols %in% names(wide_base)]
ordered_cols <- c(preferred_cols, setdiff(names(wide_base), preferred_cols))
setcolorder(wide_base, ordered_cols)

src_order <- paste0(setdiff(preferred_cols, key_cols), "_source")
src_order <- src_order[src_order %in% names(wide_src)]
ordered_src_cols <- c(key_cols, src_order, setdiff(names(wide_src), c(key_cols, src_order)))
setcolorder(wide_src, ordered_src_cols)

# Save
write_parquet(final_long_out, file.path(out_dir, "intraop_merged_5modules_long_v3.parquet"))
write_parquet(wide_base, file.path(out_dir, "intraop_merged_5modules_wide_v3.parquet"))
write_parquet(wide_src, file.path(out_dir, "intraop_merged_5modules_wide_source_v3.parquet"))

# Rule table
rule_summary <- unique(dict[, .(merge_group, canonical_name, table, variable, source_rank)])
if (!is.null(drop_vars) && nrow(drop_vars) > 0) {
  rule_summary <- rule_summary[!drop_vars, on = .(table = module, variable = variable)]
}
rule_summary <- rule_summary[!(table == "vitals" & variable %in% c("VITAL_UCI_ANE_GAS_ANALYZER_N2O_ETN20", "VITAL_UCI_ANE_R_FICO2", "VITAL_UCI_ANE_R_PULMONARY_ARTERY_WEDGE_PRESSURE"))]
rule_summary <- rule_summary[!(table == "labs" & variable %in% exclude_temp_labs)]
rule_summary <- rbind(
  rule_summary,
  data.table(
    merge_group = c("nbp_sbp", "nbp_map", "nbp_dbp", "ibp_sbp", "ibp_map", "ibp_dbp", "bp_fused"),
    canonical_name = c("NBP_SBP", "NBP_MAP", "NBP_DBP", "IBP_SBP", "IBP_MAP", "IBP_DBP", "BP_SBP/BP_MAP/BP_DBP"),
    table = c("vitals", "vitals", "vitals", "vitals", "vitals", "vitals", "derived"),
    variable = c("NBP_*", "NBP_*+CUFF_MAP", "NBP_*", "IBP_*", "IBP_*+A_LINE_MAP", "IBP_*", "IBP first then NBP; enforce SBP>=MAP>=DBP"),
    source_rank = c(1, 1, 1, 1, 1, 1, NA_real_)
  ),
  fill = TRUE
)
setorder(rule_summary, merge_group, source_rank, table, variable)
fwrite(rule_summary, file.path(out_dir, "merge_rules_applied_v3.csv"))

# QC
dup_long <- final_long_out[, .N, by = .(LOG_ID, RECORDED_TIME, canonical_name)][N > 1, .N]
dup_wide <- wide_base[, .N, by = .(LOG_ID, RECORDED_TIME)][N > 1, .N]
source_use <- final_long_out[, .N, by = .(canonical_name, source_table)][order(canonical_name, -N)]
fwrite(source_use, file.path(out_dir, "source_usage_by_variable_v3.csv"))

qc <- data.table(
  metric = c(
    "rows_long_final", "rows_wide_final", "vars_wide_final", "cases_n",
    "dup_long_keys", "dup_wide_keys",
    "bp_triplet_rows_pre", "bp_triplet_violation_pre",
    "bp_triplet_violation_post",
    "ibp_invalid_rows",
    "bp_nbp_fallback_rows"
  ),
  value = c(
    nrow(final_long_out), nrow(wide_base), ncol(wide_base) - 2, uniqueN(final_long_out$LOG_ID),
    dup_long, dup_wide,
    pre_triplet_n, pre_violate_n, post_violate_n,
    ibp_invalid_n, ibp_fallback_n
  )
)
fwrite(qc, file.path(out_dir, "qc_merged_summary_v3.csv"))

readme <- c(
  "# merged_5modules_v3",
  "",
  "- BP split: NBP_SBP/NBP_MAP/NBP_DBP and IBP_SBP/IBP_MAP/IBP_DBP are kept separate",
  "- BP fused: BP_SBP/BP_MAP/BP_DBP derived with IBP quality gate first, then fallback to NBP",
  "- Vital_DB alignment: BP zero->NA, BP physiological clip ranges, and IBP-vs-NBP persistent gap check",
  "- BP consistency: enforce SBP >= MAP >= DBP",
  "- Wide column order: clinical workflow blocks (core vitals -> ventilation/gas -> anesthetic agents -> BP -> IO -> labs -> aux/other)",
  "- CI/CO/SV/SVR kept (user request)",
  "- Dropped channels: Other_Vital_Uci_Ane_Gas_Analyzer_N2O_Etn20, Other_Vital_Uci_Ane_R_Fico2, Other_Vital_Uci_Ane_R_Pulmonary_Artery_Wedge_Pressure",
  "",
  "Outputs:",
  "- intraop_merged_5modules_long_v3.parquet",
  "- intraop_merged_5modules_wide_v3.parquet",
  "- intraop_merged_5modules_wide_source_v3.parquet",
  "- merge_rules_applied_v3.csv",
  "- source_usage_by_variable_v3.csv",
  "- qc_merged_summary_v3.csv"
)
writeLines(readme, con = file.path(out_dir, "README_merged_5modules_v3.md"))

message("[DONE] merged v3 outputs saved to: ", out_dir)

#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

root <- "/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026"
cohort_file <- "/N/project/analgesia_perioperation/data/MOVER/processed/final_single_mrn_single_login_with_definable_intraop_time.csv"
in_wide_file <- file.path(root, "v2_module_outputs/merged_5modules_v3/intraop_merged_5modules_wide_v3.parquet")
out_dir <- file.path(root, "v2_module_outputs/minute_grid_v3")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_grid_full_file <- file.path(out_dir, "minute_grid_full_v3.parquet")
out_grid_core_file <- file.path(out_dir, "minute_grid_core_v3.parquet")
out_grid_aux_file <- file.path(out_dir, "minute_grid_aux_v3.parquet")
out_qc_file <- file.path(out_dir, "qc_minute_grid_v3.csv")
out_missing_full_file <- file.path(out_dir, "missingness_minute_grid_full_v3.csv")
out_missing_core_file <- file.path(out_dir, "missingness_minute_grid_core_v3.csv")
out_missing_aux_file <- file.path(out_dir, "missingness_minute_grid_aux_v3.csv")
out_readme <- file.path(out_dir, "README_minute_grid_v3.md")

parse_time <- function(x) suppressWarnings(as.POSIXct(x, tz = "UTC"))

message("[INFO] load cohort: ", cohort_file)
coh <- as.data.table(fread(cohort_file, select = c(
  "LOG_ID", "anesthesia_start_time", "anesthesia_stop_time", "or_in_time", "or_out_time"
)))
coh[, start_time := parse_time(anesthesia_start_time)]
coh[, stop_time := parse_time(anesthesia_stop_time)]
coh[is.na(start_time), start_time := parse_time(or_in_time)]
coh[is.na(stop_time), stop_time := parse_time(or_out_time)]
coh <- coh[!is.na(LOG_ID) & !is.na(start_time) & !is.na(stop_time)]
coh <- coh[stop_time >= start_time]
coh[, duration_min := as.integer(floor(as.numeric(difftime(stop_time, start_time, units = "mins"))))]
coh <- coh[duration_min >= 0]
setkey(coh, LOG_ID)

message("[INFO] cohort cases for grid: ", nrow(coh))

message("[INFO] build minute grid (LOG_ID + minute_index)")
grid <- rbindlist(
  lapply(seq_len(nrow(coh)), function(i) {
    x <- coh[i]
    idx <- 0:x$duration_min
    data.table(
      LOG_ID = x$LOG_ID,
      minute_index = idx,
      minute_time = x$start_time + idx * 60
    )
  }),
  use.names = TRUE
)
setkey(grid, LOG_ID)

message("[INFO] grid rows: ", nrow(grid))

message("[INFO] load merged wide: ", in_wide_file)
w <- as.data.table(read_parquet(in_wide_file))
w[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
setkey(w, LOG_ID)

# Map observed timestamps onto relative minute_index
message("[INFO] map observed RECORDED_TIME -> minute_index (no imputation)")
obs <- w[coh, on = "LOG_ID", nomatch = 0L]
obs[, minute_index := as.integer(floor(as.numeric(difftime(RECORDED_TIME, start_time, units = "mins"))))]
obs <- obs[minute_index >= 0 & minute_index <= duration_min]

# In rare cases multiple rows fall into same minute_index, keep one per minute by RECORDED_TIME nearest to minute start.
obs[, minute_target := start_time + minute_index * 60]
obs[, dt_abs := abs(as.numeric(difftime(RECORDED_TIME, minute_target, units = "secs")))]
setorder(obs, LOG_ID, minute_index, dt_abs, RECORDED_TIME)
obs <- obs[, .SD[1], by = .(LOG_ID, minute_index)]

keep_cols <- setdiff(names(w), c("LOG_ID", "RECORDED_TIME"))
obs_cols <- c("LOG_ID", "minute_index", intersect(keep_cols, names(obs)))
obs2 <- obs[, ..obs_cols]
obs2 <- unique(obs2, by = c("LOG_ID", "minute_index"))
setkey(obs2, LOG_ID, minute_index)

setkey(grid, LOG_ID, minute_index)
grid_wide <- obs2[grid]
setorder(grid_wide, LOG_ID, minute_index)

# Bring minute_time to front
setcolorder(grid_wide, c("LOG_ID", "minute_index", "minute_time", setdiff(names(grid_wide), c("LOG_ID", "minute_index", "minute_time"))))

keys <- c("LOG_ID", "minute_index", "minute_time")

# Core/Aux split for modeling convenience.
core_vars <- c(
  "HR", "SpO2", "Temperature", "Resp_rate",
  "EtCO2", "FiO2", "PEEP", "PIP", "Tidal_volume", "Minute_volume",
  "Sevoflurane", "Isoflurane", "Desflurane", "Nitric_oxide",
  "BP_SBP", "BP_MAP", "BP_DBP", "IBP_SBP", "IBP_MAP", "IBP_DBP", "NBP_SBP", "NBP_MAP", "NBP_DBP",
  "BP_SBP_source", "BP_MAP_source", "BP_DBP_source",
  "Intake_fluid", "Urine_output", "EBL",
  "CI", "CO", "SV", "SVR"
)
core_vars <- core_vars[core_vars %in% names(grid_wide)]
aux_vars <- setdiff(names(grid_wide), c(keys, core_vars))

grid_full <- grid_wide
grid_core <- grid_wide[, c(keys, core_vars), with = FALSE]
grid_aux <- grid_wide[, c(keys, aux_vars), with = FALSE]

message("[INFO] save full/core/aux minute grids")
write_parquet(grid_full, out_grid_full_file)
write_parquet(grid_core, out_grid_core_file)
write_parquet(grid_aux, out_grid_aux_file)

# QC
expected_rows <- coh[, sum(duration_min + 1L)]
actual_rows <- nrow(grid_full)
dup_keys <- grid_full[, .N, by = .(LOG_ID, minute_index)][N > 1, .N]
var_cols_full <- setdiff(names(grid_full), keys)
var_cols_core <- setdiff(names(grid_core), keys)
var_cols_aux <- setdiff(names(grid_aux), keys)
qc <- data.table(
  metric = c("cases_n", "expected_rows", "actual_rows", "dup_keys", "variables_full_n", "variables_core_n", "variables_aux_n"),
  value = c(nrow(coh), expected_rows, actual_rows, dup_keys, length(var_cols_full), length(var_cols_core), length(var_cols_aux))
)
fwrite(qc, out_qc_file)

build_missing <- function(dt, vars) {
  miss <- data.table(variable = vars)
  miss[, nonmissing_n := vapply(variable, function(v) sum(!is.na(dt[[v]])), FUN.VALUE = integer(1))]
  miss[, missing_n := nrow(dt) - nonmissing_n]
  miss[, missing_pct := if (nrow(dt) > 0) missing_n / nrow(dt) * 100 else NA_real_]
  setorder(miss, -missing_pct, variable)
  miss
}

fwrite(build_missing(grid_full, var_cols_full), out_missing_full_file)
fwrite(build_missing(grid_core, var_cols_core), out_missing_core_file)
fwrite(build_missing(grid_aux, var_cols_aux), out_missing_aux_file)

readme <- c(
  "# minute_grid_v3",
  "",
  "- Time axis: LOG_ID + minute_index (relative to intraop start_time).",
  "- Grid coverage: minute_index = 0..duration_min for each LOG_ID.",
  "- Mapping: RECORDED_TIME from merged wide v3 mapped by floor((RECORDED_TIME-start_time)/60).",
  "- Missing handling: no forward/backward fill, no interpolation, no nearest-neighbor future borrowing.",
  "- Cleaning: relies on upstream anomaly-cleaned merged wide v3 (no extra imputation here).",
  "- Layered outputs: full table + core table + aux table.",
  "",
  "Outputs:",
  "- minute_grid_full_v3.parquet",
  "- minute_grid_core_v3.parquet",
  "- minute_grid_aux_v3.parquet",
  "- qc_minute_grid_v3.csv",
  "- missingness_minute_grid_full_v3.csv",
  "- missingness_minute_grid_core_v3.csv",
  "- missingness_minute_grid_aux_v3.csv"
)
writeLines(readme, out_readme)

message("[DONE] minute grid v3 (full/core/aux) saved: ", out_dir)

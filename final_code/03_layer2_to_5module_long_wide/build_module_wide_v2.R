#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

base_dir <- "/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026"
dict_file <- file.path(base_dir, "variable_dictionary/timeseries_extraction_dictionary_v1.csv")
labs_rules_file <- file.path(base_dir, "labs_clean_v1/labs_extended_v1_keep_variables_and_rules.csv")

out_dir <- file.path(base_dir, "v2_module_outputs")
out_long_dir <- file.path(out_dir, "long")
out_wide_dir <- file.path(out_dir, "wide")
dir.create(out_long_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_wide_dir, recursive = TRUE, showWarnings = FALSE)

global_sentinels <- c(9999999, 999999, 99999, 9999, -9999, -99999, -999999, -9999999)

message("[INFO] loading dictionary: ", dict_file)
dict <- as.data.table(fread(dict_file))
dict[, table := tolower(table)]
dict[, variable := as.character(variable)]
vitals_main_vars <- unique(dict[table == "vitals" & include_for_timeseries == TRUE, variable])

minute_floor <- function(x) {
  # Keep second precision out; align to one-minute bins.
  as.POSIXct(format(x, "%Y-%m-%d %H:%M:00", tz = "UTC"), tz = "UTC")
}

safe_parse_time <- function(x) {
  x <- as.character(x)
  t1 <- suppressWarnings(as.POSIXct(x, tz = "UTC", format = "%Y-%m-%d %H:%M:%S"))
  miss <- is.na(t1)
  if (any(miss)) t1[miss] <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC", format = "%Y-%m-%d %H:%M:%OS"))
  miss <- is.na(t1)
  if (any(miss)) t1[miss] <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC", format = "%m/%d/%y %H:%M"))
  miss <- is.na(t1)
  if (any(miss)) t1[miss] <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC", format = "%m/%d/%Y %H:%M"))
  t1
}

apply_range_override <- function(dt, override_dt) {
  if (is.null(override_dt) || nrow(override_dt) == 0) return(dt)
  out <- merge(dt, override_dt, by = "variable", all.x = TRUE)
  out[, lo_use := fifelse(!is.na(vd_min), vd_min, valid_min)]
  out[, hi_use := fifelse(!is.na(vd_max), vd_max, valid_max)]
  out[(is.na(lo_use) | value >= lo_use) & (is.na(hi_use) | value <= hi_use)]
}

mark_frozen_minute_runs <- function(dt_minute, target_vars, min_run_minutes = 15L) {
  if (is.null(dt_minute) || nrow(dt_minute) == 0 || length(target_vars) == 0) return(dt_minute)
  dt <- copy(dt_minute)
  dt[, row_id_tmp := .I]
  sub <- dt[variable %in% target_vars]
  if (nrow(sub) == 0) {
    dt[, row_id_tmp := NULL]
    return(dt)
  }
  setorder(sub, LOG_ID, variable, RECORDED_TIME)
  sub[, grp := rleid(value), by = .(LOG_ID, variable)]
  sub[, run_n := .N, by = .(LOG_ID, variable, grp)]
  sub[!is.na(value) & run_n >= min_run_minutes, value := NA_real_]
  sub <- sub[, .(row_id_tmp, value_new = value)]
  dt <- merge(dt, sub, by = "row_id_tmp", all.x = TRUE)
  dt[!is.na(value_new), value := value_new]
  dt[, c("value_new", "row_id_tmp") := NULL]
  dt
}

apply_bp_qc_and_fuse <- function(wide) {
  need <- c("IBP_SBP", "IBP_MAP", "IBP_DBP", "NBP_SBP", "NBP_MAP", "NBP_DBP")
  for (cname in need) if (!cname %in% names(wide)) wide[, (cname) := NA_real_]

  # Vital_DB baseline + requested stricter MAP gate.
  wide[, `:=`(
    IBP_SBP = fifelse(IBP_SBP == 0, NA_real_, IBP_SBP),
    IBP_MAP = fifelse(IBP_MAP == 0, NA_real_, IBP_MAP),
    IBP_DBP = fifelse(IBP_DBP == 0, NA_real_, IBP_DBP)
  )]
  wide[!is.na(IBP_SBP) & (IBP_SBP < 30 | IBP_SBP > 280), IBP_SBP := NA_real_]
  wide[!is.na(IBP_MAP) & (IBP_MAP < 25 | IBP_MAP > 180), IBP_MAP := NA_real_]
  wide[!is.na(IBP_DBP) & (IBP_DBP < 10 | IBP_DBP > 180), IBP_DBP := NA_real_]
  wide[!is.na(NBP_SBP) & (NBP_SBP < 40 | NBP_SBP > 260), NBP_SBP := NA_real_]
  wide[!is.na(NBP_MAP) & (NBP_MAP < 30 | NBP_MAP > 200), NBP_MAP := NA_real_]
  wide[!is.na(NBP_DBP) & (NBP_DBP < 20 | NBP_DBP > 160), NBP_DBP := NA_real_]

  hard_equal <- !is.na(wide$IBP_SBP) & !is.na(wide$IBP_MAP) & !is.na(wide$IBP_DBP) &
    (wide$IBP_SBP == wide$IBP_MAP) & (wide$IBP_MAP == wide$IBP_DBP)
  hard_order <- (!is.na(wide$IBP_SBP) & !is.na(wide$IBP_MAP) & (wide$IBP_SBP <= wide$IBP_MAP)) |
    (!is.na(wide$IBP_MAP) & !is.na(wide$IBP_DBP) & (wide$IBP_MAP <= wide$IBP_DBP)) |
    (!is.na(wide$IBP_SBP) & !is.na(wide$IBP_DBP) & (wide$IBP_SBP <= wide$IBP_DBP))
  hard_pp_nonpos <- !is.na(wide$IBP_SBP) & !is.na(wide$IBP_DBP) & ((wide$IBP_SBP - wide$IBP_DBP) <= 0)

  wide[, diff_map := fifelse(!is.na(IBP_MAP) & !is.na(NBP_MAP), abs(IBP_MAP - NBP_MAP), NA_real_)]
  wide[, diff_ge40 := !is.na(diff_map) & diff_map >= 40]
  wide[, diff_ge30 := !is.na(diff_map) & diff_map >= 30]
  wide[, grp40 := rleid(LOG_ID, diff_ge40)]
  wide[, grp30 := rleid(LOG_ID, diff_ge30)]
  wide[, run40 := fifelse(diff_ge40, .N, 0L), by = .(LOG_ID, grp40)]
  wide[, run30 := fifelse(diff_ge30, .N, 0L), by = .(LOG_ID, grp30)]
  suspect_diff <- (wide$diff_ge40 & wide$run40 >= 3) | (wide$diff_ge30 & wide$run30 >= 5)

  wide[, BP_qc_reason := fcase(
    hard_equal, "ibp_flatline_equal",
    hard_order, "ibp_triplet_order_invalid",
    hard_pp_nonpos, "ibp_pp_nonpositive",
    diff_ge40 & run40 >= 3, "ibp_nbp_gap_ge40_run3",
    diff_ge30 & run30 >= 5, "ibp_nbp_gap_ge30_run5",
    default = "ibp_ok_or_missing"
  )]
  wide[, IBP_is_valid := !(hard_equal | hard_order | hard_pp_nonpos | suspect_diff)]

  wide[, BP_SBP := fifelse(IBP_is_valid & !is.na(IBP_SBP), IBP_SBP, NBP_SBP)]
  wide[, BP_MAP := fifelse(IBP_is_valid & !is.na(IBP_MAP), IBP_MAP, NBP_MAP)]
  wide[, BP_DBP := fifelse(IBP_is_valid & !is.na(IBP_DBP), IBP_DBP, NBP_DBP)]

  wide[, BP_SBP_source := fifelse(IBP_is_valid & !is.na(IBP_SBP), "IBP", fifelse(!is.na(NBP_SBP), "NBP", NA_character_))]
  wide[, BP_MAP_source := fifelse(IBP_is_valid & !is.na(IBP_MAP), "IBP", fifelse(!is.na(NBP_MAP), "NBP", NA_character_))]
  wide[, BP_DBP_source := fifelse(IBP_is_valid & !is.na(IBP_DBP), "IBP", fifelse(!is.na(NBP_DBP), "NBP", NA_character_))]
  wide[, BP_qc_flag := fifelse(BP_MAP_source == "IBP", "ibp_used",
                        fifelse(BP_MAP_source == "NBP", "nbp_fallback", "bp_missing"))]

  # Final triplet sanity for fused BP
  wide[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP) & !(BP_SBP >= BP_MAP & BP_MAP >= BP_DBP),
       c("BP_SBP", "BP_MAP", "BP_DBP", "BP_SBP_source", "BP_MAP_source", "BP_DBP_source", "BP_qc_flag") :=
         .(NA_real_, NA_real_, NA_real_, NA_character_, NA_character_, NA_character_, "bp_missing")]

  wide[, c("diff_map", "diff_ge40", "diff_ge30", "grp40", "grp30", "run40", "run30", "IBP_is_valid") := NULL]
  wide
}

clean_standard_module <- function(module_key, input_file, name_col, value_col, agg_fun = "median") {
  message("[INFO] module=", module_key, " input=", input_file)
  dt <- as.data.table(read_parquet(input_file, col_select = c("LOG_ID", "RECORDED_TIME", name_col, value_col)))
  setnames(dt, c(name_col, value_col), c("variable", "value"))

  # For vitals, keep dictionary-approved main timeseries candidates in v2 default run.
  if (module_key == "vitals" && length(vitals_main_vars) > 0) {
    dt <- dt[variable %in% vitals_main_vars]
  }

  dt[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
  dt[, RECORDED_TIME := minute_floor(RECORDED_TIME)]
  dt[, value := suppressWarnings(as.numeric(value))]

  pre_rows <- nrow(dt)
  dt <- dt[!is.na(LOG_ID) & !is.na(RECORDED_TIME) & !is.na(variable) & is.finite(value)]
  non_missing_rows <- nrow(dt)

  dt <- dt[!(value %in% global_sentinels) & abs(value) < 1e6]
  no_sentinel_rows <- nrow(dt)

  # Bring in valid range rules when available; keep variables without rules.
  rules <- dict[table == module_key, .(variable, valid_min, valid_max)]
  rules <- unique(rules, by = "variable")
  dt <- merge(dt, rules, by = "variable", all.x = TRUE)

  # Temp harmonization for vitals Temp_C-like fields.
  if (module_key == "vitals") {
    temp_idx <- dt$variable == "Temp_C" & dt$value > 70
    if (any(temp_idx, na.rm = TRUE)) {
      dt[temp_idx, value := (value - 32.0) * (5.0 / 9.0)]
    }
    # Vital_DB ZERO_IS_NAN adaptation.
    dt[variable %in% c("HR_combined", "IBP_SBP", "IBP_MAP", "IBP_DBP") & value == 0, value := NA_real_]
  }

  # Vital_DB-style override ranges for core monitoring variables.
  vd_override <- NULL
  if (module_key == "vitals") {
    vd_override <- data.table(
      variable = c(
        "HR_combined", "SpO2_cleaned", "Temp_C", "EtCO2", "FiO2", "RR",
        "VITAL_UC_ANE_R_VENT_PIP_OBSERVED", "VITAL_UC_ANE_R_VENT_PEEP",
        "VITAL_UCI_ANE_R_EXPIRED_MINUTE_VOLUME_MV",
        "VITAL_UCI_ANE_R_SET_TV", "VITAL_UC_ANE_R_VENT_TIDAL_VOLUME_OBSERVED",
        "BIS"
      ),
      vd_min = c(20, 50, 30, 5, 21, 4, 5, 0, 0.3, 50, 50, 0),
      vd_max = c(300, 100, 42, 80, 100, 60, 80, 25, 25, 1500, 1500, 100)
    )
  } else if (module_key == "respiratory") {
    vd_override <- data.table(
      variable = c(
        "RESP_UCI_ANE_ETCO2_PERCENT", "PEEP", "RESP_UC_ANE_R_VENT_PIP_OBSERVED",
        "RESP_UCI_ANE_R_SET_TV", "Tidal_Volume", "RESP_UC_ANE_R_SET_RESP_RATE",
        "RESP_RESPIRATIONS", "RESP_PULSE_OXIMETRY"
      ),
      vd_min = c(5, 0, 5, 50, 50, 4, 4, 50),
      vd_max = c(80, 25, 80, 1500, 1500, 60, 60, 100)
    )
  } else if (module_key == "neuro") {
    vd_override <- data.table(
      variable = "BIS",
      vd_min = 0,
      vd_max = 100
    )
  }

  dt <- apply_range_override(dt, vd_override)
  if ("lo_use" %in% names(dt) && "hi_use" %in% names(dt)) {
    dt <- dt[(is.na(lo_use) | value >= lo_use) & (is.na(hi_use) | value <= hi_use)]
    drop_cols <- intersect(c("valid_min", "valid_max", "vd_min", "vd_max", "lo_use", "hi_use"), names(dt))
    if (length(drop_cols) > 0) dt[, (drop_cols) := NULL]
  } else {
    dt <- dt[(is.na(valid_min) | value >= valid_min) & (is.na(valid_max) | value <= valid_max)]
    drop_cols <- intersect(c("valid_min", "valid_max"), names(dt))
    if (length(drop_cols) > 0) dt[, (drop_cols) := NULL]
  }
  post_rows <- nrow(dt)

  if (agg_fun == "sum") {
    agg <- dt[, .(value = sum(value, na.rm = TRUE)), by = .(LOG_ID, RECORDED_TIME, variable)]
  } else {
    agg <- dt[, .(value = median(value, na.rm = TRUE)), by = .(LOG_ID, RECORDED_TIME, variable)]
  }
  setorder(agg, LOG_ID, variable, RECORDED_TIME)

  if (module_key == "vitals") {
    agg <- mark_frozen_minute_runs(
      agg,
      target_vars = c("HR_combined", "Temp_C", "EtCO2", "BIS", "IBP_MAP"),
      min_run_minutes = 15L
    )
  } else if (module_key == "respiratory") {
    agg <- mark_frozen_minute_runs(
      agg,
      target_vars = c("RESP_UCI_ANE_ETCO2_PERCENT", "RESP_UC_ANE_R_VENT_PIP_OBSERVED"),
      min_run_minutes = 15L
    )
  } else if (module_key == "neuro") {
    agg <- mark_frozen_minute_runs(
      agg,
      target_vars = c("BIS"),
      min_run_minutes = 15L
    )
  }

  long_clean <- agg[, .(LOG_ID, RECORDED_TIME, variable, value)]
  setorder(long_clean, LOG_ID, RECORDED_TIME, variable)
  long_file <- file.path(out_long_dir, sprintf("intraop_%s_long_clean_v2.parquet", module_key))
  write_parquet(long_clean, long_file)

  wide <- dcast(agg, LOG_ID + RECORDED_TIME ~ variable, value.var = "value")
  if (module_key == "vitals") {
    wide <- apply_bp_qc_and_fuse(wide)
  }
  setorder(wide, LOG_ID, RECORDED_TIME)
  wide_file <- file.path(out_wide_dir, sprintf("intraop_%s_wide_v2.parquet", module_key))
  write_parquet(wide, wide_file)

  data.table(
    module = module_key,
    pre_rows = pre_rows,
    non_missing_rows = non_missing_rows,
    no_sentinel_rows = no_sentinel_rows,
    post_rows = post_rows,
    variable_n = uniqueN(long_clean$variable),
    case_n = uniqueN(long_clean$LOG_ID),
    wide_rows = nrow(wide),
    wide_cols = ncol(wide)
  )
}

clean_labs_extended <- function() {
  input_file <- file.path(base_dir, "intraop_labs_layer2_standardized.parquet")
  message("[INFO] module=labs input=", input_file)
  dt <- as.data.table(read_parquet(input_file, col_select = c("LOG_ID", "RECORDED_TIME", "LAB_NAME", "LAB_VALUE")))
  setnames(dt, c("LAB_NAME", "LAB_VALUE"), c("variable", "value"))

  rules <- as.data.table(fread(labs_rules_file))
  rules <- rules[, .(variable = as.character(LAB_NAME), value_min, value_max)]
  rules <- unique(rules, by = "variable")

  dt[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
  dt[, RECORDED_TIME := minute_floor(RECORDED_TIME)]
  dt[, value := suppressWarnings(as.numeric(value))]

  pre_rows <- nrow(dt)
  dt <- dt[!is.na(LOG_ID) & !is.na(RECORDED_TIME) & !is.na(variable) & is.finite(value)]
  non_missing_rows <- nrow(dt)

  dt <- dt[!(value %in% global_sentinels) & abs(value) < 1e6]
  no_sentinel_rows <- nrow(dt)

  # labs_extended_v1 selected list only
  dt <- merge(dt, rules, by = "variable", all = FALSE)
  dt <- dt[(is.na(value_min) | value >= value_min) & (is.na(value_max) | value <= value_max)]
  post_rows <- nrow(dt)

  long_clean <- dt[, .(LOG_ID, RECORDED_TIME, variable, value)]
  setorder(long_clean, LOG_ID, RECORDED_TIME, variable)

  long_file <- file.path(out_long_dir, "intraop_labs_long_clean_extended_v2.parquet")
  write_parquet(long_clean, long_file)

  agg <- long_clean[, .(value = median(value, na.rm = TRUE)), by = .(LOG_ID, RECORDED_TIME, variable)]
  wide <- dcast(agg, LOG_ID + RECORDED_TIME ~ variable, value.var = "value")
  setorder(wide, LOG_ID, RECORDED_TIME)
  wide_file <- file.path(out_wide_dir, "intraop_labs_wide_extended_v2.parquet")
  write_parquet(wide, wide_file)

  data.table(
    module = "labs",
    pre_rows = pre_rows,
    non_missing_rows = non_missing_rows,
    no_sentinel_rows = no_sentinel_rows,
    post_rows = post_rows,
    variable_n = uniqueN(long_clean$variable),
    case_n = uniqueN(long_clean$LOG_ID),
    wide_rows = nrow(wide),
    wide_cols = ncol(wide)
  )
}

clean_poct <- function() {
  input_file <- file.path(base_dir, "archive_data/layer1/intraop_labs_poct_layer1.parquet")
  if (!file.exists(input_file)) {
    warning("skip poct: missing file ", input_file)
    return(NULL)
  }
  message("[INFO] module=poct input=", input_file)
  dt <- as.data.table(read_parquet(input_file, col_select = c("LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME", "MEAS_VALUE")))
  setnames(dt, c("FLO_MEAS_NAME", "MEAS_VALUE"), c("variable", "value"))
  dt[, variable := "POCT_Glucose"]

  dt[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
  dt[, RECORDED_TIME := minute_floor(RECORDED_TIME)]
  dt[, value := suppressWarnings(as.numeric(value))]

  pre_rows <- nrow(dt)
  dt <- dt[!is.na(LOG_ID) & !is.na(RECORDED_TIME) & is.finite(value)]
  non_missing_rows <- nrow(dt)
  dt <- dt[!(value %in% global_sentinels) & abs(value) < 1e6]
  no_sentinel_rows <- nrow(dt)
  dt <- dt[value >= 20 & value <= 1200]
  post_rows <- nrow(dt)

  long_clean <- dt[, .(LOG_ID, RECORDED_TIME, variable, value)]
  setorder(long_clean, LOG_ID, RECORDED_TIME, variable)
  long_file <- file.path(out_long_dir, "intraop_poct_long_clean_v2.parquet")
  write_parquet(long_clean, long_file)

  agg <- long_clean[, .(value = median(value, na.rm = TRUE)), by = .(LOG_ID, RECORDED_TIME, variable)]
  wide <- dcast(agg, LOG_ID + RECORDED_TIME ~ variable, value.var = "value")
  setorder(wide, LOG_ID, RECORDED_TIME)
  wide_file <- file.path(out_wide_dir, "intraop_poct_wide_v2.parquet")
  write_parquet(wide, wide_file)

  data.table(
    module = "poct",
    pre_rows = pre_rows,
    non_missing_rows = non_missing_rows,
    no_sentinel_rows = no_sentinel_rows,
    post_rows = post_rows,
    variable_n = uniqueN(long_clean$variable),
    case_n = uniqueN(long_clean$LOG_ID),
    wide_rows = nrow(wide),
    wide_cols = ncol(wide)
  )
}

clean_other_selected <- function() {
  input_file <- file.path(base_dir, "archive_data/layer1/intraop_other_layer1.parquet")
  if (!file.exists(input_file)) {
    warning("skip other_selected: missing file ", input_file)
    return(NULL)
  }
  message("[INFO] module=other_selected input=", input_file)
  dt <- as.data.table(read_parquet(input_file, col_select = c("LOG_ID", "RECORDED_TIME", "FLO_MEAS_NAME", "MEAS_VALUE")))
  setnames(dt, c("FLO_MEAS_NAME", "MEAS_VALUE"), c("variable", "value"))

  keep_pattern <- paste(
    c(
      "TOF",
      "PAIN INTENSITY",
      "URINARY DRAIN OUTPUT",
      "PULSE$",
      "BLOOD PRESSURE",
      "PULSE OXIMETRY",
      "RESPIRATIONS",
      "TEMPERATURE"
    ),
    collapse = "|"
  )
  dt <- dt[grepl(keep_pattern, toupper(variable))]

  dt[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
  dt[, RECORDED_TIME := minute_floor(RECORDED_TIME)]
  dt[, value := suppressWarnings(as.numeric(value))]

  pre_rows <- nrow(dt)
  dt <- dt[!is.na(LOG_ID) & !is.na(RECORDED_TIME) & !is.na(variable) & is.finite(value)]
  non_missing_rows <- nrow(dt)
  dt <- dt[!(value %in% global_sentinels) & abs(value) < 1e6]
  no_sentinel_rows <- nrow(dt)
  post_rows <- nrow(dt)

  long_clean <- dt[, .(LOG_ID, RECORDED_TIME, variable, value)]
  setorder(long_clean, LOG_ID, RECORDED_TIME, variable)
  long_file <- file.path(out_long_dir, "intraop_other_selected_long_clean_v2.parquet")
  write_parquet(long_clean, long_file)

  agg <- long_clean[, .(value = median(value, na.rm = TRUE)), by = .(LOG_ID, RECORDED_TIME, variable)]
  wide <- dcast(agg, LOG_ID + RECORDED_TIME ~ variable, value.var = "value")
  setorder(wide, LOG_ID, RECORDED_TIME)
  wide_file <- file.path(out_wide_dir, "intraop_other_selected_wide_v2.parquet")
  write_parquet(wide, wide_file)

  data.table(
    module = "other_selected",
    pre_rows = pre_rows,
    non_missing_rows = non_missing_rows,
    no_sentinel_rows = no_sentinel_rows,
    post_rows = post_rows,
    variable_n = uniqueN(long_clean$variable),
    case_n = uniqueN(long_clean$LOG_ID),
    wide_rows = nrow(wide),
    wide_cols = ncol(wide)
  )
}

clean_lda_events <- function() {
  input_file <- file.path(base_dir, "archive_data/layer1/intraop_lda_layer1_raw.parquet")
  if (!file.exists(input_file)) {
    warning("skip lda_events: missing file ", input_file)
    return(NULL)
  }
  message("[INFO] module=lda_events input=", input_file)
  dt <- as.data.table(read_parquet(input_file, col_select = c("LOG_ID", "placement_instant", "removal_instant", "Line_Group_Name")))

  dt_place <- dt[!is.na(placement_instant), .(
    LOG_ID = LOG_ID,
    RECORDED_TIME = minute_floor(safe_parse_time(placement_instant)),
    variable = paste0("LDA_PLACE_", gsub("[^A-Za-z0-9]+", "_", toupper(as.character(Line_Group_Name)))),
    value = 1
  )]
  dt_remove <- dt[!is.na(removal_instant), .(
    LOG_ID = LOG_ID,
    RECORDED_TIME = minute_floor(safe_parse_time(removal_instant)),
    variable = paste0("LDA_REMOVE_", gsub("[^A-Za-z0-9]+", "_", toupper(as.character(Line_Group_Name)))),
    value = 1
  )]
  out <- rbindlist(list(dt_place, dt_remove), fill = TRUE)

  pre_rows <- nrow(out)
  out <- out[!is.na(LOG_ID) & !is.na(RECORDED_TIME) & !is.na(variable)]
  non_missing_rows <- nrow(out)
  no_sentinel_rows <- non_missing_rows
  post_rows <- non_missing_rows

  long_file <- file.path(out_long_dir, "intraop_lda_events_long_v2.parquet")
  write_parquet(out, long_file)

  wide <- dcast(out, LOG_ID + RECORDED_TIME ~ variable, value.var = "value", fun.aggregate = max, fill = 0)
  setorder(wide, LOG_ID, RECORDED_TIME)
  wide_file <- file.path(out_wide_dir, "intraop_lda_events_wide_v2.parquet")
  write_parquet(wide, wide_file)

  data.table(
    module = "lda_events",
    pre_rows = pre_rows,
    non_missing_rows = non_missing_rows,
    no_sentinel_rows = no_sentinel_rows,
    post_rows = post_rows,
    variable_n = uniqueN(out$variable),
    case_n = uniqueN(out$LOG_ID),
    wide_rows = nrow(wide),
    wide_cols = ncol(wide)
  )
}

module_cfg <- list(
  list(key = "vitals", file = file.path(base_dir, "intraop_vitals_layer2_standardized.parquet"), name_col = "VITAL_NAME", value_col = "VITAL_VALUE", agg = "median"),
  list(key = "io", file = file.path(base_dir, "intraop_io_layer2_standardized.parquet"), name_col = "IO_NAME", value_col = "IO_VALUE", agg = "sum"),
  list(key = "respiratory", file = file.path(base_dir, "intraop_respiratory_layer2_standardized.parquet"), name_col = "VITAL_NAME", value_col = "VITAL_VALUE", agg = "median"),
  list(key = "neuro", file = file.path(base_dir, "intraop_neuro_layer2_standardized.parquet"), name_col = "VITAL_NAME", value_col = "VITAL_VALUE", agg = "median")
)

args <- commandArgs(trailingOnly = TRUE)
selected_modules <- NULL
if (length(args) > 0) {
  # Example:
  # Rscript build_module_wide_v2.R io,respiratory,neuro,labs,poct,other_selected,lda_events
  selected_modules <- unique(trimws(unlist(strsplit(args[1], ","))))
  selected_modules <- selected_modules[nzchar(selected_modules)]
  message("[INFO] selected modules: ", paste(selected_modules, collapse = ", "))
}

should_run <- function(module_name) {
  if (is.null(selected_modules)) return(TRUE)
  module_name %in% selected_modules
}

stats <- list()
for (cfg in module_cfg) {
  if (!should_run(cfg$key)) next
  if (!file.exists(cfg$file)) {
    warning("skip missing file: ", cfg$file)
    next
  }
  stats[[length(stats) + 1]] <- clean_standard_module(
    module_key = cfg$key,
    input_file = cfg$file,
    name_col = cfg$name_col,
    value_col = cfg$value_col,
    agg_fun = cfg$agg
  )
}

if (should_run("labs") && file.exists(file.path(base_dir, "intraop_labs_layer2_standardized.parquet")) && file.exists(labs_rules_file)) {
  stats[[length(stats) + 1]] <- clean_labs_extended()
} else {
  if (should_run("labs")) {
    warning("skip labs: missing labs standardized file or labs_extended rules file")
  }
}

if (should_run("poct")) {
  poct_stats <- clean_poct()
  if (!is.null(poct_stats)) stats[[length(stats) + 1]] <- poct_stats
}

if (should_run("other_selected")) {
  other_stats <- clean_other_selected()
  if (!is.null(other_stats)) stats[[length(stats) + 1]] <- other_stats
}

if (should_run("lda_events")) {
  lda_stats <- clean_lda_events()
  if (!is.null(lda_stats)) stats[[length(stats) + 1]] <- lda_stats
}

stats_dt <- rbindlist(stats, fill = TRUE)
stats_file <- file.path(out_dir, "v2_module_cleaning_stats.csv")
fwrite(stats_dt, stats_file)

readme <- file.path(out_dir, "README_v2_module_outputs.md")
readme_lines <- c(
  "# V2 Module Outputs",
  "",
  "- Scope: extract + clean + per-module wide tables only.",
  "- No cross-module merge is performed.",
  "- No vitals/resp backup merge is performed at this stage.",
  "",
  "## Output folders",
  sprintf("- long: %s", out_long_dir),
  sprintf("- wide: %s", out_wide_dir),
  "",
  "## Main files",
  "- intraop_vitals_long_clean_v2.parquet",
  "- intraop_vitals_wide_v2.parquet",
  "- intraop_labs_long_clean_extended_v2.parquet",
  "- intraop_labs_wide_extended_v2.parquet",
  "- intraop_io_long_clean_v2.parquet",
  "- intraop_io_wide_v2.parquet",
  "- intraop_respiratory_long_clean_v2.parquet",
  "- intraop_respiratory_wide_v2.parquet",
  "- intraop_neuro_long_clean_v2.parquet",
  "- intraop_neuro_wide_v2.parquet",
  "- intraop_poct_long_clean_v2.parquet",
  "- intraop_poct_wide_v2.parquet",
  "- intraop_other_selected_long_clean_v2.parquet",
  "- intraop_other_selected_wide_v2.parquet",
  "- intraop_lda_events_long_v2.parquet",
  "- intraop_lda_events_wide_v2.parquet",
  "- v2_module_cleaning_stats.csv"
)
writeLines(readme_lines, con = readme)

message("[DONE] v2 module outputs saved to: ", out_dir)
message("[DONE] stats file: ", stats_file)

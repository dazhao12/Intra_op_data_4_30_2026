#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

base_dir <- "/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026/v2_module_outputs"
long_dir <- file.path(base_dir, "long")
wide_dir <- file.path(base_dir, "wide")
out_dir <- file.path(base_dir, "dictionary_5modules")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

files <- list(
  list(module = "vitals", long = file.path(long_dir, "intraop_vitals_long_clean_v2.parquet"), wide = file.path(wide_dir, "intraop_vitals_wide_v2.parquet")),
  list(module = "labs", long = file.path(long_dir, "intraop_labs_long_clean_extended_v2.parquet"), wide = file.path(wide_dir, "intraop_labs_wide_extended_v2.parquet")),
  list(module = "io", long = file.path(long_dir, "intraop_io_long_clean_v2.parquet"), wide = file.path(wide_dir, "intraop_io_wide_v2.parquet")),
  list(module = "respiratory", long = file.path(long_dir, "intraop_respiratory_long_clean_v2.parquet"), wide = file.path(wide_dir, "intraop_respiratory_wide_v2.parquet")),
  list(module = "neuro", long = file.path(long_dir, "intraop_neuro_long_clean_v2.parquet"), wide = file.path(wide_dir, "intraop_neuro_wide_v2.parquet"))
)

sentinels <- c(9999999, 999999, 99999, 9999, -9999, -99999, -999999, -9999999)

iqr_outlier_n <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 20) return(NA_integer_)
  q1 <- as.numeric(quantile(x, 0.25, na.rm = TRUE))
  q3 <- as.numeric(quantile(x, 0.75, na.rm = TRUE))
  i <- q3 - q1
  lo <- q1 - 3 * i
  hi <- q3 + 3 * i
  sum(x < lo | x > hi)
}

all_case_ids <- list()
for (f in files) {
  if (!file.exists(f$long)) next
  dt <- as.data.table(read_parquet(f$long, col_select = c("LOG_ID")))
  all_case_ids[[length(all_case_ids) + 1]] <- unique(dt$LOG_ID)
}
cohort_all_case_n <- uniqueN(unlist(all_case_ids))

rows <- list()
issues <- list()

for (f in files) {
  if (!file.exists(f$long) || !file.exists(f$wide)) next
  module <- f$module
  message("[INFO] module=", module)

  dt_long <- as.data.table(read_parquet(f$long))
  dt_wide <- as.data.table(read_parquet(f$wide))

  module_case_n <- uniqueN(dt_long$LOG_ID)
  wide_rows <- nrow(dt_wide)

  # Duplicate key check in long
  uniq_key_n <- uniqueN(dt_long, by = c("LOG_ID", "RECORDED_TIME", "variable"))
  dup_key_n <- nrow(dt_long) - uniq_key_n
  issues[[length(issues) + 1]] <- data.table(
    module = module,
    issue_type = "duplicate_key_in_long",
    issue_count = dup_key_n,
    note = "duplicate LOG_ID+RECORDED_TIME+variable in long table"
  )

  vars <- sort(unique(dt_long$variable))
  var_cols <- setdiff(names(dt_wide), c("LOG_ID", "RECORDED_TIME"))

  # Stats from long table
  var_stats <- dt_long[, {
    x <- value[is.finite(value)]
    out_n <- iqr_outlier_n(x)
    .(
      obs_n = .N,
      case_n = uniqueN(LOG_ID),
      value_min = min(x, na.rm = TRUE),
      value_p01 = as.numeric(quantile(x, 0.01, na.rm = TRUE)),
      value_p05 = as.numeric(quantile(x, 0.05, na.rm = TRUE)),
      value_p50 = as.numeric(quantile(x, 0.50, na.rm = TRUE)),
      value_p95 = as.numeric(quantile(x, 0.95, na.rm = TRUE)),
      value_p99 = as.numeric(quantile(x, 0.99, na.rm = TRUE)),
      value_max = max(x, na.rm = TRUE),
      value_mean = mean(x, na.rm = TRUE),
      value_sd = sd(x, na.rm = TRUE),
      sentinel_n = sum(x %in% sentinels | abs(x) >= 1e6, na.rm = TRUE),
      iqr_outlier_n = out_n
    )
  }, by = .(variable)]

  # Missingness from wide table, per variable column
  nm <- data.table(variable = vars)
  if (length(var_cols) > 0) {
    nonmissing_vec <- vapply(var_cols, function(col) sum(!is.na(dt_wide[[col]])), FUN.VALUE = integer(1))
    nm <- data.table(variable = names(nonmissing_vec), wide_nonmissing_n = as.integer(nonmissing_vec))
  } else {
    nm[, wide_nonmissing_n := 0L]
  }

  out <- merge(var_stats, nm, by = "variable", all.x = TRUE)
  out[is.na(wide_nonmissing_n), wide_nonmissing_n := 0L]
  out[, wide_missing_n := wide_rows - wide_nonmissing_n]
  out[, wide_missing_pct := if (wide_rows > 0) wide_missing_n / wide_rows * 100 else NA_real_]
  out[, minute_n := wide_nonmissing_n]
  out[, case_coverage_pct_module := if (module_case_n > 0) case_n / module_case_n * 100 else NA_real_]
  out[, case_coverage_pct_all5 := if (cohort_all_case_n > 0) case_n / cohort_all_case_n * 100 else NA_real_]
  out[, iqr_outlier_pct := ifelse(is.na(iqr_outlier_n) | obs_n == 0, NA_real_, iqr_outlier_n / obs_n * 100)]
  out[, module := module]
  out[, `:=`(suspicious_flag = FALSE, suspicious_reason = "")]

  rows[[length(rows) + 1]] <- out[, .(
    module, variable, obs_n, minute_n, case_n, case_coverage_pct_module, case_coverage_pct_all5,
    wide_nonmissing_n, wide_missing_n, wide_missing_pct,
    value_min, value_p01, value_p05, value_p50, value_p95, value_p99, value_max, value_mean, value_sd,
    sentinel_n, iqr_outlier_n, iqr_outlier_pct, suspicious_flag, suspicious_reason
  )]
}

dict_dt <- rbindlist(rows, fill = TRUE)

# Heuristic suspicious flags
dict_dt[module == "vitals" & variable == "Temp_C" & (value_min < 30 | value_max > 43),
        `:=`(suspicious_flag = TRUE, suspicious_reason = "Temp_C outside 30-43C")]
dict_dt[module == "vitals" & variable == "HR_combined" & value_max > 220,
        `:=`(suspicious_flag = TRUE, suspicious_reason = "HR_combined max > 220")]
dict_dt[module == "vitals" & variable == "SpO2_cleaned" & (value_min < 40 | value_max > 100),
        `:=`(suspicious_flag = TRUE, suspicious_reason = "SpO2_cleaned outside expected 40-100")]
dict_dt[module == "io" & grepl("Blood_Products", variable, fixed = TRUE) & case_coverage_pct_all5 < 0.2,
        `:=`(suspicious_flag = TRUE, suspicious_reason = "Very low blood products coverage; verify coding")]
dict_dt[sentinel_n > 0,
        `:=`(suspicious_flag = TRUE, suspicious_reason = fifelse(nchar(suspicious_reason) > 0, paste0(suspicious_reason, "; sentinel remained"), "sentinel remained"))]

setorder(dict_dt, module, -case_n, variable)

issues_dt <- rbindlist(issues, fill = TRUE)

fwrite(dict_dt, file.path(out_dir, "variable_dictionary_5modules_v2.csv"))
fwrite(issues_dt, file.path(out_dir, "module_extraction_issues_5modules_v2.csv"))

# Module summary
summary_dt <- dict_dt[, .(
  variable_n = .N,
  suspicious_var_n = sum(suspicious_flag, na.rm = TRUE),
  median_missing_pct = median(wide_missing_pct, na.rm = TRUE),
  high_missing_var_n = sum(wide_missing_pct > 95, na.rm = TRUE)
), by = module][order(module)]
fwrite(summary_dt, file.path(out_dir, "module_summary_5modules_v2.csv"))

md <- c(
  "# 5-Module Dictionary (V2)",
  "",
  "- Modules: vitals, labs, io, respiratory, neuro",
  sprintf("- Total variables: %s", nrow(dict_dt)),
  sprintf("- Total cases across 5 modules (union LOG_ID): %s", cohort_all_case_n),
  "",
  "## Output files",
  "- variable_dictionary_5modules_v2.csv",
  "- module_summary_5modules_v2.csv",
  "- module_extraction_issues_5modules_v2.csv",
  "",
  "## Notes",
  "- Distribution statistics are from long clean tables.",
  "- Missingness statistics are calculated on each module's wide table.",
  "- suspicious_flag is heuristic and intended for manual review."
)
writeLines(md, con = file.path(out_dir, "README_dictionary_5modules_v2.md"))

message("[DONE] dictionary saved: ", out_dir)

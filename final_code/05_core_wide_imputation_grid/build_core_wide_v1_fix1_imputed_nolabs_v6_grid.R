#!/usr/bin/env Rscript
# =============================================================================
# build_core_wide_v1_fix1_imputed_nolabs_v6_grid.R
#
# v6 改动（相对 v5）：
#   1. 时间边界清洗（新增）
#      - 麻醉时长 > 24h → 用 OR 时间替代（麻醉 stop 记录可信度低）
#      - 麻醉时长 < OR时长 - 60min → 用 OR 时间替代（记录错误）
#      - 麻醉时长缺失 → 用 OR 时间
#      - OR 也缺失 → 兜底用数据 min/max（v4/v5 原逻辑）
#      - 新增 QC 列 time_boundary_source 标记来源
#   2. minute_index（新增）
#      - 相对时间：从 ane_start 算起的第几分钟（t_min 对应 minute_index=0）
#      - 便于时序建模直接用，不需要再算相对时间
#   3. {var}_locf_stale_flag（新增）
#      - 当 _time_since_last_obs_min 超过变量专属 cap 时标记为 1
#      - 表示该值是由 "过时" LOCF 填充，建模时可降权或 mask
#      - Cap 设置依据临床更新频率（见下方 stale_caps 列表）
#
# 基于：build_core_wide_v1_fix1_imputed_nolabs_v5_grid.R
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

base_dir <- "/N/project/analgesia_perioperation/data/MOVER/processed/Intra_op_data_4_30_2026"
in_dir   <- file.path(base_dir, "v2_module_outputs/core_wide_v1_fix1")
out_dir  <- file.path(base_dir, "v2_module_outputs/core_wide_v1_fix1_imputed_nolabs_v6_grid")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ---------------------------------------------------------------------------
# 0. LOCF stale cap 配置（超过此分钟数打 stale_flag = 1）
#    NA = 不设 cap（麻醉剂用 0 填充，语义明确不需要 stale 概念）
# ---------------------------------------------------------------------------
stale_caps <- list(
  HR_core            = 30L,   # 心率：30min 内有临床意义
  SpO2_core          = 30L,   # 血氧：监护断开 30min 后不可信
  RR_core            = 30L,   # 呼吸率
  EtCO2_core         = 30L,   # 呼末CO2：通气状态改变快
  PIP_core           = 30L,   # 气道峰压
  PEEP_core          = 60L,   # PEEP：调整频率低，60min 可接受
  Tidal_volume_core  = 30L,
  Minute_volume_core = 30L,
  FiO2_core          = 60L,   # 吸入氧浓度：调整后较稳定
  BP_SBP             = 60L,   # 合并 BP：IBP 连续，NBP 间断
  BP_MAP             = 60L,
  BP_DBP             = 60L,
  IBP_SBP            = 30L,   # 有创 BP：连续监测，断开 30min = 异常
  IBP_MAP            = 30L,
  IBP_DBP            = 30L,
  NBP_SBP            = 60L,   # 无创 BP：间断，60min 内有参考
  NBP_MAP            = 60L,
  NBP_DBP            = 60L,
  Temp_C_core        = 120L,  # 体温：变化慢，2h 内仍有参考价值
  BIS_core           = 30L,   # BIS：监测断开 30min 后不可信
  Nitric_oxide_core  = 30L,
  Sevoflurane_core   = NA_integer_,   # 麻醉剂：0填充语义明确，不设cap
  Isoflurane_core    = NA_integer_,
  Desflurane_core    = NA_integer_
)

# ---------------------------------------------------------------------------
# 1. 加载元数据，应用时间边界清洗规则
# ---------------------------------------------------------------------------
META_FILE <- "/N/project/analgesia_perioperation/data/MOVER/processed/anesthesia_surgery_info_final_cohort_latest/mover_anesthesia_surgery_info_corrected_with_esc_risk.csv"

message("[INFO] loading anesthesia timing metadata: ", META_FILE)
meta <- fread(META_FILE, select = c("LOG_ID", "anesthesia_start_time", "anesthesia_stop_time",
                                    "or_in_time", "or_out_time"))

minute_floor <- function(x) {
  as.POSIXct(format(x, "%Y-%m-%d %H:%M:00", tz = "UTC"), tz = "UTC")
}

safe_parse_posix <- function(x) {
  x <- as.character(x)
  t1 <- suppressWarnings(as.POSIXct(x, tz = "UTC", format = "%Y-%m-%d %H:%M:%S"))
  miss <- is.na(t1)
  if (any(miss)) t1[miss] <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC", format = "%m/%d/%y %H:%M"))
  miss <- is.na(t1)
  if (any(miss)) t1[miss] <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC", format = "%m/%d/%Y %H:%M"))
  t1
}

meta[, ane_start := minute_floor(safe_parse_posix(anesthesia_start_time))]
meta[, ane_stop  := minute_floor(safe_parse_posix(anesthesia_stop_time))]
meta[, or_in     := minute_floor(safe_parse_posix(or_in_time))]
meta[, or_out    := minute_floor(safe_parse_posix(or_out_time))]

meta[, ane_dur_min := as.numeric(difftime(ane_stop, ane_start, units = "mins"))]
meta[, or_dur_min  := as.numeric(difftime(or_out,  or_in,    units = "mins"))]

# ---- 时间边界清洗规则（v6 新增）----
# 每条记录标注最终选用的时间边界来源
meta[, time_boundary_source := "ane"]   # 默认用麻醉时间

# 规则1：麻醉时长 > 24h（1440 min）→ 可疑，改用 OR 时间
n_rule1 <- meta[!is.na(ane_dur_min) & ane_dur_min > 1440, .N]
meta[!is.na(ane_dur_min) & ane_dur_min > 1440,
     `:=`(ane_start = or_in, ane_stop = or_out, time_boundary_source = "or_rule1_ane_over24h")]

# 规则2：麻醉时长比 OR 在室时长短超过 60min → 记录错误，改用 OR 时间
n_rule2 <- meta[!is.na(ane_dur_min) & !is.na(or_dur_min) & (ane_dur_min - or_dur_min) < -60, .N]
meta[!is.na(ane_dur_min) & !is.na(or_dur_min) & (ane_dur_min - or_dur_min) < -60,
     `:=`(ane_start = or_in, ane_stop = or_out, time_boundary_source = "or_rule2_ane_shorter_than_or")]

# 规则3：麻醉时间缺失 → 用 OR 时间
n_rule3 <- meta[is.na(ane_start) | is.na(ane_stop) | ane_start >= ane_stop, .N]
meta[is.na(ane_start) | is.na(ane_stop) | ane_start >= ane_stop,
     `:=`(ane_start = or_in, ane_stop = or_out, time_boundary_source = "or_rule3_ane_missing")]

# 重新计算清洗后的时长
meta[, ane_dur_min_clean := as.numeric(difftime(ane_stop, ane_start, units = "mins"))]

message(sprintf("[INFO] time boundary cleaning: rule1(>24h)=%d, rule2(ane<or-60)=%d, rule3(missing)=%d",
                n_rule1, n_rule2, n_rule3))

# 只保留有效的清洗后时间对
meta_valid <- meta[!is.na(ane_start) & !is.na(ane_stop) & ane_start < ane_stop,
                   .(LOG_ID, t_meta_min = ane_start, t_meta_max = ane_stop,
                     time_boundary_source, ane_dur_min_clean)]
message("[INFO] cases with valid (cleaned) meta timing: ", nrow(meta_valid))

# ---------------------------------------------------------------------------
# 2. 加载核心宽表
# ---------------------------------------------------------------------------
in_file <- file.path(in_dir, "core_wide_v1_fix1.parquet")
dt_raw  <- as.data.table(read_parquet(in_file))
dt_raw[, RECORDED_TIME := as.POSIXct(RECORDED_TIME, tz = "UTC")]
setorder(dt_raw, LOG_ID, RECORDED_TIME)

# 去除 labs
lab_cols <- grep("^lab__", names(dt_raw), value = TRUE)
if (length(lab_cols) > 0) dt_raw[, (lab_cols) := NULL]

# ---------------------------------------------------------------------------
# 3. 构建完整分钟网格（用清洗后的时间边界）
# ---------------------------------------------------------------------------
data_span <- dt_raw[, .(
  t_data_min = min(RECORDED_TIME, na.rm = TRUE),
  t_data_max = max(RECORDED_TIME, na.rm = TRUE)
), by = LOG_ID]

case_span <- merge(data_span, meta_valid, by = "LOG_ID", all.x = TRUE)

# 最终网格边界（同 v5 逻辑，但用的是清洗后的 t_meta_min/max）
case_span[, t_min := fifelse(
  !is.na(t_meta_min),
  pmin(t_meta_min, t_data_min),
  t_data_min
)]
case_span[, t_max := fifelse(
  !is.na(t_meta_max),
  pmax(t_meta_max, t_data_max),
  t_data_max
)]
case_span[is.na(time_boundary_source), time_boundary_source := "data_fallback"]

# 统计
n_meta_anchor    <- case_span[time_boundary_source != "data_fallback", .N]
n_fallback       <- case_span[time_boundary_source == "data_fallback", .N]
n_extended_start <- case_span[!is.na(t_meta_min) & t_meta_min < t_data_min, .N]
n_extended_end   <- case_span[!is.na(t_meta_max) & t_meta_max > t_data_max, .N]

message("[INFO] cases meta-anchored (after cleaning): ", n_meta_anchor,
        " | fallback to data min/max: ", n_fallback)
message("[INFO] grid extended at start: ", n_extended_start,
        " | extended at end: ", n_extended_end)

# 构建网格
grid <- case_span[, .(RECORDED_TIME = seq(t_min, t_max, by = "1 min")), by = LOG_ID]
setorder(grid, LOG_ID, RECORDED_TIME)

# ---- minute_index（v6 新增）----
# t_min 对应 minute_index = 0，即麻醉开始时刻
grid <- merge(grid, case_span[, .(LOG_ID, t_min)], by = "LOG_ID")
grid[, minute_index := as.integer(round(as.numeric(difftime(RECORDED_TIME, t_min, units = "mins"))))]
grid[, t_min := NULL]
setorder(grid, LOG_ID, minute_index)

dt <- merge(grid, dt_raw, by = c("LOG_ID", "RECORDED_TIME"), all.x = TRUE, sort = FALSE)
setorder(dt, LOG_ID, minute_index)

# ---------------------------------------------------------------------------
# 4. 连续变量 LOCF + leading NA → 全局中位数 + locf_stale_flag（v6 新增）
# ---------------------------------------------------------------------------
cont_vars <- c(
  "HR_core", "SpO2_core",
  "BP_SBP", "BP_MAP", "BP_DBP", "IBP_SBP", "IBP_MAP", "IBP_DBP", "NBP_SBP", "NBP_MAP", "NBP_DBP",
  "RR_core", "EtCO2_core", "FiO2_core", "PIP_core", "PEEP_core", "Tidal_volume_core", "Minute_volume_core",
  "BIS_core", "Sevoflurane_core", "Isoflurane_core", "Desflurane_core",
  "Temp_C_core", "Nitric_oxide_core"
)
cont_vars <- cont_vars[cont_vars %in% names(dt)]

agent_zero_fill_vars <- intersect(c("Sevoflurane_core", "Isoflurane_core", "Desflurane_core"), cont_vars)

io_vars <- c("Intake_fluid_core", "Urine_output_core", "EBL_core", "Blood_products_core", "Other_output_core")
io_vars <- io_vars[io_vars %in% names(dt)]

impute_stats <- data.table(
  variable              = character(),
  fill_method           = character(),
  locf_stale_cap_min    = integer(),
  non_missing_raw       = integer(),
  non_missing_after     = integer(),
  filled_by_locf_n      = integer(),
  filled_by_median_lead = integer(),
  locf_stale_n          = integer(),
  still_na_after_n      = integer(),
  fill_value            = numeric()
)

for (v in cont_vars) {
  raw_col          <- paste0(v, "__raw")
  miss_col         <- paste0(v, "_missing_flag")
  age_col          <- paste0(v, "_time_since_last_obs_min")
  locf_flag_col    <- paste0(v, "_filled_by_locf_flag")
  leading_flag_col <- paste0(v, "_leading_na_flag")
  median_flag_col  <- paste0(v, "_leading_filled_by_median_flag")
  stale_flag_col   <- paste0(v, "_locf_stale_flag")   # v6 新增

  dt[, (raw_col)  := get(v)]
  dt[, (miss_col) := as.integer(is.na(get(raw_col)))]

  # 距上次真实观测分钟数（仅向后计，leading NA 为 NA）
  dt[, (age_col) := {
    x <- get(raw_col)
    t <- as.numeric(RECORDED_TIME)
    last_obs_t <- fifelse(!is.na(x), t, NA_real_)
    last_obs_t <- nafill(last_obs_t, type = "locf")
    out <- (t - last_obs_t) / 60
    out[is.na(last_obs_t)] <- NA_real_
    out
  }, by = LOG_ID]

  # Step A: LOCF
  dt[, (v) := nafill(get(raw_col), type = "locf"), by = LOG_ID]
  dt[, (locf_flag_col)    := as.integer(is.na(get(raw_col)) & !is.na(get(v)))]
  dt[, (leading_flag_col) := as.integer(is.na(get(raw_col)) & is.na(get(v)))]

  # Step B: 全局中位数 / 专属兜底
  global_med <- dt[!is.na(get(raw_col)), as.numeric(median(get(raw_col)))]
  if (!is.finite(global_med)) global_med <- NA_real_

  fill_method <- "locf_only"
  fill_value  <- NA_real_

  if (v %in% agent_zero_fill_vars) {
    dt[is.na(get(v)), (v) := 0]
    dt[, (median_flag_col) := 0L]
    fill_method <- "locf_then_zero"
    fill_value  <- 0

  } else if (v == "BIS_core") {
    med <- if (is.finite(global_med)) global_med else 43
    dt[is.na(get(v)), (v) := med]
    dt[, (median_flag_col) := as.integer(get(leading_flag_col) == 1)]
    fill_method <- "locf_then_global_median"
    fill_value  <- med

  } else {
    if (!is.na(global_med) && dt[get(leading_flag_col) == 1, .N] > 0) {
      dt[get(leading_flag_col) == 1, (v) := global_med]
      fill_method <- "locf_then_median_for_leading"
      fill_value  <- global_med
    }
    dt[, (median_flag_col) := as.integer(get(leading_flag_col) == 1 & !is.na(get(v)))]
  }

  # ---- locf_stale_flag（v6 新增）----
  cap <- stale_caps[[v]]
  if (!is.null(cap) && !is.na(cap)) {
    # stale = 1 当：该值是 LOCF 填充（非 leading 中位数）且距上次观测 > cap
    dt[, (stale_flag_col) := as.integer(
      get(locf_flag_col) == 1 &
      !is.na(get(age_col)) &
      get(age_col) > cap
    )]
    stale_n <- dt[get(stale_flag_col) == 1, .N]
  } else {
    # 麻醉剂不设 cap：stale flag 置 0
    dt[, (stale_flag_col) := 0L]
    stale_n <- 0L
    cap <- NA_integer_
  }

  impute_stats <- rbind(
    impute_stats,
    data.table(
      variable              = v,
      fill_method           = fill_method,
      locf_stale_cap_min    = cap,
      non_missing_raw       = dt[!is.na(get(raw_col)), .N],
      non_missing_after     = dt[!is.na(get(v)), .N],
      filled_by_locf_n      = dt[get(locf_flag_col) == 1, .N],
      filled_by_median_lead = dt[get(median_flag_col) == 1, .N],
      locf_stale_n          = stale_n,
      still_na_after_n      = dt[is.na(get(v)), .N],
      fill_value            = fill_value
    )
  )
}

# ---------------------------------------------------------------------------
# 5. IBP 三元组 QC
# ---------------------------------------------------------------------------
dt[, IBP_triplet_corrected_flag := 0L]
if (all(c("IBP_SBP", "IBP_MAP", "IBP_DBP") %in% names(dt))) {
  bad_ibp <- !is.na(dt$IBP_SBP) & !is.na(dt$IBP_MAP) & !is.na(dt$IBP_DBP) & (
    !(dt$IBP_SBP >= dt$IBP_MAP & dt$IBP_MAP >= dt$IBP_DBP) |
    (dt$IBP_SBP == dt$IBP_MAP & dt$IBP_MAP == dt$IBP_DBP) |
    ((dt$IBP_SBP - dt$IBP_DBP) <= 0)
  )
  bad_ibp[is.na(bad_ibp)] <- FALSE
  dt[bad_ibp, `:=`(
    IBP_SBP = NA_real_, IBP_MAP = NA_real_, IBP_DBP = NA_real_,
    IBP_triplet_corrected_flag = 1L
  )]
}

# ---------------------------------------------------------------------------
# 6. BP 融合（IBP 优先 > NBP 兜底）+ 三元组校验
# ---------------------------------------------------------------------------
dt[, BP_SBP_source := fifelse(!is.na(IBP_SBP), "IBP", fifelse(!is.na(NBP_SBP), "NBP", NA_character_))]
dt[, BP_MAP_source := fifelse(!is.na(IBP_MAP), "IBP", fifelse(!is.na(NBP_MAP), "NBP", NA_character_))]
dt[, BP_DBP_source := fifelse(!is.na(IBP_DBP), "IBP", fifelse(!is.na(NBP_DBP), "NBP", NA_character_))]

dt[, BP_SBP := fifelse(!is.na(IBP_SBP), IBP_SBP, NBP_SBP)]
dt[, BP_MAP := fifelse(!is.na(IBP_MAP), IBP_MAP, NBP_MAP)]
dt[, BP_DBP := fifelse(!is.na(IBP_DBP), IBP_DBP, NBP_DBP)]
dt[, BP_qc_flag   := fifelse(!is.na(BP_MAP), "bp_available", "bp_missing")]
dt[, BP_qc_reason := fifelse(!is.na(BP_MAP), "recomputed_after_ibp_qc", NA_character_)]

bp_bad_before <- dt[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP) &
                    !(BP_SBP >= BP_MAP & BP_MAP >= BP_DBP), .N]
dt[, BP_triplet_corrected_flag := 0L]
bad_bp <- !is.na(dt$BP_SBP) & !is.na(dt$BP_MAP) & !is.na(dt$BP_DBP) &
          !(dt$BP_SBP >= dt$BP_MAP & dt$BP_MAP >= dt$BP_DBP)
bad_bp[is.na(bad_bp)] <- FALSE
if (any(bad_bp)) {
  dt[bad_bp, `:=`(
    BP_SBP = NA_real_, BP_MAP = NA_real_, BP_DBP = NA_real_,
    BP_SBP_source = NA_character_, BP_MAP_source = NA_character_, BP_DBP_source = NA_character_,
    BP_qc_flag = "bp_missing", BP_qc_reason = "triplet_violation_after_recompute",
    BP_triplet_corrected_flag = 1L
  )]
}
bp_bad_after <- dt[!is.na(BP_SBP) & !is.na(BP_MAP) & !is.na(BP_DBP) &
                   !(BP_SBP >= BP_MAP & BP_MAP >= BP_DBP), .N]

# ---------------------------------------------------------------------------
# 7. I/O 变量：NA → 0
# ---------------------------------------------------------------------------
for (v in io_vars) {
  rec_col <- paste0(v, "_recorded_flag")
  dt[, (rec_col) := as.integer(!is.na(get(v)))]
  dt[is.na(get(v)), (v) := 0]
}
if (length(io_vars) > 0) {
  rec_cols <- paste0(io_vars, "_recorded_flag")
  dt[, io_any_recorded_flag := as.integer(rowSums(.SD, na.rm = TRUE) > 0), .SDcols = rec_cols]
}

# 删除临时 __raw 列
raw_cols <- grep("__raw$", names(dt), value = TRUE)
if (length(raw_cols) > 0) dt[, (raw_cols) := NULL]

# 整理列顺序：LOG_ID, minute_index, RECORDED_TIME 在最前
front_cols <- c("LOG_ID", "minute_index", "RECORDED_TIME")
other_cols <- setdiff(names(dt), front_cols)
setcolorder(dt, c(front_cols, other_cols))
setorder(dt, LOG_ID, minute_index)

# ---------------------------------------------------------------------------
# 8. 输出
# ---------------------------------------------------------------------------
out_base <- "core_wide_v1_fix1_imputed_nolabs_v6_grid"
write_parquet(dt, file.path(out_dir, paste0(out_base, ".parquet")))
fwrite(dt,        file.path(out_dir, paste0(out_base, ".csv.gz")))

# QC 汇总
qc <- data.table(
  metric = c(
    "rows", "cases", "cols_total",
    "labs_removed_n",
    "bp_triplet_bad_before", "bp_triplet_bad_after",
    "ibp_triplet_corrected_rows", "bp_triplet_corrected_rows",
    "bis_non_missing_after",
    "cases_meta_anchored", "cases_fallback_to_data",
    "cases_grid_extended_start", "cases_grid_extended_end",
    "time_boundary_rule1_n", "time_boundary_rule2_n", "time_boundary_rule3_n"
  ),
  value = c(
    nrow(dt), uniqueN(dt$LOG_ID), ncol(dt),
    length(lab_cols),
    bp_bad_before, bp_bad_after,
    dt[IBP_triplet_corrected_flag == 1L, .N],
    dt[BP_triplet_corrected_flag  == 1L, .N],
    dt[!is.na(BIS_core), .N],
    n_meta_anchor, n_fallback,
    n_extended_start, n_extended_end,
    n_rule1, n_rule2, n_rule3
  )
)
fwrite(qc,           file.path(out_dir, paste0("qc_", out_base, ".csv")))
fwrite(impute_stats, file.path(out_dir, "impute_stats_v6_grid.csv"))

# 时间边界来源汇总
boundary_summary <- case_span[, .N, by = time_boundary_source][order(-N)]
fwrite(boundary_summary, file.path(out_dir, "time_boundary_source_summary_v6.csv"))

# 样本（最长 50 例）
sample_cases <- dt[, .N, by = LOG_ID][order(-N)][1:min(50, .N), LOG_ID]
sample50     <- dt[LOG_ID %in% sample_cases]
setorder(sample50, LOG_ID, minute_index)
fwrite(sample50, file.path(out_dir, paste0("sample_top50_cases_", out_base, ".csv")))
fwrite(data.table(LOG_ID = sample_cases), file.path(out_dir, "sample_top50_case_ids_v6_grid.csv"))

message("[DONE] v6 grid saved to: ", out_dir)
message("       Columns per variable: value | _missing_flag | _time_since_last_obs_min")
message("                             | _filled_by_locf_flag | _leading_na_flag")
message("                             | _leading_filled_by_median_flag | _locf_stale_flag")

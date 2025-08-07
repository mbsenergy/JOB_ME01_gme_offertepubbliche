print(glue::glue("{crayon::cyan('[INIT - 01.Retrive]')}"))

print('[00/03]')


#1. GET DATA ------------------------------------------------------------

dt_all_raw = list()
username = "PIASARACENO"
password = "18N15C9R"
output_dir = "data"

# MGP --------------------------------------------------------------

if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_mgp_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}

# Get filename list -----------------------------------
mgp_offers_files = gme_offers_get_files(
  data_type = 'MGP',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[01/07]')


# Extract the last n elements
last_files = mgp_offers_files[
  as.Date(substr(mgp_offers_files, 1, 8), format = "%Y%m%d") > from_data
]
if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_mgp_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'MGP',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_mgp_offers = rbindlist(list_mgp_offers, fill = TRUE)

# check
check_load = (nrow(dt_mgp_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_mgp_offers retrieved')}"))
  dt_all_raw[['ME01_gme_mgp_offers']] = dt_mgp_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mgp_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_mgp_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mgp_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_mgp_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_mgp_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}


# MSD --------------------------------------------------------------

if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_msd_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}

# Get filename list -----------------------------------
msd_offers_files = gme_offers_get_files(
  data_type = 'MSD',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[02/07]')

# max(as.Date(substr(msd_offers_files, 1, 8), format = "%Y%m%d"), na.rm = TRUE)

# Extract the last n elements
last_files = msd_offers_files[
  as.Date(substr(msd_offers_files, 1, 8), format = "%Y%m%d") > from_data
]
if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_msd_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'MSD',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_msd_offers = rbindlist(list_msd_offers, fill = TRUE)

# check
check_load = (nrow(dt_msd_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_msd_offers retrieved')}"))
  dt_all_raw[['ME01_gme_msd_offers']] = dt_msd_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_msd_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_msd_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_msd_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_msd_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_msd_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}


# MB --------------------------------------------------------------

if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_mb_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}
# from_data <- as.Date('2025-04-27')

# Get filename list -----------------------------------
mb_offers_files = gme_offers_get_files(
  data_type = 'MB',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[03/04]')


# Extract the last n elements
last_files = mb_offers_files[
  as.Date(substr(mb_offers_files, 1, 8), format = "%Y%m%d") > from_data
]
if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_mb_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'MB',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_mb_offers = rbindlist(list_mb_offers, fill = TRUE)

# check
check_load = (nrow(dt_mb_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_mb_offers retrieved')}"))
  dt_all_raw[['ME01_gme_mb_offers']] = dt_mb_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mb_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_mb_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mb_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_mb_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_mb_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}


# XBID --------------------------------------------------------------

if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_xbid_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}


# Get filename list -----------------------------------
xbid_offers_files = gme_offers_get_files(
  data_type = 'XBID',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[04/07]')


# Extract the last n elements
last_files = xbid_offers_files[
  as.Date(substr(xbid_offers_files, 1, 8), format = "%Y%m%d") > from_data
]
if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_xbid_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'XBID',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_xbid_offers = rbindlist(list_xbid_offers, fill = TRUE)
dt_xbid_offers[,
  bid_offer_date_dt_parsed := as.IDate(sub("T.*", "", timestamp))
]

# check
check_load = (nrow(dt_xbid_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)
dt_all_raw[['ME01_gme_xbid_offers']] = dt_xbid_offers
check_files = T

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_xbid_offers retrieved')}"))
  dt_all_raw[['ME01_gme_xbid_offers']] = dt_xbid_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_xbid_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_xbid_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_xbid_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_xbid_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_xbid_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}


# MI-A1 --------------------------------------------------------------
if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_mia1_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}


# from_data <- as.Date('2025-06-01')
# Get filename list -----------------------------------
mia1_offers_files = gme_offers_get_files(
  data_type = 'MI-A1',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[05/07]')


file_dates <- as.Date(substr(mia1_offers_files, 1, 8), format = "%Y%m%d")
#last_files <- mia1_offers_files[file_dates >= from_data & file_dates <= to_data]

# Extract the last n elements
last_files = mia1_offers_files[
  as.Date(substr(mia1_offers_files, 1, 8), format = "%Y%m%d") > from_data
]

if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_mia1_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'MI-A1',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_mia1_offers = rbindlist(list_mia1_offers, fill = TRUE)

# check
check_load = (nrow(dt_mia1_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_mia1_offers retrieved')}"))
  dt_all_raw[['ME01_gme_mia1_offers']] = dt_mia1_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mia1_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_mia1_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mia1_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_mia1_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_mia1_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}


# MI-A2 --------------------------------------------------------------
if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_mia2_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}


# from_data <- as.Date('2025-06-01')
# Get filename list -----------------------------------
mia2_offers_files = gme_offers_get_files(
  data_type = 'MI-A2',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[06/07]')


# Extract the last n elements
last_files = mia2_offers_files[
  as.Date(substr(mia2_offers_files, 1, 8), format = "%Y%m%d") > from_data
]
if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_mia2_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'MI-A2',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_mia2_offers = rbindlist(list_mia2_offers, fill = TRUE)
names(dt_mia2_offers)

# saveRDS(dt_mia2_offers, file = "dt_mia2_offers_2025.rds")

# list_mia2_offers <- dt_mia2_offers

# check
check_load = (nrow(dt_mia2_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_mia2_offers retrieved')}"))
  dt_all_raw[['ME01_gme_mia2_offers']] = dt_mia2_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mia2_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_mia2_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mia2_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_mia2_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_mia2_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}


# MI-A3 --------------------------------------------------------------
if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_mia3_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
}


# Get filename list -----------------------------------
mia3_offers_files = gme_offers_get_files(
  data_type = 'MI-A3',
  output_dir = output_dir,
  username = username,
  password = password
)

print('[07/07]')


# Extract the last n elements
last_files = mia3_offers_files[
  as.Date(substr(mia3_offers_files, 1, 8), format = "%Y%m%d") >= from_data
]
if (
  !exists("last_files") || length(last_files) == 0 || all(is.na(last_files))
) {
  last_files = character(0)
}

list_mia3_offers <- lapply(last_files, function(file) {
  tryCatch(
    {
      # Call gme_download_offers_file with explicit arguments
      gme_download_offers_file(
        filename = file,
        data_type = 'MI-A3',
        output_dir = output_dir,
        username = username, # FTP username for authentication
        password = password, # FTP password for authentication
        raw = FALSE
      )
    },
    error = function(e) {
      # In case of an error (e.g., failed download or processing), return NULL
      message("Error processing file: ", file, " - ", e$message)
      return(NULL)
    }
  )
})

dt_mia3_offers = rbindlist(list_mia3_offers, fill = TRUE)

names(dt_mia3_offers)

# check
check_load = (nrow(dt_mia3_offers) > 0)
check_files = (is.character(last_files) &
  !is.null(last_files) &
  length(last_files) == 0)

if (isTRUE(check_load)) {
  print(glue("{crayon::green('ME01_gme_mia3_offers retrieved')}"))
  dt_all_raw[['ME01_gme_mia3_offers']] = dt_mia3_offers[
    bid_offer_date_dt_parsed > from_data
  ]
  new_data_tables = rbind(
    new_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mia3_offers',
      num_rows = 0,
      type = 'NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isTRUE(check_files)) {
  print(glue("{crayon::yellow('ME01_gme_mia3_offers no new data')}"))
  nonew_data_tables = rbind(
    nonew_data_tables,
    data.table::data.table(
      table_name = 'ME01_gme_mia3_offers',
      num_rows = 0,
      type = 'NO_NEW_DATA'
    )
  )
}

if (isFALSE(check_load) & isFALSE(check_files)) {
  print(glue("{crayon::red('ME01_gme_mia3_offers error')}"))
  failed_tables_retrieve = rbind(
    failed_tables_retrieve,
    data.table::data.table(
      table_name = 'ME01_gme_mia3_offers',
      num_rows = 0,
      type = 'ERROR_RETRIEVE'
    )
  )
}

# EXPORT RAW ===========================================================================

saveRDS(dt_all_raw, 'dt_all_raw.rds')

# xxx. CHECK ===========================================================================

check_process_01 = nrow(new_data_tables) +
  nrow(nonew_data_tables) +
  nrow(failed_tables_retrieve) ==
  7

objects_to_keep = c(
  'new_data_tables',
  'new_tables',
  'nonew_data_tables',
  'failed_tables_retrieve',
  'failed_tables_prepare',
  'failed_tables_push',
  "check_process_01",
  'check_process_02',
  'check_process_03',
  "dt_all_raw",
  'use_DATE',
  'con',
  'start_time'
)
rm(list = setdiff(ls(), objects_to_keep))

print(glue("{crayon::bgCyan('[DONE - 01.Retrieve]')}"))

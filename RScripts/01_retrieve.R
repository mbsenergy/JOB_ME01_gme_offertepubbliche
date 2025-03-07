
print(glue::glue("{crayon::cyan('[INIT - 01.Retrive]')}"))

# Required libraries
box::use(data.table[...])
box::use(magrittr[...])
box::use(curl[...])
box::use(xml2[...])
box::use(DBI[...])
box::use(fluxer[...])


print('[00/03]')


#1. GET DATA ------------------------------------------------------------

dt_all_raw = list()
failed_tables_retrieve = data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)


# MGP --------------------------------------------------------------
 
if (use_DATE) {
    # Step 1: Get min and max dates
    date_info <- db_get_minmax_dates(con, 'ME01_gme_mgp_offers', 'DATE')
    from_data <- as.Date(date_info$max_date)
  } 


data_type <- 'MGP'
username <- "PIASARACENO"
password <- "18N15C9R"
output_dir = "data"

# Get filename list -----------------------------------
mgp_offers_files = gme_offers_get_files(data_type = data_type, output_dir = output_dir, username = username, password = password)

# check 
check_dt_01 = all(!is.null(mgp_offers_files))


print('[01/04]')


# Extract the last n elements
last_n_files = mgp_offers_files[as.Date(substr(mgp_offers_files, 1, 8), format = "%Y%m%d") > from_data]

list_mgp_offers <- lapply(last_n_files, function(file) {
    tryCatch({
        # Call gme_download_offers_file with explicit arguments
        gme_download_offers_file(
            filename = file,  
            data_type = data_type,
            output_dir = output_dir,
            username = username,       # FTP username for authentication
            password = password,       # FTP password for authentication
            raw = FALSE
        )
    }, error = function(e) {
        # In case of an error (e.g., failed download or processing), return NULL
        message("Error processing file: ", file, " - ", e$message)
        return(NULL)
    })
})
 
dt_mgp_offers = rbindlist(list_mgp_offers, fill=TRUE)



# check 
check_dt_02 = all(!is.null(dt_mgp_offers))

if(isTRUE(check_dt_02)) {
    dt_all_raw[['dt_mgp_offers']] = dt_mgp_offers
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_mgp_offers',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}
 
 
# MSD --------------------------------------------------------------
 
data_type <- 'MSD'
username <- "PIASARACENO"
password <- "18N15C9R"
output_dir = "data"

if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_msd_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
} 

# Get filename list -----------------------------------
msd_offers_files = gme_offers_get_files(data_type = data_type, output_dir = output_dir, username = username, password = password)

# check 
check_dt_02 = all(!is.null(msd_offers_files))


print('[02/04]')


# Extract the last n elements
last_n_files = msd_offers_files[as.Date(substr(msd_offers_files, 1, 8), format = "%Y%m%d") > from_data]

list_msd_offers <- lapply(last_n_files, function(file) {
    tryCatch({
        # Call gme_download_offers_file with explicit arguments
        gme_download_offers_file(
            filename = file,  
            data_type = data_type,
            output_dir = output_dir,
            username = username,       # FTP username for authentication
            password = password,       # FTP password for authentication
            raw = FALSE
        )
    }, error = function(e) {
        # In case of an error (e.g., failed download or processing), return NULL
        message("Error processing file: ", file, " - ", e$message)
        return(NULL)
    })
})
 
dt_msd_offers = rbindlist(list_msd_offers, fill=TRUE)

# check 
check_dt_02 = all(!is.null(dt_msd_offers))

if(isTRUE(check_dt_02)) {
    dt_all_raw[['dt_msd_offers']] = dt_msd_offers
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_msd_offers',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}
 
 
# MB --------------------------------------------------------------

data_type <- 'MB'
username <- "PIASARACENO"
password <- "18N15C9R"
output_dir = "data"

if (use_DATE) {
  # Step 1: Get min and max dates
  date_info <- db_get_minmax_dates(con, 'ME01_gme_mb_offers', 'DATE')
  from_data <- as.Date(date_info$max_date)
} 

# Get filename list -----------------------------------
mb_offers_files = gme_offers_get_files(data_type = data_type, output_dir = output_dir, username = username, password = password)

# check 
check_dt_03 = all(!is.null(mb_offers_files))


print('[03/04]')


# Extract the last n elements
last_n_files = mb_offers_files[as.Date(substr(mb_offers_files, 1, 8), format = "%Y%m%d") > from_data]

list_mb_offers <- lapply(last_n_files, function(file) {
    tryCatch({
        # Call gme_download_offers_file with explicit arguments
        gme_download_offers_file(
            filename = file,  
            data_type = data_type,
            output_dir = output_dir,
            username = username,       # FTP username for authentication
            password = password,       # FTP password for authentication
            raw = FALSE
        )
    }, error = function(e) {
        # In case of an error (e.g., failed download or processing), return NULL
        message("Error processing file: ", file, " - ", e$message)
        return(NULL)
    })
})
 
dt_mb_offers = rbindlist(list_mb_offers, fill=TRUE)

# check 
check_dt_03 = all(!is.null(dt_mb_offers))

if(isTRUE(check_dt_03)) {
    dt_all_raw[['dt_mb_offers']] = dt_mb_offers
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_mb_offers',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}
 
 
# XBID --------------------------------------------------------------
 
data_type <- 'XBID'
username <- "PIASARACENO"
password <- "18N15C9R"
output_dir = "data"

# Get filename list -----------------------------------
xbid_offers_files = gme_offers_get_files(data_type = data_type, output_dir = output_dir, username = username, password = password)

# check 
check_dt_04 = all(!is.null(xbid_offers_files))


print('[04/04]')


# Extract the last n elements
last_n_files = xbid_offers_files[as.Date(substr(xbid_offers_files, 1, 8), format = "%Y%m%d") > from_data]
 
list_xbid_offers <- lapply(last_n_files, function(file) {
    tryCatch({
        # Call gme_download_offers_file with explicit arguments
        gme_download_offers_file(
            filename = file,  
            data_type = data_type,
            output_dir = output_dir,
            username = username,       # FTP username for authentication
            password = password,       # FTP password for authentication
            raw = FALSE
        )
    }, error = function(e) {
        # In case of an error (e.g., failed download or processing), return NULL
        message("Error processing file: ", file, " - ", e$message)
        return(NULL)
    })
})
 
dt_xbid_offers = rbindlist(list_xbid_offers, fill=TRUE)

# check 
check_dt_04 = all(!is.null(dt_xbid_offers))

if(isTRUE(check_dt_04)) {
    dt_all_raw[['dt_xbid_offers']] = dt_xbid_offers
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_xbid_offers',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}
 
 
# MI-A1 --------------------------------------------------------------
 
# data_type <- 'MGP_Transiti'
# username <- "PIASARACENO"
# password <- "18N15C9R"
# output_dir = "data"

# # Get filename list -----------------------------------
# mgp_tran_files = gme_offers_get_files(data_type = data_type, output_dir = output_dir, username = username, password = password)

# # check 
# check_dt_05 = all(!is.null(mgp_tran_files))


# print('[05/11]')


# # Extract the last n elements
# last_n_files <- tail(mgp_tran_files, n)
# print(last_n_files)

# list_mgp_tran <- lapply(last_n_files, function(file) {
#     tryCatch({
#         # Call gme_download_offers_file with explicit arguments
#         gme_download_offers_file(
#             filename = file,  
#             data_type = data_type,
#             output_dir = output_dir,
#             username = username,       # FTP username for authentication
#             password = password,       # FTP password for authentication
#             raw = FALSE
#         )
#     }, error = function(e) {
#         # In case of an error (e.g., failed download or processing), return NULL
#         message("Error processing file: ", file, " - ", e$message)
#         return(NULL)
#     })
# })
 
# dt_mgp_tran = rbindlist(list_mgp_tran)

# # check 
# check_dt_04 = all(!is.null(dt_mgp_tran))

# if(isTRUE(check_dt_04)) {
#     dt_all_raw[['dt_mgp_tran']] = dt_mgp_tran
# } else {
#     failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
#     table_name = 'dt_mgp_tran',
#     num_rows = 0,
#     type = 'ERROR_RETRIEVE'
#     ))
# }
# print('[05/11]') 
 
 

# EXPORT RAW ===========================================================================


# xxx. CHECK ===========================================================================

# check_process_01 = all(check_dt_01, check_dt_02, check_dt_03, check_dt_04, check_dt_05, check_dt_06, check_dt_07, check_dt_08, check_dt_09, check_dt_10, check_dt_11)
check_process_01 = all(check_dt_01, check_dt_02, check_dt_03, check_dt_04)

objects_to_keep = c('n_elements', 'failed_tables_retrieve', "check_process_01", "dt_all_raw", 'database_name', 'job_name', 'use_DATE', 'conn')
rm(list = setdiff(ls(), objects_to_keep))

print(glue::glue("{crayon::bgCyan('[DONE - 01.Retrieve]')}"))

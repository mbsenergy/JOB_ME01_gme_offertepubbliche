
print(glue("{crayon::cyan('[INIT - 04.Logs]')}"))

box::use(duckdb[...])
box::use(DBI[...])
box::use(glue[...])
box::use(data.table[...])
box::use(magrittr[...])
box::use(fluxer[...])


# Push Log file ------------------------

## Check Processes
if(!is.null(check_process_01)) {
    if(!check_process_01) {error_zone_1 = '01_RETRIEVED'} else {error_zone_1 = NULL}
} else {
   error_zone_1 = '01_RETRIEVED'
   check_process_01 = FALSE
}

if(!is.null(check_process_02)) {
    if(!check_process_02) {error_zone_2 = '02_PREPARE'} else {error_zone_2 = NULL}
} else {
   error_zone_2 = '02_PREPARE'
   heck_process_02 = FALSE
}

if(!is.null(check_process_03)) {
    if(!check_process_03) {error_zone_3 = '03_PUSH'} else {error_zone_3 = NULL}
} else {
   error_zone_3 = '03_PUSH'
   check_process_03 = FALSE
}


if(all(check_process_01, check_process_02, check_process_03)) {
    error_zone = '00_OK'
    } else {
     error_zone = paste(error_zone_1, error_zone_2, error_zone_3, collapse = '-')
    }

### Check Tables
if(!is.null(failed_tables_retrieve)) {
    FAILED_TABLES_01 = failed_tables_retrieve$table_name
} else {
   FAILED_TABLES_01 = 'xx_RUNERROR'
}

if(!is.null(failed_tables_prepare)) {
    FAILED_TABLES_02 = failed_tables_prepare$table_name
} else {
   FAILED_TABLES_02 = 'xx_RUNERROR'
}

if(!is.null(failed_tables)) {
    FAILED_TABLES_03 = failed_tables$table_name
} else {
   FAILED_TABLES_03 = 'xx_RUNERROR'
}

if(!is.null(nonew_data_tables)) {
    UPDATED_TABLES_NO = nonew_data_tables$table_name
} else {
   UPDATED_TABLES_NO = 'xx_RUNERROR'
}

if(!is.null(new_data_tables)) {
    UPDATED_TABLES_YES = new_data_tables$table_name
    UPDATED_TABLES_YES_N = new_data_tables$num_rows
} else {
   UPDATED_TABLES_YES = 'xx_RUNERROR'
   UPDATED_TABLES_YES_N = 'xx_RUNERROR'
}

if(!is.null(new_tables)) {
    NEW_TABLES = new_tables$table_name
    NEW_TABLES_N = new_tables$num_rows
} else {
   NEW_TABLES = 'xx_RUNERROR'
   NEW_TABLES_N = 'xx_RUNERROR'
}

## Create log table by run
dt_log = data.table(
    JOB_NAME = job_name,
    DB_NAME = database_name,
    RUN_DATE = as.character(Sys.Date()),
    ERROR_ZONE = error_zone,
    FAILED_TABLES_01 = paste(FAILED_TABLES_01, collapse = '-'),
    FAILED_TABLES_02 = paste(FAILED_TABLES_02, collapse = '-'),
    FAILED_TABLES_03 = paste(FAILED_TABLES_03, collapse = '-'),
    UPDATED_TABLES_NO = paste(UPDATED_TABLES_NO, collapse = '-'),
    UPDATED_TABLES_YES = paste(UPDATED_TABLES_YES, collapse = '-'),
    NEW_TABLES = paste(NEW_TABLES, collapse = '-'),
    UPDATED_TABLES_YES_N = paste(UPDATED_TABLES_YES_N, collapse = '-'),
    NEW_TABLES_N = paste(NEW_TABLES_N, collapse = '-')   
)


# PUSH TABLE ----------------------------------------------------------

if(!exists("conn")) {connect_md(store_conn = TRUE)}
exist_db = check_database_md(connection = conn, database_name = 'central_db')
if(isFALSE(exist_db)) {glue::glue("{crayon::bgRed('[ERROR]')} Database '{database_name}' does not exist")}
exist_dt = check_table_md(conn, 'job_logs_dt', verbose = TRUE)
if(isFALSE(exist_dt)) {glue::glue("{crayon::bgRed('[ERROR]')} Database job_logs_dt does not exist")}


tryCatch({
    dbExecute(conn, paste('USE', 'central_db'))
    dbAppendTable(conn, 'job_logs_dt', dt_log) 
    dbExecute(conn, paste(' UPDATE SHARE', 'central_db'))
    message(glue("{crayon::bgGreen('[OK]')} Logs table uploaded successfully!"))
    posted = TRUE 
 },
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        posted = FALSE 
})  


# GENERAL CHECKS ----------------------------------------------------------

check_logs = !is.null(dt_log)
if(check_logs) {
    check_process_04 = TRUE
} else {
    check_process_04 = FALSE
}

if(posted) {
    check_process_04 = TRUE
} else {
    check_process_04 = FALSE
    stop(message(glue::glue("{crayon::bgRed('[ERROR]')} Couldnt post log into central_db :{e$message}")))
}

print(glue("{crayon::bgCyan('[DONE - 04.Logs]')}"))


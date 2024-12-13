
print(glue("{crayon::cyan('[INIT - 01.Retrive]')}"))

# Required libraries
box::use(data.table[...])
box::use(magrittr[...])


print('[00/03]')

#1. GET DATA ------------------------------------------------------------

dt_all_raw = list()
failed_tables_retrieve = data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)


## 1.1 mtcars------------
dt_01 <- tryCatch({
    dt  = copy(mtcars)
    setDT(dt , keep.rownames = TRUE)
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})

# check 
check_dt_01 = all(!is.null(dt_01))

if(isTRUE(check_dt_01)) {
    dt_all_raw[['dt_01']] = dt_01
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_01',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}

print('[01/03]')


## 1.2 iris ----------------
dt_02 <- tryCatch({
    dt  = copy(iris)
    setDT(dt , keep.rownames = TRUE)
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
# check 
check_dt_02 = all(!is.null(dt_02))

if(isTRUE(check_dt_02)) {
    dt_all_raw[['dt_02']] = dt_02
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_02',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}
print('[02/03]')



## 1.3 airquality ------------
dt_03 <- tryCatch({
    dt  = copy(airquality)
    setDT(dt , keep.rownames = TRUE)
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  
        NULL  
})
# check 
check_dt_03 = all(!is.null(dt_03))

if(isTRUE(check_dt_03)) {
    dt_all_raw[['dt_03']] = dt_03
} else {
    failed_tables_retrieve <- rbind(failed_tables_retrieve, data.table::data.table(
    table_name = 'dt_03',
    num_rows = 0,
    type = 'ERROR_RETRIEVE'
    ))
}
print('[03/03]')

# EXPORT RAW ===========================================================================

saveRDS(dt_all_raw, 'dt_all_raw.rds')

# xxx. CHECK ===========================================================================

check_process_01 = all(check_dt_01, check_dt_02, check_dt_03)

objects_to_keep = c('n_elements', 'failed_tables_retrieve', "check_process_01", "dt_all_raw", 'database_name', 'job_name', 'use_DATE', 'conn')
rm(list = setdiff(ls(), objects_to_keep))

print(glue("{crayon::bgCyan('[DONE - 01.Retrieve]')}"))

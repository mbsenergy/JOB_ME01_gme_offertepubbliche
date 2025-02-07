print(glue("{crayon::cyan('[INIT - 02.Prepare]')}"))

box::use(data.table[...])
box::use(magrittr[...])

# if(length(dt_all_raw) == 0) {
#     dt_all_raw = readRDS('dt_all_raw.rds')
#   }


# 2. TRANSFORM TABLE----------------------------------
dt_all_elaborated = list()
failed_tables_prepare = data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)

##  2.1 MGP ------------

dt_01_01 <- tryCatch({
    dt = copy(dt_all_raw$dt_mgp_offers) 
    setDT(dt)
    setnames(dt, toupper(names(dt)))

    dt[, md_source := "GME"] 
    dt[, md_table := "MGP Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_1_a = all(nrow(dt_01_01) > 0 & ncol(dt_01_01) == 28)
check_dt_1_b = !is.null(dt_01_01)  
check_dt_1 = all(check_dt_1_a, check_dt_1_b)

if(isTRUE(check_dt_1)) {
    dt_all_elaborated[["ME01_gme_mgp_offers"]] = dt
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mgp_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}
print('[01/11]')
 
 
##  2.2 MSD ------------

dt_01_02 <- tryCatch({
    dt = copy(dt_all_raw$dt_msd_offers) 
    setDT(dt)
    setnames(dt, toupper(names(dt)))
    dt[, md_source := "GME"] 
    dt[, md_table := "MSD Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_2_a = all(nrow(dt_01_02) > 0 & ncol(dt_01_02) == 28)
check_dt_2_b = !is.null(dt_01_02)  
check_dt_2 = all(check_dt_2_a, check_dt_2_b)

if(isTRUE(check_dt_2)) {
    dt_all_elaborated[["ME01_gme_msd_offers"]] = dt_01_02
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_msd_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}
print('[02/11]') 
 
 
##  2.3 MB ------------

dt_01_03 <- tryCatch({
    dt = copy(dt_all_raw$dt_mb_offers) 
    setDT(dt)
    setnames(dt, toupper(names(dt)))
    dt[, md_source := "GME"] 
    dt[, md_table := "MB Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_3_a = all(nrow(dt_01_03) > 0 & ncol(dt_01_03) == 29)
check_dt_3_b = !is.null(dt_01_03)  
check_dt_3 = all(check_dt_3_a, check_dt_3_b)

if(isTRUE(check_dt_3)) {
    dt_all_elaborated[["ME01_gme_mb_offers"]] = dt_01_03
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mb_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}
print('[03/11]')  
 
 
##  2.4 XBID ------------

dt_01_04 <- tryCatch({
    dt = copy(dt_all_raw$dt_xbid_offers) 
    setDT(dt)
    setnames(dt, toupper(names(dt)))
    dt[, md_source := "GME"] 
    dt[, md_table := "XBID Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_4_a = all(nrow(dt_01_04) > 0 & ncol(dt_01_04) == 15)
check_dt_4_b = !is.null(dt_01_04)  
check_dt_4 = all(check_dt_4_a, check_dt_4_b)

if(isTRUE(check_dt_4)) {
    dt_all_elaborated[["ME01_gme_xbid_offers"]] = dt_01_04
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_xbid_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}
print('[04/11]')  


# EXPORT ELABORATED ===========================================================================
saveRDS(dt_all_elaborated, 'dt_all_elaborated.rds')



# xxx. CHECK ===========================================================================

# check_process_02 = all(check_dt_1, check_dt_2, check_dt_3, check_dt_4, check_dt_5, check_dt_6, check_dt_7, check_dt_8, check_dt_9, check_dt_10, check_dt_11)
check_process_02 = all(check_dt_1, check_dt_2, check_dt_3, check_dt_4)

objects_to_keep = c('n_elements', 'failed_tables_retrieve', 'failed_tables_prepare', 'check_process_01', "check_process_02", 'check_process_03', "dt_all_elaborated", 'database_name', 'job_name', 'use_DATE', 'conn')
rm(list = setdiff(ls(), objects_to_keep))

print(glue("{crayon::bgCyan('[DONE - 02.Prepare]')}"))

print(glue("{crayon::cyan('[INIT - 02.Prepare]')}"))

box::use(data.table[...])
box::use(magrittr[...])

# 2. TRANSFORM TABLE----------------------------------
dt_all_elaborated = list()
failed_tables_prepare = data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)

##  2.1 mtcars ------------

dt_01_01 <- tryCatch({
    dt = copy(dt_all_raw$dt_01) 
    setDT(dt)

    dt[, md_source := "R_base"] 
    dt[, md_table := "Famous < mtcars > table for examples in R"] 
    dt[, md_last_update := Sys.Date()] 

    setnames(dt, 
           old = c("rn", "mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb"),
           new = c("CAR_NAME", "MPG", "CYL", "DISP", "HP", "DRAT", "WT", "QSEC", "VS", "AM", "GEAR", "CARB")
             )

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update", 
                         "CAR_NAME", "MPG", "CYL", "DISP", "HP", "DRAT", "WT", "QSEC", "VS", "AM", "GEAR", "CARB")
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_1_a = all(nrow(dt_01_01) > 0 & ncol(dt_01_01) == 15 & nrow(dt_01_01) == 32)
check_dt_1_b = !is.null(dt_01_01)  
check_dt_1 = all(check_dt_1_a, check_dt_1_b)

if(isTRUE(check_dt_1)) {
    dt_all_elaborated[["dt_01_01"]] = dt_01_01
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'dt_01_01',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}
print('[01/03]')



## 2.2 iris ----------------
dt_02_01 <- tryCatch({
    dt = copy(dt_all_raw$dt_02) 
    setDT(dt)

    dt[, md_source := "R_base"] 
    dt[, md_table := "Famous < iris > table for examples in R"] 
    dt[, md_last_update := Sys.Date()] 

    setnames(dt, 
            old = c("rn", "Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", "Species"),
            new = c("SAMPLE", "SEPAL_LENGTH", "SEPAL_WIDTH", "PETAL_LENGTH", "PETAL_WIDTH", "SPECIES")
         )

    dt[, SPECIES := as.character(SPECIES)]

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update", 
                         "SAMPLE", "SEPAL_LENGTH", "SEPAL_WIDTH", "PETAL_LENGTH", "PETAL_WIDTH", "SPECIES")
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_2_a = all(nrow(dt_02_01) > 0 & ncol(dt_02_01) == 9 & nrow(dt_02_01) == 150)
check_dt_2_b = !is.null(dt_02_01)  
check_dt_2 = all(check_dt_2_a, check_dt_2_b) 


if(isTRUE(check_dt_2)) {
    dt_all_elaborated[["dt_02_01"]] = dt_02_01
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'dt_02_01',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}

print('[02/03]')



## 2.3 airquality ----------------
dt_03_01 <- tryCatch({
    dt = copy(dt_all_raw$dt_03) 
    setDT(dt)

    dt[, md_source := "R_base"] 
    dt[, md_table := "Famous < airquality > table for examples in R"] 
    dt[, md_last_update := Sys.Date()] 

    dt[, rn := NULL] 

    setnames(dt, 
         old = c("Ozone", "Solar.R", "Wind", "Temp", "Month", "Day"),
         new = c("OZONE", "SOLAR_RADIATION", "WIND", "TEMP", "MONTH", "DAY")
         )

    ## Create date and long format
    dt[, YEAR := as.character(2024)]
    dt[, DATE := as.Date(paste(YEAR, MONTH, DAY, sep = '-'))]

    dt = melt(
     data = dt,
     id.vars = c('DATE', 'YEAR', 'MONTH', 'DAY', 'md_source', 'md_table', 'md_last_update'),
     variable.name = 'VARIABLE', variable.factor = FALSE,
     value.name = 'VALUE'
     )

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update", 
                         'DATE', 'YEAR', 'MONTH', 'DAY',
                         'VARIABLE', 'VALUE')
                         )
    dt }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})
check_dt_3_a = all(nrow(dt_03_01) > 0 & ncol(dt_03_01) == 9 & nrow(dt_03_01) == 150)
check_dt_3_b = !is.null(dt_03_01)  
check_dt_3 = all(check_dt_3_a, check_dt_3_b)   

if(isTRUE(check_dt_3)) {
    dt_all_elaborated[["dt_03_01"]] = dt_03_01
} else {
    failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'dt_03_01',
    num_rows = 0,
    type = 'ERROR_PREPARE'
    ))
}

print('[03/03]')


# EXPORT ELABORATED ===========================================================================
saveRDS(dt_all_elaborated, 'dt_all_elaborated.rds')


# xxx. CHECK ===========================================================================

check_process_02 = all(check_dt_1, check_dt_2, check_dt_3)

objects_to_keep = c('n_elements', 'failed_tables_retrieve', 'failed_tables_prepare', 'check_process_01', "check_process_02", 'check_process_03', "dt_all_elaborated", 'database_name', 'job_name', 'use_DATE', 'conn')
rm(list = setdiff(ls(), objects_to_keep))

print(glue("{crayon::bgCyan('[DONE - 02.Prepare]')}"))



print(glue("{crayon::cyan('[INIT - 03.Push]')}"))
# source(fs::path('RScripts', 'functions.R'))

# Required libraries
box::use(DBI[...])
box::use(RPostgreSQL[...])
box::use(RPostgres[...])
box::use(glue[...])
box::use(data.table[...])
box::use(magrittr[...])



## 3. conECT TO PG AND LOAD ----------------------------------

##  3.1 Log into PostgresSQL ------------
 
con = DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv('PG_FLUX_HOST'),
    port = Sys.getenv('PG_FLUX_PORT'),
    dbname = Sys.getenv('PG_FLUX_DBNAME'),
    user = Sys.getenv('PG_FLUX_USER'),
    password = Sys.getenv('PG_FLUX_PSW'),
    sslmode = "require"  
  )
 
if(exists("con")) {
    print(glue("{crayon::green('[CONNECTED PG]')}"))
} else {
    print(glue("{crayon::green('[ERROR PG]')}"))
}
 
 ##  3.2 Update tables in MD ------------

# Initialize a list to store names of failed table creations
failed_tables <- data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)
nonew_data_tables <- data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)
new_data_tables <- data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)
new_tables <- data.table::data.table(
    table_name = character(),
    num_rows = integer(),
    type = character()
)

use_DATE = FALSE

# Loop through the list of data frames (dt_all_elaborated)
for (i in seq_along(dt_all_elaborated)) {

    # Extract info
    table_name <- names(dt_all_elaborated)[i]
    data <- dt_all_elaborated[[i]] 

    message(glue('{table_name}'))

    table_exists = DBI::dbListTables(con)
    table_exists = any(table_exists %in% table_name)

    if(isTRUE(table_exists)) {
    # Get the last date and filter
    
    if(isTRUE(use_DATE)) {
        dt_lastdate <- dbGetQuery(con, glue('SELECT "DATE" FROM "{table_name}";'))
        dt_lastdate = as.Date(dt_lastdate$DATE, format = "%Y-%m-%d")
        dt_lastdate = dt_lastdate |> max()

        if(!is.null(dt_lastdate)) {
            data_new = data[DATE > dt_lastdate]
        } else {
           message(glue::glue("{crayon::bgRed('[ERROR]')} Last date cant be retrieved"))
        }
    } else {
           data_new = copy(data)
    }
    
    if(nrow(data_new) > 0) {

     nrows = nrow(data_new)

    tryCatch({
        dbAppendTable(con, table_name, data_new)   
            new_data_tables <- rbind(new_data_tables, data.table::data.table(
                table_name = table_name,
                num_rows = nrows,
                type = 'NEW_DATA'
            ))
        message(glue("{crayon::bgGreen('[OK]')} Data appended table '{table_name}': '{nrows}'"))
        }, error = function(e) {
            failed_tables <- rbind(failed_tables, data.table::data.table(
                table_name = table_name,
                num_rows = 0,
                type = 'ERROR_wNEWDATA'
            ))
        message(glue("{crayon::bgRed('[ERROR]')} Failed to append table '{table_name}': {e$message}"))
    })
    } else {
        message(glue("{crayon::bgYellow('[OK]')} No new data to append '{table_name}'"))
        nonew_data_tables <- rbind(nonew_data_tables, data.table::data.table(
                table_name = table_name,
                num_rows = nrow(data_new),
                type = 'NO_NEWDATA'
            ))
    }

    } else {
        data_new = copy(data)
        dbWriteTable(con, table_name, data_new, overwrite = FALSE, append = FALSE)
        new_tables <- rbind(new_tables, data.table::data.table(
                table_name = table_name,
                num_rows = nrow(data_new),
                type = 'NEW_TABLE'
            ))   
        message(glue("{crayon::bgYellow('[NEW TABLE]')} New table created '{table_name}'"))
    }
     
}




# 4. GENERAL CHECKS ----------------------------------------------------------

# Print the list of failed table names
check_log_1 = nrow(failed_tables) > 0
check_log_2 = nrow(new_tables) > 0
check_log = all(!check_log_1 & !check_log_2)

if(check_log) {
    message(glue("{crayon::bgGreen('[OK]')} All tables updated successfully!"))
} else {
    message(glue("{crayon::bgRed('[ERROR]')} The following tables failed to be updated: {paste(failed_tables, collapse = ', ')}"))
}

if(check_log_2) {
    message(glue("{crayon::bgYellow('[NOTE]')} The following tables were created from scratch: {paste(new_tables, collapse = ', ')}"))
}

if(check_log) {
    check_process_03 = TRUE
} else {
    check_process_03 = FALSE
}

objects_to_keep = c('n_elements', 'failed_tables_retrieve', 'failed_tables_prepare', 'check_process_01', "check_process_02", "check_process_03", "failed_tables", 'nonew_data_tables', 'new_tables', 'new_data_tables', 'database_name', 'job_name', 'con')
rm(list = setdiff(ls(), objects_to_keep))


# 5. UPDATE SHARE DB ----------------------------------------------------------

# if(check_process_03) {
#     dbExecute(con, paste('USE', database_name))
#     dbExecute(con, paste(' UPDATE SHARE', database_name, ';'))
#     message(glue("{crayon::bgGreen('[OK]')} DB Share updated correctly"))
# }

print(glue("{crayon::bgCyan('[DONE - 03.Push]')}"))



print(glue::glue("{crayon::magenta('[JOB - Starting...]')}"))

# ENV Variables -------------------------------------------------------------------

use_DATE = TRUE

# Setup ----------------------------------------------------------------------

box::use(data.table[...])
box::use(DBI[...])
box::use(magrittr[...])
box::use(curl[...])
box::use(xml2[...])
box::use(RPostgreSQL[...])
box::use(RPostgres[...])
box::use(glue[...])
box::use(fluxer[...])

## Log into PostgresSQL ------------

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

source('RScripts/functions.R')

check_process_01 = NULL
check_process_02 = NULL
check_process_03 = NULL

failed_tables_retrieve = data.table::data.table(
  table_name = character(),
  num_rows = integer(),
  type = character()
)

failed_tables_prepare = data.table::data.table(
  table_name = character(),
  num_rows = integer(),
  type = character()
)

failed_tables_push = data.table::data.table(
  table_name = character(),
  num_rows = integer(),
  type = character()
)

nonew_data_tables = data.table::data.table(
  table_name = character(),
  num_rows = integer(),
  type = character()
)
new_data_tables = data.table::data.table(
  table_name = character(),
  num_rows = integer(),
  type = character()
)
new_tables = data.table::data.table(
  table_name = character(),
  num_rows = integer(),
  type = character()
)


# 01. Retrieve data from FRED --------------------------------------------------------

source('RScripts/01_retrieve.R')
if(!check_process_01) {
  glue::glue("{crayon::bgRed('[ATTENTION] - 01.Retrieve')} ")  # Print error message
}


# 02. Prepare data according to Schema --------------------------------------------------------

source('RScripts/02_prepare.R')
if(!check_process_02) {
  glue::glue("{crayon::bgRed('[ATTENTION] - Push')} ")  # Print error message
}




# 03. Push data to PRODUCTION CLUSTER - ECONOMICS Database  --------------------------------------------------------

source('RScripts/03_push.R')

if(!check_process_03) {
  glue::glue("{crayon::bgRed('[ATTENTION] - Push')} ")  # Print error message
}


print(glue("{crayon::bgMagenta('[JOB - COMPLETED!]')}"))

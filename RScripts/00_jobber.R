log_file = "jobber.log"
sink(log_file, append = TRUE, split = TRUE)

start_time = Sys.time()

print(glue::glue(
  "{crayon::magenta('[JOB ME01_gme_offertepubbliche - Starting...]')} {start_time}"
))


# Setup ----------------------------------------------------------------------

library(data.table)
library(DBI)
library(magrittr)
library(curl)
library(xml2)
library(RPostgreSQL)
library(RPostgres)
library(glue)
library(fluxer)

use_DATE = TRUE

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

if (exists("con")) {
  print(glue("{crayon::green('[CONNECTED PG]')}"))
} else {
  print(glue("{crayon::green('[ERROR PG]')}"))
}


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
if (!check_process_01) {
  glue::glue("{crayon::bgRed('[ATTENTION] - 01.Retrieve')} ") # Print error message
}


# 02. Prepare data according to Schema --------------------------------------------------------

source('RScripts/02_prepare.R')
if (!check_process_02) {
  glue::glue("{crayon::bgRed('[ATTENTION] - Push')} ") # Print error message
}


# 03. Push data to PRODUCTION CLUSTER - ECONOMICS Database  --------------------------------------------------------

source('RScripts/03_push.R')

if (!check_process_03) {
  glue::glue("{crayon::bgRed('[ATTENTION] - Push')} ") # Print error message
}


elapsed = Sys.time() - start_time

print(glue::glue(
  "{crayon::bgMagenta('[JOB ME01_gme_offertepubbliche - COMPLETED!]')} Elapsed time: {round(elapsed, 2)} {units(elapsed)}"
))

sink()

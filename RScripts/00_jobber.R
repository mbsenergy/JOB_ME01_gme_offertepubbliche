box::use(glue[...])


print(glue::glue("{crayon::magenta('[JOB - Starting...]')}"))

# ENV Variables -------------------------------------------------------------------

job_name = 'JOB_XX01_template'
database_name = 'trial_db'
use_DATE = FALSE

# Setup ----------------------------------------------------------------------

source('RScripts/functions.R')

check_process_01 = NULL
check_process_02 = NULL
check_process_03 = NULL

failed_tables_retrieve = NULL
failed_tables_prepare = NULL
failed_tables = NULL
nonew_data_tables = NULL
new_data_tables = NULL
new_tables = NULL


# 01. Retrieve data from FRED --------------------------------------------------------

source('RScripts/01_retrieve.R')
if(!check_process_01) {
    glue::glue("{crayon::bgRed('[ATTENTION] - 01.Retrieve')} ")  # Print error message
}


# 02. Prepare data according to Schema --------------------------------------------------------

source('RScripts/02_prepare.R')

if(!check_process_02) {
    glue::glue("{crayon::bgRed('[ATTENTION] - 02.Prepare')} ")  # Print error message
}



# 03. Push data to PRODUCTION CLUSTER - ECONOMICS Database  --------------------------------------------------------
 
source('RScripts/03_push.R')
 
if(!check_process_03) {
    glue::glue("{crayon::bgRed('[ATTENTION] - Push')} ")  # Print error message
 }



# 04. Upload logs to LOGS CLUSTER - LOGS Database  --------------------------------------------------------

source('RScripts/04_log_checks.R')
 
if(!check_process_04) {
    glue::glue("{crayon::bgRed('[ERROR] - 04.Logs')} ")  # Print error message
 }

print(glue("{crayon::bgMagenta('[JOB - COMPLETED!]')}"))

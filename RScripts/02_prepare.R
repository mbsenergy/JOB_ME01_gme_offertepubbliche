print(glue("{crayon::cyan('[INIT - 02.Prepare]')}"))

# 2. TRANSFORM TABLE----------------------------------
dt_all_elaborated = list()

##  2.1 MGP ------------

dt_01_01 <- tryCatch({
    dt = copy(dt_all_raw[['ME01_gme_mgp_offers']]) 
    if(!is.null(dt)) {
    setDT(dt)
    setnames(dt, toupper(names(dt)))
    setnames(dt, 'BID_OFFER_DATE_DT_PARSED', 'DATE')
    
    dt[, md_source := "GME"] 
    dt[, md_table := "MGP Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }
    }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})

if(!is.null(dt_01_01)) {
  check_dt = all(nrow(dt_01_01) > 0)
} else {
  check_dt = TRUE
}

if(!is.null(dt_01_01) & isTRUE(check_dt)) {
  print(glue("{crayon::green('ME01_gme_mgp_offers processed')}"))
  dt_all_elaborated[["ME01_gme_mgp_offers"]] = dt_01_01
} 
if(!is.null(dt_01_01) & isFALSE(check_dt)) {
  print(glue("{crayon::red('ME01_gme_mgp_offers failed to process')}"))
  failed_tables_prepare = rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mgp_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
  ))
}  

if (is.null(dt_01_01)) {
  print(glue("{crayon::yellow('ME01_gme_mgp_offers nothing to process')}"))
}
print('[01/07]')
 
 
##  2.2 MSD ------------

dt_01_02 <- tryCatch({
    dt = copy(dt_all_raw[['ME01_gme_msd_offers']]) 
    if(!is.null(dt)) {
    setDT(dt)
    setnames(dt, toupper(names(dt)))
    colnames(dt)[which(names(dt) == "INTERVAL_NO")[1]] = "INTERVAL_NO_OLD"
    dt[, INTERVAL_NO_OLD := NULL]
    setnames(dt, 'BID_OFFER_DATE_DT_PARSED', 'DATE')
    
    dt[, md_source := "GME"] 
    dt[, md_table := "MSD Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }
    }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})

if(!is.null(dt_01_02)) {
  check_dt = all(nrow(dt_01_02) > 0)
} else {
  check_dt = TRUE
}

if(!is.null(dt_01_02) & isTRUE(check_dt)) {
  print(glue("{crayon::green('ME01_gme_msd_offers processed')}"))
  dt_all_elaborated[["ME01_gme_msd_offers"]] = dt_01_02
} 
if(!is.null(dt_01_02) & isFALSE(check_dt)) {
  print(glue("{crayon::red('ME01_gme_msd_offers failed to process')}"))
  failed_tables_prepare = rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_msd_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
  ))
}  

if (is.null(dt_01_02)) {
  print(glue("{crayon::yellow('ME01_gme_msd_offers nothing to process')}"))
}
print('[02/07]')
 
 
##  2.3 MB ------------

dt_01_03 <- tryCatch({
    dt =  copy(dt_all_raw[['ME01_gme_mb_offers']]) 
    if(!is.null(dt)) {
    setDT(dt)
    setnames(dt, toupper(names(dt)))
    setnames(dt, 'BID_OFFER_DATE_DT_PARSED', 'DATE')
    
    dt[, md_source := "GME"] 
    dt[, md_table := "MB Offers"] 
    dt[, md_last_update := Sys.Date()] 

    setcolorder(dt, 
            neworder = c("md_source","md_table", "md_last_update")
                         )
    dt }
    }, 
    error = function(e) {
        message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))  # Print error message
        NULL  
})


if(!is.null(dt_01_03)) {
  check_dt = all(nrow(dt_01_03) > 0)
} else {
  check_dt = TRUE
}

if(!is.null(dt_01_03) & isTRUE(check_dt)) {
  print(glue("{crayon::green('ME01_gme_mb_offers processed')}"))
  dt_all_elaborated[["ME01_gme_mb_offers"]] = dt_01_03
} 
if(!is.null(dt_01_03) & isFALSE(check_dt)) {
  print(glue("{crayon::red('ME01_gme_mb_offers failed to process')}"))
  failed_tables_prepare = rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mb_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
  ))
}  

if (is.null(dt_01_03)) {
  print(glue("{crayon::yellow('ME01_gme_mb_offers nothing to process')}"))
}
print('[03/07]')
 
 
##  2.4 XBID ------------
dt_01_04 <- tryCatch({
  dt = copy(dt_all_raw[['ME01_gme_xbid_offers']])
  setDT(dt)
  setnames(dt, toupper(names(dt)))
  setnames(dt, 'TIMESTAMP', 'DATETIME')
  
  dt[, md_source := "GME"]
  dt[, md_table := "XBID Offers"]
  dt[, md_last_update := Sys.Date()]
  
  # Estrazione robusta da PRODOTTO
  dt[, c("DATE", "H", "Q", "ZONA") := {
    matches <- regexec("^([0-9]{8})-H([0-9]{2})(?:-QH([0-9]{2}))?-(\\w+)$", PRODOTTO)
    parsed <- regmatches(PRODOTTO, matches)
    lapply(1:4, function(i) sapply(parsed, function(x) if (length(x) >= i + 1) x[[i + 1]] else NA_character_))
  }]
  
  # Forza Q vuoti a NA
  dt[Q == "", Q := NA_character_]
  
  # Converte DATE in formato Date
  dt[, DATE := as.Date(DATE, format = "%Y%m%d")]
  
  # TIPO corretto
  dt[, TIPO := ifelse(!is.na(Q), "Q", ifelse(!is.na(H), "H", NA_character_))]
  
  setcolorder(dt, neworder = c(  "md_source", "md_table", "md_last_update",
                                 "TRANSACTION_REFERENCE_NO", "PURPOSE_CD", "STATUS_CD", "DATETIME",
                                 "MARKET_CD", "PRODOTTO", "DATE", "H", "Q", "ZONA", "TIPO"))
  dt
}, error = function(e) {
  message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))
  NULL
})
dt_01_04[, H := as.numeric(H)]
dt_01_04[, Q := as.numeric(Q)]


check_dt_4_a = all(nrow(dt_01_04) > 0 & ncol(dt_01_04) == 21)
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
print('[04/07]')



##  2.5 MI-A1 ------------
dt_01_05 <- tryCatch({
  dt = copy(dt_all_raw[['ME01_gme_mia1_offers']])
  setDT(dt)
  setnames(dt, toupper(names(dt)))
  setnames(dt, 'BID_OFFER_DATE_DT_PARSED', 'DATE')
  
  dt[, md_source := "GME"]
  dt[, md_table := "mia1 Offers"]
  dt[, md_last_update := Sys.Date()]
  
  setcolorder(dt, neworder = c(  "md_source", "md_table", "md_last_update"))
  dt
}, error = function(e) {
  message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))
  NULL
})


dt_01_05[, MINIMUM_ACCEPTANCE_RATIO:= NULL ]
dt_01_05[, BLOCK_ID:= NULL ]
dt_01_05[, OFFER_TYPE:= NULL ]
dt_01_05[, TIME:= NA_character_ ]


check_dt_5_a = all(nrow(dt_01_05) > 0 & ncol(dt_01_05) == 27)
check_dt_5_b = !is.null(dt_01_05)
check_dt_5 = all(check_dt_5_a, check_dt_5_b)

if(isTRUE(check_dt_5)) {
  dt_all_elaborated[["ME01_gme_mia1_offers"]] = dt_01_05
} else {
  failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mia1_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
  ))
}



print('[05/07]')


##  2.6 MI-A2 ------------
dt_01_06 <- tryCatch({
  dt = copy(dt_all_raw[['ME01_gme_mia2_offers']])
  setDT(dt)
  setnames(dt, toupper(names(dt)))
  setnames(dt, 'BID_OFFER_DATE_DT_PARSED', 'DATE')
  
  dt[, md_source := "GME"]
  dt[, md_table := "mia2 Offers"]
  dt[, md_last_update := Sys.Date()]

  setcolorder(dt, neworder = c(  "md_source", "md_table", "md_last_update"))
  dt
}, error = function(e) {
  message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))
  NULL
})

dt_01_06[, MINIMUM_ACCEPTANCE_RATIO:= NULL ]
dt_01_06[, BLOCK_ID:= NULL ]
dt_01_06[, OFFER_TYPE:= NULL ]
dt_01_06[, TIME:= NA_character_ ]

check_dt_6_a = all(nrow(dt_01_06) > 0 & ncol(dt_01_06) == 27)
check_dt_6_b = !is.null(dt_01_06)
check_dt_6 = all(check_dt_6_a, check_dt_6_b)

if(isTRUE(check_dt_6)) {
  dt_all_elaborated[["ME01_gme_mia2_offers"]] = dt_01_06
} else {
  failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mia2_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
  ))
}
print('[06/07]')




##  2.7 MI-A3 ------------
dt_01_07 <- tryCatch({
  dt = copy(dt_all_raw[['ME01_gme_mia3_offers']])
  setDT(dt)
  setnames(dt, toupper(names(dt)))
  setnames(dt, 'BID_OFFER_DATE_DT_PARSED', 'DATE')
  
  dt[, md_source := "GME"]
  dt[, md_table := "mia1 Offers"]
  dt[, md_last_update := Sys.Date()]

  setcolorder(dt, neworder = c(  "md_source", "md_table", "md_last_update"))
  dt
}, error = function(e) {
  message(glue::glue("{crayon::bgRed('[ERROR]')} {e$message}"))
  NULL
})

dt_01_07[, MINIMUM_ACCEPTANCE_RATIO:= NULL ]
dt_01_07[, BLOCK_ID:= NULL ]
dt_01_07[, OFFER_TYPE:= NULL ]
dt_01_07[, TIME:= NA_character_ ]

check_dt_7_a = all(nrow(dt_01_07) > 0 & ncol(dt_01_07) == 27)
check_dt_7_b = !is.null(dt_01_07)
check_dt_7 = all(check_dt_7_a, check_dt_7_b)

if(isTRUE(check_dt_7)) {
  dt_all_elaborated[["ME01_gme_mia3_offers"]] = dt_01_07
} else {
  failed_tables_prepare <- rbind(failed_tables_prepare, data.table::data.table(
    table_name = 'ME01_gme_mia3_offers',
    num_rows = 0,
    type = 'ERROR_PREPARE'
  ))
}
print('[07/07]')

# EXPORT ELABORATED ===========================================================================
saveRDS(dt_all_elaborated, 'dt_all_elaborated.rds')


# xxx. CHECK ===========================================================================

check_process_02 = length(dt_all_elaborated) == length(dt_all_raw)

objects_to_keep = c('new_data_tables', 'new_tables', 'nonew_data_tables', 'failed_tables_push', 'failed_tables_retrieve', 'failed_tables_prepare', "check_process_01", 'check_process_02', 'check_process_03', "dt_all_raw", 'dt_all_elaborated', 'use_DATE', 'con')
rm(list = setdiff(ls(), objects_to_keep))

print(glue("{crayon::bgCyan('[DONE - 02.Prepare]')}"))

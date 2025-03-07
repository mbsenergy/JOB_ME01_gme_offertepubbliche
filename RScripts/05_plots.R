
print(glue::glue("{crayon::cyan('[INIT - 05.Plots]')}"))

# Required libraries
box::use(DBI[...])
box::use(RPostgreSQL[...])
box::use(RPostgres[...])
box::use(glue[...])
box::use(data.table[...])
box::use(magrittr[...])
box::use(fluxer[...])
box::use(echarts4r[...])


## 5. CONNECT TO PG AND CHECK ----------------------------------

##  5.0 Log into PostgresSQL ------------

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

##  5.1 Dates ------------

dates_mgp = db_get_minmax_dates(con, table_name = 'ME01_gme_mgp_offers', date_column = "DATE")
dates_msd = db_get_minmax_dates(con, table_name = 'ME01_gme_msd_offers', date_column = "DATE")
dates_mb = db_get_minmax_dates(con, table_name = 'ME01_gme_mb_offers', date_column = "DATE")



##  5.2 Count ------------

mgp_count <- table_obs_dist(con, "ME01_gme_mgp_offers", "DATE")
msd_count <- table_obs_dist(con, "ME01_gme_msd_offers", "DATE")
mb_count <- table_obs_dist(con, "ME01_gme_mb_offers", "DATE")

setDT(mgp_count) ; setDT(msd_count) ; setDT(mb_count)

##  5.3 Date Completition ------------
find_missing_dates(mgp_count$DATE)
find_missing_dates(msd_count$DATE)
find_missing_dates(mb_count$DATE)


### VIEW
dt_count = rbindlist(list(mgp_count[, id := 'ME01_gme_mgp_offers'], msd_count[, id := 'ME01_gme_msd_offers'], mb_count[, id := 'ME01_gme_mb_offers']))
dt_count = dcast(dt_count, DATE ~ id, value.var = 'count')
dt_count[DATE > '2024-12-01'] %>% 
  e_charts(DATE) %>% 
  e_line(ME01_gme_mgp_offers, name = "ME01_gme_mgp_offers", symbol = "none") %>% 
  e_line(ME01_gme_msd_offers, name = "ME01_gme_msd_offers", symbol = "none") %>% 
  e_line(ME01_gme_mb_offers, name = "ME01_gme_mb_offers", symbol = "none")  %>% 
  e_title("Tables Observation count by Date") 

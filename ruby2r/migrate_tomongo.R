
# PACKAGES -----------------------------------------------------------
box::use(data.table[...],
         magrittr[...])



# DATA LOAD ----------------------------------------------------------

dt_old = readRDS(file.path('ruby2r', 'dt_mgp_offers_oldjob.rds'))
dt_new = readRDS(file.path('ruby2r', 'dt_mgp_offers_newjob.rds'))

setDT(dt_old)
dt_old = dt_old[as.Date(bid_offer_date_dt_parsed) == as.Date('2024-12-31')]

setDT(dt_new)
dt_new = dt_new[as.Date(bid_offer_date_dt_parsed) == as.Date('2024-12-31')]


# STRUCTURE CHECK --------------------------------------------------------

## NROW & NCOL CHECK 

nrow(dt_old) == nrow(dt_new)
ncol(dt_old) == ncol(dt_new)

## COLNAMES CHECK 
# Get column names

# Check for column name presence
in_old_only = setdiff(colnames(dt_old), colnames(dt_new))  # Present only in dt_old
in_new_only = setdiff(colnames(dt_new), colnames(dt_old))  # Present only in dt_new
in_both = intersect(colnames(dt_old), colnames(dt_new))    # Present in both

cols_old = colnames(dt_old)
dt_new = dt_new[, ..cols_old]


# PUSH TO MONGO --------------------------------------------------------
box::use(mongolite[...])

## Connect to MongoDB ----------------------------------

host <- "ec2-54-229-109-121.eu-west-1.compute.amazonaws.com"
port <- "27017"
username <- "userRW"
password <- "muKoxONfgwRygD1"
db_name <- "production"  

# Create the connection string
URL <- sprintf(
    "mongodb://%s:%s@%s:%s/%s",
    username, password, host, port, db_name
)

## MGP OFFERTE ---------------------------
mongo_conn <- mongo(collection = 'mgpofferte', url = URL)

# Insert the new data into the collection
if (!is.null(dt_new) && nrow(dt_new) > 0) {
  mongo_conn$insert(dt_new)
  message("New data successfully inserted into the collection.")
} else {
  message("No new data available to insert.")
}

# Close the connection
mongo_conn$disconnect()
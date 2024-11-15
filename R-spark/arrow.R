# These are prerequisites,add them to the environment if not present
# install.packages("arrow")
# install.packages("duckdb")

library(arrow, warn.conflicts = FALSE)
library(dplyr)
library(duckdb)
library(DBI)


bucket <- s3_bucket("customer-airlines", access_key=Sys.getenv("access_key"),
                    secret_key=Sys.getenv("secret_key"))

bucket$ls("", recursive = TRUE)

# file_path <- bucket$path("combined_87_00.csv")
file_path <- bucket$path("combined_87_93.csv")

# Load the dataset
tic = Sys.time()

# Read the file from S3
# df  = read_csv_arrow(file_path)

# Load the data from the Domino dataset instead of S3
df = read_csv_arrow('/domino/datasets/local/combined_airline/combined_87_90.csv')
cat(sprintf("Reading df took %.3f seconds \n\n ", Sys.time() - tic))

# Get a description of the data set
df |> glimpse()



# Filtering operations not natively supported will require a collect() call first
tic = Sys.time()
out <- arrow_table(df) %>%
  group_by(YEAR) %>%
  count() %>%
  arrange(YEAR) %>%
  collect()
cat(sprintf("Flights per year took %.3f seconds \n\n ", Sys.time() - tic))
print(out)

# this will not run because of the string filter and no collect
tic = Sys.time()
try (out <- arrow_table(df) %>%
  filter(str_detect(dest , "LAX")) %>%
  group_by(YEAR) %>%
  count() %>%
  arrange(YEAR) %>%
  collect())
cat(sprintf("Flights per year took %.3f seconds \n\n ", Sys.time() - tic))
print(out)


# Lets use DuckDB to get around these restrictions
con <- DBI::dbConnect(duckdb::duckdb())
flights_ddb_tbl <- to_duckdb(df, con, "flights")

tic = Sys.time()
flt_per_year = flights_ddb_tbl %>%
  filter(dest == "LAX") %>%
  group_by(YEAR) %>%
  count()%>%
  arrange(YEAR)
cat(sprintf("Flights per year took %.3f seconds \n\n ", Sys.time() - tic))
print(flt_per_year)

# DuckDB also supports SQL queries
tic = Sys.time()
y <- dbSendQuery(con, "SELECT COUNT(*) FROM flights WHERE DEST=='LAX';")
lax_flight_count = dbFetch(y)
cat(sprintf("Flights from LAX took %.3f seconds \n\n ", Sys.time() - tic))
print(lax_flight_count)

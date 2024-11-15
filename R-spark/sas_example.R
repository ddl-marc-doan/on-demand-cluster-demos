# Load the required libraries
library(sparklyr)

library(spark.sas7bdat)

master <- paste(Sys.getenv("SPARK_MASTER_HOST"), Sys.getenv("SPARK_MASTER_PORT"), sep = ":")
url <- paste("spark://", master, sep = "")
config <- spark_config()
config$sparklyr.connect.enablehivesupport <- TRUE
config["sparklyr.shell.packages"] <- "saurfang:spark-sas7bdat:3.0.0-s_2.12"


# Set the Driver memory here
config$`sparklyr.shell.driver-memory` <- "4G"
#Set the amount of Executor memory
config$`sparklyr.shell.executor-memory` <- "4G"

#Use run to execute the statements to get run time, sourcing the file won't print
# run time for the different statements

#Connect to the Spark cluster
sc <- spark_connect(master=url, config=config, app_name="SparklyRDemo")


x <- spark_read_sas(sc, path = 's3a://moderna-airlines/airline.sas7bdat', table = "sas_example")
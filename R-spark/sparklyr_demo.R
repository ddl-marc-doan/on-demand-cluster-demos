# Load the required libraries
library(sparklyr)
library(tidyverse)

master <- paste(Sys.getenv("SPARK_MASTER_HOST"), Sys.getenv("SPARK_MASTER_PORT"), sep = ":")
dataset_path <- paste(Sys.getenv("DOMINO_DATASETS_DIR"),Sys.getenv("DOMINO_PROJECT_NAME"), sep = "/")
data_path <- paste(dataset_path,"2013*.csv",sep = "/")
url <- paste("spark://", master, sep = "")
config <- spark_config()
config$sparklyr.connect.enablehivesupport <- FALSE

# To illustrate that the driver and executor can have lower memory than the 
# data set while using the 3.2GB / 6GB / 14GB data set. Comment during actual use

# Driver memory for the 3.2GB / 6GB / 14GB data sets
config$`sparklyr.shell.driver-memory` <- "6G"
#Set the amount of Executor memory
config$`sparklyr.shell.executor-memory` <- "12G"

#Use run to execute the statements to get run time, sourcing the file won't print
# run time for the different statements

lppub_column_names <- c("LOAN_ID", "ACT_PERIOD", "CHANNEL", "SELLER", "SERVICER",
                        "MASTER_SERVICER", "ORIG_RATE", "CURR_RATE", "ORIG_UPB", "ISSUANCE_UPB",
                        "CURRENT_UPB", "ORIG_TERM", "ORIG_DATE", "FIRST_PAY", "LOAN_AGE",
                        "REM_MONTHS", "ADJ_REM_MONTHS", "MATR_DT", "OLTV", "OCLTV",
                        "NUM_BO", "DTI", "CSCORE_B", "CSCORE_C", "FIRST_FLAG", "PURPOSE",
                        "PROP", "NO_UNITS", "OCC_STAT", "STATE", "MSA", "ZIP", "MI_PCT",
                        "PRODUCT", "PPMT_FLG", "IO", "FIRST_PAY_IO", "MNTHS_TO_AMTZ_IO",
                        "DLQ_STATUS", "PMT_HISTORY", "MOD_FLAG", "MI_CANCEL_FLAG", "Zero_Bal_Code",
                        "ZB_DTE", "LAST_UPB", "RPRCH_DTE", "CURR_SCHD_PRNCPL", "TOT_SCHD_PRNCPL",
                        "UNSCHD_PRNCPL_CURR", "LAST_PAID_INSTALLMENT_DATE", "FORECLOSURE_DATE",
                        "DISPOSITION_DATE", "FORECLOSURE_COSTS", "PROPERTY_PRESERVATION_AND_REPAIR_COSTS",
                        "ASSET_RECOVERY_COSTS", "MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS",
                        "ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY", "NET_SALES_PROCEEDS",
                        "CREDIT_ENHANCEMENT_PROCEEDS", "REPURCHASES_MAKE_WHOLE_PROCEEDS",
                        "OTHER_FORECLOSURE_PROCEEDS", "NON_INTEREST_BEARING_UPB", "PRINCIPAL_FORGIVENESS_AMOUNT",
                        "ORIGINAL_LIST_START_DATE", "ORIGINAL_LIST_PRICE", "CURRENT_LIST_START_DATE",
                        "CURRENT_LIST_PRICE", "ISSUE_SCOREB", "ISSUE_SCOREC", "CURR_SCOREB",
                        "CURR_SCOREC", "MI_TYPE", "SERV_IND", "CURRENT_PERIOD_MODIFICATION_LOSS_AMOUNT",
                        "CUMULATIVE_MODIFICATION_LOSS_AMOUNT", "CURRENT_PERIOD_CREDIT_EVENT_NET_GAIN_OR_LOSS",
                        "CUMULATIVE_CREDIT_EVENT_NET_GAIN_OR_LOSS", "HOMEREADY_PROGRAM_INDICATOR",
                        "FORECLOSURE_PRINCIPAL_WRITE_OFF_AMOUNT", "RELOCATION_MORTGAGE_INDICATOR",
                        "ZERO_BALANCE_CODE_CHANGE_DATE", "LOAN_HOLDBACK_INDICATOR", "LOAN_HOLDBACK_EFFECTIVE_DATE",
                        "DELINQUENT_ACCRUED_INTEREST", "PROPERTY_INSPECTION_WAIVER_INDICATOR",
                        "HIGH_BALANCE_LOAN_INDICATOR", "ARM_5_YR_INDICATOR", "ARM_PRODUCT_TYPE",
                        "MONTHS_UNTIL_FIRST_PAYMENT_RESET", "MONTHS_BETWEEN_SUBSEQUENT_PAYMENT_RESET",
                        "INTEREST_RATE_CHANGE_DATE", "PAYMENT_CHANGE_DATE", "ARM_INDEX",
                        "ARM_CAP_STRUCTURE", "INITIAL_INTEREST_RATE_CAP", "PERIODIC_INTEREST_RATE_CAP",
                        "LIFETIME_INTEREST_RATE_CAP", "MARGIN", "BALLOON_INDICATOR",
                        "PLAN_NUMBER", "FORBEARANCE_INDICATOR", "HIGH_LOAN_TO_VALUE_HLTV_REFINANCE_OPTION_INDICATOR",
                        "DEAL_NAME", "RE_PROCS_FLAG", "ADR_TYPE", "ADR_COUNT", "ADR_UPB", "DUMMY1","DUMMY2")

#Use run to execute the statements to get run time, sourcing the file won't print
# run time for the different statements

#Connect to the Spark cluster
options(sparklyr.log.console = TRUE)
sc <- spark_connect(master=url, config=config, app_name = "SparkRDemo")
system.time(perf_data <- spark_read_csv(sc, name = "perf_data", path=data_path, memory = FALSE, header = FALSE, delimiter = "|", columns = lppub_column_names))


# dplyr based query

print("Group and summarize")
system.time(mean_loan_age <- perf_data %>%
              group_by(STATE,MSA) %>%
              summarise(mean_loan_age = mean(LOAN_AGE)))

# the variable mean_loan_age is a spark table and the data wont be in local memory
res <- try(View(mean_loan_age))
if(inherits(res, "try-error"))
{
  print("mean_loan_age doesn't exist \n")
}

# collect the data from Spark to local and
# convert Spark DataFrame into R data frame
system.time(local_mean_loan_age <- collect(mean_loan_age))
View(local_mean_loan_age)

# SQL query example, construct and run a SQL query
sql_query = 'SELECT DISTINCT SERVICER FROM perf_data'
#Use the inbuilt DBI package to execute the query
system.time(res <- DBI::dbGetQuery(sc, statement = sql_query))
print(res)


#select a few columns and get list of unique
system.time(out <- perf_data %>%
              select(SERVICER, ZIP, FORECLOSURE_COSTS) %>%
              distinct()%>%
              collect())
print(out)


#Disconnect from the cluster
spark_disconnect(sc)

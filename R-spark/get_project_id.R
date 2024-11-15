install.packages("httr",dependencies = T)

library(httr)

library(jsonlite)

# Get Domino environment variables
api_key <- Sys.getenv("DOMINO_USER_API_KEY")
owner <- Sys.getenv("DOMINO_PROJECT_OWNER")
project <- Sys.getenv("DOMINO_PROJECT_NAME")

# API call gets list of runs for project
proj_url <- paste0("https://prod-field.cs.domino.tech/v1/projects/", owner, "/", project, "/runs")

# Set headers
headers <- c(
  "X-Domino-Api-Key" = api_key
)

# Make GET request
x <- GET(proj_url, add_headers(headers))

# Parse JSON response
response <- fromJSON(content(x, "text", encoding = "UTF-8"))

# Return project id from first record
my_id <- response$data['projectId'][[1]][1]
cat("Your Project ID is:", my_id, "\n")


# Define the API endpoint
url <- paste0("https://prod-field.cs.domino.tech/v4/projects/", my_id,"/environmentVariables")



# Set the headers
headers <- c(
  "Content-Type" = "application/json",
  "X-Domino-Api-Key" = api_key
)

# Set the data (request body) - replace with the actual environment variables you want to set
data <- jsonlite::toJSON(list(
  vars = list(
    list(name = "ENV_VAR_NAME_1", value = "ENV_VAR_VALUE_1"),
    list(name = "ENV_VAR_NAME_2", value = "ENV_VAR_VALUE_2")
  )
), auto_unbox = TRUE)

print(data)

# Call the API using the POST() function
response <- POST(url, add_headers(headers), body = data)
# response <- GET(url, add_headers(headers))

# Check the response status and content
if (status_code(response) == 200) {
  cat("Successfully set environment variables\n")
  print(content(response))
} else {
  cat("Failed to set environment variables\n")
  print(status_code(response))
  print(content(response, "text"))
}

Sys.getenv("ENV_VAR_NAME_1")

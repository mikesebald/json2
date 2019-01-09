library(DBI)
library(odbc)
library(jsonlite)
library(tidyr)

rm(list= ls())

system.time(
record <- fromJSON("1k.json")
)


fields <- names(record)
entities <- fields[-c(1:6, length(fields))]

organizations <- split(record, record$type)$organization
persons <- split(record, record$type)$person

organization <- unnest(organizations[, c("source", "key", "revision", "organization")], organization)
organization$validation_message <- NULL
organization$origin <- NULL
organization$ref <- NULL

person <- unnest(persons[, c("source", "key", "revision", "person")], person)
person$validation_message <- NULL
person$origin <- NULL
person$ref <- NULL

postal_address <- unnest(record[, c("source", "key", "revision", "postal_address")], postal_address)
postal_address$validation_message <- NULL
postal_address$origin <- NULL
postal_address$ref <- NULL



# con <- DBI::dbConnect(odbc::odbc(),
#                       Driver = "Hortonworks Hive ODBC Driver",
#                       Host   = "localhost",
#                       Schema = "default",
#                       UID    = "maria_dev",
#                       PWD    = rstudioapi::askForPassword("Database password"),
#                       Port   = 10000)

dbCon <- dbConnect(odbc::odbc(), dsn = "Sandbox", encoding ="UTF-8")
dbCon <- dbConnect(odbc::odbc(), dsn = "AzureHDInsight")
# con <- dbConnect(odbc::odbc(), dsn = "Mariadb")

system.time({
  dbWriteTable(dbCon, "organizations", organization, overwrite = TRUE)
  dbWriteTable(dbCon, "persons", person, overwrite = TRUE)
  dbWriteTable(dbCon, "postal_addresses", postal_address, overwrite = TRUE, fileEncoding = "UTF-8")
})

write.csv(postal_address, file = "c:/temp/postal_addresses.csv")
write_json(postal_address, "pa.json")

dbDisconnect(dbCon)

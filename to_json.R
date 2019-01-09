library(readr)
library(jsonlite)
library(tidyr)
library(dplyr)
library(httr)

rm(list = ls())

server <- "https://km-t01.uniserv-cdh.com"
cdh_url <- paste0(server, "/api/rest/1/match?format=json")
user <- rstudioapi::askForPassword("Username")
pwd <- rstudioapi::askForPassword("Password")

logging <- function(level = "INFO", messageText = "") {
  options(digits.secs = 3)
  cat(paste(format(Sys.time(), "%F %H:%M:%OS %z"), 
            Sys.getpid(), 
            level, "-", messageText, "\n"))
}

logFileName <- paste0("json_log-", format(Sys.Date(), "%F"), ".log")
sink()
sink(logFileName, append = TRUE, split = TRUE)

logging("INFO", "START Script")

args <- commandArgs(TRUE)
if (length(args) == 0) {
  cat("\n------------------------------\n")
  cat("No input file name provided.\n")
  cat("Exiting...")
  quit(status = 1)
}
# args <- c("ETL_KM/big_utf8.txt")
logging("INFO", paste0("Reading file \'", args, "\'"))
infile <- as.data.frame(read_delim(args[1],
                                   delim = "|",
                                   col_types = cols(.default = "c"), 
                                   n_max = Inf))
logging("INFO", paste0("Total number of rows in input file: ", nrow(infile)))

# Analysis
# columns <- colnames(infile)
# subset <- as.data.frame(t(sample_n(infile, 5)))
# subset$n <- c(1:ncol(infile))
# subset <- subset[, c(ncol(subset), 1:(ncol(subset)-1))]
# View(subset)
# Amount of values provided (non-NAs)
# View(100 - apply(is.na(infile), 2, sum) / nrow(infile) * 100)

# Column selection and naming
# df <- infile[, c(1, 2, 5, 14, 16, 29:30, 63:67, 69, 94:98)]
column_ids <- c(1:6, 8:9, 14:18, 20:21, 26, 30)
# View(subset[column_ids, ])
df <- infile[, column_ids]
colnames(df) <- c("record.key1",
                  "record.key2",
                  "organization.organization_name",
                  "organization.organization_name2",
                  "organization.organization_name3",
                  "organization.organization_name4",
                  "person.given_names_full",
                  "person.surname_first",
                  "postal_address.str",
                  "postal_address.str2",
                  "postal_address.str3",
                  "postal_address.str4",
                  "postal_address.hno",
                  "postal_address.zip",
                  "postal_address.city",
                  "postal_address.country_code",
                  "email_address.address")
                  
# Assign additional columns, if required
# View(colnames(df))
df$record.source <- "c4c"
df$record.type <- "organization"
df$postal_address.type <- "STANDARD"
df$email_address.type <- "DEFAULT"
df$record.key <- paste0(df$record.key1, "_", df$record.key2)

# Rearrange columns, if necessary
# View(colnames(df))
df <- df[, c(18, 22, 19, 3:8, 20, 9:16, 21, 17)]

keys <- unique(df[, c(1:2)])
logging("INFO", paste0("Total number of unique keys in file: ", nrow(keys)))

# Person entity
person <- unique(df[, c("record.source", 
                        "record.key", 
                        "record.type",
                        "person.given_names_full", 
                        "person.surname_first")])
person <-person[!is.na(person$person.given_names_full), ]
colnames(person) <- sub("person.", "", colnames(person))
person <- nest(person, 
               c(4:ncol(person)), 
               .key = "person")
colnames(person) <- sub("record.", "", colnames(person))
logging("INFO", paste0("Entity \'person\' with ", nrow(person), " rows"))

# Organization entity
organization <- unique(df[, c("record.source", 
                              "record.key", 
                              "record.type",
                              "organization.organization_name",
                              "organization.organization_name2",
                              "organization.organization_name3",
                              "organization.organization_name4")])
organization <- organization[!is.na(organization$organization.organization_name), ]
colnames(organization) <- sub("organization.", "", colnames(organization))
organization <- nest(organization, 
                     c(4:ncol(organization)), 
                     .key = "organization")
colnames(organization) <- sub("record.", "", colnames(organization))
logging("INFO", paste0("Entity \'organization\' with ", nrow(organization), " rows"))

# Postal address entity
postal_address <- unique(df[, c("record.source", 
                                "record.key", 
                                "record.type",
                                "postal_address.type", 
                                "postal_address.str", 
                                "postal_address.str2", 
                                "postal_address.str3", 
                                "postal_address.str4", 
                                "postal_address.hno", 
                                "postal_address.zip", 
                                "postal_address.city", 
                                "postal_address.country_code")])
postal_address <- postal_address[!is.na(postal_address$postal_address.type), ]
colnames(postal_address) <- sub("postal_address.", "", colnames(postal_address))
postal_address <- nest(postal_address, 
                       c(4:ncol(postal_address)), 
                       .key = "postal_address")
colnames(postal_address) <- sub("record.", "", colnames(postal_address))
logging("INFO", paste0("Entity \'postal_address\' with ", nrow(postal_address), " rows"))

# E-Mail entity
email_address <- unique(df[, c("record.source", 
                               "record.key", 
                               "record.type",
                               "email_address.type", 
                               "email_address.address")])
email_address <- email_address[!is.na(email_address$email_address.type), ]
colnames(email_address) <- sub("email_address.", "", colnames(email_address))
email_address <- nest(email_address, 
                      c(4:ncol(email_address)), 
                      .key = "email_address")
colnames(email_address) <- sub("record.", "", colnames(email_address))
logging("INFO", paste0("Entity \'email_address\' with ", nrow(email_address), " rows"))

# Put everything together
# final <- merge(person, organization, all = TRUE) %>% 
#   merge(postal_address, all = TRUE) %>%
#   merge(email_address, all = TRUE)
final <- merge(organization, postal_address, all = TRUE) %>% 
  merge(email_address, all = TRUE)
logging("INFO", paste0("Final result set with ", nrow(final), " rows"))

# Store everything as JSON file
# write_json(final, "final.json", pretty = TRUE, auto_unbox = TRUE)

num_matches <- 0
num_nomatches <- 0
num_errors <- 0
num_warn <- 0
total_records <- nrow(final)
for (i in 1:total_records) {
  record <- toJSON(final[i, ], auto_unbox = TRUE)
  source = final[i, "source"]
  key = final[i, "key"]
  logging("INFO", paste0("Requesting matches for record ", i, " of ", 
                         total_records, " (", source, "*", key, ") from ", 
                         server))
  response <- POST(url = cdh_url, 
                   config = authenticate(user, pwd), 
                   body = record,
                   encode = "json")

  if (response$status_code == "500") {
    logging("INFO", paste0("No match found for ", source, "*", key))
    num_nomatches <- num_nomatches + 1
  } else if (response$status_code == "200") {
    content <- content(response)
    if (length(content) == 0) {
      logging("WARN", paste0("No content in response for ", source, "*", key))
      num_warn <- num_warn + 1
    } else {
      logging("INFO", paste0("Match(es) found for ", source, "*", key))
      write_file(toJSON(content[[1]]), paste0("out/", source, "_", key, ".json"))
      num_matches <- num_matches + 1
    }
  } else {
    logging("ERROR", paste0("Error in reponse from server"))
    num_errors <- num_errors + 1
  }
}

logging("INFO", paste("Number of matches found:", num_matches))
logging("INFO", paste("Number of non-matches found:", num_nomatches))
logging("INFO", paste("Number of errors:", num_errors))
logging("INFO", paste("Number of warnings:", num_warn))
logging("INFO", "END Script")

sink()

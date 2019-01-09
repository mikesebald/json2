library(readr)
library(jsonlite)
library(tidyr)
library(dplyr)
library(httr)

rm(list = ls())

server <- "https://springer-p01.uniserv-cdh.com"
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

# args <- commandArgs(TRUE)
# if (length(args) == 0) {
#   cat("\n------------------------------\n")
#   cat("No input file name provided.\n")
#   cat("Exiting...")
#   quit(status = 1)
# }
args <- c("springer_cdh_import.txt")
logging("INFO", paste0("Reading file \'", args, "\'"))
infile <- as.data.frame(read_delim(args[1],
                                   delim = ";",
                                   col_types = cols(.default = "c"), 
                                   n_max = Inf))
logging("INFO", paste0("Total number of rows in input file: ", nrow(infile)))

num_out_files <- length(list.files("out/"))
if (num_out_files > 0) {
  logging("INFO", paste("Removing", num_out_files, "files from output folder"))
  file.remove(file.path("out/", list.files("out/"))) 
}

# Analysis
# columns <- colnames(infile)
# subset <- as.data.frame(t(sample_n(infile, 5)))
# subset$n <- c(1:ncol(infile))
# subset <- subset[, c(ncol(subset), 1:(ncol(subset)-1))]
# View(subset)
# Amount of values provided (non-NAs)
# View(100 - apply(is.na(infile), 2, sum) / nrow(infile) * 100)

column_ids <- c(1:4, 6:12, 16:19)
# View(subset[column_ids, ])
df <- infile[, column_ids]

# Rearrange columns, if necessary
# View(colnames(df))

keys <- unique(df[, c(1:2)])
logging("INFO", paste0("Total number of unique keys in file: ", nrow(keys)))

# Person entity
person <- unique(df[, c("record.source", 
                        "record.key", 
                        "record.type",
                        "person.form_of_address", 
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
                              "organization.name",
                              "organization.department",
                              "organization.subdepartment")])
organization <- organization[!is.na(organization$organization.name), ]
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
                                "postal_address.zip", 
                                "postal_address.city", 
                                "postal_address.state", 
                                "postal_address.country_code")])
postal_address <- postal_address[!is.na(postal_address$postal_address.type), ]
colnames(postal_address) <- sub("postal_address.", "", colnames(postal_address))
postal_address <- nest(postal_address, 
                       c(4:ncol(postal_address)), 
                       .key = "postal_address")
colnames(postal_address) <- sub("record.", "", colnames(postal_address))
logging("INFO", paste0("Entity \'postal_address\' with ", nrow(postal_address), " rows"))

# Put everything together
final <- merge(person, organization, all = TRUE) %>%
  merge(postal_address, all = TRUE)
logging("INFO", paste0("Final result set with ", nrow(final), " rows"))

# Store everything as JSON file
# write_json(final, "final.json", pretty = TRUE, auto_unbox = TRUE)

num_matches <- 0
num_nomatches <- 0
num_errors <- 0
num_warn <- 0
found_matches <- data.frame(record_source = character(), 
                            record_key = character(),
                            record_person_given_names_full = character(),
                            record_person_surname_first = character(),
                            record_organization_name = character(),
                            record_city = character(),
                            match_source = character(),
                            match_key = character(),
                            match_person_given_names_full = character(),
                            match_person_surname_first = character(),
                            match_organization_name = character(),
                            match_city = character())
total_records <- nrow(final)
for (i in 1:total_records) {
  record <- toJSON(final[i, ], auto_unbox = TRUE)
  source = final[i, "source"]
  key = final[i, "key"]
  record_person_given_names_full <- ifelse(is.na(final[i, "person"]),
                                           "",
                                           final[i, "person"][[1]]$given_names_full)
  record_person_surname_first <- ifelse(is.na(final[i, "person"]),
                                        "",
                                        final[i, "person"][[1]]$surname_first)
  record_organization_name <- ifelse(is.na(final[i, "organization"]), 
                                     "", 
                                     paste(final[i, "organization"][[1]]$name))
  record_city <- ifelse(is.na(final[i, "postal_address"]), 
                        "", 
                        final[i, "postal_address"][[1]]$city)
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
    matches_returned <- length(content)
    if (matches_returned == 0) {
      logging("WARN", paste0("No content in response for ", source, "*", key))
      num_warn <- num_warn + 1
    } else {
      logging("INFO", paste0(matches_returned, " match(es) found for ", source, "*", key))
      for (j in 1:matches_returned) {
        write_file(paste0(toJSON(content[[j]], auto_unbox = TRUE), "\n"), 
                   paste0("out/", source, "_", key, ".json"), append = TRUE)
        match_source <- content[[j]]$source
        match_key <- content[[j]]$key
        match_person_given_names_full <- ifelse(is.null(content[[j]]$person), 
                                                "", 
                                                content[[j]]$person[[1]]$given_names_full)
        match_person_surname_first <- ifelse(is.null(content[[j]]$person), 
                                             "", 
                                             content[[j]]$person[[1]]$surname_first)
        match_organization_name <- ifelse(is.null(content[[j]]$organization), 
                                          "", 
                                          content[[j]]$organization[[1]]$name)
        match_city <- ifelse(is.null(content[[j]]$postal_address), 
                             "",
                             content[[j]]$postal_address[[1]]$city)
        found_matches <- rbind(found_matches, 
                               c(source, 
                                 key, 
                                 record_person_given_names_full,
                                 record_person_surname_first,
                                 record_organization_name,
                                 record_city,
                                 match_source, 
                                 match_key,
                                 match_person_given_names_full,
                                 match_person_surname_first,
                                 match_organization_name,
                                 match_city),
                               stringsAsFactors = FALSE)
      }
      num_matches <- num_matches + 1
    }
  } else {
    logging("ERROR", paste0("Error in reponse from server"))
    num_errors <- num_errors + 1
  }
}

colnames(found_matches) <- c("record_source",
                             "record_key",
                             "record_person_given_names_full",
                             "record_person_surname_first",
                             "record_organization_name",
                             "record_city",
                             "match_source",
                             "match_key",
                             "match_person_given_names_full",
                             "match_person_surname_first",
                             "match_organization_name",
                             "match_city")

write_csv(found_matches, "matches_found.csv")

logging("INFO", paste("Number of matches found:", num_matches))
logging("INFO", paste("Number of non-matches found:", num_nomatches))
logging("INFO", paste("Number of errors:", num_errors))
logging("INFO", paste("Number of warnings:", num_warn))
logging("INFO", "END Script")

sink()

# Demo of normalising free-form jobcentre addresses for input into the API,
# requesting the responses and checking how good they are vs our manual effort.

library(tidyverse)
library(readxl)
library(poster) # libpostal wrapper, parse free-form addresses into sections
library(curl)
library(jsonlite)
library(RegistersClientR)
library(RPostgreSQL)

spreadsheet <- read_excel("./jobcentres.xlsx")
add_free <- paste(spreadsheet$`Office Address`, spreadsheet$`Postcode`)
add_norm <- as_tibble(parse_addr(add_free))

# poster (R wrapper of libpostal) parses free-form addresses into sections.
glimpse(parse_addr("101 Old Street, Ashton-U-Lyne, Lancs, OL6 6BJ")) # 100012739938
glimpse(parse_addr("Ebury House, 14 Dee Street, Aberdeen, AB11 6DR")) # 10091750659
glimpse(parse_addr("Garden Flat, Zero The Vale, London"))
glimpse(parse_addr("Garden Flat, Zero, The Vale, London"))
glimpse(parse_addr("Zero, Garden Flat, The Vale, London"))
glimpse(parse_addr("Zero The Vale, London"))
glimpse(parse_addr("One, The Vale, London"))
glimpse(parse_addr("V XX Settembre, 20"))

con <- dbConnect(RPostgreSQL::PostgreSQL(), dbname = "addressbase")
glimpse(dbGetQuery(con, "SELECT * FROM deliverypointaddress where uprn = '100012739938'"))
glimpse(dbGetQuery(con, "SELECT * FROM deliverypointaddress where uprn = '10091750659'"))
glimpse(dbGetQuery(con, "SELECT * FROM deliverypointaddress where uprn = '10024357076'"))
dbDisconnect(con)

glimpse(parse_addr("Unit 2, Site 9, Amlwch Business Park, Amlwch, Ynys Mon, LL68 9EA")) # 10024357076
filter(urls, line1 == "amlwch business park, unit 2 site 9")$url
glimpse(adrs_matches(filter(urls, line1 == "amlwch business park, unit 2 site 9")$url))
adrs_matches("http://localhost:9022/v2/uk/addresses?postcode=ll68+9ea&line1=unit+2")
adrs_matches("http://localhost:9022/v2/uk/addresses?postcode=ll68+9bq&line1=unit+2")

adrs_matches("http://localhost:9022/v2/uk/addresses?postcode=E1+8DX")

glimpse(adrs_matches(filter(urls, line1 == "101")$url))
glimpse(adrs_matches(filter(urls, line1 == "ebury house, 14")$url))

# Map the fields in elasticsearch to fields from libpostal
# A better way would be to map the AddressBase fields to libpostal inside
# elasticsearch
#
# line1: house unit house_number
# line2: road
# town: suburb city state_district (often more than one of these is populated)
# postcode: postal_code

# A paste function that doesn't turns NAs into blanks rather than "NA" strings
# https://stackoverflow.com/a/15673180/937932
paste3 <- function(...,sep=", ") {
     L <- list(...)
     L <- lapply(L,function(x) {x[is.na(x)] <- ""; x})
     ret <-gsub(paste0("(^",sep,"|",sep,"$)"),"",
                 gsub(paste0(sep,sep),sep,
                      do.call(paste,c(L,list(sep=sep)))))
     is.na(ret) <- ret==""
     ret
     }

# Construct URLs from the libpostal addresses for querying elasticsearch
urls <-
  add_norm %>%
  mutate(line1 = paste3(house, unit, house_number),
         line2 = road,
         town = paste3(suburb, city, state_district),
         postcode = postal_code) %>%
  select(line1, line2, town, postcode) %>%
  mutate(url = paste0("http://localhost:9022/v2/uk/addresses?postcode=", postcode,
                      "&line1=", line1,
                      "&line2=", line2,
                      "&town=", town),
         url = map_chr(url, URLencode),
         curl = paste0("curl --header \"X-LOCALHOST-Origin: 0\" '", url, "'"))

# The elasticsearch app demands this header
adrs_handle <-
  new_handle() %>%
  handle_setheaders("X-LOCALHOST-ORIGIN" = "0")

# Template for a return dataframe of matches, useful when there are either no
# matches, or not all the expected fields.
adrs_blank_matches <-
  tibble(location = character(),
         uprn = character(),
         address.postcode = character(),
         address.county = character(),
         address.town = character(),
         address.lines = character())

# Function to request matches from elasticsearch and parse the JSON into a data
# frames
adrs_matches <- function(url) {
  print(url)
  x <- curl_fetch_memory(url, adrs_handle)
  matches <-
    fromJSON(rawToChar(x$content),
             flatten = TRUE) %>%
    as_tibble()
  if (nrow(matches) == 0) {
    return(adrs_blank_matches)
  } else {
    out <-
      matches %>%
      bind_rows(adrs_blank_matches) %>%
      select(location,
             uprn,
             address.postcode,
             address.county,
             address.town,
             address.lines) %>%
      mutate(location = map_chr(location, paste, collapse = ", "),
             uprn = as.character(uprn),
             address.lines = map_chr(address.lines, paste, collapse = " | "))
  }
  out
}

# Request the actual matches
matched <-
  urls %>%
  mutate(results = map(url, adrs_matches)) %>%
  mutate(n = map_int(results, nrow))

# How many addresses have no matches, one match, two matches, etc.
matched %>%
  select(-url, -curl, -results) %>%
  arrange(-n) %>%
  ggplot(aes(n)) +
  stat_count() +
  ggtitle("Distribution of the number of potential matches for each jobcentre")
ggsave("distribution.png")

# Addresses with exactly one match.
auto_uprns <-
  matched %>%
  filter(n == 1) %>%
  mutate(uprn = map_chr(results, pull, uprn),
         address.lines = map_chr(results, pull, address.lines)) %>%
  select(-url, -curl, -results)

# Manual matches from the alpha jobcentre register
manual_matches <-
  rr_records("jobcentre", "alpha") %>%
  mutate(address = as.character(address))
manual_uprns <- select(manual_matches, name, address)

# How does auto (exactly one match) compare with manual matching?
both <- inner_join(manual_uprns, auto_uprns, by = c("address" = "uprn"))
auto_not_manual <- anti_join(auto_uprns, manual_uprns, by = c("uprn" = "address"))
manual_not_auto <- anti_join(manual_uprns, auto_uprns, by = c("address" = "uprn"))

nrow(both)
# [1] 286

nrow(auto_not_manual)
# [1] 20

nrow(manual_not_auto)
# [1] 409

# 73 not-auto-matched records were among multi-matched ones
matched %>%
  mutate(uprn = map(results, pull, uprn)) %>%
  select(uprn) %>%
  unnest() %>%
  inner_join(manual_not_auto, by = c("uprn"= "address")) %>%
  nrow()

urls <- c("http://localhost:9022/v2/uk/addresses?line1=garmonsway",
          "http://localhost:9022/v2/uk/addresses?line2=garmonsway",
          "http://localhost:9022/v2/uk/addresses?line3=garmonsway",
          "http://localhost:9022/v2/uk/addresses?line4=garmonsway")
garmonsway <- map_df(urls, adrs_matches)
write_tsv(garmonsway, "~/temp/temp.tsv")

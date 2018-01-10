# Demo of normalising free-form addresses for input into the API

library(tidyverse)
library(readxl)
library(poster)

spreadsheet <- read_excel("./jobcentres.xlsx")
add_free <- paste(spreadsheet$`Office Address`, spreadsheet$`Postcode`)
add_norm <- as_tibble(parse_addr(add_free))

# Coincidence matrix: phew, what a lot of hacks!  I googled for an easier way,
# found a couple using table() and apply() (slow) or as part of packages that
# do mainly other stuff
add_norm %>%
  mutate(i = row_number()) %>%
  gather("field", "value", -i) %>%
  filter(!is.na(value)) %>%
  select(-value) %>%
  inner_join(., ., by = "i") %>%
  filter(field.x != field.y) %>%
  arrange(i, field.x, field.y) %>%
  select(-i) %>%
  mutate(V1 = pmin(field.x, field.y),
         V2 = pmax(field.x, field.y)) %>%
  count(V1, V2) %>%
  # arrange(n, V1, V2) %>%
  arrange(V1, V2) %>%
  print(n = Inf)

add_norm %>%
  map(unique)

# Map
#
# line1: house unit house_number
# line2: road
# line3:
# line4
# town: suburb city state_district (often more than one of these is populated)
# postcode: postal_code

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

urls <-
  add_norm %>%
  mutate(line1 = paste3(house, unit, house_number),
         line2 = road,
         town = paste3(suburb, city, state_district),
         postcode = postal_code) %>%
  select(line1, line2, town, postcode) %>%
  mutate(query = paste0("http://localhost:9022/v2/uk/addresses?postcode=", postcode,
                        "&line1=", line1,
                        "&line2=", line2,
                        "&town=", town),
         query = URLencode(query),
         curl = paste0("curl --header \"X-LOCALHOST-Origin: 0\" '", query, "'"))

cat(urls$curl[1], "\n")

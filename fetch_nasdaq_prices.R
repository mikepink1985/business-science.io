# BUSINESS SCIENCE UNIVERSITY ----
# LEARNING LAB 65: SPARK IN R ----
# THIS WILL TAKE 5+ MINUTES TO RUN
# **** ----

library(tidyquant)
library(furrr)

fetch_nasdaq_prices <- function(workers = 6) {
    
    if (!dir.exists("raw_data")) {
        dir.create("raw_data")
    }
    
    nasdaq_tbl <- tq_exchange("NASDAQ")
    
    nasdaq_tbl %>% write_rds("raw_data/nasdaq_index.rds")
    
    future::plan("multisession", workers = workers)
    
    nasdaq_data <- nasdaq_tbl$symbol %>%
        future_map(~ tq_get(.), .progress = TRUE)
    
    nasdaq_data[is.na(nasdaq_data)] <- NULL
    
    nasdaq_data_tbl <- nasdaq_data %>%
        bind_rows()
    
    nasdaq_data_tbl %>% select(symbol, date, adjusted) %>%
        write_rds("raw_data/nasdaq_data_tbl.rds")
    
}




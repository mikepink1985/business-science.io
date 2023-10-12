# BUSINESS SCIENCE UNIVERSITY ----
# LEARNING LAB 65: SPARK IN R ----
# **** ----

# AGENDA:
# 1. PART 1: SPARK IN R ESSENTIALS (3 TOP TIPS / IMMEDIATE VALUE FROM SPARK)
# 2. PART 2: BIGGISH DATA: 5.8M ROWS / 4500+ STOCKS / ENTIRE NASDAQ / 10-YEAR HISTORY 

# ESSENTIAL SPARK in R RESOURCES ----
#  https://spark.rstudio.com
#  https://therinspark.com/


# PART 1: SPARK ESSENTIALS ----
# - CREDIT Much of Part 1 came from: https://therinspark.com/
#   Amazing book - check it out.

# * SYSTEM REQUIREMENTS:
# - JAVA (I'm using Java 11 for this training)

# * LIBRARIES ----

library(fs)
library(tidyverse)
library(DBI)
library(sparklyr)

# * SPARK INSTALLATION ----

# Available for installation
spark_available_versions() 

# Install Spark
# spark_install(version = "3.1")

# The version that is installed on your machine
spark_installed_versions() 

# To remove a spark installation
# spark_uninstall("2.2.0")

# * CONNECTION TO SPARK ----

?spark_connect

# Configuration Setup (Optional)
conf <- list()
conf$`sparklyr.cores.local`         <- 6
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction          <- 0.9

# Connects to Spark Locally
sc <- spark_connect(
    master  = "local", 
    version = "3.1.1", 
    config  = conf
)
sc

# Databricks Connection (for Databricks Users)
# - Extremely popular in enterprises
# - Developed by creators of Spark
# - Every instance of Databricks comes with sparklyr installed
# - Resource: https://docs.databricks.com/spark/latest/sparkr/sparklyr.html

# Connects to Spark inside of Databricks
# sc <- spark_connect(method = "databricks")


# Disconnecting 
# spark_disconnect_all()

# * WEB INTERFACE ----
# - Useful for troubleshooting and killing long-running jobs

spark_web(sc)



# * ADDING DATA TO SPARK ---- 

cars <- copy_to(sc, mtcars, "mtcars")

src_tbls(sc)

# These are the same because cars stores a reference to the Spark table
tbl(sc, "mtcars")

cars

# If you want to quickly get number of rows
nrow(cars)

sdf_nrow(cars)


# TIP 1: DATA WRANGLING: SPARK SQL VS SPARK DPLYR ----

# * Spark SQL ----

DBI::dbGetQuery(sc, "SELECT count(*) AS `n` FROM mtcars ")

# * DPLYR ----

# ** Basics of Dplyr SQL Translation ----

count(cars)

count(cars) %>% show_query()

DBI::dbGetQuery(
    conn = sc,
    statement = "SELECT COUNT(*) AS `n`
FROM `mtcars`"
)

# ** Advantage of Dplyr SQL Translation ----

# Summarize + Across
cars %>%
    summarise(across(everything(), mean, na.rm = TRUE)) %>%
    show_query()

# Grouped Functions
cars %>%
    mutate(transmission = case_when(am == 1 ~ "automatic", TRUE ~ "manual")) %>%
    group_by(transmission) %>%
    summarise(across(mpg:carb, mean, na.rm = TRUE)) %>%
    show_query()

# Complex Structures (Lists)
cars %>%
    summarise(mpg_percentile = percentile(mpg, array(0, 0.25, 0.5, 0.75, 1))) %>%
    mutate(mpg_percentile = explode(mpg_percentile))

# TIP 2: MODELING AT SCALE ----

# * Make a Model ----
model <- ml_linear_regression(cars, mpg ~ hp)
model

# * Predict New Data ----
more_cars <- copy_to(sc, tibble(hp = 250 + 10 * 1:10))

model %>%
    ml_predict(more_cars)

model %>% summary()

# * Correlations at Scale ----
ml_corr(cars)


# TIP 3: STREAMING (DYNAMIC DATASETS) ----
# - Kafka is a technology that Spark works well with

# * Input Directory ----
dir_create("stream_input")

mtcars %>% write_csv("stream_input/cars_1.csv")

# * Start a Stream ----
stream <- stream_read_csv(sc, "stream_input/") 

stream %>%
    select(mpg, cyl, disp) %>%
    stream_write_csv("stream_output/")

mtcars %>% write_csv("stream_input/cars_2.csv")

# * Stop the stream ----
# (if this doesn't work, just disconnect spark)
stream_stop(stream)

spark_disconnect_all()


# PART 2: NASDAQ FINANCIAL ANALYSIS ----

# * LIBRARIES ----
library(sparklyr)
library(tidyverse)
library(tidyquant)
library(timetk)
library(janitor)
library(plotly)



# * DATA ----

nasdaq_data_tbl <- read_rds("raw_data/nasdaq_data_tbl.rds")

# Connects to Spark Locally
sc <- spark_connect(
    master  = "local", 
    version = "3.1.1", 
    config  = conf
)

# Copy data to spark
nasdaq_data <- copy_to(
    sc, 
    nasdaq_data_tbl, 
    name      = "nasdaq_data", 
    overwrite = TRUE
)

sdf_nrow(nasdaq_data)

# * SPARK + DPLYR: NASDAQ STOCK RETURNS ANALYSIS ----

nasdaq_metrics_query <- nasdaq_data %>%
    
    group_by(symbol) %>%
    arrange(date) %>%
    mutate(lag = lag(adjusted, n = 1)) %>%
    ungroup() %>%
    
    mutate(returns = (adjusted / lag) - 1 ) %>%
    
    group_by(symbol) %>%
    summarise(
        mean      = mean(returns, na.rm = TRUE),
        sd        = sd(returns, na.rm = TRUE),
        count     = n(),
        last_date = max(date, na.rm = TRUE)
    ) %>%
    ungroup() 

nasdaq_metrics_query %>% show_query()

nasdaq_metrics_tbl <- nasdaq_metrics_query %>% collect()

nasdaq_metrics_tbl

nasdaq_metrics_tbl %>% write_rds("processed_data/nasdaq_metrics.rds")


# * R DPLYR: APPLY SCREENING -----
#  - Market Cap > $1B (More stable)
#  - SD < 1 (Less Volatile)
#  - Count > 5 * 365 (More stock history to base performance)
#  - Last Date = Max Date (Makes sure stock is still active)
#  - Reward Metric: Variation of Sharpe Ratio (Mean Return / Standard Deviation, Higher Better)

nasdaq_metrics_tbl <- read_rds("processed_data/nasdaq_metrics.rds")
nasdaq_index_tbl   <- read_rds("raw_data/nasdaq_index.rds") %>%
    clean_names()

nasdaq_metrics_screened_tbl <- nasdaq_metrics_tbl %>%
    
    inner_join(
        nasdaq_index_tbl %>% select(symbol, company, market_cap), 
        by = "symbol"
    ) %>%
    
    filter(market_cap > 1e9) %>%
    
    arrange(-sd) %>%
    filter(
        sd < 1, 
        count > 365 * 5,
        last_date == max(last_date)
    ) %>%
    mutate(reward_metric = 2500*mean/sd) %>%
    mutate(desc = str_glue("
                           Symbol: {symbol}
                           Mean: {round(mean, 3)}
                           SD: {round(sd, 3)}
                           N: {count}"))

# * Visualize Screening ----
g <- nasdaq_metrics_screened_tbl %>%
    ggplot(aes(log(sd), log(mean))) +
    geom_point(aes(text = desc, color = reward_metric), 
               alpha = 0.5, shape = 21, size = 4) +
    geom_smooth() +
    scale_color_distiller(type = "div") +
    theme_minimal() +
    theme(
        panel.background = element_rect(fill = "black"),
        plot.background = element_rect(fill = "black"),
        text = element_text(colour = "white")
    ) +
    labs(title = "NASDAQ Financial Analysis")

ggplotly(g)

# * Visualize best symbols ----

n <- 9

best_symbols_tbl <- nasdaq_metrics_screened_tbl %>%
    arrange(-reward_metric) %>%
    slice(1:n) 

best_symbols <- best_symbols_tbl$symbol

stock_screen_data_tbl <- nasdaq_data_tbl %>%
    filter(symbol %in% best_symbols) 

g <- stock_screen_data_tbl %>%
    left_join(
        best_symbols_tbl %>% select(symbol, company)
    ) %>%
    group_by(symbol, company) %>%
    plot_time_series(date, adjusted, .smooth = TRUE, .facet_ncol = 3, .interactive = F) +
    geom_line(color = "white") +
    theme_minimal() +
    theme(
        panel.background = element_rect(fill = "black"),
        plot.background = element_rect(fill = "black"),
        text = element_text(colour = "white"),
        strip.text = element_text(colour = "white")
    ) +
    labs(title = "NASDAQ Financial Analysis")

ggplotly(g)

# CONCLUSIONS ----



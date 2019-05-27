
# CANCAN descriptive statistics -------------------------------------------
# 1 week : 25-31 march 2019

options(scipen = 10)

install.packages("~/mobicount-project/minior-master.tar.gz", repos = NULL)

library(tidyverse)
library(minior)
library(data.table)


# # Export inputs to Minio
# 
# inputs <- list.files("~/inputs", full.names = T)
# 
# map(inputs, ~put_local_file(., 
#                             dir_minio = "MobiCount/cancan/desc_stats",
#                             bucket = "etgtk6"))

# Import inputs from Minio

paths <- list_files("etgtk6", "MobiCount/cancan/desc_stats")

map(paths, ~get_file_local(., ext_dir = "~/inputs", bucket = "etgtk6"))



# Records per technology ------------------------------------------------------

techno <- read_csv("~/inputs/records_per_techno_agg.csv", 
                   col_names = c("techno", "count"))

tot_records <- round(sum(techno$count) / 1E9, 1)

techno %>%
  group_by(techno) %>%
  summarise(mean_day = round(mean(count) / 1E9, 2)) %>%
  mutate(techno = c("2G", "3G", "4G")) %>%
  ggplot(aes(x = reorder(techno, mean_day), y = mean_day)) + 
  geom_bar(stat = "identity") +
  # geom_text(aes(label = mean_day), vjust=-0.5, size = 5) +
  labs(y = "Records per day (in billions)",
       subtitle = str_c("Number of total records over the week :",
                        tot_records, "billions", sep = " ")) +
  theme_bw() +
  theme(panel.border = element_blank(),
        axis.line = element_line(colour = "black"),
        axis.text.x = element_text(size = 12, colour = "black"),
        axis.text.y = element_text(size = 12, colour = "black"),
        axis.title.x = element_blank(),
        axis.title.y = element_text(size = 13), 
        plot.subtitle = element_text(size = 13))



# Distribution of daily cell appearance ---------------------------------------

cell_appear <- read_csv("~/inputs/daily_cell_appear.csv", 
                   col_names = c("lac", "ci_sac", "day", "month"))

n_cells <- cell_appear %>%
  select(lac, ci_sac) %>%
  distinct() %>%
  nrow()


cell_appear %>%
  group_by(lac, ci_sac) %>%
  summarise(n_days = n()) %>%
  ggplot(aes(x = factor(n_days))) + 
  geom_bar() +
  scale_y_continuous(labels = function(x) x/1000) +
  labs(x = "Distinct days of appearance", y = "Cells (in thousands)",
       subtitle = str_c("Number of distinct cells : ",
                        format(n_cells, big.mark=","))) +
  theme_bw() +
  theme(panel.border = element_blank(),
        axis.line = element_line(colour = "black"),
        axis.text.x = element_text(size = 12, colour = "black"),
        axis.text.y = element_text(size = 12, colour = "black"),
        axis.title.x = element_text(size = 13),
        axis.title.y = element_text(size = 13),
        plot.subtitle = element_text(size = 13))





# Distribution of daily user appearance ---------------------------------------

imsi_appear <- read_csv("~/inputs/n_days_user_appear.csv", 
                        col_names = c("n_days"))

n_unique_imsi <- nrow(user_appear)

imsi_appear %>%
  ggplot(aes(x = factor(n_days))) + 
  geom_bar() +
  scale_y_continuous(labels = function(x) x / 1E6) +
  labs(x = "Distinct days of appearance", y = "Users (in millions)",
       subtitle = str_c("Number of distinct users : ",
                        format(n_unique_imsi, big.mark=","))) +
  theme_bw() +
  theme(panel.border = element_blank(),
        axis.line = element_line(colour = "black"),
        axis.text.x = element_text(size = 12, colour = "black"),
        axis.text.y = element_text(size = 12, colour = "black"),
        axis.title.x = element_text(size = 13),
        axis.title.y = element_text(size = 13),
        plot.subtitle = element_text(size = 13))





# Distributions of daily number of events/locations per user ------------------

avg_daily_records <- fread("~/inputs/avg_n_daily_events_user.csv", 
                           col.names = c("n"))
avg_daily_records <- avg_daily_records[n < 1000,]
avg_daily_records$Type <- "Records"

avg_daily_locations <- fread("~/inputs/avg_n_daily_locations_user.csv",
                             col.names = c("n"))
avg_daily_locations <- avg_daily_locations[n < 160,]
avg_daily_locations$Type <- "Locations"

daily_rec_loc <- rbind(avg_daily_records, avg_daily_locations)

# daily_rec_loc[sample(1:nrow(daily_rec_loc), 10000),]
daily_rec_loc %>%
  ggplot(aes(n, colour = Type)) +
  stat_ecdf(size = 1) +
  scale_colour_manual(values=c("red", "blue")) +
  labs(x = "Daily records/locations per user", y = "CDF") +
  theme_bw() +
  theme(panel.border = element_blank(),
        axis.line = element_line(colour = "black"),
        axis.text.x = element_text(size = 12, colour = "black"),
        axis.text.y = element_text(size = 12, colour = "black"),
        axis.title.x = element_text(size = 13),
        axis.title.y = element_text(size = 13),
        legend.title = element_text(size = 14),
        legend.text = element_text(size = 12))






# Hourly distributions of records/unique users ----------------------------

# Records

hourly_records <- read_csv("~/inputs/hourly_records.csv",
                           col_names = c("hour", "n", "day", "month"))

hourly_records <- hourly_records %>%
  select(-month) %>%
  filter(!(day == 28 & hour == 23),
         !(day == 29 & hour == 23)) %>% # remove anomalies in the data
  mutate(Day = fct_recode(factor(day),
                           "Mon" = "25", "Tue"  = "26", "Wed" = "27",
                           "Thu" = "28", "Fri" = "29", "Sat" = "30", 
                           "Sun" = "31")) 

hourly_records %>%
  mutate(n = n / 1E6) %>%
  ggplot(aes(x = hour, y = n, col = Day)) +
  geom_line() +
  labs(x = "Hour", y = "Records (in millions)") +
  theme_bw() +
  theme(panel.border = element_rect(size = 1),
        axis.text.x = element_text(size = 12, colour = "black"),
        axis.text.y = element_text(size = 12, colour = "black"),
        axis.title.x = element_text(size = 13),
        axis.title.y = element_text(size = 13),
        legend.title = element_text(size = 14),
        legend.text = element_text(size = 12))
    
# Unique users

hourly_unique_imsi <- read_csv("~/inputs/hourly_unique_users.csv",
                           col_names = c("hour", "n", "day", "month"))

hourly_unique_imsi <- hourly_unique_imsi %>%
  select(-month) %>%
  filter(!(day == 28 & hour == 23),
         !(day == 29 & hour == 23)) %>% # remove anomalies in the data
  mutate(Day = fct_recode(factor(day),
                          "Mon" = "25", "Tue"  = "26", "Wed" = "27",
                          "Thu" = "28", "Fri" = "29", "Sat" = "30", 
                          "Sun" = "31")) 

hourly_unique_imsi %>%
  mutate(n = n / n_unique_imsi * 100) %>%
  ggplot(aes(x = hour, y = n, col = Day)) +
  geom_line() +
  labs(x = "Hour", y = "% of users that appear at time t") +
  theme_bw() +
  theme(panel.border = element_rect(size = 1),
        axis.text.x = element_text(size = 12, colour = "black"),
        axis.text.y = element_text(size = 12, colour = "black"),
        axis.title.x = element_text(size = 13),
        axis.title.y = element_text(size = 13),
        legend.title = element_text(size = 14),
        legend.text = element_text(size = 12))



  
  

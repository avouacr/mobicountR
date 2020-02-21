
# -----------------------------------------------------------------------------
# --- Comparison Home Detection with/without prior - Tables preparation
# --- Heuristic : Max activity with time restriction (7PM-7AM)
# --- Month : September 2007
# --- Area : Paris urban unit
# -----------------------------------------------------------------------------

rm(list=ls())

# Libraries declaration -------------------------------------------------------

library(tidyverse)
library(sf) 





# Tables preparation for counts comparison ------------------------------------

# # # Import RFL 2011, convert coordinates to L93 and export as new csv
# # filo2011 <- filo2011 %>%
# #   na.omit() %>%
# #   st_as_sf(coords = c("x", "y"), crs = 27572) %>%
# #   st_transform(crs = 2154) %>%
# #   sfc_as_cols() %>%
# #   st_set_geometry(NULL) %>%
# #   write_csv("Tables/RFL/rfl_nbp_2011_L93.csv")
# 
# Import grid

grid <- st_read("Tables/Grid/grid_500_UUParis.shp", crs = 2154)

gridBB <- st_bbox(grid)

gridBB_sf <- st_as_sfc(gridBB)

# # Import RFL data
# 
# filo2011 <- read_csv("Tables/RFL/rfl_nbp_2011_L93.csv")
# 
# # Keep only data in relevant bouding box
# filo2011 <- filo2011 %>%
#   filter(between(x, gridBB$xmin, gridBB$xmax)) %>%
#   filter(between(y, gridBB$ymin, gridBB$ymax))
# 
# # Computes intersections with grid
# filo2011_grid <- filo2011 %>%
#   st_as_sf(coords = c("x", "y"), crs = 2154) %>%
#   st_intersection(grid) %>%
#   st_set_geometry(NULL)
# 
# st_geometry(grid) <- NULL
# 
# # Count people by cell in RFL data
# filo2011_grid <- filo2011_grid %>%
#   group_by(grid_id) %>%
#   summarise(n_filo = sum(nbpersm)) %>%
#   right_join(grid) %>%
#   mutate(n_filo = replace_na(n_filo, 0))
# 
# # Export counts
# write_csv(filo2011_grid, "Tables/Counts/count_filo2011_grid500_UUParis.csv")

# Import RFL 2011 counts at grid level

filo2011_grid <- read_csv("Tables/Counts/count_filo2011_grid500_UUParis.csv")

# Import home detection results, with and without prior

df_HD_noprior <- read_csv("Tables/Counts/MA_noprior_sept_Paris.csv", 
                         col_names = c("month", "CodeId", "grid_id"))

df_HD_noprior <- select(df_HD_noprior, c("CodeId", "grid_id"))

df_HD_prior <- read_csv("Tables/Counts/MA_prior_sept_Paris.csv", 
                                 col_names = c("month", "CodeId", "grid_id"))

df_HD_prior <- select(df_HD_prior, c("CodeId", "grid_id"))

# Count people by cell

n_grid_noprior <- df_HD_noprior %>% 
  select(c("CodeId", "grid_id")) %>%
  group_by(grid_id) %>%
  summarise(n_noprior = n())

n_grid_prior <- df_HD_prior %>% 
  select(c("CodeId", "grid_id")) %>%
  group_by(grid_id) %>%
  summarise(n_prior = n())

df_compar <- filo2011_grid %>% 
  left_join(n_grid_noprior) %>%
  left_join(n_grid_prior) %>%
  mutate(n_noprior = replace_na(n_noprior, 0),
         n_prior = replace_na(n_prior, 0))

write_csv(df_compar, "Tables/Counts/MA_compar_sept_Paris.csv")

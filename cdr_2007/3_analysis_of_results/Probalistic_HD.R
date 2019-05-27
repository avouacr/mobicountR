

# -----------------------------------------------------------------------------
# --- Voronoi_to_grid with priors from BD Topo
# -----------------------------------------------------------------------------

# Libraries declaration -------------------------------------------------------

install.packages("pryr")
install.packages("packages/minior-master.tar.gz", repos = NULL)

library(minior)
library(tidyverse)
library(data.table)
library(sf)
library(leaflet)
library(ggplot2)

minio_bucket <- "etgtk6"




# # Count people at tile level and export results to Minio ----------------------
# 
# # Import probabilistic home detection results
# 
# save_minio_file(file_path = "MobiCount/home_detection/PHD_bdtopo_france_09.csv",
#                 bucket_name = minio_bucket, 
#                 ext_path = "~/")
# 
# PHD_bdtopo <- fread("~/PHD_bdtopo_france_09.csv",
#                     col.names = c("month", "caller_id", "grid_id", "proba"))
# 
# PHD_bdtopo <- PHD_bdtopo[, 'month' := NULL]
# 
# PHD_bdtopo[,uniqueN(caller_id)] # 16,6 millions people home detected
# 
# # Normalize each caller's probabilities so that they sum to one
# 
# PHD <- PHD_bdtopo[, .(proba_norm = proba / sum(proba)), by = .(caller_id)]
# PHD <- PHD[order(caller_id, proba_norm)]
# PHD_grid <- PHD_bdtopo[order(caller_id, proba)]
# PHD <- PHD[, grid_id := PHD_grid[grid_id]]
# 
# # Aggregate at tile level
# 
# counts <- PHD[, .(n = sum(proba_norm)), by = .(grid_id)]
# 
# # Export counts to minio
# 
# write_csv(counts, "~/counts_PHD_bdtopo_france_09.csv")
# put_minio_file("~/counts_PHD_bdtopo_france_09.csv",
#                "MobiCount/counts/counts_PHD_bdtopo_france_09.csv",
#                "etgtk6")





# Import inputs ---------------------------------------------------------------

# Counts at tile level from PHD with BD Topo prior

save_minio_file("MobiCount/counts/counts_PHD_bdtopo_france_09.csv",
                minio_bucket,
                ext_path = "~")

counts <- read_csv("~/counts_PHD_bdtopo_france_09.csv")

# France grid

lapply(files_in_bucket(minio_bucket, "MobiCount/grids/grid_500_france"),
       function(x){
         save_minio_file(x, 
                         bucket_name = minio_bucket, 
                         ext_path = "~/grid_france")
       })

grid <- st_read("~/grid_france", crs = 2154)
grid$grid_id <- as.character(grid$grid_id)

# Voronoi tesselation

lapply(files_in_bucket(minio_bucket, "MobiCount/shp/voronoi"),
       function(x){
         save_minio_file(x, 
                         bucket_name = minio_bucket, 
                         ext_path = "~/voronoi")
       })

voronoi <- st_read("~/voronoi", crs = 2154)


# Table of intersections between grid and voronoi tesselation

save_minio_file("MobiCount/grids/grid_inter_voronoi_france.csv",
                minio_bucket,
                ext_path = "~")

table_inter <- read_csv("~/grid_inter_voronoi_france.csv")


# RFL counts at tile level

rfl <- read_csv("~/rfl_counts.csv")


# France departments shapefiles

lapply(files_in_bucket(minio_bucket, "MobiCount/shp/departements_fr"),
       function(x){
         save_minio_file(x, 
                         bucket_name = minio_bucket, 
                         ext_path = "~/shp_dep")
       })

dep_shp <- st_read("~/shp_dep/departements-20180101.shp") %>%
  st_transform(2154)

# Prepare data for comparison -------------------------------------------------

df_compar_geo <- grid %>%
  left_join(counts) %>%
  rename(n_bdtopo = n) %>%
  left_join(rfl) %>%
  rename(n_rfl = nbpersm) %>%
  mutate(n_bdtopo = replace_na(n_bdtopo, 0),
         n_rfl = replace_na(n_rfl, 0))
  
df_plot_rfl <- df_compar_geo %>%
  filter(n_rfl != 0) %>%
  mutate(log_n_rfl = log(n_rfl)) %>%
  select(-n_bdtopo)

df_plot_bdtopo <- df_compar_geo %>%
  filter(n_bdtopo != 0) %>%
  mutate(log_n_bdtopo = log(n_bdtopo)) %>%
  select(-n_rfl)


# Localized analysis ----------------------------------------------------------

compar_dep <- function(df_compar_geo, dep_shp, num_dep) {
  
  # Filter comparison table for specified department
  
  dep_shp <- dep_shp %>% 
    filter(code_insee == as.character(num_dep))
  
  vec_inter <- sapply(st_intersects(df_compar_geo, dep_shp), length)
  df_compar_sub <- df_compar_geo[vec_inter == 1,]
  df_compar_sub_ng <- df_compar_sub %>% st_set_geometry(NULL)
  
  # Compute correlation between PHD with BD Topo and RFL
  
  cor <- cor(df_compar_sub_ng$n_bdtopo, df_compar_sub_ng$n_rfl)
  
  # Compare densities
  
  df_compar_sub_ng %>%
    gather(source, "n", n_bdtopo:n_rfl) %>%
    filter(n >= 1) %>%
    mutate(n = round(n),
           log_n = log(n)) %>%
    ggplot(aes(x = log_n, colour = source, fill = source)) +
    geom_density(alpha = 0.1)
  
  # 
  
}


# Compare densities of (log) population counts at France level

df_compar_ng %>%
  gather(source, "n", n_bdtopo:n_rfl) %>%
  filter(n >= 1) %>%
  mutate(n = round(n),
         #n = replace(n, n==0, 1),
         log_n = log(n)) %>%
  ggplot(aes(x = log_n, colour = source, fill = source)) +
  geom_density(alpha = 0.1) + 
  theme_bw()







# -----------------------------------------------------------------------------
# --- Voronoi_to_grid with priors from BD Topo
# -----------------------------------------------------------------------------

# Libraries declaration -------------------------------------------------------

install.packages(c('pryr', 'doParallel', 'aws.s3'))
install.packages("packages/minior-master.tar.gz", repos = NULL)
# install.packages("packages/mobunit_0.1.0.tar.gz", repos = NULL)
# install.packages("packages/FiloPhone_0.1.0.tar.gz", repos = NULL)

library(minior)
library(sf)
library(tidyverse)
library(doParallel)
library(aws.s3)

minio_bucket <- "etgtk6"

# Functions declaration -------------------------------------------------------

sfc_as_cols <- function(x, names = c("x","y")) {
  stopifnot(inherits(x,"sf") && inherits(sf::st_geometry(x),"sfc_POINT"))
  ret <- sf::st_coordinates(x)
  ret <- tibble::as_tibble(ret)
  stopifnot(length(names) == ncol(ret))
  x <- x[ , !names(x) %in% names]
  ret <- setNames(ret,names)
  dplyr::bind_cols(x,ret)
}




# Recuperation of inputs on Minio ---------------------------------------------

# BD Topo shapefiles

path_local_bdtopo <- "~/shp_bd_topo"

save_minio_file(file_path = "MobiCount/shp/BD Topo.zip",
            bucket_name = minio_bucket, 
            ext_path = path_local_bdtopo)

unzip("~/shp_bd_topo/BD Topo.zip", exdir = path_local_bdtopo)

file.remove(paste(path_local_bdtopo, "BD Topo.zip", sep = "/"))

list_shp_zip <- list.files(path_local_bdtopo)

sapply(list_shp_zip, function(x) {
  unzip(paste(path_local_bdtopo, x, sep = "/"),
        exdir = paste(path_local_bdtopo, str_split(x, "\\.")[[1]][1], sep = "/"))
})

sapply(list_shp_zip, function(x) {
  file.remove(paste(path_local_bdtopo, x, sep = "/"))
})

# Grid at national level

path_grid_minio <- 'MobiCount/grids/grid_500_france'
path_grid_local <- "~/grid_france"

lapply(files_in_bucket(minio_bucket, path_grid_minio),
       function(x){
         save_minio_file(x, 
                         bucket_name = minio_bucket, 
                         ext_path = path_grid_local)
       })

# Table of intersections between grid and voronoi tesselation

save_minio_file("MobiCount/grids/grid_inter_voronoi_france.csv",
                "etgtk6",
                ext_path = "~")


# Computation of prior on grid ------------------------------------------------


prior_grid_dep <- function(path_grid,
                       dir_shp_bdtopo,
                       dir_output,
                       area_min_max = NULL,
                       height_min_max = NULL,
                       impute = TRUE,
                       crs = 2154,
                       n_workers = 5)
{
  
  # Import grid
  grid <- st_read(dsn = path_grid, crs = crs)
  
  # Create ouput directory if it doesn't exist
  if (!dir.exists(dir_output)) {dir.create(dir_output)}
  
  # Instance parallelization
  cl <- makeCluster(n_workers)
  registerDoParallel(cl)
  
  shp_names <- list.files(path = dir_shp_bdtopo)
  
  if (!dir.exists(dir_output)) {
    dir.create(dir_output)
    shp_to_do <- shp_names
  } else {
    shp_done <- paste0(str_match(list.files(dir_output), "(bati[\\w\\d_]+).")[,2],
                        "_2015")
    shp_to_do <- shp_names[!(shp_names %in% shp_done)]
    }
  
  prior_france <- foreach(i = shp_to_do, 
                          .packages = c('dplyr', 'stringr', 'sf', 'readr', 'tidyr'), 
                          .export = c('sfc_as_cols'),
                          .verbose = TRUE) %dopar% {
                            
                            # Import BD Topo "Bati indifférencié" shapefiles
                            if (str_sub(i,-4,-1) == "2015") {
                              df_shp <- st_read(dsn = paste0(dir_shp_bdtopo, "/", i), crs = crs)
                            } else if (str_sub(i,-4,-1) == "2016") {
                              df_shp <- st_read(dsn = paste0(dir_shp_bdtopo, "/", i)) %>%
                                st_transform(crs = crs)
                            }
                            
                            # Compute buildings area and their centroids (x,y)
                            df_shp <- df_shp %>%
                              select(ID, HAUTEUR, geometry) %>%
                              rename("id" = ID, "height" = HAUTEUR) %>%
                              mutate(area = as.numeric(st_area(.))) %>% 
                              filter(!(height == 0 & area < 20)) # Filter invalid data
                            
                            # Keep only buildings within min and max heights if specified
                            if (!is.null(height_min_max)) {
                              if (is.na(height_min_max[1]) & !is.na(height_min_max[2])) {
                                df_shp <- df_shp %>%
                                  filter(height <= as.numeric(height_min_max[2]))
                              } else if (!is.na(height_min_max[1]) & is.na(height_min_max[2])) {
                                df_shp <- df_shp %>%
                                  filter(height >= as.numeric(height_min_max[1]))
                              } else {
                                df_shp <- df_shp %>%
                                  filter(height >= as.numeric(height_min_max[1]), 
                                         height <= as.numeric(height_min_max[2]))
                              }
                            }
                            
                            
                            # Keep only buildings within min and max areas if specified
                            if (!is.null(area_min_max)) {
                              if (is.na(area_min_max[1]) & !is.na(area_min_max[2])) {
                                df_shp <- df_shp %>%
                                  filter(area <= as.numeric(area_min_max[2]))
                              } else if (!is.na(area_min_max[1]) & is.na(area_min_max[2])) {
                                df_shp <- df_shp %>%
                                  filter(area >= as.numeric(area_min_max[1]))
                              } else {
                                df_shp <- df_shp %>%
                                  filter(area >= as.numeric(area_min_max[1]), 
                                         area <= as.numeric(area_min_max[2]))
                              }
                            }
                            
                            # Impute height for buildings with 0 height (invalid)
                            # Rule : assign clothest neighbour in area among buildings of the same department
                            if (impute) {
                              df_shp_centro <- df_shp %>%
                                st_centroid() %>%
                                sfc_as_cols() %>%
                                st_set_geometry(NULL)
                              
                              df_to_impute <- df_shp_centro %>%
                                filter(height == 0)
                              
                              df_donors <- df_shp_centro %>%
                                filter(height > 0)
                              
                              vec_imp <- sapply(1:nrow(df_to_impute), function(i) {
                                
                                height_imp <- df_donors$height[which.min(abs(df_donors$area-df_to_impute$area[i]))]
                                
                                return(height_imp)
                              })
                              
                              df_to_impute$height <- vec_imp
                              
                              df_shp_imputed <- rbind(df_to_impute, df_donors) %>%
                                select(-x, -y)
                              
                              df_shp <- df_shp %>% 
                                select(id, area, geometry) %>%
                                left_join(df_shp_imputed)
                                
                            }
                                
                            
                            # Restriction of grid to bouding box of BD topo shapefile
                            bbox_shp <- st_bbox(df_shp)
                            
                            grid_sub <- grid %>%
                              separate(grid_id, c("x_cent", "y_cent"), remove = F) %>%
                              mutate(x_cent = as.numeric(x_cent) * 100,
                                     y_cent = as.numeric(y_cent) * 100) %>%
                              filter(x_cent >= bbox_shp$xmin & x_cent <= bbox_shp$xmax &
                                       y_cent >= bbox_shp$ymin & y_cent <= bbox_shp$ymax)
                            
                            #  Computation of intersection areas between buildings and grid
                            inter_build_grid <- grid_sub %>%
                              st_intersection(df_shp) %>%
                              mutate(area = as.numeric(st_area(.)), 
                                     volume = area * height) %>%
                              st_set_geometry(NULL) %>%
                              select(grid_id, area, volume)
                            
                            # Agregate building volume at tile level
                            prior_grid_dep <- inter_build_grid %>%
                              group_by(grid_id) %>%
                              summarise(build_volume = sum(volume))
                            
                            # Save result for each department locally
                            path_output <- paste0(dir_output, "/prior_vol_",
                                                        str_sub(i, 1, -6), ".csv")
                            write_csv(prior_grid_dep, path_output)
                          }
  
  stopCluster(cl)
  
  return(TRUE)
}

table_prob_prior <- function(dir_prior_dep,
                         path_inter_grid_voro) {
  
  # Import intersection table between grid and voronoi tesselation
  inter_grid_voro <- read_csv(path_inter_grid_voro) %>%
    separate(id, c("NIDT", "grid_id"), remove = FALSE, sep = ":")
  
  # Merge all prior dataframes in a single dataframe
  list_csv_prior <- list.files(dir_prior_dep, full.names = TRUE)
  list_df_prior <- lapply(list_csv_prior, read_csv)
  prior_france <- do.call("rbind", list_df_prior)
  
  # Compute prior at tile level
  prior_france <- prior_france %>%
    group_by(grid_id) %>%
    summarise(build_volume = sum(build_volume)) %>%
    mutate(prior_build = build_volume / sum(build_volume))
  
  # Join prior and grid-voronoi intersections tables
  prior_final <- inter_grid_voro %>%
    left_join(prior_france) %>%
    mutate(prior_build = replace_na(prior_build, 0),
           proba_build = proba_inter*prior_build) %>%
    select(id, proba_inter, prior_build, proba_build)
  
  return(prior_final)
}




# For 5 workers, provide at least 70Go of ram on the innovation platform

prior_grid_dep(path_grid = "~/grid_france/grid_500_france.shp",
               dir_shp_bdtopo = "~/shp_bd_topo",
               dir_output = "~/prior_grid_dep",
               area_min_max = c(NA, 4000),
               height_min_max = c(NA, 135),
               impute = TRUE,
               n_workers = 7)

table_prob_prior <- table_prob_prior(dir_prior_dep = "~/prior_grid_dep",
                 path_inter_grid_voro = "~/grid_inter_voronoi_france.csv")

write_csv(table_prob_prior, "~/prior_bdtopo_france.csv")
  

# Merge priors in a single table

prior_bdtopo_france <- read_csv("~/prior_bdtopo_france.csv")
prior_rfl_france <- read_csv("~/prior_rfl_france.csv")

prior_merge <- prior_bdtopo_france %>%
  left_join(prior_rfl_france)
  
write_csv(prior_merge, "~/prior_rfl_bdtopo_france.csv")









bati_paris_uu <- extract_bati("Paris", 
                              path_citiesSHP2011 = pathSHP2011,
                              pathcityUU = path_inputs,
                              area_min_max = c(20, 4000),
                              height_min_max = c(2, 135))

# Choix area_max = 3000 : immeuble de 130*30 ?
# Choix height_min = 2 : pas de 1m dans la base (cf. doc) => exclusion des 0
# Choix height_max = 135 : cf. 
# https://fr.wikipedia.org/wiki/Liste_des_plus_hautes_structures_de_France




df_prob <- grid_intersection(voronoi = voronoi_paris,
                             grid = grid_paris$grid,
                             prior = bati_paris_uu)

df_prob_nogeo <- df_prob
st_geometry(df_prob_nogeo) <- NULL

write_csv(df_prob_nogeo, "prob_grid_prior.csv")


# Diagnostics -----------------------------------------------------------------

### Buildings areas

boxplot(bati_paris_uu$area)

q <- c(0.05, (1:9)/10, 0.95, 0.99)
quantile(bati_paris_uu$area, probs = q)

length(bati_paris_uu$area[bati_paris_uu$area < 20]) / 
  length(bati_paris_uu$area)
# 12% de batiments ont une surface inférieure à 20m^2
# Pas logique : que les bat de + de 20m^2 selon la doc

bati_paris_uu$area[bati_paris_uu$area > 3000]

### Buildings height

boxplot(bati_paris_uu$height)

quantile(bati_paris_uu$height, probs = q)

length(bati_paris_uu$height[bati_paris_uu$height == 0]) / 
  length(bati_paris_uu$height) 
# 13% de batiments ont une hauteur nulle
# => imputation

length(bati_paris_uu$height[bati_paris_uu$height < 2]) / 
  length(bati_paris_uu$height) 
# Pas de batiments dont la hauteur est comprise entre 0 et 2 m exclus

### Combination of both errors ?

length(bati_paris_uu$area[bati_paris_uu$area < 20 & bati_paris_uu$height == 0]) /
  nrow(bati_paris_uu) 
# 8% des batiments combinent les 2 erreurs

test <- bati_paris_uu %>% filter(area >= 20 & height >= 2)


### Verification of share of areas without buildings

length(df_prob$prob_final[df_prob$prob_final == 0]) / nrow(df_prob)
# 25% of areas have 0 building (volume)

plot(df_prob[,"proba_final"])










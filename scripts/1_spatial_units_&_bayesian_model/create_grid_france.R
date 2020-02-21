
library(minior)
library(sf)


# General parameters ------------------------------------------------------

minio_bucket <- "etgtk6"
dir_grid_minio <- "MobiCount/grids"
dir_grid_local <- "~/grid_france"
dir_voro_local <- "~/voronoi"
dir_voro_minio <- "MobiCount/shp/voronoi"


# Grid creation at France level -------------------------------------------


create_grid_france <- function(shp_france,
                               grid_size,
                               export = TRUE,
                               path_out) {
  
  
  # Compute France bounding box
  bbox_france <- sf::st_bbox(shp_france)
  xmin <- floor(bbox_france$xmin/grid_size)*grid_size
  xmax <- ceiling(bbox_france$xmax/grid_size)*grid_size 
  ymin <- floor(bbox_france$ymin/grid_size)*grid_size
  ymax <- ceiling(bbox_france$ymax/grid_size)*grid_size 
  
  # Create grid from bounding box
  x <- seq(xmin, xmax, grid_size)
  y <- seq(ymin, ymax, grid_size)
  grid <- merge(x,y)
  geogrid <- btb::dfToGrid(grid, 
                           st_crs(shp_france)$epsg, 
                           grid_size)
  
  # Restrict grid to metropolitan France
  inter_grid_metro <- sf::st_intersects(geogrid, 
                                        shp_france, 
                                        sparse = T)
  len_inter <- sapply(inter_grid_metro, length)
  grid_france <- geogrid[len_inter == 1,]
  
  # Create IDs of tiles as "x/100:y/100"
  grid_france <- grid_france %>% 
    mutate(grid_id = paste(as.character(x/100), 
                           as.character(y/100), 
                           sep = "_")) %>%
    select(-x, -y)
  
  # Export if specified
  if (export) {
    st_write(grid_france, path_out)
    message("Grid exported.")
    return(TRUE)
    }
  else {
    return(grid_france)
  }
}

# Create grid

shp_france <- sf::st_read("inputs/shp/france_metro", crs = 2154)

grid <- create_grid_france(shp_france = shp_france,
                           grid_size = 500,
                           export = TRUE,
                           path_out = paste(dir_grid_local, 
                                            "grid_500_france.shp", 
                                            sep = "/"))



# Export grid to Minio

files_grid <- list.files(dir_grid_local, full.names = TRUE)

lapply(files_grid, function(x) {
  put_minio_file(x,
                 paste(dir_grid_minio, basename(x), sep = "/"),
                 minio_bucket)
})





# Make valid voronoi shapefile and export to Minio -----------------

library(lwgeom) # Can't be installed from CRAN, have to clone github repos

dir_voro_repo <- "inputs/shp/voronoi"

voronoi <- st_read(dir_voro_repo, crs = 2154)
voronoi <- voronoi %>% st_make_valid()

files_voro <- list.files(dir_voro_repo, full.names = TRUE)

lapply(files_grid, function(x) {
  put_minio_file(x,
                 paste(dir_voro_minio, basename(x), sep = "/"),
                 minio_bucket)
})




# Computation of intersections between grid and voronoi tesselation -------

# Import grid from Minio

list_grids <- files_in_bucket(minio_bucket, dir_grid_minio)
files_grid_france <- grep("grid_500_france\\.", list_grids, value = T)

lapply(files_grid_france,
       function(x){
         save_minio_file(x, "etgtk6", ext_path = dir_grid_local)
       })

grid <- st_read(dir_grid_local, crs = 2154)

# Import voronoi from Minio

files_voronoi <- files_in_bucket(minio_bucket, dir_voro_minio)

lapply(files_voronoi,
       function(x){
         save_minio_file(x, "etgtk6", ext_path = dir_voro_local)
       })

voronoi <- st_read(dir_voro_local, crs = 2154)

# Compute intersections

df_inter <- grid %>%
  st_intersection(voronoi) %>%
  mutate(area_inter = as.numeric(st_area(.))) %>%
  st_set_geometry(NULL)

df_voro <- voronoi %>%
  mutate(area_voro = as.numeric(st_area(.))) %>%
  st_set_geometry(NULL)

inter_grid_voro <- df_inter %>%
  left_join(df_voro) %>%
  mutate(proba_inter = area_inter / area_voro) %>%
  unite(id, NIDT, grid_id, sep = ":", remove = FALSE) %>%
  select(id, proba_inter)

# Export intersection table to Minio

write_csv(inter_grid_voro, "~/grid_inter_voronoi_france.csv")

put_minio_file("~/grid_inter_voronoi_france.csv",
               "MobiCount/grids/grid_inter_voronoi_france.csv",
               "etgtk6")



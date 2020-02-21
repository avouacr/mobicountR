
sfc_as_cols <- function(x, names = c("x","y")) {
  stopifnot(inherits(x,"sf") && inherits(sf::st_geometry(x),"sfc_POINT"))
  ret <- sf::st_coordinates(x)
  ret <- tibble::as_tibble(ret)
  stopifnot(length(names) == ncol(ret))
  x <- x[ , !names(x) %in% names]
  ret <- setNames(ret,names)
  df_final <- dplyr::bind_cols(x,ret)
  return(df_final)
}




df_compar_idf <- df_compar_geo %>%
  filter(dep %in% c(75,77,78,91,92,93,94,95))

# Regression of estimation errors  ----------------------------------------

# Import shapefiles for Parisian area

get_file_local(path_minio = "MobiCount/shp/reg_errors_2007/bpe_2011.csv",
               bucket = "etgtk6", 
               ext_dir = "~/shp_reg")

shp_zip <- str_c("MobiCount/shp/reg_errors_2007/",
                 c("airports_idf.zip", "hospitals_idf.zip", 
                   "train_stations_idf.zip"))

map(shp_zip, function(x) {
  get_file_local(path_minio = x,
                 bucket = "etgtk6", 
                 ext_dir = "~/shp_reg",
                 uncompress = T, remove = T)
})

bpe <- read_csv2("~/shp_reg/bpe_2011.csv")

stations <- st_read("~/shp_reg/emplacement-des-gares-idf-data-generalisee.shp")

airports <- st_read("~/shp_reg/les-aeroports-dile-de-france.shp") %>%
  st_transform(2154)

hospitals <- st_read("~/shp_reg/les_etablissements_hospitaliers_franciliens.shp")

# Import Voronoi

get_file(path_minio = "MobiCount/grids/grid_inter_voronoi_france.csv")

get_file_local(path_minio = "MobiCount/shp/voronoi/",
               bucket = "etgtk6", 
               ext_dir = "~/shp_reg")

### Preprocessing of spatial data

# shops, restaurants, university residences, hospitals, cinema-theatres, 
# hostels-campings

bpe_to_grid <- bpe %>% 
  rename(dep = `Code département`, coords = `Coordonnées géo (commune)`,
         equip = `Code d'équipement`) %>%
  filter(dep %in% c("75","77","78","91","92","93","94","95")) %>%
  separate(coords, into = c("y","x"), sep = ", ") %>%
  mutate(x = as.numeric(x), y = as.numeric(y),
         type_equip = ifelse(str_sub(equip,1,1)=="B","shop",
                      ifelse(equip %in% c("A504","C702"),"restaurant",
                      ifelse(equip=="C701","res_univ",
                      # ifelse(str_sub(equip,1,2)=="D1","hospital",
                      ifelse(str_sub(equip,1,2)=="F3","cinema_theatre",
                      ifelse(equip %in% c("G102","G103"),"hostel_camping", NA))))))) %>%
  select(x, y, type_equip) %>%
  drop_na() %>%
  st_as_sf(coords = c("x","y"), crs = 4326) %>%
  st_transform(2154) %>%
  sfc_as_cols() %>%
  st_set_geometry(NULL) %>%
  mutate(x = round(x/500)*500/100, y = round(y/500)*500/100) %>%
  unite(col = "grid_id", x, y, sep = "_", remove = T) %>%
  group_by(grid_id) %>%
  summarise(n_shops = sum(type_equip == "shop"),
         # n_hospitals = sum(type_equip == "hospital"),
         n_restaurants = sum(type_equip == "restaurant"),
         n_cinema_theatres = sum(type_equip == "cinema_theatre"),
         n_hostels = sum(type_equip == "hostel_camping"),
         n_res_univ = sum(type_equip == "res_univ"))

# hospitals

hospitals_to_grid <- hospitals %>%
  st_transform(2154) %>%
  sfc_as_cols() %>%
  st_set_geometry(NULL) %>%
  select(x, y) %>%
  mutate(x = round(x/500)*500/100, y = round(y/500)*500/100) %>%
  unite(col = "grid_id", x, y, sep = "_", remove = T) %>%
  unique() %>%
  mutate(hospital = 1)

# airports

airports_to_grid <- df_compar_idf %>%
  st_intersects(airports) %>%
  map_lgl(function(x) {
    if (length(x) == 0) return(FALSE)
    else return(TRUE)
  }) %>%
  df_compar_idf[.,] %>%
  st_set_geometry(NULL) %>%
  select(grid_id) %>%
  mutate(airport = 1)

# train stations

train_to_grid <- stations %>%
  st_set_geometry(NULL) %>%
  filter(train == 1) %>%
  select(x, y) %>%
  mutate(x = round(x/500)*500/100, y = round(y/500)*500/100) %>%
  unite(col = "grid_id", x, y, sep = "_", remove = T) %>%
  unique() %>%
  mutate(train_station = 1)
  
### Merge tables

df_reg <- df_compar_idf %>%
  st_set_geometry(NULL) %>%
  left_join(bpe_to_grid) %>%
  left_join(hospitals_to_grid) %>%
  left_join(airports_to_grid) %>%
  left_join(train_to_grid)

df_reg[is.na(df_reg)] <- 0
  

### Regression

df_reg %>%
  # filter(n_rfl != 0) %>%
  mutate(err = (n_bdtopo - n_rfl)) %>%
  filter(err > 0) %>%
  lm(formula = log(err) ~ n_shops + n_restaurants + n_cinema_theatres +
       n_hostels + n_res_univ + hospital + airport + train_station + factor(dep)) %>%
  summary()





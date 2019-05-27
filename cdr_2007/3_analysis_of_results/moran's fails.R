# Tests de Moran

# Avec le package spdep (trop long)

df_compar_geo_sp <- rgdal::readOGR("~/df_compar.shp")

nb <- poly2nb(df_compar_geo_sp)
list_nbw <- nb2listw(nb)

moran_uniform <- moran.test(x = , listw = list_nbw)

# Avec les ids en caractères (trop long)

list_coords <- str_split(grid_ids, pattern = "_") %>%
  map(~as.numeric(.)*100)

nb_list <- map(list_coords, function(id) {
  x_range <- c(id[1]-500, id[1], id[1]+500) / 100
  y_range <- c(id[2]-500, id[2], id[2]+500) / 100
  nb <- merge(x_range, y_range)
  nb <- unite(nb, col = nb_ids, x, y, sep = "_", remove = T)
  nb <- filter(nb, nb_ids %in% grid_ids)
  nb <- nb$nb_ids
  return(nb)
})
names(nb_list) <- grid_ids

nb <- merge(x_range, y_range) %>%
  unite(col = tile_id, x, y, sep = "_", remove = T) %>%
  filter(tile_id %in% grid_ids) %>%
  .$tile_id

n_tiles <- length(grid_ids)
cont_mat <- Matrix(0, nrow = n_tiles, ncol = n_tiles,
                   dimnames = list(grid_ids, grid_ids), sparse = TRUE)

for (id in grid_ids) {cont_mat[id, nb_list[[id]]] <- 1}





# Computations of Moran's local Is correlations

local_moran <- function(var, x, y, tile_size) {
  
  n <- length(var)
  
  # Compute contiguity matrix and keep only ones indices
  cont_mat <- cont_matrix(x = x, y = y, tile_size = tile_size)
  ind_ones <- summary(cont_mat) %>% 
    mutate(i = as.character(i), j = as.character(j))
  
  # Compute unique indices of tiles based on their x, y
  x_ind <- (x - min(x)) / tile_size + 1
  y_ind <- (y - min(y)) / tile_size + 1
  ids <- as.character((x_ind-1) * max(y_ind) + y_ind)
  
  # Compute centered values and index them in an environment (hash table)
  # for rapid access
  var_centered <- var - mean(var)
  hash_env <- new.env()
  for (i in seq_along(ids)) {
    hash_env[[ids[i]]] <- var_centered[i]
  }
  
  # Compute local Moran's Is
  # Source : https://en.wikipedia.org/wiki/Indicators_of_spatial_association
  m2 <- sum(var_centered^2) / (n-1)
  local_Is <- map_dbl(ids, function(id) {
    (hash_env[[id]] / m2) * 
      sum(map_dbl(ind_ones$j[ind_ones$i == id], function(a) {
        if (!is.null(hash_env[[a]])) {
          return(hash_env[[a]])
        } else {return(NA)}
      }), na.rm = T)
  })
  
  return(local_Is)
  
}

moran_loc_uni <- local_moran(var = df_moran$n_uniform, x = df_moran$x, 
                             y = df_moran$y, tile_size = 500)

moran_loc_bdt <- local_moran(var = df_moran$n_bdtopo, x = df_moran$x, 
                             y = df_moran$y, tile_size = 500)

moran_loc_rfl <- local_moran(var = df_moran$n_rfl, x = df_moran$x, 
                             y = df_moran$y, tile_size = 500)

df_moran_loc <- tibble(
  "uniform" = moran_loc_uni,
  "bdtopo" = moran_loc_bdt,
  "rfl" = moran_loc_rfl
) %>% round(digits = 2)

put_df(df_moran_loc, 
       filename = "local_moran_FR.csv", 
       dir_minio = "MobiCount", bucket = "etgtk6")

cor(moran_loc_uni, moran_loc_rfl)
cor(moran_loc_bdt, moran_loc_rfl)




# Production of section 3 results (Orange 2007 CDR) -----------------------

install.packages("~/mobicount-project/minior-master.tar.gz", repos = NULL)

install.packages(c("btb", 'Metrics', 'stargazer'))

library(tidyverse)
library(minior)
library(sf)
library(btb)
library(Metrics)
library(stargazer)
library(rlang)
library(spdep)
library(data.table)
library(Matrix)


set_minio_shared()



# Build comparison table --------------------------------------------------

# # Import PHD with different priors
# 
# phd_uniform <- get_file("MobiCount/home_detection/PHD_uniform_france_09.csv",
#                         "etgtk6")
# phd_uniform <- read_csv(phd_uniform, col_names = c("grid_id", "n_uniform"))
# 
# phd_bdtopo <- get_file("MobiCount/home_detection/PHD_bdtopo_france_09.csv",
#                        "etgtk6")
# phd_bdtopo <- read_csv(phd_bdtopo, col_names = c("grid_id", "n_bdtopo"))
# 
# phd_rfl <- get_file("MobiCount/home_detection/PHD_rfl_france_09.csv",
#                     "etgtk6")
# phd_rfl <- read_csv(phd_rfl, col_names = c("grid_id", "n_rfl_phd"))
# 
# # Import RFL benchmark
# 
# rfl <- read_csv("~/rfl_counts.csv")
# rfl <- rfl %>% rename(n_rfl = nbpersm)
# 
# # Import grid
# 
# get_file_local("MobiCount/grids/grid_500_france.zip", "etgtk6",
#                ext_dir = "~/grid", uncompress = T)
# grid <- st_read("~/grid/grid_500_france.shp", crs = 2154, 
#                 stringsAsFactors = FALSE)
# 
# grid_ng <- grid %>%
#   st_set_geometry(NULL) %>%
#   as_tibble()
# 
# # Build comparison table
# 
# df_compar <- grid_ng %>%
#   left_join(phd_bdtopo) %>%
#   left_join(phd_uniform) %>%
#   left_join(phd_rfl) %>%
#   left_join(rfl) %>%
#   replace(is.na(.), 0) %>%
#   as_tibble()
# 
# # Import grid - French departments correpondance table
# 
# get_file_local("MobiCount/shp/departements-20180101-shp.zip", "etgtk6",
#                ext_dir = "~/fr_departments", uncompress = T)
# 
# fr_dep <- st_read("~/fr_departments/dep_francemetro_2018.shp",
#                   stringsAsFactors = FALSE, crs = 2154) %>%
#   rename(dep = code) %>%
#   filter(!(dep %in% c("2A", "2B"))) %>%
#   mutate(dep = as.numeric(dep))
# 
# # df_compar_points <- df_compar %>% 
# #   st_as_sf(coords = c("x", "y"), crs = 2154)
# # 
# # grid_dep <- bind_cols(df_compar_points,
# #           fr_dep[as.numeric(st_intersects(df_compar_points, fr_dep)),]) %>%
# #   select(grid_id, code_insee) %>%
# #   fill(direction = "up") %>%
# #   fill(direction = "down")
# # 
# # put_df(grid_dep,
# #        "corresp_grid500_dep.csv",
# #        "MobiCount/grids/",
# #        "etgtk6")
# 
# grid_dep <- read_csv(get_file("MobiCount/grids/corresp_grid500_dep.csv", "etgtk6"))
# 
# # Import table with Orange shares by department
# 
# dep_shares <- read_csv("~/market_share.csv") %>%
#   select(Dep, ms) %>%
#   rename(dep = Dep)
# 
# # Reweighting by Orange local shares
# 
# df_compar <- grid_dep %>%
#   left_join(df_compar) %>%
#   left_join(dep_shares) %>%
#   mutate(n_bdtopo = n_bdtopo / ms,
#          n_uniform = n_uniform / ms,
#          n_rfl_phd = n_rfl_phd / ms) %>%
#   mutate_at(c("n_bdtopo", "n_uniform", "n_rfl_phd"), round)  %>%
#   mutate(bdt_rfl = n_bdtopo - n_rfl, uni_rfl = n_uniform - n_rfl)
# 
# df_compar_geo <- df_compar %>%
#   left_join(grid) %>%
#   select(-ms) %>%
#   st_sf()
# 
# # Export comparison shapefile
# st_write(df_compar_geo, "~/df_compar.shp")
# zip(zipfile = "/home/etgtk6/df_compar_PHD.zip",
#     files = dir("/home/etgtk6", "df_compar", full.names = T), 
# )
# put_local_file("~/df_compar_PHD.zip",
#                dir_minio = "MobiCount/home_detection", bucket = "etgtk6")

get_file_local("MobiCount/home_detection/df_compar_PHD.zip", "etgtk6",
               ext_dir = "~/df_compar", uncompress = T)

df_compar_geo <- st_read("~/df_compar/df_compar.shp", crs = 2154, 
                         stringsAsFactors = FALSE)

df_compar <- df_compar_geo %>% st_set_geometry(NULL)


# Descriptive statistics of users activity --------------------------------

# summary_fun <- function(x, ...){
#   c(mean=mean(x, ...),
#     sd=sd(x, ...),
#     min=min(x, ...),
#     p10 <- quantile(x, probs = 0.1, ...),
#     p25 <- quantile(x, probs = 0.25, ...),
#     median=median(x, ...),
#     p75 <- quantile(x, probs = 0.75, ...),
#     p90 <- quantile(x, probs = 0.9, ...),
#     max=max(x,...))
# }
# 
# 
# stats_desc <- get_file("MobiCount/home_detection/Stats_Desc_Sept07.csv",
#                        "etgtk6")
# stats_desc <- read_csv(stats_desc, 
#                        col_names = c("user_id", "avg_daily_events", "nb_dist_days"))
# 
# 
# 
# table_sd <- apply(stats_desc[,c("avg_daily_events", "nb_dist_days")], 2, summary_fun)
# table_sd <- t(table_sd)
# table_sd <- round(table_sd, 1)
# 
# 
# stats_desc_19h_09h <- get_file("MobiCount/home_detection/Stats_Desc_Sept07_19h_09h.csv",
#                                "etgtk6")
# stats_desc_19h_09h <- read_csv(stats_desc_19_07, 
#                                col_names = c("user_id", "avg_daily_events", "nb_dist_days"))
# 
# stats_desc_19h_09h <- stats_desc %>%
#   select(-avg_daily_events, -nb_dist_days) %>%
#   left_join(stats_desc_19h_09h) %>%
#   mutate(avg_daily_events = replace_na(avg_daily_events, 0),
#          nb_dist_days = replace_na(nb_dist_days, 0))
# 
# table_sd_19h_09h <- apply(stats_desc_19h_09h[,c("avg_daily_events", "nb_dist_days")], 
#                           2, summary_fun)
# table_sd_19h_09h <- t(table_sd_19h_09h)
# table_sd_19h_09h <- round(table_sd_19h_09h, 1)
# rownames(table_sd_19h_09h) <- str_c(rownames(table_sd_19h_09h), "_19h_09h")
# 
# df_sd <- as.data.frame(rbind(table_sd, table_sd_19h_09h))
# 
# stargazer(df_sd, summary = F, rownames = F, digits = 1)

stats_activity <- get_file("MobiCount/home_detection/Stats_Desc_Sept07.csv",
                           "etgtk6")
stats_activity <- read_csv(stats_activity, 
                           col_names = c("user_id", 
                                         "avg_daily_events", 
                                         "nb_dist_days"))

stats_activity_19h_09h <- get_file("MobiCount/home_detection/Stats_Desc_Sept07_19h_09h.csv",
                                   "etgtk6")
stats_activity_19h_09h <- read_csv(stats_activity_19h_09h, 
                                   col_names = c("user_id", 
                                                 "avg_daily_events", 
                                                 "nb_dist_days"))

summary_fun <- function(x){
  q1 <- quantile(x, probs = 0.25, names = F)
  median <- median(x)
  q3 <- quantile(x, probs = 0.75, names = F)
  iqr <- q3 - q1
  
  lower_whisker <- min(x[x >= q1 - 1.5*iqr])
  upper_whisker <- max(x[x <= q3 + 1.5*iqr])
  
  res <- c("lower_whisker" = lower_whisker, "q1" = q1, "median" = median,
           "q3" = q3, "upper_whisker" = upper_whisker)
  return(res)
}

time_ranges <- c("Full day", "19PM-09AM")

# Boxplots of average daily events depending on time range

rbind(
  summary_fun(stats_activity$avg_daily_events),
  summary_fun(stats_activity_19h_09h$avg_daily_events)
) %>% 
  as_tibble() %>%
  mutate(time_range = factor(time_ranges, time_ranges)) %>%
  ggplot(aes(x = time_range, ymin = lower_whisker, lower = q1, 
                 middle = median, upper = q3, ymax = upper_whisker)) +
  geom_boxplot(stat = "identity", fill = "#EBEBEB", width = 0.5) + 
  theme_bw() +
  theme(panel.border = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line = element_line(colour = "black"),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 13, colour = "black"),
        axis.text.y = element_text(size = 13, colour = "black"))
  
# Boxplots of number of distinct days users appear depending on time range
  
rbind(
  summary_fun(stats_activity$nb_dist_days),
  summary_fun(stats_activity_19h_09h$nb_dist_days)
) %>% 
  as_tibble() %>%
  mutate(time_range = factor(time_ranges, time_ranges)) %>%
  ggplot(aes(x = time_range, ymin = lower_whisker, lower = q1, 
             middle = median, upper = q3, ymax = upper_whisker)) +
  geom_boxplot(stat = "identity", fill = "#EBEBEB", width = 0.5) + 
  theme_bw() +
  theme(panel.border = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line = element_line(colour = "black"),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 13, colour = "black"),
        axis.text.y = element_text(size = 13, colour = "black"))



# Descriptive statistics of spatial units ---------------------------------

inter_grid_voro <- get_file("MobiCount/grids/grid_inter_voronoi_france.csv",
                            "etgtk6")
inter_grid_voro <- read_csv(inter_grid_voro)

inter_grid_voro <- inter_grid_voro %>% 
  separate("id", into = c("nidt", "grid_id"), sep = ":") %>%
  filter(grid_id %in% grid_dep$grid_id)

# Number of voronois

length(unique(inter_grid_voro$nidt))

# Number of tiles

length(unique(inter_grid_voro$nidt))

# Number of tiles per voronoi

tile_per_voronoi <- inter_grid_voro %>%
  group_by(nidt) %>%
  summarise(n_tile = n()) %>%
  select(n_tile)

sd_tile_per_voronoi <- summary_fun(tile_per_voronoi$n_tile)

# Number of voronois per tile

voronoi_per_tile <- inter_grid_voro %>%
  group_by(grid_id) %>%
  summarise(n_voronoi = n()) %>%
  select(n_voronoi)

sd_voronoi_per_tile <- summary_fun(voronoi_per_tile $n_voronoi)

sd_spatial_units <- as.data.frame(rbind(sd_tile_per_voronoi, sd_voronoi_per_tile))

stargazer(sd_spatial_units, summary = F, rownames = F, digits = 0)






# Statistical comparison of distributions ----------------------------------

stats_compar <- function(data, var_estim, var_ref) {
  
  # Compute comparison statistics
  pearson <- round(cor(data[[var_estim]], data[[var_ref]]), 2)
  spearman <- round(cor(data[[var_estim]], data[[var_ref]], 
                                method = "spearman"), 2)
  rmse <- round(rmse(data[[var_estim]], data[[var_ref]]), 1)
  mae <- round(mae(data[[var_estim]], data[[var_ref]]), 1)
  
  # Return a vector of statistics
  prior <- ifelse(var_estim == "n_uniform", "Uniform", "BD Topo")
  stats <- c(prior, pearson, spearman, rmse, mae)
  names(stats) <- c("Prior", "Pearson", "Spearman", "RMSE", "MAE")
  return(stats)
}

# Statistical comparison of PHD and validation data

compar_uniform <- stats_compar(df_compar, "n_uniform", "n_rfl")
compar_bdtopo <- stats_compar(df_compar, "n_bdtopo", "n_rfl")
stats_mat <- rbind(compar_uniform, compar_bdtopo)
stargazer(stats_mat, summary = F, rownames = F)

# Comparison of scatterplots

spec_pal <- rev(RColorBrewer::brewer.pal(11, "Spectral"))

ggplot(data = df_compar, aes(x = n_uniform, y = n_rfl)) + 
  scale_fill_gradientn(colours = spec_pal, trans='log10', labels = comma) +
  stat_binhex(bins=180) +
  # geom_smooth(method = 'lm', formula = y~x, 
  #             colour = "#585858", size = 0.5, linetype = "dashed") +
  xlab("Tile population (PHD with uniform prior)") +
  ylab("Tile population (tax data)") +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))  +
  theme_bw() 

ggplot(data = df_compar, aes(x = n_bdtopo, y = n_rfl)) + 
  scale_fill_gradientn(colours = spec_pal, trans='log10', labels = comma) +
  stat_binhex(bins=180) +
  # geom_smooth(method = 'lm', formula = y~x, 
  #             colour = "#585858", size = 0.5, linetype = "dashed") +
  xlab("Tile population (PHD with bdtopo prior)") +
  ylab("Tile population (tax data)") +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))  +
  theme_bw() 




# Comparison bdtopo/no bdtopo : 
# - cells wrongly declared as empty by HD
# - cells rightly declared as empty by HD

nrow(df_compar[df_compar$n_uniform == 0 & df_compar$n_rfl == 0,]) /
  nrow(df_compar)
nrow(df_compar[df_compar$n_bdtopo == 0 & df_compar$n_rfl == 0,]) /
  nrow(df_compar)
# In 2% of cells, no people with uniform prior as well as with RFL data
# In 50% of cells, no people with bdtopo as well as with RFL data
# => inclusion of bdtopo increases the amount of empty cells detected

seuils <- c(1:100)

wrongly_empty_uniform <- sapply(seuils, function(x) {
  return(sum(df_compar$n_uniform == 0 & near(df_compar$n_rfl, x, 0.51)) / nrow(df_compar))
})
wrongly_empty_bdtopo <- sapply(seuils, function(x) {
  return(sum(df_compar$n_bdtopo == 0 & near(df_compar$n_rfl, x, 0.51)) / nrow(df_compar))
})
wrongly_empty_df <- tibble(seuils,
                           "uniform" = wrongly_empty_uniform,
                           "bdtopo" = wrongly_empty_bdtopo) %>%
  gather(type, n, uniform:bdtopo)

wrongly_empty_df %>%
  ggplot(aes(x = seuils, y = n, color = type)) +
  geom_line(size = 1) + 
  scale_colour_manual(values=c("red", "blue"), name="Prior") +
  scale_x_continuous(limits = c(1,100)) +
  scale_y_continuous(limits = c(0, 0.05), breaks = seq(0, 0.05, 0.01)) +
  xlab("Tile population") +
  ylab("Number of tiles") +
  labs(subtitle = paste0("Number of total tiles : ", nrow(df_compar))) +
  theme_bw() +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))

# ggsave("compar_empty_PHD.jpg",
#        path = "~",
#        device = "jpg")


wrongly_populated_uniform <- sapply(seuils, function(x) {
  return(sum(df_compar$n_uniform == x & near(df_compar$n_rfl, 0, 0.51)) / nrow(df_compar))
})
wrongly_populated_bdtopo <- sapply(seuils, function(x) {
  return(sum(df_compar$n_bdtopo == x & near(df_compar$n_rfl, 0, 0.51)) / nrow(df_compar))
})
wrongly_populated_df <- tibble(seuils,
                               "uniform" = wrongly_populated_uniform,
                               "bdtopo" = wrongly_populated_bdtopo) %>%
  gather(type, n, uniform:bdtopo)


wrongly_populated_df %>%
  ggplot(aes(x = seuils, y = n, color = type)) +
  geom_line(size = 1) + 
  scale_x_continuous(limits = c(1,100)) +
  scale_y_continuous(limits = c(0, 0.05), breaks = c(0,0.01,0.02,0.03,0.04,0.05)) +
  scale_colour_manual(values=c("red", "blue"), name="Prior") +
  xlab("Tile population") +
  ylab("Number of tiles") +
  labs(subtitle = paste0("Number of total tiles : ", nrow(df_compar))) +
  theme_bw() +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))

# ggsave("compar_overpop_PHD.jpg",
#        path = "~",
#        device = "jpg")

# Compare densities

df_compar %>%
  select(-n_rfl_phd, -bdt_rfl, -uni_rfl) %>%
  gather(source, "n", n_bdtopo:n_rfl) %>%
  filter(n >= 1) %>%
  mutate(log_n = log(n)) %>%
  ggplot(aes(x = log_n, colour = source, fill = source)) +
  scale_colour_manual(values=c("red", "green", "blue")) +
  scale_fill_manual(values=c("red", "green", "blue")) +
  geom_density(alpha = 0.1) +
  theme_bw()

ggsave("compar_densities_PHD.jpg",
       path = "~",
       device = "jpg")





# Maps comparison : export of shapefiles for QGIS -------------------------

# Threshold for maps at France level
sort(as.vector(kmeans(log(df_compar$n_rfl[df_compar$n_rfl != 0]), 
                      4)$centers))



### Correlations at department level

# pop_dep <- read_csv("~/pop_dep_2011.csv", col_names = c("dep", "pop_2011"))
# 
# pop_dep <- pop_dep %>%
#   filter(!(dep %in% c("2A", "2B"))) %>%
#   mutate(dep = as.numeric(dep))

compar_diff_dep <- df_compar %>%
  group_by(dep) %>%
  summarise(rmse_uni_rfl = rmse(n_uniform, n_rfl),
            rmse_bdt_rfl = rmse(n_bdtopo, n_rfl),
            diff_rmse = (rmse_bdt_rfl - rmse_uni_rfl) / rmse_uni_rfl,
            mae_uni_rfl = mae(n_uniform, n_rfl),
            mae_bdt_rfl = mae(n_bdtopo, n_rfl),
            diff_mae = (mae_bdt_rfl - mae_uni_rfl) / mae_uni_rfl) %>%
  left_join(fr_dep) %>%
  select(dep, libelle, diff_rmse, diff_mae, geometry) %>%
  st_sf()


st_write(compar_diff_dep, "~/compar_diff_dep.shp")

# K-means to choose ranges on QGIS

sort(as.vector(kmeans(cor_dep$uni_rfl, 4)$centers))
sort(as.vector(kmeans(cor_dep$bdt_rfl, 4)$centers))

# Bbox to represent IDF on QGIS maps

fr_dep %>% filter(dep %in% c(75, 92, 93, 94)) %>% st_bbox()



### Zoom on specific areas

paris_lisse <- df_compar %>%
  filter(dep %in% c(75, 92, 93, 94)) %>%
  select(bdt_rfl, uni_rfl, x, y) %>%
  kernelSmoothing(sEPSG = 2154, 
                  iBandwidth = 2000, 
                  iCellSize = 100,
                  vQuantiles = NULL,
                  dfCentroids = NULL,
                  fUpdateProgress = NULL,
                  iNeighbor = NULL,
                  iNbObsMin = 250) %>%
  select(-x, -y) %T>%
  st_write("~/paris_lisse.shp")

meuse <- df_compar_geo %>%
  filter(dep == 55) %>%
  select(bdt_rfl, uni_rfl, geometry) %T>%
  st_write("~/meuse.shp")




# Autocorrelation analysis ------------------------------------------------

df_moran <- df_compar %>%
  separate(grid_id, c("x", "y")) %>%
  mutate(x = as.numeric(x) * 100, y = as.numeric(y) * 100)

# Computation of Moran's global I

cont_matrix <- function(x, y, tile_size) {
  
  x <- (x - min(x)) / tile_size + 1
  y <- (y - min(y)) / tile_size + 1
  
  id_row <- (x-1) * max(y) + y
  
  liste_ind <- list()
  for (xx in c(-1,0,1))
  {
    for (yy in c(-1,0,1))
    {
      xb <- x + xx
      yb <- y + yy
      id_col <- (xb-1) * max(y) + yb
      valable = (xb>0) & (xb<=max(x)) & (yb>0) & (yb<=max(y))
      liste_ind[[length(liste_ind)+1]] <- data.frame(id_row[valable],id_col[valable])
    }
  }
  index = do.call(rbind, liste_ind)
  cont_mat <- sparseMatrix(index[,1], index[,2], x = 1) 
  diag(cont_mat) <- 0
  cont_mat <- drop0(cont_mat)
  return(cont_mat)
}

glob_moran <- function(var, x, y, tile_size, h0 = "randomisation") {
  
  n <- length(var)
  
  # Compute contiguity matrix and keep only ones indices
  cont_mat <- cont_matrix(x = x, y = y, tile_size = tile_size)
  ind_ones <- summary(cont_mat) %>% 
    as_tibble() %>%
    mutate(i = as.character(i), j = as.character(j)) %>%
    select(-x)
  
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
  
  # Compute Moran's I
  num1 <- length(var_centered)
  denom1 <- sum(cont_mat)
  num2 <- sum(
    map_dbl(ind_ones$i, function(id) {
      if (!is.null(hash_env[[id]])) {
        return(hash_env[[id]])
      } else {return(NA)}
    }) * 
      map_dbl(ind_ones$j, function(id) {
        if (!is.null(hash_env[[id]])) {
          return(hash_env[[id]])
        } else {return(NA)}
      }),
    na.rm = T)
  denom2 <- sum((var_centered - mean(var_centered))^2)
  I <- (num1 / denom1) * (num2 / denom2)
  
  # Compute Moran's I expectation
  expec <- -1 / (n - 1)
  
  # Compute Moran's I variance
  # Source : http://www.lpc.uottawa.ca/publications/moransi/moran.htm
  s0 <- sum(cont_mat)
  s1 <- 2 * s0
  s2 <- 4 * sum(rowSums(cont_mat^2))
  
  if (h0 == "randomisation"){
    k <- (sum(var_centered^4) / n) / (sum(var_centered^2) / n)^2
    var <- (n*((n^2 - 3*n + 3)*s1 - n*s2 + 3*s0^2) - k*((n^2-n)*s1 - 2*n*s2 + 6*s0^2)) /
    ((n-1)*(n-2)*(n-2)*s0^2) - expec^2
    
  } else if (h0 == "normality") {
      var <- (1/(s0^2*(n^2-1))*(n^2*s1 - 2*s2 + 3*s0^2)) - expec^2
  } else {
    stop("Wrong input for h0 parameters (must be either 'randomisation' or 'normality').")
      }

  # Compute z-score and do one sided test (greater side)
  z <- (I - expec) / sqrt(var)
  p_val <- pnorm(z, lower.tail = FALSE)
  
  return(list("I" = I, "p-value" = p_val))
}

moran_glob_res <- map(c("n_uniform", "n_bdtopo", "n_rfl", "uni_rfl", "bdt_rfl"),
                      ~glob_moran(var = df_moran[[.]], x = df_moran$x, 
                                  y = df_moran$y, tile_size = 500))

moran_glob_df <- tibble(
  "Variable" = c("PHD with uniform prior", "PHD with land use prior",
                 "Local tax data", "Estimation error with uniform prior",
                 "Estimation error with land use prior"),
  "Moran's I" = round(map_dbl(moran_glob_res, ~.$I), 2),
  "p-value" = round(map_dbl(moran_glob_res, ~.$`p-value`), 2)
)

stargazer(moran_glob_df, summary = F, rownames = F)


# Scale analysis ----------------------------------------------------------

entropy <- function(x, alpha = 1, base = 2) {
  if (alpha == 1) {
    return(-sum(x * log(x, base = base)))
  } else {
    return((1 / (1 - alpha)) * log(sum(x^(alpha)), base = base))
  }
}


compar_diff_scales <- function(df_compar, 
                               scales = (c(0.5, 1, 2, 4, 8, 16) * 1000), 
                               alpha = 1, 
                               base_log = 2) {
  
  df_scale <- df_compar %>%
    select(x, y, n_uniform, n_bdtopo, n_rfl)
  
  I_uniform <- rep(0, length(scales))
  I_bdtopo <- rep(0, length(scales))
  I_rfl <- rep(0, length(scales))
  
  D_uniform <- rep(0, length(scales)-1)
  D_bdtopo <- rep(0, length(scales)-1)
  D_rfl <- rep(0, length(scales)-1)
  
  for (i in 1:length(scales)) {
    
    agg_i <- df_scale %>% 
      mutate(x_i = round(x / scales[i]) * scales[i],
             y_i = round(y / scales[i]) * scales[i]) %>%
      group_by(x_i, y_i) %>%
      summarise(n_uniform = sum(n_uniform),
                n_bdtopo = sum(n_bdtopo),
                n_rfl = sum(n_rfl)) %>%
      ungroup() %>%
      mutate(n_uniform = n_uniform / sum(.$n_uniform),
             n_bdtopo = n_bdtopo / sum(.$n_bdtopo),
             n_rfl = n_rfl / sum(.$n_rfl)) %>%
      replace(. == 0, 1)
    
    I_uniform[i] <- entropy(agg_i$n_uniform, alpha = alpha)
    I_bdtopo[i] <- entropy(agg_i$n_bdtopo, alpha = alpha)
    I_rfl[i] <- entropy(agg_i$n_rfl, alpha = alpha)
    
    if (i != 1) {
      D_uniform[i-1] <- - (I_uniform[i] - I_uniform[i-1])
      D_bdtopo[i-1] <- - (I_bdtopo[i] - I_bdtopo[i-1])
      D_rfl[i-1] <- - (I_rfl[i] - I_rfl[i-1])
    }
  }
  
  D_levels <- c("I1000-I500", "I2000-I1000","I4000-I2000",
                "I8000-I4000", "I16000-I8000")
  
  D_compar <- tibble(D_uniform, D_bdtopo, D_rfl, D_range = D_levels) %>%
    gather(key = "data", value = "D", -D_range)
  
  D_plot <- D_compar %>%
    mutate(D_range = fct_relevel(D_range, D_levels),
           data = fct_recode(data, 
                             "PHD with uniform prior" = "D_uniform",
                             "PHD with bdtopo prior" = "D_bdtopo",
                             "Tax data" = "D_rfl"),
           data = fct_relevel(data, 
                              "PHD with uniform prior",
                              "PHD with bdtopo prior",
                              "Tax data")) %>%
    ggplot(aes(x = D_range, y = D, group = data)) +
    geom_line(aes(color = data), size = 1) +
    scale_colour_manual(values=c("blue", "red", "#1a710e"), name="Data") +
    ylab("Entropy difference between subsequent dimensions") +
    theme_bw() +
    theme(legend.title = element_text(size = 14),
          legend.text = element_text(size = 13),
          axis.title.x = element_blank(),
          axis.text.x = element_text(size = 12),
          axis.text.y = element_text(size = 12),
          axis.title.y = element_text(size = 14))
  
  return(D_plot)
  
}

compar_diff_scales(df_compar = df_compar, 
                   scales = (c(0.5, 1, 2, 4, 8, 16) * 1000), 
                   alpha = 1, 
                   base_log = 2)





# Replication of statistical analysis at disaggregated level --------------

get_file_local("MobiCount/shp/duu10_francemetro.zip", "etgtk6",
               ext_dir = "~/uu_shp", uncompress = T)
uu <- st_read("~/uu_shp/duu10_francemetro.shp", crs = 2154,
                stringsAsFactors = FALSE)
uu_types <- read_csv("~/uu_shp/uu2010.csv")

uu <- uu %>% 
  left_join(uu_types, by = c("code" = "UU2010")) %>% 
  na.omit()

# See files at https://www.insee.fr/fr/information/2115018 for details 
# Variable : "Tranche d'unité urbaine 2016"
splits_uu <- list(8, c(6,7), c(3,4,5), c(1,2))

indices_inter_uu <- splits_uu %>%
  map(.f = function(x) {
    indices_inter <- uu %>%
      filter(TUU2016 %in% x) %>% 
      st_intersects(df_compar_geo) %>%
      unlist()
    return(indices_inter)
  })

names(indices_inter_uu) <- str_c("uu", map_chr(splits_uu, ~str_c(., collapse = "_")), sep = "_")

indices_inter_uu[["not_in_uu"]] <- as.numeric(rownames(df_compar)[!(rownames(df_compar) 
                                                                 %in% unlist(indices_inter_uu))])

stats_uu <- names(indices_inter_uu) %>% 
  map(.f = function(x) {
    df_compar_sub <- df_compar[indices_inter_uu[[x]],]
    
    compar_sub_uniform <- stats_compar(df_compar_sub, "n_uniform", "n_rfl")
    compar_sub_uniform <- c("Type_UU" = x, compar_sub_uniform)
    compar_sub_bdtopo <- stats_compar(df_compar_sub, "n_bdtopo", "n_rfl")
    compar_sub_bdtopo <- c("Type_UU" = x, compar_sub_bdtopo)
    compar_sub_stats <- rbind(compar_sub_uniform, compar_sub_bdtopo)
    
    return(compar_sub_stats)
  })

stats_uu <- stats_uu %>% 
  reduce(rbind) %>%
  as_tibble() %>%
  mutate_at(c("Pearson", "Spearman", "RMSE", "MAE"), as.numeric)



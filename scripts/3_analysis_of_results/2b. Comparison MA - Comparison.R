
# -----------------------------------------------------------------------------
# --- Comparison Home Detection with/without prior - Comparisons and diagnostic
# --- Heuristic : Max activity with time restriction (7PM-7AM)
# --- Month : September 2007
# --- Area : Paris urban unit
# -----------------------------------------------------------------------------


# Libraries declaration -------------------------------------------------------

#install.packages(c("cartography", "lsa"))

library(sf) 
library(tidyverse)
library(data.table)
library(leaflet)
library(lsa)
library(RColorBrewer)
library(htmlwidgets)

# Functions declaration -------------------------------------------------------

source("0. Modification functions mobunit & filophone.R")
source("0. New functions.R")


df_compar <- read_csv("tables/counts/MA_compar_sept_Paris.csv")



# Comparison with counts at tiles level (MA) --------------------------------

# Compare correlations

cor(df_compar$n_noprior, df_compar$n_filo)
cor(df_compar$n_prior, df_compar$n_filo)
cor(df_compar$n_noprior, df_compar$n_prior)

cosine(df_compar$n_noprior, df_compar$n_filo)
cosine(df_compar$n_prior, df_compar$n_filo)
cosine(df_compar$n_noprior, df_compar$n_prior)




# Comparison with counts at voronoi level (DD) --------------------------------

# Import grid

grid <- st_read("tables/grids/grid_500_UUParis.shp", crs = 2154)
gridBB <- st_bbox(grid)
gridBB_sf <- st_as_sfc(gridBB)

# Import France voronois shapefile
voronoi_shp <- st_read('inputs/shp/voronoi/Antenne_voronoi_rev.mif', crs = 2154, 
                       stringsAsFactors = F)
voronoi_shp <- voronoi_shp %>%
  select(NIDT, geometry) %>%
  rename(nidt = NIDT)

# Import RFL 2011 counts at voronoi level using distinct days heuristic
voronoi_dd <- read_csv('tables/counts/comp_voro.csv')
voronoi_dd <- voronoi_dd %>%
  select(nidt, m9_pop_mv_dd, pop_insee)

# Restricting to voronois within Paris Urban unit
voronoi_dd_parisUU <- voronoi_dd %>%
  left_join(voronoi_shp, by = "nidt") %>%
  st_as_sf(sf_column_name = "geometry") %>%
  st_intersection(grid) %>%
  group_by(nidt) %>%
  summarise(n_cells = n()) %>%
  select(nidt) %>%
  left_join(voronoi_dd)

# Computing correlations between RFL counts and mobile counts
cor(voronoi_dd_parisUU$m9_pop_mv_dd, voronoi_dd_parisUU$pop_insee)
cosine(voronoi_dd_parisUU$m9_pop_mv_dd, voronoi_dd_parisUU$pop_insee)




# Diagnostics -----------------------------------------------------------------

# Comparison prior/no prior : cells wrongly declared as empty by HD

c1_seuils <- c(1:3000)
c1_noprior <- sapply(c1_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_noprior == 0 & df_compar$n_filo >= x,]))
})

c1_prior <- sapply(c1_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_prior == 0 & df_compar$n_filo >= x,]))
})

data.frame(c1_seuils, 
           "no_prior" = c1_noprior, 
           "prior" = c1_prior) %>%
  gather(type, n, no_prior:prior) %>%
  ggplot(aes(x = c1_seuils, y = n, color = type)) +
  geom_line(size = 2) + scale_x_continuous(limits = c(1,3000),
                                           breaks = c(1, seq(1000, 3000, 1000))) +
  xlab("x") +
  labs(title = "Number of tiles such that n_tiles_HD = 0 & n_tiles_RFL >= x",
       subtitle = "Number of total tiles : 12180") + 
  theme_bw()

ggsave("compar_empty_HD.jpg", 
       path = "Output/Comparison prior-noprior/Plots comparison", 
       device = "jpg")

# data.frame(c1_seuils, 
#            "no_prior" = c1_noprior / nrow(df_compar), 
#            "prior" = c1_prior / nrow(df_compar)) %>%
#   gather(type, freq, no_prior:prior) %>%
#   ggplot(aes(x = c1_seuils, y = freq, color = type)) +
#   geom_line(size = 2) + xlab("x") +
#   labs(title = "P(n_cell_HD = 0 & n_cell_RFL >= x)") + 
#   theme_bw()




# Comparison prior/no prior : cells rightly declared as empty by HD

nrow(df_compar[df_compar$n_noprior == 0 & df_compar$n_filo == 0,]) / 
  nrow(df_compar)
nrow(df_compar[df_compar$n_prior == 0 & df_compar$n_filo == 0,]) / 
  nrow(df_compar)
# In 17% of cells, no people with no prior as well as with RFL data
# In 28% of cells, no people with prior as well as with RFL data
# => inclusion of prior increases the amount of empty cells detected

c2_seuils <- c(1:1000)
c2_noprior <- sapply(c2_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_noprior >= x & df_compar$n_filo == 0,]))
})

c2_prior <- sapply(c2_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_prior >= x & df_compar$n_filo == 0,]))
})

data.frame(c2_seuils, 
           "no_prior" = c2_noprior, 
           "prior" = c2_prior) %>%
  gather(type, n, no_prior:prior) %>%
  ggplot(aes(x = c2_seuils, y = n, color = type)) +
  geom_line(size = 2) + scale_x_continuous(limits = c(1,1000),
                                           breaks = c(1, 250, 500, 750, 1000)) +
  xlab("x") +
  labs(title = "Number of tiles such that n_tiles_HD >= x & n_tiles_RFL = 0",
       subtitle = "Number of total tiles : 12180") + 
  theme_bw()

ggsave("compar_overpop_HD.jpg", 
       path = "Output/Comparison prior-noprior/Plots comparison", 
       device = "jpg")



## Distribution of gaps between home detection with and without prior

# Import home detection results, with and without prior

df_HD_noprior <- read_csv("Tables/Counts/MA_noprior_sept_Paris.csv", 
                          col_names = c("month", "CodeId", "grid_id"))

df_HD_noprior <- select(df_HD_noprior, c("CodeId", "grid_id"))

df_HD_prior <- read_csv("Tables/Counts/MA_prior_sept_Paris.csv", 
                        col_names = c("month", "CodeId", "grid_id"))

df_HD_prior <- select(df_HD_prior, c("CodeId", "grid_id"))

# Compare distributions

grid_centro <- grid %>%
  st_centroid() %>%
  sfc_as_cols() %>%
  st_set_geometry(NULL)

df_compar_dist <- df_HD_noprior %>%
  rename(grid_id_noprior = grid_id) %>%
  full_join(df_HD_prior) %>%
  rename(grid_id_prior = grid_id) %>%
  right_join(grid_centro, by = c("grid_id_prior" = "grid_id")) %>%
  na.omit() %>%
  rename(x_prior = x, y_prior = y) %>%
  right_join(grid_centro, by = c("grid_id_noprior" = "grid_id")) %>%
  na.omit() %>%
  rename(x_noprior = x, y_noprior = y) %>%
  mutate(diff_x = x_prior - x_noprior,
         diff_y = y_prior - y_noprior,
         dist = sqrt(diff_x^2 + diff_y^2) / 1000)

df_compar_dist %>% ggplot(aes(x=dist)) + 
  geom_histogram(colour="black", fill="white", binwidth = 0.05) +
  scale_x_continuous(limits = c(NA,10), breaks = seq(0, 10, 0.5)) +
  labs(title="Distribution of distances between home detection tiles with and without prior",
       x ="Distance (km)", y = "Frequency")

ggsave("distrib_dist_HD.jpg", 
       path = "Output/Comparison prior-noprior/Plots comparison", 
       device = "jpg")

q <- seq(1:9) / 10
q <- c(q, 0.99)
quantile(df_compar_dist$dist, probs = q)

max(df_compar_dist$dist)





# Highlighting tiles with important gap between prior/no prior HD

df_compar_diff = df_compar %>% 
  mutate(diff_noprior_filo = n_noprior - n_filo,
         diff_prior_filo = n_prior - n_filo,
         sign_gap = sign(diff_noprior_filo) * sign(diff_prior_filo)) %>%
  filter(sign_gap == -1) %>%
  select(-sign_gap) %>%
  mutate(prior_sup = ifelse(diff_prior_filo > 0, 1, 0))

df_compar_diff_geo = df_compar_diff %>%
  left_join(grid) %>%
  mutate(prior_sup = as.factor(prior_sup)) %>%
  st_as_sf(sf_column_name = "geometry") %>%
  st_transform(crs = 4326)

factpal <- colorFactor(topo.colors(2), df_compar_diff_geo$prior_sup)

df_compar_diff_geo %>%
  leaflet() %>%
  addTiles() %>%
  setView(lng = 2.333333, lat = 48.866667, zoom = 10) %>%
  addPolygons(data = df_compar_diff_geo, 
              color = ~factpal(prior_sup))




## Maps to compare counts : RFL, prior, no prior

df_compar_geo = df_compar %>%
  left_join(grid) %>%
  st_as_sf(sf_column_name = "geometry") %>%
  st_transform(crs = 4326)

df_plot_filo <- df_compar_geo %>%
  filter(n_filo != 0) %>%
  mutate(log_n_filo = log(n_filo))

pal <- colorBin("Purples", df_plot_filo$log_n_filo, 6, pretty = FALSE)

# Map of RFL counts

df_plot_filo <- df_compar_geo %>%
  filter(n_filo != 0) %>%
  mutate(log_n_filo = log(n_filo))

pal_filo <- colorBin("Purples", df_plot_filo$log_n_filo, 6, pretty = FALSE)

voronoi_dd_parisUU <- voronoi_dd_parisUU %>% st_transform(crs = 4326)

df_plot_filo %>%
  leaflet() %>%
  addTiles() %>%
  setView(lng = 2.333333, lat = 48.866667, zoom = 10) %>%
  addPolygons(color = ~pal_filo(log_n_filo), fillOpacity = 0.6, opacity = 1) %>%
  addPolygons(data = voronoi_dd_parisUU, stroke = TRUE, fillOpacity = 0, 
              weight = 0.2, smoothFactor = 0.5, color = "black", opacity = 1) %>%
  addLegend("bottomright", pal = pal_filo, values = ~log_n_filo,
            title = "Resident population (log scale)",
            opacity = 1) %>%
  saveWidget("~/mobicount/Output/Comparison prior-noprior/HTML counts maps/counts_filo.html",
             selfcontained = TRUE)

# Map of HD counts without prior

df_plot_noprior <- df_compar_geo %>%
  filter(n_noprior != 0) %>%
  mutate(log_n_noprior = log(n_noprior))

pal_noprior <- colorBin("Purples", df_plot_noprior$log_n_noprior, 6, pretty = FALSE)

df_plot_noprior %>%
  leaflet() %>%
  addTiles() %>%
  setView(lng = 2.333333, lat = 48.866667, zoom = 10) %>%
  addPolygons(color = ~pal_noprior(log_n_noprior), fillOpacity = 0.6, opacity = 0.6) %>%
  addPolygons(data = voronoi_dd_parisUU, stroke = TRUE, fillOpacity = 0, 
              weight = 0.2, smoothFactor = 0.5, color = "black", opacity = 1) %>%
  addLegend("bottomright", pal = pal_noprior, values = ~log_n_noprior,
            title = "Home detection without prior (log scale)",
            opacity = 1) %>%
  saveWidget("~/mobicount/Output/Comparison prior-noprior/HTML counts maps/counts_noprior.html",
             selfcontained = TRUE)

# Map of HD counts with prior

df_plot_prior <- df_compar_geo %>%
  filter(n_prior != 0) %>%
  mutate(log_n_prior = log(n_prior))

pal_prior <- colorBin("Purples", df_plot_prior$log_n_prior, 6, pretty = FALSE)
attr(pal_prior, which = "colorArgs")$bins <- attr(pal_noprior, which = "colorArgs")$bins

df_plot_prior %>%
  leaflet() %>%
  addTiles() %>%
  setView(lng = 2.333333, lat = 48.866667, zoom = 10) %>%
  addPolygons(color = ~pal_prior(log_n_prior), fillOpacity = 0.6, opacity = 0.6) %>%
  addPolygons(data = voronoi_dd_parisUU, stroke = TRUE, fillOpacity = 0, 
              weight = 0.2, smoothFactor = 0.5, color = "black", opacity = 1) %>%
  addLegend("bottomright", pal = pal_prior, values = ~log_n_prior,
            title = "Home detection with prior (log scale)",
            opacity = 1) %>%
  saveWidget(file="~/mobicount/Output/Comparison prior-noprior/HTML counts maps/counts_prior.html",
             selfcontained = TRUE)



grid_nogeo <- grid %>% st_set_geometry(NULL)
test <- df_HD_noprior %>% right_join(grid_nogeo) %>% na.omit()













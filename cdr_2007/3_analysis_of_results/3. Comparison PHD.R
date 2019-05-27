
# -----------------------------------------------------------------------------
# --- Comparison PHD with different priors
# --- Time restriction (7PM-7AM)
# --- Month : September 2007
# --- Area : France
# -----------------------------------------------------------------------------

install.packages(c("lsa"))

install.packages("~/mobicount-project/minior-master.tar.gz", repos = NULL)

library(tidyverse)
library(minior)
library(sf)
library(lsa)


set_minio_shared()


# Build comparison table ------------------------------------------------------

# Import PHD with different bdtopos

phd_uniform <- get_file("MobiCount/home_detection/PHD_uniform_france_09.csv",
                        "etgtk6")
phd_uniform <- read_csv(phd_uniform, col_names = c("grid_id", "n_uniform"))

phd_bdtopo <- get_file("MobiCount/home_detection/PHD_bdtopo_france_09.csv",
                       "etgtk6")
phd_bdtopo <- read_csv(phd_bdtopo, col_names = c("grid_id", "n_bdtopo"))

phd_rfl <- get_file("MobiCount/home_detection/PHD_rfl_france_09.csv",
                        "etgtk6")
phd_rfl <- read_csv(phd_rfl, col_names = c("grid_id", "n_rfl_phd"))

# Import RFL benchmark

rfl <- read_csv("~/rfl_counts.csv")
rfl <- rfl %>% rename(n_rfl = nbpersm)

# Import grid

get_file_local("MobiCount/grids/grid_500_france.zip", "etgtk6",
               ext_dir = "~/grid", uncompress = T)
grid <- st_read("~/grid/grid_500_france.shp")

grid_ng <- grid %>%
  st_set_geometry(NULL) %>%
  as_tibble() %>%
  separate(grid_id, c("x", "y"), sep = "_", remove = F) %>%
  mutate(x = as.numeric(x), y = as.numeric(y))

# Merge tables

df_compar <- grid_ng %>%
  as_tibble() %>%
  left_join(phd_bdtopo) %>%
  left_join(phd_uniform) %>%
  left_join(phd_rfl) %>%
  left_join(rfl) %>%
  replace(is.na(.), 0) %>%
  mutate_at(.vars = c("n_bdtopo", "n_uniform", "n_rfl_phd"), .funs = round)
  






# Compare correlations --------------------------------------------------------

cor(df_compar$n_uniform, df_compar$n_rfl)
cor(df_compar$n_bdtopo, df_compar$n_rfl)
cor(df_compar$n_uniform, df_compar$n_bdtopo)
# cor(df_compar$n_rfl_phd, df_compar$n_rfl)
# cor(df_compar$n_rfl_phd, df_compar$n_bdtopo)
# cor(df_compar$n_uniform, df_compar$n_bdtopo)
# cor(df_compar$n_uniform, df_compar$n_rfl_phd)

cosine(df_compar$n_uniform, df_compar$n_rfl)
cosine(df_compar$n_bdtopo, df_compar$n_rfl)
cosine(df_compar$n_uniform, df_compar$n_bdtopo)





# Diagnostics -----------------------------------------------------------------



# Comparison bdtopo/no bdtopo : cells wrongly declared as empty by HD

c1_seuils <- c(1:50)
c1_uniform <- sapply(c1_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_uniform == 0 & df_compar$n_rfl == x,]))
})

c1_bdtopo <- sapply(c1_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_bdtopo == 0 & df_compar$n_rfl == x,]))
})

data.frame(c1_seuils,
           "uniform" = c1_uniform,
           "bdtopo" = c1_bdtopo) %>%
  gather(type, n, uniform:bdtopo) %>%
  ggplot(aes(x = c1_seuils, y = n, color = type)) +
  geom_line(size = 1) + scale_x_continuous(limits = c(1,50)) +
  scale_colour_manual(values=c("red", "blue")) +
  xlab("Tile population") +
  ylab("Number of tiles wrongly declared as empty") +
  labs(subtitle = paste0("Number of total tiles : ", nrow(grid_ng))) +
  theme_bw()

ggsave("compar_empty_PHD.jpg",
       path = "~",
       device = "jpg")



# Comparison bdtopo/no bdtopo : cells rightly declared as empty by HD

nrow(df_compar[df_compar$n_uniform == 0 & df_compar$n_rfl == 0,]) /
  nrow(df_compar)
nrow(df_compar[df_compar$n_bdtopo == 0 & df_compar$n_rfl == 0,]) /
  nrow(df_compar)
# In 0.01% of cells, no people with uniform prior as well as with RFL data
# In 47% of cells, no people with bdtopo as well as with RFL data
# => inclusion of bdtopo increases the amount of empty cells detected

c2_seuils <- c(1:50)
c2_uniform <- sapply(c2_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_uniform == x & df_compar$n_rfl == 0,]))
})

c2_bdtopo <- sapply(c2_seuils, function(x) {
  return(nrow(df_compar[df_compar$n_bdtopo == x & df_compar$n_rfl == 0,]))
})

data.frame(c2_seuils,
           "uniform" = c2_uniform,
           "bdtopo" = c2_bdtopo) %>%
  gather(type, n, uniform:bdtopo) %>%
  ggplot(aes(x = c2_seuils, y = n, color = type)) +
  geom_line(size = 1) + scale_x_continuous(limits = c(1,50)) +
  scale_colour_manual(values=c("red", "blue")) +
  xlab("Tile population") +
  ylab("Number of tiles wrongly declared as inhabited") +
  labs(subtitle = paste0("Number of total tiles : ", nrow(grid_ng))) +
  theme_bw()

ggsave("compar_overpop_PHD.jpg",
       path = "~",
       device = "jpg")

# Compare densities

df_compar %>%
  select(-n_rfl_phd) %>%
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


# Maps at France level --------------------------------------------------------

# Import France departments shapefile















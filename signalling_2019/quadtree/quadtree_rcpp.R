
install.packages("Rcpp", "RcppArmadillo")

library(aws.s3)
library(httr)
library(tidyverse)
library(sf)
library(Rcpp)
library(RcppArmadillo)
library(microbenchmark)

set_config(config(ssl_verifypeer = 0L))
Sys.setenv("AWS_ACCESS_KEY_ID" = "minio",
           "AWS_SECRET_ACCESS_KEY" = "minio123",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_S3_ENDPOINT" = "minio.alpha.innovation.insee.eu",
           "LIBCURL_BUILD"="winssl")

###########################
# Calcul ##
###########################
save_object(object = "points_bdtopo_2012.Rdata", bucket = "h529p3",
            file="/home/rstudio/my-notebooks/points_bdtopo_2012.Rdata")



load("/home/rstudio/my-notebooks/points_bdtopo_2012.Rdata")
df=coord_france_indif_indus_remar

size_init <- 200

### base_ag
df['var'] <- 1
df$x <- floor(df$x/size_init) * size_init 
df$y <- floor(df$y/size_init) * size_init  
df_ag <- df %>% group_by(x, y) %>% summarise(var=sum(var))

### creation_matrice carrée dont la taille est une puissance 
df_ag$i <- (df_ag$x - min(df_ag$x)) / size_init + 1 
df_ag$j <- (df_ag$y - min(df_ag$y)) / size_init + 1 
shape <- 2^(ceiling(log2(max(df_ag$i)))) # also equal to 2^(ceiling(log2(max(df_ag$j))))

matrice_france <- matrix(0, shape, shape, 
                         dimnames = list(1:shape, 1:shape)) 
matrice_france[cbind(df_ag$i, df_ag$j)] <- df_ag$var

mat_init <- matrice_france

# For test :
# ind_start <- 2001
# ind_end <- 3024
# long_cote <- (ind_end - ind_start + 1)
# n_levels <- long_cote / 2
# mat_init = matrice_france[ind_start:ind_end,ind_start:ind_end]
n_rows <- nrow(mat_init)
n_cols <- nrow(mat_init)

# Define shutoff parameter (minimum size per tile)
min_size <- 10000

# Quadtree
sourceCpp("R codes/2019/quadtree/quadtree.cpp")

add_i <- add_cpp[[1]]
add_j <- add_cpp[[2]]

flat_mat = tibble(position = str_c(as.vector(add_i), as.vector(add_j), sep = "_"),
                  val = as.vector(mat_init),
                  i = as.numeric(rep(row.names(mat_init), ncol(mat_init))),
                  j = as.numeric(rep(colnames(mat_init),each = nrow(mat_init)))
)

grid_agg <- flat_mat %>%
  mutate(x = (i-1) * size_init + min(df_ag$x),
         y = (j-1) * size_init + min(df_ag$y)) %>%
  group_by(position) %>%
  summarise(val = sum(val),
            x_cent = (max(x) + min(x)) / 2,
            y_cent = (max(y) + min(y)) / 2,
            size = sqrt(n()) * size_init) %>%
  ungroup() %>%
  mutate(geometry = sprintf("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", 
                            x_cent - size/2, y_cent + size/2, x_cent + size/2, y_cent + size/2,
                            x_cent + size/2, y_cent - size/2, x_cent - size/2, y_cent - size/2,
                            x_cent - size/2, y_cent + size/2)) %>%
  st_as_sf(wkt = "geometry", crs = 2154)

plot(grid_agg$geometry)




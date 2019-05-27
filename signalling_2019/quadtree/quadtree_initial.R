library(aws.s3)
library(httr)
library(tidyverse)
library(sf)

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

ind_start <- 2001
ind_end <- 2512
long_cote <- (ind_end - ind_start + 1)
n_levels <- long_cote / 2
mat_init = matrice_france[ind_start:ind_end,ind_start:ind_end]

# mat_init[3,3] <- 11
# mat_init <- mat_init[2:5, 3:6]


# matrice est une grille avec le nb de batîments
# en sortie on obtient deux matrices (d'adresse binaire)
# add_i et add_j qui donnent les grands carreaux d'appartenance

taille_min = 10

add_i = add_j = add_level = matrix(0,nrow(mat_init), ncol(mat_init))

# mat_level <- mat_inter <- matrix(0,nrow(mat_init), ncol(mat_init))

quadtree <- function(min_i, max_i, min_j, max_j, level)
{
  cat(min_i,max_i,min_j,max_j,level,'\n')
  if ((max_i>min_i) & (max_j>min_j))
  {
    pivot_i = (max_i + min_i - 1) /2
    pivot_j = (max_j + min_j - 1) /2
    
    ag0 = sum(mat_init[min_i:pivot_i, min_j:pivot_j])
    ag1 = sum(mat_init[(pivot_i+1):max_i, min_j:pivot_j ])
    ag2 = sum(mat_init[min_i:pivot_i, (pivot_j+1):max_j ])
    ag3 = sum(mat_init[(pivot_i+1):max_i, (pivot_j+1):max_j ])
    
    if ((ag0 > taille_min) & (ag1 > taille_min) & 
        (ag2 > taille_min) & (ag3 > taille_min))
    {
      # mat_level <<- matrix(0,nrow(mat_init), ncol(mat_init))
      # mat_level[(pivot_i+1):max_i,min_j:pivot_j] <<- mat_level[(pivot_i+1):max_i,min_j:pivot_j] + 1
      # mat_level[min_i:pivot_i,(pivot_j+1):max_j] <<- mat_level[min_i:pivot_i,(pivot_j+1):max_j] + 2
      # mat_level[(pivot_i+1):max_i,(pivot_j+1):max_j] <<- mat_level[(pivot_i+1):max_i,(pivot_j+1):max_j] + 3
      # mat_inter[min_i:max_i, min_j:max_j] <<- str_c(mat_inter[min_i:max_i, min_j:max_j],
      #                                               mat_level[min_i:max_i, min_j:max_j])
      
      add_i[(pivot_i+1):max_i,min_j:max_j] <<- add_i[(pivot_i+1):max_i,min_j:max_j] + 2^(nrow(mat_init) -level-1)
      add_j[min_i:max_i,(pivot_j+1):max_j] <<- add_j[min_i:max_i,(pivot_j+1):max_j] + 2^(ncol(mat_init) -level-1)
      # add_level[min_i:max_i,min_j:max_j] <<- add_level[min_i:max_i,min_j:max_j] + 1
      
      quadtree(min_i,pivot_i,min_j,pivot_j,level+1)
      quadtree(min_i,pivot_i,pivot_j+1,max_j,level+1)
      quadtree(pivot_i+1,max_i,min_j,pivot_j,level+1)
      quadtree(pivot_i+1,max_i,pivot_j+1,max_j,level+1)
    }
  } 
}

quadtree(min_i = 1,
         max_i = nrow(mat_init),
         min_j = 1,
         max_j = ncol(mat_init),
         level = 0)


# par(mfrow=c(1,2))
# image(log(add_i),asp=1,axes=F)
# image(log(add_j),asp=1,axes=F)
# 
# image(-log(add_level),asp=1,axes=F)

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







grid_compar <- flat_mat %>%
  mutate(x = (i-1) * size_init + min(df_ag$x),
         y = (j-1) * size_init + min(df_ag$y),
         size = size_init,
         geometry = sprintf("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", 
                            x - size/2, y + size/2, x + size/2, y + size/2,
                            x + size/2, y - size/2, x - size/2, y - size/2,
                            x - size/2, y + size/2)) %>%
  st_as_sf(wkt = "geometry", crs = 2154)


### Comparison on the sample (using QGIS)

par(mfrow=c(1,2))
plot(grid_agg[,"size"])

df_ag_sub <- df_ag %>% 
  filter(between(i, ind_start, ind_end), between(j, ind_start, ind_end)) %>%
  mutate(geometry = sprintf("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", 
                            x - size_init/2, y + size_init/2, x + size_init/2, y + size_init/2,
                            x + size_init/2, y - size_init/2, x - size_init/2, y - size_init/2,
                            x - size_init/2, y + size_init/2)) %>%
  st_as_sf(wkt = "geometry", crs = 2154) %>%
  st_write("~/compar_quadtri.shp")
  


# 
# 
addi=df_add$add_i
addj=df_add$add_j
ii = rep(0,length(addi))
jj = rep(0,length(addi))

for (i in 0:(n_levels-1))
{
  ii[(addi %/% 2^i) == 1] <- 2^i + ii[(addi %/% 2^i) == 1]
  jj[(addj %/% 2^i) == 1] <- 2^i + jj[(addj %/% 2^i) == 1]
}
# 
# flat_mat$cote = 200 * 2^(log2(long_cote) - flat_mat$add_level)
# flat_mat$x = (ii - 1) * size_init + min(df_ag$x)
# flat_mat$y = (jj - 1) * size_init + min(df_ag$y)
# 
# flat_mat_ag = flat_mat %>% group_by(x,y,cote) %>% summarise(val=sum(val))
# 
# library(btb)
# grid = dfToGrid(flat_mat_ag,2154,iCellsize_init = flat_mat_ag$cote)
# plot(grid$geometry)



# -------------------------------------------------------------------------
# Example graphs for bayesian model of spatial mapping --------------------
# -------------------------------------------------------------------------


library(tidyverse)
library(sf)


# Declare functions -------------------------------------------------------

grid_intersection <- function(voronoi,grid, grid_var = "grid_id",
                              antenna_var = "nidt"){
  
  # Ensure we have sf objects
  if (!('sf' %in% class(voronoi))) voronoi <- voronoi %>% sf::st_as_sf()
  if (!('sf' %in% class(grid))) grid <- grid %>% sf::st_as_sf()
  
  # Put in Lambert 93 to get meter unit
  grid <- grid %>% sf::st_transform(2154)
  voronoi_sf <- voronoi %>% sf::st_transform(sf::st_crs(grid))

  # Compute intersection area (m^2)
  pi <- grid %>% sf::st_intersection(voronoi_sf)
  pi <- pi %>% dplyr::mutate(area = sf::st_area(.) %>% as.numeric())
  
  # Transform that as intersection area
  pi2 <- pi %>% dplyr::group_by(!!rlang::sym(antenna_var)) %>%
    dplyr::summarise(area2 = sum(.data$area)) %>%
    sf::st_set_geometry(NULL) %>% dplyr::as_tibble()
  
  # Compute intersection area
  pi3 <- pi %>% sf::st_set_geometry(NULL) %>% dplyr::left_join(pi2) %>%
    dplyr::mutate(proba = .data$area/.data$area2) %>%
    dplyr::select(!!c(grid_var, antenna_var, "proba", "area", "area2"))
  
  # Arrange
  pi3 <- pi3 %>% dplyr::tbl_df(.) %>%
    tidyr::unite_("id", c(antenna_var, grid_var), sep = ":") %>%
    dplyr::rename(S_intersect = .data$area, S_tot = .data$area2) %>%
    dplyr::select(.data$id, .data$proba, .data$S_intersect, .data$S_tot)
  
  return(pi3)
}

compute_voronoi <- function(df){
  
  # function to get polygon from boundary box
  bbox_polygon <- function(x) {
    bb <- sf::st_bbox(x)
    
    p <- matrix(
      c(bb["xmin"], bb["ymin"], 
        bb["xmin"], bb["ymax"],
        bb["xmax"], bb["ymax"], 
        bb["xmax"], bb["ymin"], 
        bb["xmin"], bb["ymin"]),
      ncol = 2, byrow = T
    )
    
    sf::st_polygon(list(p))
  }
  
  box <- st_sfc(bbox_polygon(df))
  
  v <- st_voronoi(st_union(df), box)
  v <- st_cast(v)
  
  box <- st_sfc(bbox_polygon(v))
  
  
  return(list(df,v %>% st_sf(.),box))
}


plot_voronoi <- function(data, 
                         plot_proba = TRUE,
                         filename = NULL){
  
  bbox <- st_bbox(data[[1]])

  d2 <- data[[2]] %>% st_sf(.) %>% mutate(info = 1:10) %>% mutate(col = (info==6 | info == 2)) %>%
      mutate(pv = as.numeric(col) - 1/3*as.numeric(info==6) - 2/3*as.numeric(info==2))

  if (plot_proba) {
    p <-  ggplot() +
      geom_sf(data = d2, aes(fill = as.numeric(pv)), lwd = 1) +
      scale_fill_gradient(low = "white", high = "#6E0707") +
      geom_sf(data = data[[1]], size = 2) +
      #geom_sf(data = data[[4]], fill = NA, alpha = 0.3, color = "darkblue") +
      coord_sf(datum = NA) +
      theme_bw() +
      theme(axis.text.x = element_blank(),
            axis.text.y = element_blank(),
            axis.ticks = element_blank(),
            rect = element_blank()) +
      xlim(bbox["xmin"],bbox["xmax"]) +
      ylim(bbox["ymin"],bbox["ymax"]) +
      theme(legend.position="none")
  } else {
    
    d2 <- d2 %>% 
      mutate(v_j = NA,
             v_j = case_when(info == 2 ~ "v1",
                             info == 6 ~ "v2"))
    
    p <-  ggplot() +
      geom_sf(data = d2, aes(fill = as.numeric(pv)), lwd = 1) +
      scale_fill_gradient(low = "white", high = "white") +
      geom_sf(data = data[[1]], size = 2) +
      coord_sf(datum = NA) +
      geom_sf_text(data = d2, aes(label = v_j), 
                   position = "identity", 
                   show.legend = F,
                   size = 10) +
      theme_bw() +
      theme(axis.title.x = element_blank(),
            axis.title.y = element_blank(),
            axis.ticks = element_blank(),
            rect = element_blank()) +
      xlim(bbox["xmin"],bbox["xmax"]) +
      ylim(bbox["ymin"],bbox["ymax"]) +
      theme(legend.position="none")
  }

  if (is.null(filename)) return(p)
  
  p + ggsave(filename, device = "png", width = 7, height = 6)
}


plot_grid <- function(data, 
                      plot_number = NULL, 
                      prior = FALSE,
                      plot_prior = FALSE,
                      filename = NULL) {
  
  bbox <- st_bbox(data[[1]])
  
  # Grid data
  d1 <- data[[4]] %>% mutate(grid_id = paste0("grid_", 1:400)) %>%
    st_set_crs(2154)
  
  # Voronoi data
  d2 <- data[[2]] %>% st_sf(.) %>% mutate(info = 1:10) %>% mutate(col = (info==6 | info == 2)) %>%
    mutate(pv = as.numeric(col) - 1/3*as.numeric(info==6) - 2/3*as.numeric(info==2)) %>%
    mutate(voronoi_id = paste0("voronoi_", 1:10)) %>%
    st_set_crs(2154) %>% select(-col, -info)
  
  if (!is.null(plot_number)) {
    if (plot_number == 1) d2 <- d2 %>% mutate(pv=replace(pv, pv<0.5, 0))
    if (plot_number == 2) d2 <- d2 %>% mutate(pv=replace(pv, pv>0.5, 0))
  }
  
  # Intersection
  table_inter <- grid_intersection(d1, d2, antenna_var = "voronoi_id") %>%
    rename(pcv = proba)

  # Arrange voronoi data
  tempdf <- d2 %>% st_set_geometry(NULL)
    
  table_inter <- table_inter %>% tidyr::separate(id, into = c("voronoi_id","grid_id"), sep = ":") %>%
    left_join(tempdf)
  
  if (prior) {
    
    prior_v6 <- table_inter %>%
      filter(voronoi_id == "voronoi_6") %>%
      group_by(grid_id) %>%
      summarise(a = first(pcv)) %>%
      select(-a) %>%
      mutate(prior = runif(length(grid_id), 0.1, 0.3))
    
    prior_v2 <- table_inter %>%
      filter(voronoi_id == "voronoi_2",
             !(grid_id %in% prior_v6$grid_id)) %>%
      group_by(grid_id) %>%
      summarise(a = first(pcv)) %>%
      select(-a) %>%
      mutate(prior = runif(length(grid_id), 0.6, 0.8))
    
    prior_df <- rbind(prior_v2, prior_v6)
    
    table_inter <- table_inter %>%
      left_join(prior_df) %>%
      mutate(prior = replace_na(prior, 0)) %>%
      group_by(grid_id) %>%
      dplyr::summarise(p = sum(pcv*prior*pv))
    
  } else {
    table_inter <- table_inter %>%
      group_by(grid_id) %>%
      dplyr::summarise(p = sum(pcv*pv))
  }
  
  # Normalization (so that probabilities sum to one for the individual)
  table_inter <- table_inter %>% mutate(p = p / sum(p))
  
  # Merge with geometry
  d4 <- data[[4]] %>%
    mutate(grid_id = paste0("grid_", 1:400)) %>%
    left_join(table_inter)
  
  if (plot_prior) {
    
    d4 <- d4 %>% left_join(prior_df) %>% mutate(prior = replace_na(prior, 0))
    
    p <- ggplot() +
      geom_sf(data = d4, aes(fill = as.numeric(prior)),
              color = "darkblue") +
      scale_fill_gradient(low = "white", high = "blue") +
      geom_sf(data = data[[2]], fill = NA, lwd = 1) +
      geom_sf(data = data[[1]], size = 2) +
      coord_sf(datum = NA) +
      theme_bw() +
      theme(axis.text.x = element_blank(),
            axis.text.y = element_blank(),
            axis.ticks = element_blank(),
            rect = element_blank()) +
      xlim(bbox["xmin"],bbox["xmax"]) +
      ylim(bbox["ymin"],bbox["ymax"]) +
      theme(legend.position="none")
    
  } else {
    
    p <- ggplot() +
      geom_sf(data = d4, aes(fill = as.numeric(p)),
              color = "darkblue") +
      scale_fill_gradient(low = "white", high = "#6E0707") +
      geom_sf(data = data[[2]], fill = NA, lwd = 1) +
      geom_sf(data = data[[1]], size = 2) +
      coord_sf(datum = NA) +
      # geom_sf_text(data = d4, aes(label = round(p, 2)), 
      #              position = "identity", 
      #              show.legend = F,
      #              size = 5) +
      theme_bw() +
      theme(axis.text.x = element_blank(),
            axis.text.y = element_blank(),
            axis.ticks = element_blank(),
            rect = element_blank()) +
      xlim(bbox["xmin"],bbox["xmax"]) +
      ylim(bbox["ymin"],bbox["ymax"]) +
      theme(legend.position="none")
    
  }
  
  if (is.null(filename)) return(p)
  
  p + ggsave(filename, device = "png", width = 7, height = 6)
  
}




# Compute and plot example ------------------------------------------------

# Simulate fake antennas coordinates

set.seed(1234567)

x <- 200*runif(10)
y <- 200*runif(10)

df <- data.frame(x = x, y = y, info = 1:10)
sp::coordinates(df) <- ~x+y
df <- df %>% st_as_sf(.)

# Compute voronoi tesselation over antennae

data <- compute_voronoi(df)

# Compute grid

data[[4]] <- data[[3]] %>% st_make_grid(n=c(20,20)) %>% st_sf()

# Assign unequal probabilities to example voronois

data[[1]] <- data[[1]] %>% mutate(col = (info==6 | info == 9)) %>%
  mutate(proba = as.numeric(col) - 1/3*as.numeric(info==6) - 2/3*as.numeric(info==9))



dir_ex <- "~/voro_grid_example"

if (!dir.exists(dir_ex)) {dir.create(dir_ex)}

plot_voronoi(data, plot_proba = F, paste(dir_ex, "voro1.png", sep = "/"))
plot_voronoi(data, plot_proba = T, paste(dir_ex, "voro2.png", sep = "/"))

plot_grid(data, plot_number = 1, filename = paste(dir_ex, "grid1.png", sep = "/"))
plot_grid(data, plot_number = 2, filename = paste(dir_ex, "grid2.png", sep = "/"))
plot_grid(data, plot_number = 3, filename = paste(dir_ex, "grid3.png", sep = "/"))

plot_grid(data, prior = T, plot_prior = T, 
                filename = paste(dir_ex, "prior1.png", sep = "/"))
plot_grid(data, prior = T, plot_prior = F, 
                filename = paste(dir_ex, "prior2.png", sep = "/"))











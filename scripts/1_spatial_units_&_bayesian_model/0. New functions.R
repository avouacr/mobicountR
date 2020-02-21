
# -----------------------------------------------------------------------------
# --- Functions declaration
# -----------------------------------------------------------------------------


# Geo functions ---------------------------------------------------------------

# Convert sf POINTS geometry to x,y columns

sfc_as_cols <- function(x, names = c("x","y")) {
  stopifnot(inherits(x,"sf") && inherits(sf::st_geometry(x),"sfc_POINT"))
  ret <- sf::st_coordinates(x)
  ret <- tibble::as_tibble(ret)
  stopifnot(length(names) == ncol(ret))
  x <- x[ , !names(x) %in% names]
  ret <- setNames(ret,names)
  dplyr::bind_cols(x,ret)
}


extract_bati <- function(dep_list = NULL,
                         path_citiesSHP2011 = getwd(),
                         pathcityUU = getwd(),
                         imputation = TRUE,
                         area_min_max = NULL,
                         height_min_max = NULL) 
{
  # Import of BD Topo "Bati indifférencié" shapefiles for specified UU
  message("Importing BD Topo buildings shapefiles for specified urban unit")
  path_shp <- paste0(getwd(), "/Tables/BD Topo/UU ", city)
  shp_names <- list.files(path = path_shp)
  list_shp <- lapply(shp_names, 
                     function(x) {st_read_unzip(x, path_shp, crs = 2154)})
  df_shp <- do.call("rbind", list_shp)
  
  # Keep only relevant variables
  # Compute buildings area and their centroids (x,y)
  # Restriction to urban unit bounding box
  df_shp <- df_shp %>%
    select(ID, HAUTEUR, geometry) %>%
    rename("id" = ID, "height" = HAUTEUR) %>%
    mutate(area = as.numeric(st_area(.)), 
           volume = area * height)
  
  # Impute missing 
  
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
  
  return(df_shp)
}
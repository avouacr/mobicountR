#' Computes prior from building registers
#'
#' Computes a prior distribution over a grid using data from building registers
#' (e.g. shapefiles of buildings over the territory).
#'
#' @param paths_shp character vector; complete paths to
#' building shapefiles.
#' @param grid a \code{data.frame} of class \code{\link[sf]{sf}}.
#' @param grid_id_var character; name of the variable indicating tiles
#' unique identifier in \code{grid}.
#' @param dir_inter character; path to the desired output directory for
#' prior files.
#' @param area_min_max numeric vector of length 2; minimum and/or maximum
#' areas allowed for buildings, outside of which buildings are filtered.
#' To provide only one value, indicate the other as \code{NA}.
#' @param height_var character; name of the variable indicating building
#' heights. If set to \code{NULL}, prior is computed using only building areas.
#' @param impute logical; should heights of zero height buildings be imputed ?
#' @param height_min_max numeric vector of length 2; minimum and/or maximum
#' heights allowed for buildings, outside of which buildings are filtered.
#' To provide only one value, indicate the other as \code{NA}.
#' Minimum height is ignored if \code{impute = TRUE}.
#' @param parallel logical; if multiple building shapefiles are provided,
#' should the computations on each of these files be run in parallel ?
#' @param n_workers number of worker processes that \code{\link[sf]{doParallel}}
#' will use to execute tasks in parallel. Ignored if \code{parallel = FALSE}.
#' @param crs integer; desired projected coordinate system;
#'   defauts to 2154 (Lambert 93).
#'
#' @return A \code{data.frame} of class \code{\link[data.table]{data.table}}
#' with two columns : tiles ID and a prior distribution over tiles.
#'
#' @export
#'
#' @details If multiple shapefiles are provided (e.g. shapefiles at an
#' infra-national level), prior computation can be computed in parallel
#' using \code{\link[foreach]{foreach}}.
#' \cr \cr
#' If the provided grid was not computed using \code{\link{create_grid}},
#' tiles ID should be formatted the same way : "x_centroid/100:y_centroid/100".
#'
#' @examples
#' \dontrun{
#' # Import building shapefiles paths
#' bdtopo_shp <- list.files("~/bdtopo")
#' # Compute grid
#' grid_500_fr <- create_grid(x = fr_limits, tile_size = 500)
#' # Compute prior
#' build_prior_dt <- prior_building(paths_shp = bdtopo_shp, grid = grid_500_fr)
#' }
prior_building <- function(paths_shp,
                           grid,
                           grid_id_var = "grid_id",
                           approx = TRUE,
                           dir_inter = paste0(tempdir(), "/dir_inter"),
                           area_min_max = NULL,
                           height_var = "HAUTEUR",
                           impute = TRUE,
                           height_min_max = c(NA, 130),
                           parallel = TRUE,
                           n_workers = 5,
                           crs = 2154)
{

  # Convert grid to desired crs if necessary
  if (is.na(sf::st_crs(grid)$epsg)) {
    warning(paste0("Grid crs is missing. Assuming grid crs is "),
            crs, ".")
    st_crs(grid) <- crs
  }
  if (sf::st_crs(grid)$epsg != crs) {
    grid <- grid %>% sf::st_transform(crs = crs)
  }

  # Create ouput directory if it doesn't exist
  if (!dir.exists(dir_inter)) {dir.create(dir_inter)}

  # Instance parallelization
  if (parallel) {
    cl <- parallel::makeCluster(n_workers)
    doParallel::registerDoParallel(cl)
  }

  # Should the loop be run in parallel ?
  `%fun%` <- if (parallel) foreach::`%dopar%` else foreach::`%do%`

  prior_france <- foreach::foreach(i = paths_shp,
                                   .packages = c('dplyr', 'stringr', 'sf', 'readr', 'tidyr'),
                                   .verbose = TRUE) %fun% {

                                     # Import BD Topo "Bati indifférencié" shapefiles
                                     df_shp <- sf::st_read(dsn = i)
                                     if (is.na(sf::st_crs(df_shp)$epsg)) {
                                       warning(paste0("Building shapefile crs is missing. Assuming shapefile crs is "),
                                               crs, ".")
                                       st_crs(df_shp) <- crs
                                     }
                                     if (sf::st_crs(df_shp)$epsg != crs) {
                                       df_shp <- df_shp %>% sf::st_transform(crs = crs)
                                     }

                                     # Compute buildings area
                                     df_shp <- df_shp %>%
                                       mutate(area = as.numeric(sf::st_area(.)))

                                     # Restriction of grid to bounding box of building shapefile
                                     bbox_shp <- sf::st_bbox(df_shp)

                                     grid_sub <- grid %>%
                                       separate(!!rlang::sym(grid_id_var), c("x_cent", "y_cent"), remove = F) %>%
                                       mutate(x_cent = as.numeric(x_cent) * 100,
                                              y_cent = as.numeric(y_cent) * 100) %>%
                                       filter(x_cent >= bbox_shp$xmin & x_cent <= bbox_shp$xmax &
                                                y_cent >= bbox_shp$ymin & y_cent <= bbox_shp$ymax)

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

                                     if (!is.null(height_var)) {

                                       df_shp <- df_shp %>%
                                         rename(height = !!rlang::sym(height_var))

                                       # Keep only buildings within min and max heights if specified
                                       if (impute & !is.null(height_min_max)) {
                                         height_min_max[1] <- NA
                                         }

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

                                       # Impute height for buildings with 0 height (invalid)
                                       # Rule : assign clothest neighbour in area among buildings of the same department
                                       if (impute) {
                                         df_no_geo <- df_shp %>%
                                           sf::st_set_geometry(NULL)

                                         df_to_impute <- df_no_geo %>%
                                           filter(height == 0)

                                         df_donors <- df_no_geo %>%
                                           filter(height > 0)

                                         vec_imp <- sapply(1:nrow(df_to_impute), function(i) {

                                           height_imp <- df_donors$height[which.min(abs(df_donors$area-df_to_impute$area[i]))]

                                           return(height_imp)
                                         })

                                         df_to_impute$height <- vec_imp
                                         df_shp_imputed <- rbind(df_to_impute, df_donors)

                                         df_shp <- df_shp %>%
                                           select(-height) %>%
                                           left_join(df_shp_imputed)

                                       }
                                     }

                                     # Computation of intersection areas between buildings and grid
                                     inter_build_grid <- grid_sub %>%
                                       sf::st_intersection(df_shp) %>%
                                       mutate(area = as.numeric(sf::st_area(.)))

                                     if (!is.null(height_var)) {
                                       # If height is available, compute building volume by tile
                                       inter_build_grid <- inter_build_grid %>%
                                         mutate(volume = area * height) %>%
                                         sf::st_set_geometry(NULL) %>%
                                         select(!!rlang::sym(grid_id_var), area, volume)

                                       # Agregate building volume at tile level
                                       prior_grid_dep <- inter_build_grid %>%
                                         group_by(!!rlang::sym(grid_id_var)) %>%
                                         summarise(prior_raw = sum(volume))

                                     } else {
                                       # Else, compute building area by tile
                                       inter_build_grid <- inter_build_grid %>%
                                         sf::st_set_geometry(NULL) %>%
                                         select(!!rlang::sym(grid_id_var), area)

                                       # Agregate building area at tile level
                                       prior_grid_dep <- inter_build_grid %>%
                                         group_by(!!rlang::sym(grid_id_var)) %>%
                                         summarise(prior_raw = sum(area))

                                     }

                                    # Save result for each department locally
                                     name_out <- str_match(basename(i), "(^.+?)\\.")[,2]
                                     path_out <- paste0(dir_inter, "/prior_", name_out, ".csv")
                                     readr::write_csv(prior_grid_dep, path_out)
                                   }

  parallel::stopCluster(cl)

  # Compute prior at France level
  paths_prior_dep <- list.files(dir_inter, full.names=TRUE)
  dt_france <- rbindlist(lapply(paths_prior_dep, fread))
  # Sum volume/area by tile for buildings that intersect two tiles
  dt_france <- dt_france[, .(prior_raw = sum(prior_raw)), by = grid_id]
  dt_france <- dt_france[, prior_building := .(prior_raw / sum(prior_raw))]
  dt_france <- dt_france[, prior_raw := NULL]

  return(dt_france)
}






#' Computes prior from resident population
#'
#' Computes a prior distribution over a grid using resident population data
#' from fiscal localized income sources (e.g. RFL or FiLoSoFi data in France).
#'
#' @param rfl a \code{data.frame} of class \code{\link[data.table]{data.table}}
#' containing fiscal localized incomes data. If a simple \code{data.frame}
#' is provided, it is automatically converted to a \code{data.table}.
#' @param indiv_id_var character; name of the variable indicating household
#' unique identifier in \code{rfl}.
#' @param n_indiv_var character; name of the variable indicating household
#' sizes (number of individuals) in \code{rfl}.
#' @param x_y_var character vector of length 2; names of the variables
#' indicating x and y coordinates of the household in \code{rfl}.
#'
#' @return a \code{data.frame} of class \code{\link[data.table]{data.table}}
#' with two columns : tiles ID and a prior distribution over tiles.
#'
#' @export
#'
#' @details If the output \code{data.frame} of this function is to be used as
#' in \code{\link{}}, x and y coordinates in input data (\code{rfl}) should
#' be converted beforehand in Lambert-93 (ESPG:2154).
#' \cr \cr
#' In the output \code{data.frame}, tiles ID are formatted
#' as "x_centroid/100:y_centroid/100" in order to match the formatting of
#' grids created by \code{\link{create_grid}}.
#'
#' @examples
#' \dontrun{
#' library(sas7bdat)
#' # Import data from fiscal localized data in 2014
#' rfl_df <- read.sas7bdat("path_to_filosofi_2014/menages14.sas7bdat")
#' # Compute prior using resident population
#' res_prop_prior <- prior_res_pop(rfl = rfl_df, indiv_id_var = "IDENTIFIANT")
#' }
prior_res_pop <- function(rfl,
                          indiv_id_var = "DIRNOSEQ",
                          n_indiv_var = "nbpersm",
                          x_y_var = c("x", "y")) {

  # If rfl is not a data.table, convert by reference
  if (!inherits(rfl, "data.table")) {
    setDT(rfl)
  }

  rfl_dt <- rfl
  rfl_dt <- rfl[, c(indiv_id_var, n_indiv_var, x_y_var), with = FALSE]
  rfl_dt <- na.omit(rfl_dt)

  rfl_dt <- rfl_dt[, c("x", "y") :=
                     list(round(get(x_y_var[1])/500) * 500 / 100,
                          round(get(x_y_var[2])/500) * 500 / 100)]
  rfl_dt <- rfl_dt[, "grid_id" := paste(x, y, sep = "_")]
  rfl_dt <- rfl_dt[, c("x", "y") := NULL]

  rfl_dt <- rfl_dt[, "n" := get(n_indiv_var)]
  rfl_dt <- rfl_dt[, .(n_tile = sum(n)), by = grid_id]
  rfl_dt <- rfl_dt[, prior_rfl := .(n_tile / sum(n_tile))]

  return(rfl_dt)
}






#' Combines multiple priors
#'
#' Combines multiple prior distributions over a same grid
#' by computing a new distribution as a linear combination
#'
#' @param priors a list of \code{data.frame}s, one for each prior.
#' See 'Details' for required structure
#' @param weights numeric vector of same length as \code{priors}; weights used
#' to compute the linear combination of priors.\cr
#' Must sum to one for the output to be a probability distribution.
#'
#' @return a \code{data.frame} with two columns : tiles ID and a prior
#' distribution over tiles.
#'
#' @export
#'
#' @details \code{data.frame}s in \code{priors} should have the same structure
#' as those produced by \link{prior_building} and \link{prior_res_pop} :\cr
#' a first column indicating tiles IDs, and a second column giving the
#' distribution of the prior on the grid.
#'
#' @examples
#' \dontrun{
#' # Import priors
#' prior_bdtopo <- prior_building(paths_shp = "~/bdtopo_shp", grid = grid_500_france)
#' prior_rfl <- prior_res_pop(rfl = rfl11)
#' # Include inputs in a list
#' list_priors <- list(prior_bdtopo, prior_rfl)
#' # Define weights to be used (same order as the list of priors)
#' weights = c(0.3, 0.7)
#' # Combine priors
#' prior_comb <- prior_combine(priors = list_priors, weights = weights)
#' }
prior_combine <- function(priors, weights) {

  n_priors <- length(priors)

  # Merge priors
  df_merge <- Reduce(function(x, y) merge(x, y, all=TRUE), priors)

  df_merge[is.na(df_merge)] <- 0

  # Compute a linear computation of priors using provided weights
  for (i in 1:n_priors) {
    df_merge[[i+1]] <- df_merge[[i+1]] * weights[i]
  }

  df_merge$prior_comb <- rowSums(df_merge[,2:(n_priors+1)])
  df_merge <- df_merge[,c(1,ncol(df_merge))]

  return(df_merge)
}








# test_raw <- minior::get_file("MobiCount/prior/prior_rfl_bdtopo_france.csv", "etgtk6")
# test <- read_csv(test_raw) %>% separate(id, c("nidt", "grid_id"), ":")
#
# rfl <- test %>%
#   select(grid_id, prior_rfl) %>%
#   group_by(grid_id) %>%
#   summarise(prior_rfl = first(prior_rfl)) %>%
#   filter(prior_rfl != 0)
#
# build <- test %>%
#   select(grid_id, prior_build) %>%
#   group_by(grid_id) %>%
#   summarise(prior_build = first(prior_build)) %>%
#   filter(prior_build != 0)



# df_prior_rfl <- grid %>%
#   left_join(rfl_by_tile) %>%
#   mutate(nbpersm = replace_na(nbpersm, 0),
#          prior_rfl = nbpersm / sum(nbpersm)) %>%
#   select(grid_id, prior_rfl)
#
# prior_final <- inter_grid_voro %>%
#   separate(id, c("NIDT", "grid_id"), sep = ":", remove = FALSE) %>%
#   left_join(df_prior_rfl) %>%
#   mutate(proba_rfl = proba_inter * prior_rfl) %>%
#   select(id, proba_inter, prior_rfl, proba_rfl)



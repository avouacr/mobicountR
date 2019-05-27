#' Creates a regular grid over a spatial unit
#'
#' Creates a regular square grid over the bounding box of an sf object (e.g.
#'   a country limits shapefile imported with \code{\link[sf]{st_read}}).
#'
#'
#' @param x data.frame of class \code{\link[sf]{sf}}.
#' @param tile_size integer; length of the side of a square (tile).
#' @param crs integer; desired projected coordinate system;
#'   defauts to 2154 (Lambert 93).
#'
#' @return the created grid as a data.frame of class \code{\link[sf]{sf}}.
#'
#' @export
#'
#' @details The output data.frame contains a variable grid_id which provides
#'   an unique identifier for each tile. Identifiers are constructed as \cr
#'   x_centroid/100:y_centroid/100 so that tile centroids are easy to
#'   compute back when needed.
#'
#' @examples
#' \dontrun{
#' # Import shapefile of country limits
#' fr_limits <- sf::st_read("~/france_shp/francemetro_2015.shp")
#' # Create grid
#' grid <- create_grid(x = fr_limits, tile_size = 500)
#' }
create_grid <- function(x,
                        tile_size,
                        crs = 2154) {

  # Stop if x is not an sf object
  stopifnot('sf' %in% class(x))

  # Convert to desired crs if necessary
  if (sf::st_crs(x)$epsg != crs) {
  x <- x %>% st_transform(crs = crs)
  }

  # Compute shapefile bounding box
  bbox <- sf::st_bbox(x)
  xmin <- floor(bbox$xmin/tile_size)*tile_size
  xmax <- ceiling(bbox$xmax/tile_size)*tile_size
  ymin <- floor(bbox$ymin/tile_size)*tile_size
  ymax <- ceiling(bbox$ymax/tile_size)*tile_size

  # Create grid from bounding box
  x_vec <- seq(xmin, xmax, tile_size)
  y_vec <- seq(ymin, ymax, tile_size)
  grid <- merge(x_vec, y_vec)
  r = tile_size / 2
  grid$geometry <- sprintf("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))",
                           grid$x-r, grid$y+r, grid$x+r, grid$y+r,
                           grid$x+r, grid$y-r, grid$x-r, grid$y-r,
                           grid$x-r, grid$y+r)
  geogrid <- st_as_sf(grid, wkt = "geometry", crs = as.integer(crs))

  # Restrict grid to country boundaries
  inter_grid_metro <- sf::st_intersects(geogrid,
                                        x,
                                        sparse = T)
  len_inter <- sapply(inter_grid_metro, length)
  grid_sub <- geogrid[len_inter == 1,]

  # Create IDs of tiles as "x_vec/100:y_vec/100" where (x_vec,y_vec) are centroids
  grid_sub <- grid_sub %>%
    mutate(grid_id = paste(as.character(x/100),
                           as.character(y/100),
                           sep = "_")) %>%
    select(-x_vec, -y_vec)

    return(grid_sub)
}




#' Computes intersections between grid and voronoi tesselation
#'
#' Computes intersections between a grid (e.g. created with
#'   \code{\link{create_grid}}) and a voronoi tesselation computed over an
#'   MNO antennas map. Intersections probabilities, defined as the relative
#'   area of an intersection in the correspond voronoi, are computed during
#'   the process.
#'
#'
#'
#' @param grid data.frame of class \code{\link[sf]{sf}}.
#' @param voronoi data.frame of class \code{\link[sf]{sf}}.
#' @param sf logical; if \code{TRUE}, output is an sf object;
#' defauts to \code{FALSE}.
#' @param crs integer; desired projected coordinate system;
#' defauts to 2154 (Lambert 93).
#'
#' @return a simple data.frame if \code{sf=TRUE}. A data.frame of class
#' \code{\link[sf]{sf}} if \code{sf=FALSE}.
#'
#' @export
#'
#' @details The output data.frame contains a variable \code{proba_inter}
#'   which provides, for each intersection between a tile and a voronoi,
#'   the relative area of the tile in the corresponding voronoi. This
#'   value is comprised between 0 and 1 and thus assimilated to a
#'   probability of intersection.
#'
#'
#' @examples
#' \dontrun{
#' # Create grid
#' grid_sf <- create_grid(x = fr_limits, tile_size = 500, export = FALSE)
#' # Import shapefile of voronoi tesselation
#' voronoi_sf <- sf::st_read("~/Antenne_voronoi_rev.shp", crs = 2154)
#' # Compute grid-voronoi intersections
#' table_prob <- inter_grid_voronoi(grid = grid_sf,
#' voronoi = voronoi_sf, prob = TRUE, sf = FALSE)
#' }
inter_grid_voronoi <- function(grid,
                               voronoi,
                               grid_id = "grid_id",
                               voronoi_id = "NIDT",
                               sf = FALSE,
                               crs = 2154) {

  # Convert inputs to desired crs if necessary
  if (sf::st_crs(grid)$epsg != crs) {
    grid <- grid %>% st_transform(crs = crs)
  }
  if (sf::st_crs(voronoi)$epsg != crs) {
    voronoi <- voronoi %>% st_transform(crs = crs)
  }

  # Compute intersections
  df_inter <- grid %>%
    st_intersection(voronoi) %>%
    mutate(area_inter = as.numeric(st_area(.)))

  if (sf == FALSE) {df_inter <- df_inter %>% st_set_geometry(NULL)}

  df_voro <- voronoi %>%
    mutate(area_voro = as.numeric(st_area(.))) %>%
    st_set_geometry(NULL)

  inter_grid_voro <- df_inter %>%
    left_join(df_voro) %>%
    mutate(proba_inter = area_inter / area_voro) %>%
    unite(id, !!rlang::sym(voronoi_id), !!rlang::sym(grid_id),
          sep = ":", remove = FALSE) %>%
    select(id, proba_inter)

  return(inter_grid_voro)
}





#' Transforms a sf POINT geometry to x,y columns
#'
#' Transforms a data.frame of class \code{\link[sf]{sf}} with a POINT geometry
#' (e.g. a grid of which centroids has been computed using
#' \code{\link[sf]{st_centroid}}) to a conventional data.frame
#'  with coordinates as two numeric columns.
#'
#'
#' @param x data.frame of class \code{\link[sf]{sf}} with a sfc_POINT geometry.
#' @param names character vector of length 2; names of the two new columns
#' with x,y coordinates; defauts to \code{c("x","y")}.
#'
#' @return A data.frame with two new columns corresponding to x,y coordinates
#' of spatial units. The initial geometry column is removed.
#'
#' @export
#'
#' @details This transformation is useful for efficient filtering of coordinates,
#'   e.g. to subset a grid to a smaller area bounding box.
#' \cr
#' \cr
#' Source of the function : \url{https://github.com/r-spatial/sf/issues/231}
#'
#' @examples
#' \dontrun{
#' # Import grid and compute centroids
#' grid <- sf::st_read("~/grid_500_france.shp")
#' grid <- grid %>% st_centroid()
#' # Transform to a data.frame with centroid coordinates as new columns
#' grid_nogeo <- grid %>% sfc_as_cols()
#' }
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

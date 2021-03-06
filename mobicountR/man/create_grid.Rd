% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spatial_units.R
\name{create_grid}
\alias{create_grid}
\title{Creates a regular grid over a spatial unit}
\usage{
create_grid(x, tile_size, crs = 2154)
}
\arguments{
\item{x}{data.frame of class \code{\link[sf]{sf}}.}

\item{tile_size}{integer; length of the side of a square (tile).}

\item{crs}{integer; desired projected coordinate system;
defauts to 2154 (Lambert 93).}
}
\value{
the created grid as a data.frame of class \code{\link[sf]{sf}}.
}
\description{
Creates a regular square grid over the bounding box of an sf object (e.g.
  a country limits shapefile imported with \code{\link[sf]{st_read}}).
}
\details{
The output data.frame contains a variable grid_id which provides
  an unique identifier for each tile. Identifiers are constructed as \cr
  x_centroid/100:y_centroid/100 so that tile centroids are easy to
  compute back when needed.
}
\examples{
\dontrun{
# Import shapefile of country limits
fr_limits <- sf::st_read("~/france_shp/francemetro_2015.shp")
# Create grid
grid <- create_grid(x = fr_limits, tile_size = 500)
}
}

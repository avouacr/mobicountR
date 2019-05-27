#' Computes the bayesian posterior distribution using a voronoi tesselation
#'
#' This function implements the bayesian model for mobile events detection,
#' assuming that no information on the MNO's antennas coverage is available.
#' A voronoi tesselation is thus used to approximate the area covered by
#' each antenna. If available, prior information over tiles can be used
#' to improve the quality of the detection.
#'
#' @param df_inter a \code{data.frame} which indicates for each voronoi x grid
#' intersection the relative area of the intersection in the corresponding
#' voronoi; should be either produced by or have the same format as the
#' output of \code{\link{inter_grid_voronoi}}.
#' @param prior a \code{data.frame}; should be either produced by or have
#' the same format as those produced by the prior computation functions
#' of \code{\link{mobicount}}. Defauts to \code{NULL}, in which case a prior
#' equal to one is used (uniform).
#' @param var_prior character; name of the variable which contains the
#' prior distribution; ignored if \code{prior = NULL}.
#' @param var_final character; name of the variable which should contain
#' the posterior distribution.
#' @param filter_null logical; should voronoi x grid intersections with
#' a null posterior probability be deleted ? Defauts to \code{TRUE}.
#'
#' @return a \code{data.frame} with two columns : an ID for each
#' voronoi x grid intersection formatted as "voronoi_ID:grid_id",
#' and the posterior distribution.
#'
#' @export
#'
#' @details
#'
#' @examples
#' \dontrun{
#' # Import prior
#' prior_bdtopo <- readr::read_csv(~/bdtopo_france.csv)
#' # Import voronoi x grid intersections table
#' inter_df <- readr::read_csv(~/inter_grid_voro.csv)
#' # Compute bayesian posterior distribution
#' bay_df_bdtopo <- bay_voronoi(inter_df, prior_bdtopo, "proba_bdtopo")
#' }
bay_voronoi <- function(df_inter,
                        prior = NULL,
                        var_prior = NULL,
                        var_final = "proba_final",
                        filter_null = TRUE) {

if (is.null(prior)) {

  df_bay <- df_intersect %>% rename(!!var_final := proba_inter)

} else {

  df_bay <- df_inter %>%
    separate(id, c("nidt", "grid_id"), sep = ":", remove = FALSE) %>%
    left_join(prior) %>%
    mutate(proba_final = !!rlang::sym(var_prior) * proba_inter) %>%
    group_by(nidt) %>%
    mutate(proba_final = proba_final / sum(proba_final)) %>%
    ungroup() %>%
    select(-nidt, -grid_id, -!!rlang::sym(var_prior), -proba_inter) %>%
    rename(!!var_final := proba_final)

}

if (filter) {df_bay <- df_bay %>% filter(!!rlang::sym(var_final) > 0)}

  return(df_bay)

}





#' Computes the bayesian posterior distribution using a coverage map
#'
#' This function implements the bayesian model for mobile events detection,
#' assuming that a coverage map
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
bay_coverage <- function(df_inter, prior) {
 return()
}




# prior_test <- read_csv(get_file("MobiCount/prior/prior_rfl_bdtopo_france_prenorm_round.csv", "etgtk6"))
#
# prior_bdtopo <- prior_test %>%
#   separate(id, c("nidt", "grid_id"), sep = ":") %>%
#   select(grid_id, proba_build) %>%
#   filter(!duplicated(grid_id)) %>%
#   rename(prior_build = proba_build)
#
# prior_rfl <- prior_test %>%
#   separate(id, c("nidt", "grid_id"), sep = ":") %>%
#   select(grid_id, proba_rfl) %>%
#   filter(!duplicated(grid_id)) %>%
#   rename(prior_rfl = proba_rfl)
#
# sup_priors <- list(prior_rfl)
#
#
# df_intersect <- read_csv(get_file("MobiCount/grids/grid_inter_voronoi_france.csv", "etgtk6"))





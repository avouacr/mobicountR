#include <RcppArmadillo.h>
using namespace Rcpp;
// [[Rcpp::depends(RcppArmadillo)]]

Environment glob_env = Environment::global_env();

arma::mat mat_init = glob_env["mat_init"];
int n_rows = mat_init.n_rows;
int n_cols = mat_init.n_cols;
arma::mat add_i(n_rows, n_cols, arma::fill::zeros);
arma::mat add_j(n_rows, n_cols, arma::fill::zeros);

// [[Rcpp::export]]
int quadtree(
    int min_i,
    int max_i,
    int min_j,
    int max_j,
    int level,
    int min_size) {
  
  if ((max_i > min_i) & (max_j > min_j))
  {
    int pivot_i = (max_i + min_i - 1) /2;
    int pivot_j = (max_j + min_j - 1) /2;
    
    int ag0 = arma::accu(mat_init.submat(min_i, min_j, pivot_i, pivot_j));
    int ag1 = arma::accu(mat_init.submat(pivot_i+1, min_j, max_i, pivot_j));
    int ag2 = arma::accu(mat_init.submat(min_i, pivot_j+1, pivot_i, max_j));
    int ag3 = arma::accu(mat_init.submat(pivot_i+1, pivot_j+1, max_i, max_j));
    
    int n_rows = mat_init.n_rows;
    int n_cols = mat_init.n_cols;
    
    if ((ag0 > min_size | ag0 == 0) & (ag1 > min_size | ag1 == 0) & 
        (ag2 > min_size | ag2 == 0) & (ag3 > min_size | ag3 == 0)) {
      
      add_i.submat(pivot_i+1, min_j, max_i, max_j) = 
        (add_i.submat(pivot_i+1, min_j, max_i, max_j) + pow(2, (n_rows-level-1)));
      add_j.submat(min_i, pivot_j+1, max_i, max_j) = 
        (add_j.submat(min_i, pivot_j+1, max_i, max_j) + pow(2, (n_cols-level-1)));
      
      quadtree(min_i, pivot_i, min_j, pivot_j, level+1, min_size = min_size);
      quadtree(min_i, pivot_i, pivot_j+1, max_j, level+1, min_size = min_size);
      quadtree(pivot_i+1, max_i, min_j, pivot_j, level+1, min_size = min_size);
      quadtree(pivot_i+1, max_i, pivot_j+1, max_j, level+1, min_size = min_size);
    }
  }
  return 1;
  }


/*** R
quadtree(min_i = 0,
                max_i = (n_rows-1),
                min_j = 0,
                max_j = (n_cols-1),
                level = 0,
                min_size = min_size)
*/


// [[Rcpp::export]]
List return_add()
  {
  List res_add = List::create(add_i, add_j);
  return res_add;
}

/*** R
add_cpp = return_add()
*/


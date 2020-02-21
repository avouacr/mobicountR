# mobicountR

The mobicountR project consists in an implementation of a bayesian framework put forward by Tennekes (2018) (https://github.com/MobilePhoneESSnetBigData/mobloc) to estimate present population using mobile phone data. Specifically, it aims at improving the spatial mapping step by probabilizing phone user locations over a given input grid, while allowing to incoporate both technical information on antennae coverage and prior information on the grid.

The framework is applied to the usual case in which no technical information on antennae is available, other than geographic coordinates of antennae. The bayesian model is used to interpolate events observed on the Voronoi tesselation formed by antennae on a regular 500x500m grid in a probabilized way. The model is estimated using a 2007 CDR dataset from French MNO Orange. Building volume from the BD Topo is used as prior information on the grid. Validation is performed by comparing population estimates to population counts from French localized fiscal data (RFL) using various metrics.

The complete framework as well as the results of its application are detailled in a [working paper](https://github.com/avouacr/mobicountR/blob/master/working_paper_insee.pdf). An [R package](https://github.com/avouacr/mobicountR/tree/master/mobicountR) is provided to ensure reproducibility of results.

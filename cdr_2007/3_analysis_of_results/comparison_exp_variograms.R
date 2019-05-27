
library(gstat)
library(scales) 


# Comparison of experiemental variograms ----------------------------------

gstat_uniform <- gstat(id = 'n_uniform', formula = n_uniform ~ 1,
                       data = df_compar_geo)
expvar_uniform <- variogram(gstat_uniform,
                            cutoff = 50000)
plot_expvar_uniform <- ggplot(expvar_uniform,
                              aes(x = dist, y = gamma, size = np)) +
  geom_point() +
  scale_size_continuous(name="Number of point\npairs considered") +
  xlab("Average distance of all point pairs considered") +
  ylab("Sample variogram estimate (gamma)") +
  theme_bw() +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))

ggsave(plot = plot_expvar_uniform, 
       filename = "expvario_uniform.jpg",
       path = "~",
       device = "jpg", 
       width = 20, 
       height = 8, 
       units = "cm")

put_local_file("~/expvario_uniform.jpg", 
               dir_minio = "MobiCount/",
               bucket = "etgtk6")


gstat_bdtopo <- gstat(id = 'n_bdtopo', formula = n_bdtopo ~ 1,
                      data = df_compar_geo)
expvar_bdtopo <- variogram(gstat_bdtopo,
                           cutoff = 50000)
plot_expvar_bdtopo <- ggplot(expvar_bdtopo,
                             aes(x = dist, y = gamma, size = np)) +
  geom_point() +
  scale_size_continuous(name="Number of point\npairs considered") +
  xlab("Average distance of all point pairs considered") +
  ylab("Sample variogram estimate (gamma)") +
  theme_bw() +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))

ggsave(plot = plot_expvar_bdtopo, 
       filename = "expvario_bdtopo.jpg",
       path = "~",
       device = "jpg", 
       width = 20, 
       height = 8, 
       units = "cm")

put_local_file("~/expvario_bdtopo.jpg", 
               dir_minio = "MobiCount/",
               bucket = "etgtk6")





gstat_rfl <- gstat(id = 'n_rfl', formula = n_rfl ~ 1,
                   data = df_compar_geo)
expvar_rfl <- variogram(gstat_rfl,
                        cutoff = 50000)
plot_expvar_rfl <- ggplot(expvar_rfl,
                          aes(x = dist, y = gamma, size = np)) +
  geom_point() +
  scale_size_continuous(name="Number of point\npairs considered") +
  xlab("Average distance of all point pairs considered") +
  ylab("Sample variogram estimate (gamma)") +
  theme_bw() +
  theme(legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        axis.title.x = element_text(size = 12),
        axis.title.y = element_text(size = 12))

ggsave(plot = plot_expvar_rfl, 
       filename = "expvario_rfl.jpg",
       path = "~",
       device = "jpg", 
       width = 20, 
       height = 8, 
       units = "cm")

put_local_file("~/expvario_rfl.jpg", 
               dir_minio = "MobiCount/",
               bucket = "etgtk6")
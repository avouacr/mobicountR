
prior <- minior::get_file("MobiCount/prior/prior_rfl_bdtopo_france.csv", "etgtk6")
prior <- read_csv(prior)

prior_test <- prior %>%
  select(id, proba_inter, proba_build, proba_rfl) %>%
  separate(id, into = c("nidt", "grid_id"), sep = ":") %>%
  group_by(nidt) %>%
  mutate(proba_build = proba_build / sum(proba_build),
         proba_rfl = proba_rfl / sum(proba_rfl)) %>%
  unite(col = "id", nidt, grid_id, sep = ":")

prior_round <- prior_test %>%
  mutate(proba_inter = round(proba_inter, 4),
         proba_build = round(proba_build, 4),
         proba_rfl = round(proba_rfl, 4))

write_csv(prior_round, "~/prior.csv")

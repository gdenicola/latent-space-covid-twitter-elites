# Code for reproducing the analysis in the paper
# "Mapping the latent social space of COVID-19 Twitter elites"
# by De Nicola, Tuekam Mambou & Kauermann

# this code fits the latent cluster random effects model in two dimensions
# and with two clusters to the network of COVID-19 elites, contained in
# the file network_edgelist.csv (in edgelist form)


#install packages (if not yet installed)
#install.packages(network)
#install.packages(latentnet)


#load required packages
library(network)
library(igraph)
library(intergraph)
library(latentnet)

for (threshold in c(1000, 2000, 2200, 2500, 2800, 3000, 4000)) {
  #read (directed) edgelist
  el <-
    read.csv(paste0(
      "data_processing/data/networks/network_edgelist_",
      threshold,
      ".csv"
    ))
  #create network object
  net <- as.network(el, directed = T)
  
  i_net <- asIgraph(net)
  write_graph(
    i_net,
    paste0(
      "data_processing/data/networks/graph_",
      threshold,
      ".graphml"
    ),
    format = "graphml"
  )
  
  ordered_names = network.vertex.names(net)
  ordered_names_df = data.frame(name = ordered_names)
  write.csv(
    ordered_names_df,
    paste0(
      "data_processing/data/networks/ordered_names_",
      threshold,
      ".csv"
    ),
    row.names = FALSE
  )
  
  #fit latent cluster random effect model in 3 dimensions and with 2 clusters
  lsm <- ergmm(net ~ euclidean(d = 2, G = 2) + rsender + rreceiver,
               tofit = "mkl",
               verbose = T)
  
  #plot latent positions and posterior probabilities of cluster memberships
  plot(lsm,
       pie = T,
       print.formula = FALSE,
       labels = F)
  
  summ_lsm <-
    summary(lsm,
            point.est = c("pmean", "mkl"),
            bic.eff.obs = NULL)
  Z.pos_lsm <- summ_lsm[["mkl"]][["Z"]]
  Z.K_lsm <- summ_lsm[["pmean"]][["Z.K"]]
  Z.pZK_lsm <- summ_lsm[["pmean"]][["Z.pZK"]]
  write.csv(
    Z.pos_lsm,
    paste0(
      "data_processing/data/networks/positions_",
      threshold,
      ".csv"
    ),
    row.names = FALSE
  )
  write.csv(
    Z.K_lsm,
    paste0(
      "data_processing/data/networks/clusters_",
      threshold,
      ".csv"
    ),
    row.names = FALSE
  )
  write.csv(
    Z.pZK_lsm,
    paste0(
      "data_processing/data/networks/posteriors_",
      threshold,
      ".csv"
    ),
    row.names = FALSE
  )
}

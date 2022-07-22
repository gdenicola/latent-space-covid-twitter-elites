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
library(latentnet)


#read (directed) edgelist
el <- read.csv("network_edgelist.csv")

#create network object
net <- as.network(el, directed = T)

#fit latent cluster random effect model in 2 dimensions and with 2 clusters
lsm <- ergmm(net ~ euclidean(d=2, G=2) + rsender + rreceiver, 
                   tofit="mkl", verbose=T)  


#plot latent positions and posterior probabilities of cluster memberships
plot(lsm, pie=T, print.formula=FALSE, labels=F)  


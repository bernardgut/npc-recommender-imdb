NETFLIX-IMDB Recommendation building tool on MapReduce
===========

Map-Reduce implementation that aim at building movie recommendations to users. For this, we use a content based approach on a IMDB movie set, and a preprocessed dataset of users clustered by similarity (on the dataset of NETFLIX users). This is the second step of the process of building recommendations for users. The goal is that the final system (afetr computations) will be able to instantly output a set of movies that a given user is likely to like. In order to see that we use collaborative filtering on the set of users (Netflix dataset of users), then apply content based approach to link user clusters with movies. See the "bernardgut/recommander" repo for the Collaborative filtering part.

## Introduction

The goal of this repo is to extend the recommendation system for the Netflix prize challenge described in the "recommander" repo. We implement a clustering algorithm using Hadoop [2](an open-source MapReduce implementation). In order to build recommandation efficiently, we use a cluster of 88 worker nodes. 
using Hadoop (an open-source MapReduce  implementation). The Netflix Prize was an open competition for implementing the best collaborative filtering algorithm for recommending movies to subscribed users. The grand prize of one million dollars was given to the winning team. For more information about the Netflix prize see
http://en.wikipedia.org/wiki/Netflix_Prize.


## Description

We build a recommender system that given a new movie, return a list of users that might enjoy watching that movie.

The system make its recommendation by first constructing two sets of clusters of movies: "ClusterIMDB", which is built using content data from the IMDB dataset, and "ClusterNetflix", which is built using the V matrix from the (U,V) decomposition of the Netflix dataset (output of "recommander" program). It then establish a mapping from each cluster of movies in "ClusterIMDB" to one cluster of movies in ClusterNetflix (this mapping is established by taking for each cluster “A” in "ClusterIMDB" the cluster “B” in "ClusterNetflix" that has the maximum Jaccard distance w.r.t. “A”).
Finally, when given a new (test) movie, along with its set of IMDB features, the system first find the cluster “A” in ClusterIMDB to which it should belong and it determine the corresponding cluster “B” in ClusterNetflix based on the mapping between ClusterIMDB and ClusterNetflix. Then it use the center of the cluster “B” to compute a rating of the movie for each user in U, returning a list of users having the highest predicted rating, i.e. the users most likely to enjoy the movie. We say that a user enjoys a movie if and only if her normalized rating is greater than zero.

The goal of the process is to maximize the F-score associated with the list of users returned for a new (test set) movie. We define F-score as 1 / [1/Recall+1/Precision]. The F-score is computed based only on the users that have ratings associated with the test movie in the original Netflix dataset.

### Clustering Algorithm 

K-MEANS : Given a set of points in a n-dimensional space, a clustering algorithm is to find k center points (centroids) that minimize the distance between each point and its closest centroid. For more information read chapter 7 of the “Mining Massive Datasets” textbook [1].

### Inputs

#### The IMDB Dataset

The IMDB dataset consists of a list of movies, each on a separate line, and for every movie we are given a list of feature ids for which this movie has value 1. For example, you can think of a feature being 1 as signaling that the movie has a specific actor starring in it or it was written by a specific screenwriter. The cluster ClusterIMDB is constructed based on the cosine distance between the feature vectors of different movies.

Below is an input example consisting of two movies with ids 1 and 2, where movie 1 has
features 1, 4 and 6, and movie 2 has features 3, 7 and 8.
1, 1, 4, 6
2, 3, 7, 8

####  The (U,V) decomposition dataset

We give the U and V matrices resulting from the (U,V) decomposition of the Netflix dataset P of the previous milestone (see "recommander" repo from same user). The dimensions of the U and V matrices are (number-of-users x 10) and (10 x number-of-movies).
The format of the U and V matrices is 
\<U, userID, [1..10], value\> and
\<V, [1..10], movieID, value\>

We will consider that column j of V represents the profile of movie j (with 10 features) and the
cluster ClusterNetflix will be constructed based on the cosine distance between these profile-
vectors.

#### Inputs/Outputs

We provide a small input dataset in order to allow for local-machine test (cluster development dataset is not suitable for github, due to size concerns). Input directory contains README file with dataset information (e.g. number of users, movies, etc.). The input dataset is divided into training and test datasets, which should both reside on HDFS.
(/std11/input/train/ and /std11/input/test/).
We use the “train” dataset to build the two sets of clusters, ClusterIMDB and ClusterNetflix, and the mapping between them, and the “test” dataset to evaluate our implementation.

The system output for each test movie a set of users that might enjoy that movie. The format of each line of the output file is:

movie_id, user_id, ... , user_id

## Notes
Don’t expect great response times. Hadoop is always a bit sluggish – even if the system is not heavily loaded, it is not strikingly efficient, which is annoying for small and simple jobs, but it is scalable. Don't be frustrated about the Hadoop performance, it's not necessarily a problem in the code.

The Hadoop distribution used throughout this project is Apache Hadoop 0.21 

## References
[1] Mining of Massive Datasets -Anand Rajaraman (Kosmix, Inc). Jeffrey D. Ullman (Stanford Univ).

[2] Google MapReduce Paper

## Note on our Runtime Environment
Our cluster has four blades and it runs a virtualized Solaris-based environment with 96 hardware threads (~=cores). Four nodes are designated gateways dedicated for communication with the outside world (called “global zones” on Solaris), i.e. they are visible globally and can be connected to via ssh. These nodes are called **** (edited). Each global zone manages a blade and shares memory and I/O with 22 "local zones". These
are virtual machines that each have a hardware thread exclusively assigned to them -- so work
can run on each of these local zones in parallel. The names of these zones have the following
format: ******* (edited). Each local zone has 2GB of RAM assigned to it. Hadoop version 0.21 has been installed on ***** as follows. The system is configured with 88 worker nodes, with the nameserver and jobtracker running on *****. Status information about namenode and jobtracker can be found at:
http://******/(namenode)
http://******/(jobtracker)
Data is stored on HDFS, Hadoop's distributed and replicated file system.


A tutorial for writing MapReduce programs in Hadoop can be found on:
http://hadoop.apache.org/docs/r1.1.1/mapred_tutorial.html

It is possible (and arguably not too difficult) to create a small Hadoop installation on your own computer/laptop for early-stage testing. Please follow the instructions for “Standalone operation” on 
http://hadoop.apache.org/docs/r1.1.2/single_node_setup.html

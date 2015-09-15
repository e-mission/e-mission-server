### Tour Model Guide

In order to make a tour model from a user's e-mission location data, put all of the data in your mongodb, and then run 'python create_tour_model_matrix.py [UUID]'.

# The program is described below and illustrated in the flowchart at the bottom of the page.

* Data is either generated for a fake user using a utility model defined in input.json, or real e-mission location data is taken from the database. These different types of data end up in the same format and are indistinguishable to the cluserting pipeline.
* The trips are binned based on a 300 meter tolerance, and then filtered out if the bin is too small, leaving the clustering algorithm with only the most regularly traveled paths. 
* The remaining popular trips are passed into the k-mediods clustering algorithm provided in sk-learn, and clustered by starting and ending points to make a small number of canonical commutes, stored as a list of hashtables, containing the start location name, end location name, and a list of the trip objects that make up the commute. 
* Those commutes are passed into an intuitive converter, and are converted into Location objects and Commute objects stored inside of a TourModel object. Locations can be thought of as nodes, Commutes as edges, and a TourModel as a Markov Model graph storing them. 
* From these Markov Models, you can simulate the commutes of an average week for a user.
* From the simulation, you can create a new Markov Model graph, and so on.

The flowchart below describes the process of how clustering works. 

![Image of Flowchart](https://raw.githubusercontent.com/joshzarrabi/e-mission-server/tourModel/emission/analysis/modelling/tour_model/flowchart.JPG)


### Instructions for Open Trip Planner Install



# Download all of the following files to the same folder.

1. Download the latest .jar file from http://dev.opentripplanner.org/jars/

2. Download the open street data files for the areas you want from http://metro.teczno.com/

3. Download the GTFS files for the public transportation agencies you want by googling " <agency name> gtfs " (no quotes)
	- Save them all as <name of your choice>.gtfs.zip 
	(they will downnload as .zip files with long confusing names)


	- The agencies I have found for the bay area are : 
		* BART
		* MUNI
		* AC Transit
		* Caltrain
		* Caltrain Shuttle
		* VTA
		* Sam Trans 

	- Fot Denver I am using 
		* RTD


4. You need java 8 for the newer versions of open trip planner

	To get java 8 run the following commands:

	$ sudo add-apt-repository ppa:webupd8team/java
	$ sudo apt-get update
	$ sudo apt-get install oracle-java8-installer
# WP4 #
ESSNET work package 4

## FIRST EXAMPLE ##
First example is about counting the number of records in a (subset of) the data  

in the aistest directory, you will find a file called aistest.py  
It reads all data from a subset of files, parses it, throws away the  header and counts the number of lines in the file.  
You start the example by typing:  
>`spark-submit aistest.py`   

at the command line.  

Since i saw some speed differences between running the count in scala (via spark-shell) and via python, i decided to install SBT and create a scala program. For this, the configuration file aistest.sbt is added, together with `src/main/scala/aistest.scala`
Assuming sbt is installed, one can compile this by typing:  
>`sbt package`  

at the command line.

This program reads a filename from the command line. For instance, if you like to count the number of records for december 2015, type:  
> `spark-submit --class "AIStest" target/scala-2.10/ais-test_2.10-0.1.jar /datasets/AIS/Locations/20151231*.csv.gz`  

at the command line.



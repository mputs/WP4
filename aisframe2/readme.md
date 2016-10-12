aisframe.scala

The purpose of this code is to build a reference frame of maritime ships. This is done by pairing MMSI-numbers and corresponding IMO-numbers from the Message-files (since only maritime ships have an IMO-number this filters out other type of ships). Possibly due to glitches in the In the AIS-data, there are a lot faulty MMSI-IMO-pairs. To select only the right pairs, for each MMSI, only the most frequent MMSI-IMO-pair is selected. Of these, only pairs are selected that have a valid MMSI and a valid IMO.

 

https://github.com/mputs/WP4/blob/master/aisframe2/src/main/scala/aisframe.scala

In this code Message-files are read and only MMSI and IMO-couples are selected, segregated by a “,”and a “1”  is added . Then the number of MMSI-IMO pairs is counted by (val koppel). Then this couple is rearranged (val koppel2), and for each MMSI the most frequent pair is selected(val max_mmsi). Finally, this list of MMSI-IMOpairs is filtered for correct IMO’s (MMSI-check not yet build in, val filt_max_mmsi) and then writes the pairs into a textfile (csv).

 

This program reads a filename from the command line. If you want to analyse November 1st,writing the outcome to csv files “Novfirst”, type in the command line:

 

spark-submit --class "AISframe" target/scala-2.10/ais-frame_2.10-0.1.jar datasets/AIS/Messages/20151101*.csv.gz user/yourusername/Novfirst.csv

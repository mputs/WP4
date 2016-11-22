#howto select ships in harbours using spark

we start with a file containing bounding boxes around harbours. the CSV contains five columns: harbour,lat1,lon1,lat2,lon2

This file should be read inside of the driver node:

```scala
val data = io.Source.fromFile("abc.csv").getLines.toArray.map(_.split(","))
```



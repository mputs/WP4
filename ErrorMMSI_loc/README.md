#howto select ships arriving at the harbours using spark

we start with a file containing bounding boxes around harbours. the CSV contains five columns: harbour,lat1,lon1,lat2,lon2

This file should be read inside of the driver node:

```scala
val LocHarb = io.Source.fromFile("abc.csv").getLines.toArray.map(_.split(","))
```
This gives us an array of arrays
as already mentioned, the data is now available on the driver node. we now have distribute it to the worker nodes:

```scala
val brLocHarb = sc.broadcast(data)
```

To use the locations on the worker nodes, we can now use:

```scala
brLocHarb.value
```

which will retreive the array again.

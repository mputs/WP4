#how to select ships arriving at the harbours using spark

we start with a file containing bounding boxes around harbours. the CSV contains five columns: harbour,lat1,lon1,lat2,lon2

This file should be read inside of the driver node:

~~```scala
val LocHarb = io.Source.fromFile("abc.csv").getLines.toArray.map(_.split(","))
```~~
This gives us an array of arrays
as already mentioned, the data is now available on the driver node. we now have distribute it to the worker nodes:

```scala
val brLocHarb = sc.broadcast(LocHarb)
```

To use the locations on the worker nodes, we can now use:

```scala
brLocHarb.value
```

which will retreive the array again.

##locating the harbour of the ship
for all the lat-lon combination we have to figure out in which bounding box they occur.
first we define a function which does the job:
```scala
def findHarbour(lat: Double, lon: Double): String =
{
  val x = brLocHarb.value
    .filter(x=>x(1).toDouble<lat&x(2).toDouble>lat&x(3).toDouble<lon&x(4).toDouble>lon).map(x=>x(0))
  return (if (x.length==0) "SEA" else x(0))
}
```
This function returns "SEA" if the lat-lon is outside of any harbour box. Otherwise the function returns the harbour code.

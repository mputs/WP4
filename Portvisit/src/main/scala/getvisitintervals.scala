import Math._
object visitinterval
{
		def getvisitinterval(arrivals: List[Array[String]], departures: List[Array[String]]):List[Array[Long]] = 
		{
			var ready = false;
			var portvisits = List[Array[Long]]()
			//set initial arrival
			var arrivHarbour = arrivals(0)(0);
			var arrivTime = arrivals(0)(2).toLong;
			var arrivIndex = arrivals(0)(3).toInt;
			var depIndex = -1;
			var depTime = 0.toLong;
			while(!ready)
			{
				var dep = departures.indexWhere(x=>x(0)==arrivHarbour&&x(2).toLong>arrivTime)	
				if (dep> -1)
				{
					depIndex = departures(dep)(3).toInt
					depTime = departures(dep)(2).toLong
					portvisits = portvisits ++ List(Array(arrivTime, depTime))

					var arr = arrivals.indexWhere(x=>x(2).toLong>depTime)
					if(arr== -1) 
					{
						ready = true
					}
					else
					{
						arrivHarbour = arrivals(arr)(0)
						arrivTime = arrivals(arr)(2).toLong
						arrivIndex = arrivals(arr)(3).toInt
					}
				}
				else 
				{
					ready=true;
				}
			}
			return portvisits
		}
		def main(argv: Array[String]): Unit = {
				val data = List(
					Array("SEA", "10", "0"), 
					Array("SEA", "10", "60"), 
					Array("SEA", "15", "120"), 
					Array("SEA", "15", "180"), 
					Array("SEA", "15", "240"), 
					Array("SEA", "15", "300"), 
					Array("AMS", "15", "360"),
					Array("AMS", "15", "470"),
					Array("AMS", "15", "540"),
					Array("AMS", "0",  "600"),
					Array("AMS", "0",  "800"),
					Array("AMS", "0",  "860"),
					Array("AMS", "0",  "900"),
					Array("AMS", "10", "960"),
					Array("SEA", "10", "1000"),

					Array("SEA", "10", "2000"), 
					Array("SEA", "10", "2060"), 
					Array("SEA", "15", "2120"), 
					Array("SEA", "15", "2180"), 
					Array("SEA", "15", "2240"), 
					Array("SEA", "15", "2300"), 
					Array("AMS", "15", "2360"),
					Array("AMS", "15", "2470"),
					Array("AMS", "15", "2540"),
					Array("AMS", "0",  "2600"),
					Array("AMS", "0",  "2800"),
					Array("AMS", "0",  "2860"),
					Array("AMS", "0",  "2900"),
					Array("AMS", "10", "2960"),
					Array("SEA", "10", "3000"),

					Array("SEA", "10", "5000"), 
					Array("SEA", "10", "5060"), 
					Array("SEA", "15", "5120"), 
					Array("SEA", "15", "5180"), 
					Array("SEA", "15", "5240"), 
					Array("SEA", "15", "5300"), 
					Array("ROT", "15", "5360"),
					Array("ROT", "15", "5470"),
					Array("ROT", "15", "5540"),
					Array("ROT", "0",  "5600"),
					Array("ROT", "0",  "5800"),
					Array("ROT", "0",  "5860"),
					Array("ROT", "0",  "5900"),
					Array("ROT", "10", "5960"),
					Array("SEA", "10", "5980"),

					Array("SEA", "10", "6000"), 
					Array("SEA", "10", "6060"), 
					Array("SEA", "15", "6120"), 
					Array("SEA", "15", "6180"), 
					Array("SEA", "15", "6240"), 
					Array("SEA", "15", "6300"), 
					Array("AMS", "15", "6360"),
					Array("AMS", "15", "6470"),
					Array("AMS", "15", "6540"),
					Array("AMS", "0",  "6600"),
					Array("AMS", "0",  "6800"),
					Array("AMS", "0",  "6860"),
					Array("AMS", "0",  "6940"),
					Array("AMS", "10", "6960"),
					Array("SEA", "10", "7000")
				)
				val q = data.zipWithIndex.map(x=>x._1++Array(x._2.toString)).sliding(2).toList.map(_.toArray)
				val departures = q.filter(x=>x(0)(0)!="SEA"&&x(1)(0)=="SEA").map(x=>x(0))
				val arrivals = q.filter(x=>x(0)(0)=="SEA"&&x(1)(0)!="SEA").map(x=>x(1))
				getvisitinterval(arrivals, departures).foreach(x=>{print("\n"+x.mkString(","))})
		}
}	

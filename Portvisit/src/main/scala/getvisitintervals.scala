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
}	

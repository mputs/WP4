import Math._
object visitinterval
{
		def getvisitinterval(arrivals: List[List[String]], departures: List[List[String]]):List[List[Long]] = 
		{
			var ready = false;
			var portvisits = List[Array[Long]]()
			//set initial arrival
			var arrivHarbour = arrivals(0)(0);
			var arrivTime = arrivals(0)(1).toLong;
			var depTime = 0.toLong;
			while(!ready)
			{
				var dep = departures.indexWhere(x=>x(0)==arrivHarbour&&x(1).toLong>arrivTime)	
				if (dep> -1)
				{
					depTime = departures(dep)(1).toLong
					portvisits = portvisits ++ List(Array(arrivTime, depTime))

					var arr = arrivals.indexWhere(x=>x(1).toLong>depTime)
					if(arr== -1) 
					{
						ready = true
					}
					else
					{
						arrivHarbour = arrivals(arr)(0)
						arrivTime = arrivals(arr)(1).toLong
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

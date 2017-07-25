
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.sql.Timestamp;
import java.text._;
import java.util.Date;
import visitinterval._;
import intervals_connect._
//import org.apache.spark.sql.functions._ 

object AISframe
{
 	   

	def main(args: Array[String])
	{
		
		
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val seaships = hdfsprefix + args(0)
		val locharbdata = hdfsprefix + args(1)
		val rawdatafile = hdfsprefix + args(2)
		val outputfile = hdfsprefix + args(3)

		val conf = new SparkConf()
		conf.setAppName("Portvisit")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)
		
		//val LocHarb = io.Source.fromFile("ports_locations.csv").getLines.map(_.split(",")).toArray
		val LocHarb = sc.textFile(locharbdata).map(_.split(",")).collect()
		val brLocHarb = sc.broadcast(LocHarb)
		
		def findHarbour(lat: Double, lon: Double): String =
		{
  			val x = brLocHarb.value
    				.filter(x=>x(1).toDouble<lat&x(2).toDouble>lat&x(3).toDouble<lon&x(4).toDouble>lon).map(x=>x(0));
  			return (if (x.length==0) "SEA" else x(0))
		}
		val seashiplist = sc.textFile(seaships)
				.map(_.split(","))
				.map(x => (x(0), x(2).mkString(",")))

		def Median(x: List[Double]):Double =
		{
			x.sortWith(_<_)(x.length/2)
		}
		
		//element 0= PORT, 1 = timestamp
		def getvisitinterval(arrivals: List[List[String]], departures: List[List[String]]):List[List[Long]] = 
		{
            		val iPORT = 0;
            		val iTIMESTAMP = 1;
			var ready = false;
			var portvisits = List[List[Long]]()
			//set initial arrival 
			var arrivPort = arrivals(0)(iPORT);
			var arrivTime = arrivals(0)(iTIMESTAMP).toLong;
			var depTime = 0.toLong;
			while(!ready)
			{
				var dep = departures.indexWhere(x=>x(iPORT)==arrivPort&&x(iTIMESTAMP).toLong>arrivTime)	
				if (dep> -1)
				{
					depTime = departures(dep)(iTIMESTAMP).toLong
					portvisits = portvisits ++ List(List(arrivTime, depTime))

					var arr = arrivals.indexWhere(x=>x(1).toLong>depTime)
					if(arr== -1) 
					{
						ready = true
					}
					else
					{
						arrivPort = arrivals(arr)(iPORT)
						arrivTime = arrivals(arr)(iTIMESTAMP).toLong
					}
				}
				else 
				{
					ready=true;
				}
			}
			return portvisits
		}

			def expandIntervals(l: List[List[Long]]):List[Long] =
			{
   			 l.map(x=>(x(0) until x(1)+1).toList).flatten
			}
		
               //2015-10-08 22:00:00.001
		val rawdata = sc.textFile(rawdatafile)
				.map(_.split(","))
				.filter(x=>x(0)!="mmsi")
				.map(x => (x(0), Array(x(0), x(1), x(2), x(4), x(8)).mkString(",")))
				.join(seashiplist)
				.map(x => x._2._1.split(","))
				.mapPartitions {
					it => val df = new SimpleDateFormat("yyyy-MM-dd HH:mm"); 
					it.map(x=>x++Array((df.parse(x(4)).getTime/600000).toString)) }
				.map(x=>((x(0),x(5)), (x(1),x(2),x(3),x(4))))
		val data = rawdata.groupByKey()
				.map(x=> (x._1,(
					Median(x._2.toList.map(y=>y._1.toDouble).toList), 
					Median(x._2.toList.map(y=>y._2.toDouble).toList), 
					Median(x._2.toList.map(y=>y._3.toDouble).toList), 
					(x._2.map(y=>y._4).take(16) )
					.take(1).mkString(",")))) 
				.map(x=> (x._1, Array(x._2._1, x._2._2, x._2._3, x._2._4).mkString(","))).saveAsTextFile(outputfile)
		sc.stop()
	}
}

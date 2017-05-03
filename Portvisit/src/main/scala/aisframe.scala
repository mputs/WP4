
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.sql.Timestamp;
import java.text._;
import java.util.Date;
import visitinterval._;
import intervals_connect._
import scala.annotation.tailrec
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
		
		def connectIntervals(l:List[List[Long]]): List[List[Long]] = 
			{
		// tailrecursive helper function
			@tailrec def rec2_connectIntervals(out: List[List[Long]], begin: Long, end: Long, l:List[List[Long]]): List[List[Long]] = 
			{
				// base case: when list l is empty: return the result and terminate
				if(l.length == 0) return List(begin,end)::out  
				if(l.head(0) - end > 1) 
				//inductive step1: when the time between the time intervals is large, just add the last interval
				rec2_connectIntervals(List(begin,end)::out, l.head(0), l.head(1),l.tail)
			else
				//nductive step2: when the time between the time intervals is small, extend the interval
				rec2_connectIntervals(out, begin, l.head(1),l.tail)
			}
		if (l.length > 0)
			//call helper function
			return rec2_connectIntervals(List(): List[List[Long]],l.head(0), l.head(1), l.tail)
		else 
			return List(): List[List[Long]]
			}

		def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
  			val deltaLat = math.toRadians(pointB._1 - pointA._1)
  			val deltaLong = math.toRadians(pointB._2 - pointA._2)
  			val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
  			val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
  			6371 * greatCircleDistance
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
				.map(x=> (x._1, Array(x._2._1, x._2._2, x._2._3, x._2._4, findHarbour(x._2._1, x._2._2))))
		// tuple of ((mmsi, time), Array(lat, lon, speed, harbour))
		//orig data: mmsi timestamp lat lon speed harbour time
		
		val arrdep = data.map(x=>(x._1._1,(x._1._2, x._2)))
			.groupByKey()
			.map(x=>(x._1,x._2.toList.sortWith((a,b)=>a._1<b._1).sliding(2).toArray.filter(x=>x.length>1)))
			.flatMap(x=>x._2.map(y=>((x._1,y(0),y(1)),1)))

		arrdep.cache()
		val arrs = arrdep.filter(x=>x._1._2._2(4)=="SEA"&&x._1._3._2(4)!="SEA" ).map(a=> Array(a._1._1, a._1._3._2(4), a._1._3._1))
		val deps = arrdep.filter(x=>x._1._2._2(4)!="SEA"&&x._1._3._2(4)=="SEA" ).map(a=> Array(a._1._1, a._1._2._2(4), a._1._3._1))
		val ar = arrs.map(x=>((x(0),x(1)),List(x(1),x(2)))).groupByKey().map(x=>(x._1,x._2.toList))
		val de = deps.map(x=>((x(0),x(1)),List(x(1),x(2)))).groupByKey().map(x=>(x._1,x._2.toList))
		val grouped = ar.join(de)
		
		
		
// ((mmsi, port), List(List(begin, end),..)
val intervals = grouped.map(x=>(x._1,getvisitinterval(x._2._1.map(_.map(_.toString)), x._2._2.map(_.map(_.toString))))).filter(x=> x._2.length!=0).map(x=>(x._1, connectIntervals(x._2)))

val expendedintervals = intervals
		.flatMap(x=>x._2.flatMap(y=>(y(0) until y(1)+1).toList.map(z=>((x._1._1.toString, z.toString), (x._1._2, y.mkString(","))))))
val expandedintervals = intervals
		.flatMap(x=>expandIntervals(x._2).map(y=>((x._1._1, y.toString),  (x._1._2, x._2(0)(0),x._2(0)(1)))))
		.groupByKey()
		.map(x=>(x._1.toString,x._2.toList))


val arrdep2 = data2.map(x=>(x._1,(x._2(0), x._2(1),x._2(2), x._2(3))))
val exp_int_compl = expendedintervals.join(arrdep2)
val int_speed = exp_int_compl.map(x=> ((x._1._1,x._2._1._2),(x._2._2._1),(x._2._2._2),(x._2._2._3),(x._1._2)))//.saveAsTextFile(outputfile)
val distance2= int_speed.map(x=>(x._1,(x._5, x._2, x._3)))
		.groupByKey().map(x=>(List(x._1),x._2.toList.sortWith((a,b)=>a._1<b._1).map(l=>List(l._1,l._2,l._3))))
val distance3 = distance2.map(q=>(q._1,q._2.sliding(2).toList.map(x=>haversineDistance((x(0)(2).asInstanceOf[Double],x(0)(1).asInstanceOf[Double]), 
                                                    (x(1)(2).asInstanceOf[Double],x(1)(1).asInstanceOf[Double]))).sum))
						    
def tuple3ToList[T](t: (T,T,T)): List[T] = List(t._1, t._2,t._3)
val arrdep3 = data2.map(x=>(x._1.toString,(x._2(0), x._2(1),x._2(2), x._2(3))))
val exp_int_compl = expandedintervals.join(arrdep3)
val int_speed = exp_int_compl.map(x=> (x._1.split(",")(0).substring(1)::tuple3ToList(x._2._1(0)),x._2._2)).groupByKey().map(x=>(x._1,x._2.toList.sortWith((a,b)=>a._1<b._1).map(l=>List(l._1,l._2,l._3,l._4, l._5))))

val dist = int_speed.map(q=>(q._1,q._2.sliding(2).toList.map(x=>haversineDistance((x(0)(2).asInstanceOf[Double],x(0)(1).asInstanceOf[Double]), 
                                                    (x(1)(2).asInstanceOf[Double],x(1)(1).asInstanceOf[Double]))).sum,q._2.sliding(1).toList.map(x=>(x(0)(1).asInstanceOf[Double],x(0)(3).asInstanceOf[Double]))))//.saveAsTextFile(outputfile)
val stops = int_speed.filter(x=>(x._2.sliding(1).toList.map(x=>(x(0)(3))).count(_.asInstanceOf[Double]>0.9))>1).saveAsTextFile(outputfile)//.map(x=> x._1 + ""+x._2.mkString(","))
			

		sc.stop()
	}
}

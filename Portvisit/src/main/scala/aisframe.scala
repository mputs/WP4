
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.sql.Timestamp;
import java.text._;
import java.util.Date;
import visitinterval._;

object AISframe
{
 	   

	def main(args: Array[String])
	{
		
		
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val seaships = hdfsprefix + args(0)
		//val locharbdata = hdfsprefix + args(1)
		val rawdatafile = hdfsprefix + args(1)
		val outputfile = hdfsprefix + args(2)
		//val outputfile_arr = hdfsprefix + args(2)
		//val outputfile_dep = hdfsprefix + args(3)
		
		//val seaships = "hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/defframe6all.csv"
		//val rawdatafile = "hdfs://namenode.ib.sandbox.ichec.ie:8020/datasets/AIS/Locations/2015120100*.csv.gz"
		//val outputfile = "hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/qtest.csv"


		val conf = new SparkConf()
		conf.setAppName("Portvisit")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)
		
		//val seashipdata = sc.textFile(seaships)
		//	.map(_.split(","))
			//.filter(x => x(2)=="1")
		
		//val LocHarb = io.Source.fromFile("ports_locations.csv").getLines.map(_.split(",")).toArray
		val LocHarb = sc.textFile("hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/ports_locations.csv").map(_.split(",")).collect()
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
		val data = sc.textFile(rawdatafile).map(_.split(","))
			.filter(x=>x(0)!="mmsi")
			//.filter(x=>x(0)=="374846000")
			.map(x => (x(0), Array(x(0), x(1), x(2), x(4), x(8)).mkString(","))) // mmsi, lat, lon, speed, timestamp 
			.join(seashiplist)
			.map(x => x._2._1.split(","))
			.mapPartitions{it =>
				       val df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				       it.map(x=>x++Array((df.parse(x(4)).getTime/600000).toString))
			} //mmsi, lat, lon, speed, timestamp, time
			.map(x=>((x(0),x(5)), (x(1),x(2),x(3),x(4)))) // ((mmsi, time), (lat, lon, speed))
			.groupByKey()
			.map(x=> (x._1,(
				Median(x._2.toList.map(y=>y._1.toDouble).toList), 
				Median(x._2.toList.map(y=>y._2.toDouble).toList), 
				Median(x._2.toList.map(y=>y._3.toDouble).toList),
				(x._2.map(y=>y._4).take(16) ).take(1).mkString(","))))
			.map(x=> (x._1, Array(x._2._1, x._2._2, x._2._3, x._2._4, findHarbour(x._2._1, x._2._2))))
			// tuple of ((mmsi, time), Array(lat, lon, speed, harbour))
		//orig data7.map(a=>Array(a(1), a(2),a(3),a(4),a(5),a(6)).mkString(",")).saveAsTextFile("hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/schipje.csv")
		//orig data: mmsi timestamp lat lon speed harbour time

		//1 val enters = data.map(x=>(x._1._1, (x._1._2, x._2)))
		//1		.groupByKey()
		//1		.map(x=>(x._1,x._2.toList
		//1			 	.sortWith((a,b)=>a._1<b._1)
		//1			 	.map(_._2(3))
		//1			 	.sliding(2)
		//1			 	.toArray
		//1			 	.filter(x=>x.length>1)))
		//1		.flatMap(x=>x._2.map(y=>((x._1,y(0),y(1)),1)))
		//1		.reduceByKey(_+_)	
		//1		.filter(x=>x._1._2!="SEA" || x._1._3!="SEA") 
				//data2.map(a=>Array(a._1,a._2.mkString(",")).mkString(",")).saveAsTextFile(outputfile)
				//enters.map(a=> Array(a._1 ,a._2).mkString(",")).saveAsTextFile(outputfile);
		
		//1val arrivalsmtijd= data.map(x=>(x._1._1,(x._1._2, x._2)))
		//1		.groupByKey()
		//1		.map(x=>(x._1,x._2.toList
		//1			 	.sortWith((a,b)=>a._1<b._1)
		//1			 	.map(_._2)
		//1			 	.sliding(2)
		//1			 	.toArray
		//1			 	.filter(x=>x.length>1)))
		//1		.flatMap(x=>x._2.map(y=>((x._1,y(0),y(1)),1)))
		//1		.filter(x=>x._1._2(4)=="SEA"&&x._1._3(4)!="SEA" )
		
		val arr_MMSI_time= data.map(x=>(x._1._1,(x._1._2, x._2)))
				.groupByKey()
				.map(x=>(x._1,x._2.toList.sortWith((a,b)=>a._1<b._1)
				.sliding(2)
				.toArray
				.filter(x=>x.length>1)))
				.flatMap(x=>x._2.map(y=>((x._1,y(0),y(1)),1)))
				//.filter(x=>x._1._2._2(4)=="SEA"&&x._1._3._2(4)!="SEA" )
				.filter(x=>x._1._2._2(4)=="SEA"&&x._1._3._2(4)=="AMS" )
		val arrs = arr_MMSI_time.map(a=> Array(a._1._1, a._1._3._2(4), a._1._3._1).mkString(","))//.saveAsTextFile(outputfile_arr);
				//arrivalsmtijd.map(a=> Array(a._1._1, a._1._2.mkString(","), a._1._3.mkString(",")).mkString(","))
						//.saveAsTextFile(outputfile)
		
		val dep_MMSI_time= data.map(x=>(x._1._1,(x._1._2, x._2)))
				.groupByKey()
				.map(x=>(x._1,x._2.toList.sortWith((a,b)=>a._1<b._1)
				.sliding(2)
				.toArray
				.filter(x=>x.length>1)))
				.flatMap(x=>x._2.map(y=>((x._1,y(0),y(1)),1)))
				//.filter(x=>x._1._2._2(4)!="SEA"&&x._1._3._2(4)=="SEA" )
				.filter(x=>x._1._2._2(4)=="AMS"&&x._1._3._2(4)=="SEA" )
		val deps = dep_MMSI_time.map(a=> Array(a._1._1,a._1._2._1).mkString(","))//.saveAsTextFile(outputfile_dep);
		
		val ar = arrs.map(_.split(",")).map(x=>((x(0),x(1)),List(x(1),x(2)))).groupByKey().map(x=>(x._1,x._2.toList))
		val de = deps.map(_.split(",")).map(x=>((x(0),x(1)),List(x(1),x(2)))).groupByKey().map(x=>(x._1,x._2.toList))
		val grouped = ar.join(de)
		val intervals = grouped.map(x=>(x._1,getvisitinterval(x._2._1, x._2._2))).filter(x=> x._2.length!=0)
		val expandedintervals = intervals.flatMap(x=>expandIntervals(x._2).map(y=>((x._1._1, y),  (x._1._2, x._2(0)(0),x._2(0)(1)))))
		expandedintervals.saveAsTextFile(outputfile)
		//1 val departuresmtijd= data.map(x=>(x._1._1,(x._1._2, x._2)))
		//1		.groupByKey()
		//1		.map(x=>(x._1,x._2.toList
		//1			 	.sortWith((a,b)=>a._1<b._1)
		//1			 	.map(_._2)
		//1			 	.sliding(2)
		//1			 	.toArray
		//1			 	.filter(x=>x.length>1)))
		//1		.flatMap(x=>x._2.map(y=>((x._1,y(0),y(1)),1)))
		//1		.filter(x=>x._1._2(4)!="SEA"&&x._1._3(4)=="SEA" )
		//1		departuresmtijd.map(a=> Array(a._1._1, a._1._2.mkString(","), a._1._3.mkString(",")).mkString(","))
		//1				.saveAsTextFile(outputfile)
		
		
		
		//val enters = ship_orig.flatMap(x=>x._2.map(y=>y(4))
		//			       		.toArray.sliding(2).toArray
		//			       		.filter(x=>x.length>1)
		//			       		.map(x=>((x(0),x(1)),1)) )
		//			       .reduceByKey(_+_)
		//enters.map(a=> Array(a._1._1, a._1._2,a._1._3,a._2).mkString(",")).saveAsTextFile(outputfile);
		
		


		//val getshipsinharbour = ship_orig.flatMap(x=>x._2.map(y=>y(4))
		//			       		.toArray.sliding(2).toArray
		//			       		.filter(z=>z.length>1)
		//			       		.map(z=>((z(0),z(1),x._1),1)) )
		//					//.filter(x=> x._1._3 =="304783000")
		//			       //.filter(x=>x._1._1!="SEA" || x._1._2!="SEA")
		//			       .reduceByKey(_+_)
		//getshipsinharbour.map(a=> Array(a._1._1, a._1._2,a._1._3, a._2).mkString(",")).saveAsTextFile(outputfile);

		sc.stop()
	}
}



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.sql.Timestamp;
import java.text._;
import java.util.Date;

object AISframe
{
 	   

	def main(args: Array[String])
	{
		
		
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val seaships = hdfsprefix + args(0)
		//val locharbdata = hdfsprefix + args(1)
		val rawdatafile = hdfsprefix + args(1)
		val outputfile = hdfsprefix + args(2)
		
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
		
               //2015-10-08 22:00:00.001
		val data = sc.textFile(rawdatafile).map(_.split(","))
			.filter(x=>x(0)!="mmsi")
			//.filter(x=>x(0)=="232002165")
			.map(x => (x(0), Array(x(0), x(1), x(2), x(4), x(8)).mkString(","))) // mmsi, lat, lon, speed, timestamp 
			.join(seashiplist)
			.map(x => x._2._1.split(","))
			.mapPartitions{it =>
				       val df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				       it.map(x=>x++Array((df.parse(x(4)).getTime/60000).toString))
			} //mmsi, lat, lon, speed, timestamp, time
			.map(x=>((x(0),x(5)), (x(1),x(2),x(3)))) // ((mmsi, time), (lat, lon, speed))
			.groupByKey()
			.map(x=> (x._1,(
				Median(x._2.toList.map(y=>y._1.toDouble).toList), 
				Median(x._2.toList.map(y=>y._2.toDouble).toList), 
				Median(x._2.toList.map(y=>y._3.toDouble).toList) )) )
			.map(x=> (x._1, (x._2._1, x._2._2, x._2._3, findHarbour(x._2._1, x._2._2))))
			.map(x=> ((x._1._1),(x._2._2, x._2._1, x._2._2,x._2._3,x._2._4)) )
			// tuple of ((mmsi), (time, lat, lon, speed, harbour))
		//orig data7.map(a=>Array(a(1), a(2),a(3),a(4),a(5),a(6)).mkString(",")).saveAsTextFile("hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/schipje.csv")
		//orig data: mmsi timestamp lat lon speed harbour time

			


		
		//val shipframe = data.map(x => (x(0), Array(x(1), x(2)).mkString(",")))
		val ship_orig = data.map(x=>(x(0).toString, Array(x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString)))
			.groupByKey()
			.map(x=>(x._1,x._2.toList.sortWith((a,b)=>a(5).toLong<b(5).toLong))).cache


		// ship_orig: mmsi, (timestamp, lat, lon, speed, harbour, time)*

		

		//val enters = ship_orig.flatMap(x=>(x._2.map(y=>y(4)).toArray.sliding(2).filter(x(0)!=x(1) && x(1)!="SEA" ).map(x=>x(1)),1)).reduceByKey
		
		val enters = data.groupByKey()
			.flatMap(x=>x._2.toList.sortWith((a,b)=>a._1.toLong<b._1.toLong)
						.map(y=>y._5)
						.sliding(2)
						.toArray
						.filter(x=>x.length>1)
						.map(y=>((x._1,y(0),y(1)),1))   ).reduceByKey(_+_)
		
		//val enters = ship_orig.flatMap(x=>x._2.map(y=>y(4))
		//			       		.toArray.sliding(2).toArray
		//			       		.filter(x=>x.length>1)
		//			       		.map(x=>((x(0),x(1)),1)) )
		//			       	.filter(x=>x._1._1!="SEA" || x._1._2!="SEA")
		//			       .reduceByKey(_+_)
		//enters.map(a=> Array(a._1._1, a._1._2,a._2).mkString(",")).saveAsTextFile(outputfile);

		


		val getshipsinharbour = ship_orig.flatMap(x=>x._2.map(y=>y(4))
					       		.toArray.sliding(2).toArray
					       		.filter(z=>z.length>1)
					       		.map(z=>((z(0),z(1),x._1),1)) )
							//.filter(x=> x._1._3 =="304783000")
					       //.filter(x=>x._1._1!="SEA" || x._1._2!="SEA")
					       .reduceByKey(_+_)
		getshipsinharbour.map(a=> Array(a._1._1, a._1._2,a._1._3, a._2).mkString(",")).saveAsTextFile(outputfile);

		sc.stop()
	}
}


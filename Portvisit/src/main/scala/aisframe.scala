
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
    				.filter(x=>x(1).toDouble<lat&x(2).toDouble>lat&x(3).toDouble<lon&x(4).toDouble>lon).map(x=>x(0))
  			return (if (x.length==0) "SEA" else x(0))
		}
		val seashiplist = sc.textFile(seaships)
				.map(_.split(","))
				.map(x => (x(0), x(2).mkString(",")))
		
               //2015-10-08 22:00:00.001
		val data = sc.textFile(rawdatafile).map(_.split(","))
			.filter(x=>x(0)!="mmsi")
			.map(x => (x(0), Array(x(0), x(1), x(2), x(4), x(8)).mkString(",")))
			.join(seashiplist)
			.map(x => x._2._1.split(","))
			.map(x=> ((x(0), x(4).substring(0,x(4).lastIndexOf(":") ) ),(x(1).toDouble, x(2).toDouble, x(3).toDouble,1.toInt )))
			.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4))
			.map(x=>Array(x._1._1,x._1._2,(x._2._1/x._2._4).toString, (x._2._2/x._2._4).toString,(x._2._3/x._2._4).toString))
			.mapPartitions{it =>
				       val df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				       it.map(x=>x++Array(findHarbour(x(2).toDouble,x(3).toDouble),df.parse(x(1)).getTime.toString))
			}

		//data: mmsi timestamp lat lon speed harbour time


		//val shipframe = data.map(x => (x(0), Array(x(1), x(2)).mkString(",")))
		val ship_orig = data.map(x=>(x(0).toString, Array(x(1).toString,x(2).toString,x(3).toString,x(4).toString,x(5).toString,x(6).toString)))
			.groupByKey()
			.map(x=>(x._1,x._2.toList.sortWith((a,b)=>a(5).toLong<b(5).toLong)))


		// ship_orig: mmsi, (timestamp, lat, lon, speed, harbour, time)*

		

		//val enters = ship_orig.flatMap(x=>(x._2.map(y=>y(4)).toArray.sliding(2).filter(x(0)!=x(1) && x(1)!="SEA" ).map(x=>x(1)),1)).reduceByKey
		val enters = ship_orig.flatMap(x=>x._2.map(y=>y(4)).toArray.sliding(2).toArray.filter(x=>x.length>1).map(x=>((x(0),x(1)),1))).filter(x=>x._1._1!="SEA" || x._1._2!="SEA").reduceByKey(_+_)
		enters.map(a=> Array(a._1._0, a._1._1, a._1._2,a._2).mkString(",")).saveAsTextFile(outputfile);


		sc.stop()
	}
}

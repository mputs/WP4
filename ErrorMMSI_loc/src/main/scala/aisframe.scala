
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf




object AISframe
{
		
        
	def main(args: Array[String])
	{
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val seaships = hdfsprefix + args(0)
		val locdatafile = hdfsprefix + args(1)
		val locAreadata = hdfsprefix + args(2)
		val outputfile = hdfsprefix + args(3)
		
		
		//val seaships = "file:////home/tessa/Desktop/AIS/Data/defframe6all.csv"
		//val locdatafile = "file:////home/tessa/Desktop/AIS/Data/2015100904*.csv.gz"
		//val outputfile = "file:////home/tessa/Desktop/AIS/Data/AMS_1dec.csv"
		//val locAreadata = "file:////home/tessa/Desktop/AIS/Data/ports_locations_AMS.csv"
		
		//val tfiles = "hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/defframe6all.csv"
		//val locdatafile = "hdfs://namenode.ib.sandbox.ichec.ie:8020/datasets/AIS/Locations/20151101*.csv.gz"
		//val outputfile = "hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/AMS_1dec.csv"
		
		
		val LocArea = sc.textFile(locAreadata).map(_.split(",")).collect()
		val brArea = sc.broadcast(LocArea)
		val seashiplist = sc.textFile(seaships).map(_.split(",")).map(x => (x(0), x(2).mkString(",")))
		
		
		val conf = new SparkConf()
		conf.setAppName("AIS-frame")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)
		
		def findArea(lat: Double, lon: Double): String =
			{
  			val x = brArea.value
    				.filter(x=>x(1).toDouble<lat&x(2).toDouble>lat&x(3).toDouble<lon&x(4).toDouble>lon).map(x=>x(0));
  			return (if (x.length==0) "SEA" else x(0))
			}

		val rawdata = sc.textFile(locdatafile).map(_.split(","))
							.filter(x=>x(0)!="mmsi")
							.map(x => (x(0), Array(x(0), x(1), x(2), x(4), x(8))
							.mkString(",")))
							.join(seashiplist)
							.map(x => x._2._1.split(","))
		val NrShipsArea = rawdata.map(x=> ((x(0),findArea(x(1).toDouble, x(2).toDouble)),1))
					.filter(x=>x._1._2!="SEA")
					.reduceByKey(_+_)
					.saveAsTextFile(outputfile);

		sc.stop()
	}
}

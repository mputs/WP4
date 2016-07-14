
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import latlon._

object countuniq
{
	val nw = new Location(72.366904, -30.366904)
	val se = new Location(21.854758, 62.174983)
	val grid = new LatLonGrid(nw, se, 100);


	def main(args: Array[String])
	{
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val tfiles = hdfsprefix + args(0)
		val outputfile = hdfsprefix + args(1)

		val conf = new SparkConf()
		conf.setAppName("AIS-frame")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)
		val bcGrid = sc.broadcast(grid)

		val data = sc.textFile(tfiles)
			.map(_.split(","))
			.filter(x=> x(0)!="mmsi")
		val keys = data.map(x => bcGrid.value.getlonidx(x(1).toDouble)+","+bcGrid.value.getlatidx(x(2).toDouble)+","+mmsi)
			.distinct()
			.map(_split(","))
			.filter(x=>x(0)!="-1" && x(1)!="-1")
			.map(x=>(x(0)+","+x(1), 1))
			.reduceByKey((x,y)=>x+y)

	
			
		
		imos.saveAsTextFile(outputfile);
		
		sc.stop()
	}
}


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object AISframe
{
	def main(args: Array[String])
	{
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val tfiles = hdfsprefix + args(0)
		val outputfile = hdfsprefix + args(1)

		val conf = new SparkConf()
		conf.setAppName("AIS-frame")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)

		val data = sc.textFile(tfiles)
			.map(_.split(","))
			.filter(x=> x(0)!="mmsi")
		
		val imos = data.map(x => x(1)).distinct().filter(x=>x.length==7);
		imos.saveAsTextFile(outputfile);
		
		sc.stop()
	}
}

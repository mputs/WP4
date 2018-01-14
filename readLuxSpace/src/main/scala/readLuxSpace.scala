import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import PayLoad._

object readLuxSpace
{

	def splitandmerge(s: Iterator[String]): Iterator[Array[String]] =
	{
			(s++" ").map(_
				.toString
				.split(","))
			.filter(x=>x.length==7)
			.sliding(2) // create iterator of subsequent messages
			.filter(x=>(x(0)(0)(0)!='!') && (x(1)(0)(0)!='\\')) // filter out the wrongly merged messages
			.map(x=>if((x(0)(1)=="2") && (x(0)(2)=="1") && (x(1).length==7)) 
				Array(x(0)(0),"1","1",x(0)(3),x(0)(4),x(0)(5)+x(1)(5),x(0)(6)) // merge long messages
			else 
				x(0).toArray)
			.filter(x=>x(1)=="1")

	}
	
	def choptimestamp(s: String):String=
	{
		s.split(':').tail.head.split('*').head
	}

	def main(args: Array[String])
	{
		//val tfiles = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" + args(0)
		val tfiles = args(0);
		val conf = new SparkConf()
		conf.setAppName("AIS-test")
		//conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)

		val input = sc.textFile(tfiles).filter(x=>x.length!=0);
		val data = input.mapPartitions(x=>splitandmerge(x))
		println("")
		println(data.count)
		println("")
		
		val m123 = data.filter(x=>(istype(x(5))==1) || (istype(x(5))==2) || (istype(x(5))==3))
			.map(x=>(choptimestamp(x(0)),to123(convert6string(x(5)))))
		val m5 = data.filter(x=>istype(x(5))==5)
			.map(x=>(choptimestamp(x(0)),to5(convert6string(x(5)))))

		m5.map(x=>List(x._1,
				x._2.MMSI.toString,
				x._2.IMO.toString,
				x._2.CallSign,
				x._2.shiptype.toString,
				x._2.to_bow.toString,
				x._2.to_stern.toString,
				x._2.to_port.toString,
				x._2.to_starboard.toString,
				x._2.draught).mkString(","))
			.saveAsTextFile("qqq.csv");

		sc.stop()
	}
} 

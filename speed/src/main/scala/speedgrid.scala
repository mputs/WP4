
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import LAEA._

// mmsi,lon,lat,accuracy,speed,course,rotation,status,timestamp
object countuniq
{
	val nw = new Location(72.366904, -30.366904)
	val se = new Location(21.854758, 62.174983)
//	val grid = new LatLonGrid(nw, se, 100);


	def tuple2toList[T](t: Tuple2[T,T]):List[T] = List(t._1, t._2)

	def parsetimestamp(x: String):String = 
	{
		val dt = x.split(" ");
		val t = dt(1).split(":");
		return dt(0); // + ","+t(0);
	}

	def main(args: Array[String])
	{
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val tfiles = hdfsprefix + args(0)
	//	val seaships = hdfsprefix + args(1)
		val outputfile = hdfsprefix + args(1)

		val conf = new SparkConf()
		conf.setAppName("speedcells")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)

	//	val seashiplist = sc.textFile(seaships).map(_.split(",")).map(x => (x(0), x(2).mkString(",")))


		val data = sc.textFile(tfiles)
			.map(_.split(","))
			.filter(x=> x(0)!="mmsi")
			//.map(x=> (x(0),x))
			//.join(seashiplist)
			//.map(x => x._2._1)
		val q = data.mapPartitions{it =>
				val grid = new LAEAGrid(nw,se,200);
				// it.map(x=>grid.getlatidx(x(2).toDouble)+","+grid.getlonidx(x(1).toDouble)+","+parsetimestamp(x(8))+";"+x(0))
				it.map(x=>(tuple2toList(grid.getlatlonmid(x(2).toDouble, x(1).toDouble)).mkString(",")+","+parsetimestamp(x(8))+";"+x(0),List(1.0/(x(5).toDouble),1.0)))
			}
			.reduceByKey((x,y)=>List(x(0)+y(0),x(1)+y(1)))
			.map(x=>(x._1.split(";")(0).split(","),(x._2(1)/x._2(0),1.0)))
			.filter(x=>x._1(0).toDouble > -900 && x._1(1).toDouble > -900)
			.map(x=>(x._1.mkString(","),x._2))
			.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
			.map(x=>x._1+","+(x._2._1/x._2._2).toString)
		println (">>>>>>> count is "+q.count() + "<<<<<<<")

		q.saveAsTextFile(outputfile);
		
		sc.stop()
	}
}

/*
		val keys = data.map(x => bcGrid.value.getlonidx(x(1).toDouble)+","+bcGrid.value.getlatidx(x(2).toDouble)+","+mmsi)
			.distinct()
			.map(_split(","))
			.filter(x=>x(0)!="-1" && x(1)!="-1")
			.map(x=>(x(0)+","+x(1), 1))
			.reduceByKey((x,y)=>x+y)
*/

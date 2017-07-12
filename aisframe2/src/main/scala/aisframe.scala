
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object AISframe
{
	def checkimo(imo: String): Boolean =
	{
		if(imo.length!=7)
			return false;
		val x = imo.map(_.toInt-'0'.toInt); 
		if (((7 to 2 by -1 zip x.dropRight(1)).map{case(a,b)=>a*b}.sum-x(6))%10==0)
			return true;
		return false	
	}
    
    def checkmmsi(mmsi: String): Boolean = 
    {
        if(mmsi.length!=9)
            return false;
        return true
    }   
        
	def main(args: Array[String])
	{
		val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		val tfiles = hdfsprefix + args(0)
		val outputfile = hdfsprefix + args(1)

		val conf = new SparkConf()
		conf.setAppName("AIS-frame")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)

		// all message-data are read, except the header lines
		val data = sc.textFile(tfiles)
			.map(_.split(","))
			.filter(x=> x(0)!="mmsi")
	//Only the first 2 colums (mmsi and imo) are selected and turned into a map
	//val imommsi = data.map(x => Array(x(0),x(1)));
	val imommsi = data.map(x => Array(x(0),x(1),x(2),x(11)));	
		

	//These couples are made into a string and for each line a "1" is added.  
	//Then all couples are aggregated on mmsi-imo couple and number of couples
        val koppel = imommsi.map(z => (z.mkString(","), 1)).reduceByKey(_+_);
	
	//the mmsi-imo couple is broken and imo and number of couples are coupled 
        val koppel2 = koppel.map(b => b._1.split(",")++Array(b._2)).map(c=>(c(0),c.slice(1,4)));
	
	//on the basis of key(mmsi), value (imo-count) pair the largest value (count) is chosen 
	//by comparing the first line with the second and so on..
        val max_mmsi = koppel2.reduceByKey((b, c)=> if(b(4).toString.toInt>c(4.toString.toInt) b else c).saveAsTextFile(outputfile);
	
	//for each most frequent mmsi-imo pair, the validity of the imo is checked
       // val filt_max_mmsi = max_mmsi.filter(y=>checkimo(y._2(0).toString));
       // filt_max_mmsi.map(a=> Array(a._1,a._2.mkString(",")).mkString(",")).saveAsTextFile(outputfile);
	//	sc.stop()
					   
	}
}

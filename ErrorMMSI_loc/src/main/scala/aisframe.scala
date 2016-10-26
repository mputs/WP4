
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
		//val hdfsprefix = "hdfs://namenode.ib.sandbox.ichec.ie:8020/" 
		//val tfiles = hdfsprefix + args(0)
		//val outputfile = hdfsprefix + args(1)
		
		val tfiles = "hdfs://namenode.ib.sandbox.ichec.ie:8020/user/tessadew/defframe.csv"
		val locdatafile = "hdfs://namenode.ib.sandbox.ichec.ie:8020/datasets/AIS/Locations/2015110200*.csv.gz"


		val conf = new SparkConf()
		conf.setAppName("AIS-frame")
		conf.setMaster("yarn-client")
		val sc = new SparkContext(conf)

		val data = sc.textFile(tfiles)
			.map(_.split(","))
			.filter(x => x(2)=="1")
		
		val locdata = sc.textFile(locdatafile)
			.map(_.split(","))
			.filter(x=> x(0)!="mmsi")


		val single_mmsi = data.map(x => Array(x(0), x(2)));
		val loc_orig = locdata.map(x => Array(x(0), x(1), x(2)));
					   
		val couples = loc_orig.join(single_mmsi);
					   
		val alldata = loc_orig ++ single_mmsi;
		val result = alldata.filterByKey(x => "1" in x);


        val koppel = imommsi.map(z => (z.mkString(","), 1)).reduceByKey(_+_);
        val koppel2 = koppel.map(b => b._1.split(",")++Array(b._2)).map(c=>(c(0),c.slice(1,3)));
        val max_mmsi = koppel2.reduceByKey((b, c)=> if(b(1).toString.toInt>c(1).toString.toInt) b else c).cache;

        //val aantalmmsi = max_mmsi.map(z => (z._1.toString.toInt,1)).reduceByKey(_+_).filter(x=>x._2 > 1).cache;
        //aantalmmsi.map(a=> Array(a._1,a._2).mkString(",")).saveAsTextFile(outputfile);
        //println ("dubbele mmsi = " + aantalmmsi.count);
        val filt_max_mmsi = max_mmsi.filter(y=>checkimo(y._2(0).toString));
        filt_max_mmsi.map(a=> Array(a._1,a._2.mkString(",")).mkString(",")).saveAsTextFile(outputfile);
		sc.stop()
	}
}

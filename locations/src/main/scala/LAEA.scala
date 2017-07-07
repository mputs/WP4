import scala.math.{Pi, sqrt, sin, cos, asin, atan}

object LAEA
{
	class LAEATransformer (lat_orig:Double, lon_orig:Double)
	{
		

		def this() = this(.5*Pi, 0);
		// default constructor for northern hemisphere

		//lat_orig (phi1) = .5*Pi;
		//lon_orig (lambda0) = 0;  


		def Project ( lat: Double, lon: Double ):Tuple2[Double, Double] = 
		{
			val k:Double = sqrt(2.0/(1.0+sin(lat_orig)*sin(lat) + cos(lat_orig)*cos(lat)*cos(lon-lon_orig)));
			val x:Double = k * cos(lat)*sin(lon-lon_orig);
			val y:Double = k * (cos(lat_orig)*sin(lat) -sin(lat_orig)*cos(lat)*cos(lon-lon_orig));
			return (x,y);
			
		}
		def Project ( latlon: Tuple2[Double, Double] ): Tuple2[Double, Double] = 
		{
			Project (latlon._1, latlon._2);
		}


		def Inverse ( x: Double, y: Double):Tuple2[Double, Double] = 
		{
			val rho:Double = sqrt(x*x+y*y);
			val c:Double = 2*asin(0.5*rho);
			
			val lat = asin(cos(c)*sin(lat_orig)+y*sin(c)*cos(lat_orig)/rho);
			val lon = lon_orig + atan(x*sin(c) / (rho*cos(lat_orig)*cos(c)-y*sin(lat_orig)*sin(c)));
			return (lat,lon)
			
		}
		def Inverse ( xy: Tuple2[Double, Double] ): Tuple2[Double, Double] = 
		{
			Inverse (xy._1, xy._2);
		}
		


	}

}


import scala.math.{Pi, sqrt, sin, cos, asin, atan, max, min}

object LAEA
{

	class Location(lattitude: Double, longitude: Double) {
		val lat = lattitude;
		val lon = longitude;
		

		def this(loc: Array[Double]) = 
		{
			this(loc(0), loc(1));
		}
		
		def +(operand: Location): Location = 
		{
			new Location(lat + operand.lat, lon + operand.lon);
		}

		def -(operand: Location): Location = 
		{
			new Location(lat -operand.lat, lon - operand.lon);
		}
		
		def /(operand: Double): Location = 
		{
			new Location(lat / operand, lon / operand);
		}

		override def toString(): String = "(" + lat + ", " + lon + ")";
		def apply(i: Int): Double = 
		{
			if (i==0) 
			{
				return lat;
			}
			else if (i==1)
			{
				return lon;
			}
			throw new ArrayIndexOutOfBoundsException("use 0 for lat and 1 for lon");
		}

		
	
		
	}

	class LAEATransformer (lat_orig:Double, lon_orig:Double)
	{
		

		def this() = this(.5*Pi, 0);
		// default constructor for northern hemisphere

		//lat_orig (phi1) = .5*Pi;
		//lon_orig (lambda0) = 0;  


		def Project ( latdeg: Double, londeg: Double ):Tuple2[Double, Double] = 
		{
			val lat = Pi*latdeg/180.0;
			val lon = Pi*londeg/180.0;
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
			return (180*lat/Pi,180*lon/Pi)
			
		}
		def Inverse ( xy: Tuple2[Double, Double] ): Tuple2[Double, Double] = 
		{
			Inverse (xy._1, xy._2);
		}

		def Project ( latlon: Location): Tuple2[Double, Double] = 
		{
			Project(latlon(0), latlon(1));
		}

		


	}
	
	class LAEAGrid (nw: Location, se: Location, gridsize: Int)
	{
		val laea = new LAEATransformer();


		val (x1,y1) = laea.Project(nw);
		val (x2,y2) = laea.Project(se);
		
		
		val xmin = min(x1,x2);
		val xmax = max(x1,x2);
		val ymin = min(y1,y2);
		val ymax = max(y1,y2);
		
		val dx = (xmax-xmin)/gridsize;
		val dy = (ymax-ymin)/gridsize;

		def getlatlonidx(lat: Double, lon: Double): Tuple2[Int,Int] = 
		{
			val (xi,yi)  = laea.Project(lat,lon);
			if (xi > xmin && xi < xmax && yi > ymin && yi > ymax)
				return (((xi-xmin)/dx).toInt, ((yi-ymin)/dy).toInt);
			else
				return (-1,-1)
		}

		def getlatlonmid(lat: Double, lon: Double): Tuple2[Double, Double] = 
		{
			val (xi, yi) = getlatlonidx(lat,lon);
			if(xi==-1 || yi==-1) 
				return (xi, yi, -999.0,-999.0)
			else
			{
				val (clat, clon) = laea.Inverse(xmin+dx*(xi+0.5), ymin+dy*(yi+0.5));
				return (xi, yi, clat, clon);
			}
		}
	}
}


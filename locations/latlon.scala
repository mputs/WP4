import java.lang.Math._
var nw = new Array[Double](2)
nw(0) = 72.366904; nw(1) = -30.637513
var se = new Array[Double](2)
se(0) = 21.854758; se(1) = 62.174983

var mid = (se zip nw).map{case(a,b)=>(a+b)/2}

var radius = new Array[Double](2)
radius(0) = 6378.1; radius(1)= 6356.8

def Rphi(lat: Double): Double = 
{
	var l = lat / 90;
	return l*radius(0) + (1-l)*radius(1)
}


var dl = 10/(Rphi(mid(0))*cos(mid(0)*PI/180)*PI/180)
var f = List[Double] = List(mid(0));

var flast = mid(0);
while (flast > se(0) )
{
	var df = 100*(pow(180,2))/(pow(Rphi(flast),2) * cos(flast*PI/180) * dl * pow(PI,2));
	flast = flast - df
	f = List(flast):::f
}

flast = mid(0);
while (flast < nw(0) )
{
	
	var df = 100*(pow(180,2))/(pow(Rphi(flast),2) * cos(flast*PI/180) * dl * pow(PI,2));
	flast = flast + df
	f = List(flast):::f
}


//val idx = ((f.take(f.length-1) zip f.takeRight(f.length-1) zip (0 to f.length-1)).filter{case((a,b),idx)=>(a<2.5 && b>2.5)})(0)._2

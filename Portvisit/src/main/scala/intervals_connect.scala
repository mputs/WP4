import scala.annotation.tailrec

object intervals_connect
{
	def connectIntervals(l:List[List[Long]]): List[List[Long]] = 
	{
		// tailrecursive helper function
		@tailrec def rec2_connectIntervals(out: List[List[Long]], begin: Long, end: Long, l:List[List[Long]]): List[List[Long]] = 
		{
			// base case: when list l is empty: return the result and terminate
			if(l.length == 0) return List(begin,end)::out  
			if(l.head(0) - end > 1) 
				//inductive step1: when the time between the time intervals is large, just add the last interval
				rec2_connectIntervals(List(begin,end)::out, l.head(0), l.head(1),l.tail)
			else
				//nductive step2: when the time between the time intervals is small, extend the interval
				rec2_connectIntervals(out, begin, l.head(1),l.tail)
		}
		if (l.length > 0)
			//call helper function
			return rec2_connectIntervals(List(): List[List[Long]],l.head(0), l.head(1), l.tail)
		else 
			return List(): List[List[Long]]
	}

}

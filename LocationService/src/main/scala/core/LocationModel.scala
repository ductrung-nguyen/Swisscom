package main.scala.core

class LocationModel(val TIME_INTERVAL : Int) extends Serializable {
    var modelID = -1
    var minCellID = 0
    var maxCellID = 0
    var data : Array[Array[(Int, Double)]] = new Array[Array[(Int, Double)]](60*24/TIME_INTERVAL)
    
    /***
     * rawModelData is a MxN matrix
     * Column i is a vector of (Location,Prob), in which, 
     * each element is the location and the probability of this location at the time interval i
     * For example, if TIME_INTERVAL = 120 (minute), we have total 12 time intervals
     * 12H43 belongs to interval 6, can have some location candidates: (LocationX, 23%), (LocationY, 38%), (LocationZ, 39%)
     */
    def construct(rawModelData : Array[Array[(Int, Int)]]) = {
        var length = rawModelData.length
        data = rawModelData.map {
            locationCandidates => {
                var sum = locationCandidates.foldLeft(0)((x,y) => (x + y._2))
                var temp = locationCandidates.sortBy{ case (location, freq) => location }
                temp.map { case (loc, freq) => (loc, freq*1.0/sum) }.toArray.sortBy(x => -x._2)// sort desc by probability
            }
        }.toArray
    }
    
    /***
     * In this model, have the user gone to "location" at time "timeInterval" ?
     * If yes, what is the probability (compare to the other locations, in this model also)
     */
    private def findLocationInTimeInterval (timeInterval : Int, location : Int) : Double = {
        val locationCandidates = data(timeInterval)
        var result = locationCandidates.find{ case (loc, proba) => {
            location == loc
        }}
        //println("result in findLocationInTimeInterval:%s".format(result))
        result match {
            case Some(d) => d._2
            case None => 0
        }
    }
    
    /***
     * Calculate the probability of the previousPos belongs to this model
     * Output is the probability
     */
    def calculateProb(previousPos : Array[(Int, Int)]) : Double = {		// array[Time, location]
        var sum : Double = 0
        previousPos.foreach {
            case (prevTime, prevLoc) => {
                sum = sum + findLocationInTimeInterval(prevTime/ TIME_INTERVAL, prevLoc)
            }
        }
        sum
    }
    
    /**
     * If we use this model to predict, what is the result ?
     */
    def predict(nextTime : Int) : Array[(Int, Double)] = {
        data(nextTime/TIME_INTERVAL)
    }
    
    def printInfo() = {
        println("Model : %d\nData:\n".format(modelID))
        data.foreach(arr => print("(%s)".format(arr.mkString(","))))
        println()
    }
}
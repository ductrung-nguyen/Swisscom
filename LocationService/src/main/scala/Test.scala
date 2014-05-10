//package test

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
//import scala.collection.mutable._
import scala.collection.immutable._;
import scala.collection.immutable.Seq;



object Test {
    
    val TIME_INTERVAL = 120 // 15 minutes
    val TOP_K = 3
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("test location service")

        val context = new SparkContext(conf)
        //val input = "/Users/loveallufev/Documents/MATLAB/mobile-locations-training.txt"
        //val input = "/Users/loveallufev/Documents/MATLAB/user/training-user21.txt"
        val input = "/Users/loveallufev/Documents/MATLAB/user/smalltraining21.txt"

        val data = context.textFile(input, 1)

        val sequences = data.map(line => {
            try{
            val values = line.split(',')
            // UserID,Year,Month,DayOfMonth,DayOfWeek,Hour,Minute,Area,AreaIndex
            val (userID, year, month, day, totalmin, cellID) =
                (values(0), values(1).toInt, values(2).toInt, values(3).toInt,
                    values(5).toInt * 60 + values(6).toInt, values(8).toInt)
            ((userID, year, month, day), (totalmin, cellID))
            }
            catch{
                case s: Throwable => {
                    ((0, 0, 0, 0), (0, 0))
                }
            }
        }).filter(x => x._1 != (0,0,0,0)).groupByKey
        // we get: (userId, year, month, day),[(totalmin, cellId)]

        //sequences.collect.foreach(println)

        val timeSeries = sequences.map {
            case ((userId, year, month, day), sequence) => {
                var base: Int = TIME_INTERVAL
                var lasttime = 0

                var discreteValues = new Array[Int](24 * 60 / TIME_INTERVAL)
                var lastIndex = -1
                var max_duration = -1 // duration which user is in a region
                var best_cellID: Int = 0 // cellID which user is in for the longest time
                var last_cellID: Int = -1

                for ((time, cellID) <- sequence) {

                    val T = if (time < base) time else base

                    if (T - lasttime > max_duration) {
                        max_duration = T - lasttime
                        best_cellID = last_cellID
                    }

                    if (T == base) { // it means time >= base
                        // update the value of lastIndex
                        lastIndex = lastIndex + 1
                        discreteValues.update(lastIndex, best_cellID)

                        // update max_duration is the duration of the first location of this interval
                        max_duration = time - base // calculate the max duration for next interval
                        while (max_duration >= TIME_INTERVAL) {
                            lastIndex = lastIndex + 1
                            discreteValues.update(lastIndex, last_cellID)

                            base = base + TIME_INTERVAL // move to next interval
                            max_duration = time - base // calculate the max duration for next interval	
                        }
                        base = base + TIME_INTERVAL
                    }

                    lasttime = time
                    last_cellID = cellID
                }

                while (lastIndex < discreteValues.length -1) {
                    lastIndex = lastIndex + 1
                    discreteValues.update(lastIndex, last_cellID)

                    lasttime = lasttime + TIME_INTERVAL // move to next interval
                }
                //println("time:%s sequences:%s".format((userId, year, month, day), discreteValues.mkString(",")))
                discreteValues.map(x => (x,1))	//[(dimesion1,fre1), (dimension2,fre2), ..., (dimensionN, freN)]
                // use it for select median
            }
        }


        println("After transfrom data to time-series data:")
        timeSeries.collect.foreach(x => println(x.mkString(",")))
        
        val K = 100//00
        
	    var points = timeSeries.takeSample(false, K, 42)
	    println("sample points")
	    points.foreach(x => println(x.mkString(",")))

	    var kPoints = new HashMap[Int, Array[(Int, Int)]]	// (centroisIndex -> N_dimension_point : [(di1, frenquency), (di2,fre)...])
	    val iterations = 40
	   
	    for (i <- 0 to points.size - 1) {
	        kPoints = kPoints.+(i -> points(i))
	    }
        
        var modelsData = new HashMap[Int, Array[Array[(Int, Int)]]]()
	   
	    for (i <- 1 to iterations) {
	      val  closest = timeSeries.map ( p => (findClosetCenter(p, kPoints)(distanceFunction), p) )
	      
	      closest.collect.foreach(println)
	      
	      val abcxyz = closest.flatMap{
	           case (centerIndex, coordinationOfPoint) => {
	                 var indexes = (0 to coordinationOfPoint.length -1)
	                 
	                  (indexes zip coordinationOfPoint).map (x => ((centerIndex, x._1, x._2._1), x._2._2))
	                  // ((centerIndex, intervalIndex, location), frequencyOfLocation)
	           }
	      }.reduceByKey(_ + _)	// count how many time user is in this location at this time Interval (per cluster)
	      .map{ 
	           case ((centerIndex, intervalIndex, location), frequency) => ((centerIndex, intervalIndex), (location, frequency))
	           }.groupByKey
	           
	      abcxyz.collect.foreach(println)
	      var closetDistribution = abcxyz.map{
	           case ((centerIndex, intervalIndex), sequenceOfLocationFrequency) => {
	                 var topLocations = (sequenceOfLocationFrequency.sortBy(x => -x._2)// sort desc by frequency
	                         .take(TOP_K))
	                 (centerIndex, (intervalIndex, topLocations))
	           }
	      }.groupByKey.map{
	           case (centerIndex, seqIntervalIndexTopLocations) => {
	                 (centerIndex, seqIntervalIndexTopLocations.map (x => x._2.toArray).toArray)
	           }
	      }
	       
	       // update models
	      closetDistribution.collect.foreach{
	           case (centerIndex, matrix) => {
	                 println("index %d matrix : %s".format(centerIndex, matrix))
	                 modelsData = modelsData.updated(centerIndex, matrix)
	                  // = modelsData.+ (centerIndex -> matrix)
	                 
	                 
	                 // update centers
	                 
	                 val length = matrix.length
	                 var temp = matrix.map (x => x(0)).toArray
	                 /*
	                     new Array[(Int, Int)](length)
	                 println("length = " + length)
	                 for (j <- 0 to length-1){
	                     temp.update(j, matrix(j)(0))
	                 }
	                 * 
	                 */
	                 kPoints = kPoints.+ (centerIndex -> temp)
	           }
	      }
	    }
	    
        println("KMEAN - OK ----------------------------------------")
        
        var models = modelsData.map{
            case (centerIndex, matrix) => {
                var model = new LocationModel(TIME_INTERVAL)
                model.modelID = centerIndex
                model.construct(matrix)
                model
            }
        }.toArray
        
        models.foreach (x => { x.printInfo; println("----------------\n") } )

	    //kPoints.foreach(x => {
	    //    println("%d -> (%s)".format(x._1, x._2.mkString(",")))
	    //})
	    
	    var previousPos = new Array[(Int, Int)](3)
	    
	    previousPos.update(0, (8*60+33, 548))
	    previousPos.update(1, (10*60+26, 548))
	    previousPos.update(2, (12*60 + 49, 587))
	    
	    println("Predict:%s".format(predict(previousPos, 18*60+24, models.toVector).mkString(",")))

    }
    
    def distanceFunction(x : Array[(Int, Int)], y : Array[(Int, Int)]) : Double = {
        val length = x.length
	           var distance : Double = 0
	           for (j <- 0 to length-1){
	                 distance = distance + (x(j)._1 - y(j)._1)*(x(j)._1 - y(j)._1)
	                 
	           }
	    distance
    }
    
    def findClosetCenter[T]( point: T, centers: HashMap[Int, T])(distanceFunction : (T,T) => Double) : Int = {
        var closetCenterID = -1
        var minDistance = distanceFunction(centers.head._2, point) + 1
        centers.foreach {
            case (key, value) => {
                val distance = distanceFunction(value, point) 
                if (distance < minDistance){
                    closetCenterID = key
                    minDistance = distance
                }
            }
        }
        closetCenterID
    }
    
    def predict(previousPos : Array[(Int, Int)], nextTime : Int,
            models : Vector[LocationModel]
            ) : Array[(Int, Double)] = {	// Array of (time, Location) ==> Next location
    	var bestModelID = -1
    	var bestModel : LocationModel = null
    	var maxProb : Double = 0
    	
        models.foreach (model => {
            
            var prob = model.calculateProb(previousPos) 
            println("model:%d prob:%f".format(model.modelID, prob))
    		  if (prob > maxProb){
    		       maxProb = prob
    		       bestModelID = model.modelID
    		       bestModel = model
    		  }
            
            println("\n---------------------")
    		})
    		
    		
    	
    	println("bestModelID=%d MaxProb=%f".format(bestModelID, maxProb))
    	bestModel.predict(nextTime)
    	
    }
}


class LocationModel(val TIME_INTERVAL : Int) {
    var modelID = -1
    var minCellID = 0
    var maxCellID = 0
    var data : Array[Array[(Int, Double)]] = new Array[Array[(Int, Double)]](60*24/TIME_INTERVAL)
    
    def construct(rawModelData : Array[Array[(Int, Int)]]) = {
        var length = rawModelData.length
        data = rawModelData.map {
            locationCandidates => {
                var sum = locationCandidates.foldLeft(0)((x,y) => (x + y._2))
                var temp = locationCandidates.sortBy{ case (location, freq) => location }
                temp.map { case (loc, freq) => (loc, freq*1.0/sum) }.toArray
            }
        }.toArray
    }
    
    private def findLocationInTimeInterval (timeInterval : Int, location : Int) : Double = {
        val locationCandidates = data(timeInterval)
        var result = locationCandidates.find{ case (loc, proba) => {
            location == loc
        }}
        println("result in findLocationInTimeInterval:%s".format(result))
        result match {
            case Some(d) => d._2
            case None => 0
        }
    }
    
    /***
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
    
    def predict(nextTime : Int) : Array[(Int, Double)] = {
        data(nextTime/TIME_INTERVAL)
    }
    
    def printInfo() = {
        println("Model : %d\nData:\n".format(modelID))
        data.foreach(arr => print("(%s)".format(arr.mkString(","))))
        println()
    }
}
package main.scala.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable._
import scala.collection.immutable.Seq

object MostFrequencyModelBuilder {
    
    val TIME_INTERVAL = 60 // minutes
    val TOP_K = 3
    
    val output =  "/tmp/model21"
    
	def build(data : RDD[String]) {
        
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
                
                var mapLocationToTotalTime = HashMap[Int, Int]()
                for ((time, cellID) <- sequence) {
                    
                    val T = if (time < base) time else base
                    
                    var timeOfTheLastLocation = mapLocationToTotalTime.get(last_cellID) match {
                        case Some(temp) => temp
                        case None => 0
                    }
                    
                    mapLocationToTotalTime = mapLocationToTotalTime.+(last_cellID -> (timeOfTheLastLocation + T - lasttime))
                    
                    if (T == base)
                    {
                        
                        var maxTime = -1
                        var bestCellID = -1
                        // update the value for this interval
                        mapLocationToTotalTime.foreach {
                            case (key, value) => {
                                if (value > maxTime 
                                        && key != 1	// remove case of lost signal
                                        ) {
                                    maxTime = value
                                    bestCellID = key
                                }
                            }
                        }

                        lastIndex = lastIndex + 1
                        discreteValues.update(lastIndex, bestCellID)

                        mapLocationToTotalTime = HashMap[Int, Int]()
                        // this statement only update the time of last_cellID
                        mapLocationToTotalTime = mapLocationToTotalTime.+(last_cellID -> (time - base))
                        base = base + TIME_INTERVAL

                        while (base <= time) {
                            lastIndex = lastIndex + 1
                            discreteValues.update(lastIndex, last_cellID)

                            // this statement only update the time of last_cellID
                            mapLocationToTotalTime = mapLocationToTotalTime.+(last_cellID -> (time - base))
                            base = base + TIME_INTERVAL
                        }
                    }
                    
                    last_cellID = cellID
                    lasttime = time
                    
                }
                
                
                while (lastIndex < discreteValues.length -1) {
                    lastIndex = lastIndex + 1
                    discreteValues.update(lastIndex, last_cellID)

                    lasttime = lasttime + TIME_INTERVAL // move to next interval
                }
                //println("time:%s sequences:%s".format((userId, year, month, day), discreteValues.mkString(",")))
                
                ((userId, year, month), (day, discreteValues.map(x => (x,1))))	//[(dimesion1,fre1), (dimension2,fre2), ..., (dimensionN, freN)]))
                //discreteValues.map(x => (x,1))	//[(dimesion1,fre1), (dimension2,fre2), ..., (dimensionN, freN)]
                // use it for select median
            }
        }
        
        
        // Auto update the last location of the the first interval in each day
        
        val dataOfMonthsSorted = timeSeries.groupByKey.flatMap {
            monthlyData => {
                
	                var sortedData = monthlyData._2.sortBy(_._1)	// sort asc by day
	                var lastCellID = -1
	                var lastday = -1
	                //println("New month" + monthlyData._1._3)

                    sortedData.foreach {
                        case (day, locationsInThisDay) => {
                            //println("Current day: %d Last day:%d".format(day, lastday))
                            if (day - lastday == 1) {
                                //println("old data: %s of day %d".format(locationsInThisDay.mkString(","), day))
                                val length = locationsInThisDay.length

                                var i = 0
                                while (i < length && locationsInThisDay(i)._1 == -1) {
                                    locationsInThisDay.update(i, (lastCellID, 1))
                                    i = i + 1
                                }
                                lastCellID = locationsInThisDay(length - 1)._1

                                //println("new data: %s of day %d".format(locationsInThisDay.mkString(","), day))
                            } else {
                                lastCellID = -1
                            }

                            lastday = day

                        }
                    }
	                sortedData.map (x => x._2)
            }
        }
        
        
        //


        //println("After transfrom data to time-series data:")
        //dataOfMonthsSorted.collect.foreach(x => println(x.mkString(",")))
        
        val K = 400//00
        
	    var points = dataOfMonthsSorted.takeSample(false, K, 42)
	    //println("sample points")
	    //points.foreach(x => println(x.mkString(",")))

	    var kPoints = new Array[(Int, Array[(Int, Int)])](K)	// (centroisIndex -> N_dimension_point : [(di1, frenquency), (di2,fre)...])
	    val iterations = 30
	   
	    for (i <- 0 to points.size - 1) {
	        kPoints.update(i,(i,points(i)))
	    }
        
        var modelsData = Array[(Int, Array[Array[(Int, Int)]])]()	// (modelIndex, matrix of location candidate of each time)
	   
	    for (i <- 1 to iterations) {
	        println("Iter " + i)
	      val  closest = dataOfMonthsSorted.map ( p => (findClosetCenter(p, kPoints)(distanceFunction), p) )
	      
	      //closest.collect.foreach(println)
	      
	      // find location candidate of each interval in each cluster
	      val locationCandidates = closest.flatMap{
	           case (centerIndex, coordinationOfPoint) => {
	                 var indexes = (0 to coordinationOfPoint.length -1)
	                 
	                  (indexes zip coordinationOfPoint).map (x => ((centerIndex, x._1, x._2._1), x._2._2))
	                  // ((centerIndex, intervalIndex, location), frequencyOfLocation)
	           }
	      }.reduceByKey(_ + _)	// count how many time user is in this location at this time Interval (per cluster)
	      .map{ 
	           case ((centerIndex, intervalIndex, location), frequency) => ((centerIndex, intervalIndex), (location, frequency))
	      }.groupByKey
	           
	      //abcxyz.collect.foreach(println)
	      
	      var topNLocationCandidateOfEachTimeInEachCluster = locationCandidates.map{
	           case ((centerIndex, intervalIndex), sequenceOfLocationFrequency) => {
	                 var topLocations = (sequenceOfLocationFrequency.sortBy(x => -x._2)// sort desc by frequency
	                         .take(TOP_K))
	                 //println("Get top of index:" + centerIndex)
	                 (centerIndex, (intervalIndex, topLocations))
	           }
	      }.groupByKey.map{
	           case (centerIndex, seqIntervalIndexTopLocations) => {
	                 (centerIndex, seqIntervalIndexTopLocations.map (x => x._2.toArray).toArray)
	           }
	      }
	       
	       //println("Old kPoints:")
	       //kPoints.foreach {case (key, value) => println("%d -> %s".format(key, value.mkString(",")))}
	       println("Number of cluster:" + topNLocationCandidateOfEachTimeInEachCluster.count)
	       //kPoints =  kPoints.empty
	       // update models
	       
	       
	       modelsData = topNLocationCandidateOfEachTimeInEachCluster.map {
	             case (centerIndex, matrix) => (centerIndex -> matrix)
	       }.toArray
	       
	       kPoints = topNLocationCandidateOfEachTimeInEachCluster.map {
	             case (centerIndex, matrix) => {
	                 (centerIndex, matrix.map (x => x(0)).toArray)
	             }
	       }.toArray

	         //println("New kPoints:")
	         //kPoints.foreach {case (key, value) => println("%d -> %s".format(key, value.mkString(",")))}
	    }
	    
        println("KMEAN - OK ----------------------------------------")
        
        val modelCenter = new ModelCenter(TIME_INTERVAL)
        modelCenter.setData(modelsData)
        
        modelCenter.saveToFile(output)
        println("number of models:" + modelCenter.numberOfModel)
        modelCenter.printInfo
        
	}
    
    private def distanceFunction(x : Array[(Int, Int)], y : Array[(Int, Int)]) : Double = {
        val length = x.length
	           var distance : Double = 0
	           for (j <- 0 to length-1){
	                 distance = distance + (x(j)._1 - y(j)._1)*(x(j)._1 - y(j)._1) 
	           }
	    distance
    }
    
    private def findClosetCenter[T]( point: T, centers: Array[(Int, T)])(distanceFunction : (T,T) => Double) : Int = {
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
            case _ =>  println
        }
        closetCenterID
    }
    
    def saveModelToFile(path : String) {
    	import java.io.{File,FileInputStream,FileOutputStream}
    	val src = new File(output)
    	val dest = new File(path)
    	new FileOutputStream(dest) getChannel() transferFrom(
    			new FileInputStream(src) getChannel, 0, Long.MaxValue )
    	
    }
}
package main.scala.test

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
//import scala.collection.mutable._
import scala.collection.immutable._;
import scala.collection.immutable.Seq;


object GenerateTestSet {
	val TIME_INTERVAL_LENGTH = 60 // minutes
    val NUMBER_PREVIOUS_POINTS = 3	// use 3 points in the past to predict the next point in the future
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("test location service")

        val context = new SparkContext(conf)
        val input = "/Users/loveallufev/Documents/MATLAB/mobile-locations-training.txt"
        //val input = "/Users/loveallufev/Documents/MATLAB/user/training-user21.txt"
        //val input = "/Users/loveallufev/Documents/MATLAB/user/smalltraining21.txt"
        //val input = "/Users/loveallufev/Documents/MATLAB/fakedata"

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

        // transform the raw data into the new format
        // userid|year|month|day|(time1,location1)!(time2,location2)!....!(timeN, locationN)
        /*
        sequences.map {
            case ((userid, year, month, day), seqLocations) => {
            	"%s|%d|%d|%d|%s".format(userid, year, month, day, seqLocations.mkString("!"))
            }
        }.saveAsTextFile("/Users/loveallufev/Documents/MATLAB/data/tranformedDataPerDay.csv")
        */
        
        
        // In this section, we try to form up a testing set
        // Requirements:
        //	* There are enough NUMBER_PREVIOUS_POINTS points in the past
        //	* The points are in the different time intervals
        
        
        
        //sequences.collect.foreach(println)

        // for each user, select the location which user spent the most of time in each interval of each day
        val timeSeries = sequences.map {
            case ((userId, year, month, day), sequence) => {
                var base: Int = TIME_INTERVAL_LENGTH
                var lasttime = 0

                var discreteValues = new Array[Int](24 * 60 / TIME_INTERVAL_LENGTH)
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
                                        // IMPORTANT !!! Should we ???
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
                        base = base + TIME_INTERVAL_LENGTH

                        while (base <= time) {
                            lastIndex = lastIndex + 1
                            discreteValues.update(lastIndex, last_cellID)

                            // this statement only update the time of last_cellID
                            mapLocationToTotalTime = mapLocationToTotalTime.+(last_cellID -> (time - base))
                            base = base + TIME_INTERVAL_LENGTH
                        }
                    }
                    
                    last_cellID = cellID
                    lasttime = time
                    
                }
                
                
                while (lastIndex < discreteValues.length -1) {
                    lastIndex = lastIndex + 1
                    discreteValues.update(lastIndex, last_cellID)

                    lasttime = lasttime + TIME_INTERVAL_LENGTH // move to next interval
                }
                //println("time:%s sequences:%s".format((userId, year, month, day), discreteValues.mkString(",")))
                
                ((userId, year, month), (day, discreteValues))	//[locAtTime1, locAtTime2, ..., locAtTimeN]))

            }
        }
        
        /*
        timeSeries.map{
            case ((userid, year, month),(day, seqLocations)) => {
                "%s|%d|%d|%d|%s".format(userid, year, month, day, seqLocations.mkString("!"))
            }
        }.saveAsTextFile("/Users/loveallufev/Documents/MATLAB/data/standardDataByTimeInterval.csv")
        * 
        */
        
         // Update the last location of the the first interval in each day
        
        val dataOfMonthsSorted = timeSeries.groupByKey.flatMap {
            monthlyData => {
                	val user_year_month = monthlyData._1
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
                                while (i < length && locationsInThisDay(i) == -1) {
                                    locationsInThisDay.update(i, lastCellID)
                                    i = i + 1
                                }
                                lastCellID = locationsInThisDay(length - 1)

                                //println("new data: %s of day %d".format(locationsInThisDay.mkString(","), day))
                            } else {
                                lastCellID = -1
                            }

                            lastday = day

                        }
                    }
	                sortedData.map (x => ((user_year_month._1,user_year_month._2, user_year_month._3 , x._1), x._2))
            }
        }
        
        dataOfMonthsSorted.map {
            case ((userid, year, month, day), seqLocations) => {
            	"%s|%d|%d|%d|%s".format(userid, year, month, day, seqLocations.mkString(",")).replaceAll("[()]", "")
            }
        }.saveAsTextFile("/Users/loveallufev/Documents/MATLAB/data/newdata")

        
        val numberOfRecords : Long = dataOfMonthsSorted.count
        
        //dataOfMonthsSorted.takeSample(false, math.round(numberOfRecords*0.8f), 40);
        
        var temp = dataOfMonthsSorted.map {
            case ((userid, year, month, day), seqLocations) => {
                var count = 0;
                var lengthOfSeqLocations = seqLocations.length
                var random = new scala.util.Random
                var lastIndex : Int = 0
                var lastTimeInterval = -1
                
                
                var seqLocationsGroupByTimeInterval = seqLocations
                var numberOfTimeInterval = seqLocationsGroupByTimeInterval.length
                
                val previvousPoints = new Array[(Int, Int)](if (NUMBER_PREVIOUS_POINTS + 1 > numberOfTimeInterval) numberOfTimeInterval else (NUMBER_PREVIOUS_POINTS + 1))
                

                var consideredTimeIntervalAlready = Set[Int]()
                while (count < NUMBER_PREVIOUS_POINTS + 1 && count < numberOfTimeInterval) {
                    val randomTime = random.nextInt(24*60)
                    val randomTimeInterval = randomTime/TIME_INTERVAL_LENGTH
                    if (!consideredTimeIntervalAlready.contains(randomTimeInterval)) {
                        //val length = seqLocationsGroupByTimeInterval(randomTimeInterval)._2.length

                        var chosenTimeLocation = seqLocationsGroupByTimeInterval(randomTimeInterval)
                        //var randomIndex = random.nextInt(length)
                        //var chosenTimeLocation = seqLocationsGroupByTimeInterval(randomTimeInterval)._2(randomIndex)

                        consideredTimeIntervalAlready = consideredTimeIntervalAlready.+(randomTimeInterval)
                        previvousPoints.update(count, (randomTime, chosenTimeLocation))
                        count = count + 1
                    }
                }
                (userid,year, month, day, previvousPoints.sortBy(_._1)) // sort by time
            }
        }.map { 
            case (userid,year, month, day, seqLocation) => 
                "%s|%d|%d|%d|%s".format(userid, year,month, day, seqLocation.mkString("!")).replaceAll("[()]", "")
        }
        
        temp.saveAsTextFile("/Users/loveallufev/Documents/MATLAB/data/testingDataAllUsers")
        
        
    }
}
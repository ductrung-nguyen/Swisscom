package main.scala


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import main.scala.core.ModelCenter

object TestEvaluation {

    def main(args: Array[String]): Unit = {
        
        var modelCenter : ModelCenter = new ModelCenter()
        //modelCenter.loadModelFromFile("/Users/loveallufev/Documents/MATLAB/output/model21")
        modelCenter = modelCenter.loadModelFromFile("/tmp/model21")
        println("number of models:" + modelCenter.numberOfModel)
        
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("test location service")

        val context = new SparkContext(conf)
        val input = "/Users/loveallufev/Documents/MATLAB/data/testingDataAllUsers/part*"
        
        val testingData = context.textFile(input, 1)
        
        val testingDataUserX = testingData.map {
            line => {
                //println("line:" + line)
                val values = line.split('|')
                //println(values.mkString("|"))
                val userid = values(0)
                val year = values(1)
                val month = values(2)
                val day = values(3)
                val seqTimeLocations = values(4).split('!').map {
                    s => { 
                        val temp = s.split(',')
                        (temp(0).toInt, temp(1).toInt)
                    }
                }
                (userid, seqTimeLocations)
            }
        }.filter (x => x._1 == "21")
        
        val predictedValues = testingDataUserX.map {
            case (userid, seqTimeLocations) => {
                val length = seqTimeLocations.length
                val futureTimeLocation = seqTimeLocations.last
                val previousLocation = new Array[(Int, Int)](length - 1)
                for (i <- (0 to length -2)){
                    previousLocation.update(i, seqTimeLocations(i))
                }
                // (predictedvalue, actualvalue)
                (modelCenter.predict(previousLocation, futureTimeLocation._1), futureTimeLocation._2)
            }
        }
        
        val checkedResult = predictedValues.map {
            case (predictedValues, actualValue) =>{
                predictedValues.map(value => {
                    if (value._1 == actualValue) 1
                    else 0
                })
            }
        }
        
        
        val numberOfPredictions = checkedResult.count
        val finalResult = checkedResult.reduce((x, y) => {
            val xlength = x.length
            val ylength = y.length
            var length = if (xlength > ylength) xlength else ylength
            val result = new Array[Int](length)
            for (i <- (0 until length)){
                //println("i = %d length=%d".format(i, length))
                var a = if (i < xlength) x(i) else 0
                var b = if (i < ylength) y(i) else 0
                result.update(i, a + b)
            }
            result
        })
        
        predictedValues.collect.foreach {
            case (seq, actual) => {
                println("predict:%s actual:%d".format(seq.mkString(","), actual))
            }
        }
        
        //checkedResult.collect.foreach (x => println(x.mkString(",")))
        println("numberOf Prediction:" + numberOfPredictions)
        println("result:" + finalResult.mkString(","))
        
        //modelCenter.p
        
    }
}
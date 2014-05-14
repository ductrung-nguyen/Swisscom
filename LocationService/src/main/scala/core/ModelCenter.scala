package main.scala.core

import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream

class ModelCenter (val lengthOfTimeInterval : Int = 1) extends Serializable {
    var models : Vector[LocationModel] = Vector[LocationModel]()
        
    def numberOfModel = models.length
    
    def setData(data : Array[(Int, Array[Array[(Int, Int)]])]){
        if (data != null){
	        models = data.map{
	            case (centerIndex, matrix) => {
	                var model = new LocationModel(lengthOfTimeInterval)
	                model.modelID = centerIndex
	                model.construct(matrix)
	                model
	            }
	        }.toVector
        }
    }
        
    def predict(previousPos : Array[(Int, Int)], nextTime : Int
            ) : Array[(Int, Double)] = {	// Array of (time, Location) ==> Next location
    	var bestModelID = -1
    	var bestModel : LocationModel = null
    	var maxProb : Double = -1
    	
        models.foreach (model => {
            
            var prob = model.calculateProb(previousPos) 
            //println("model:%d prob:%f predict:%s".format(model.modelID, prob, model.predict(nextTime).mkString(",")))
    		  if (prob > maxProb){
    		       maxProb = prob
    		       bestModelID = model.modelID
    		       bestModel = model
    		  }
            
            //println("\n---------------------")
    		})
    		
    		
    	
    	println("bestModelID=%d MatchingPercentage=%f".format(bestModelID, maxProb))
    	bestModel.predict(nextTime)
    	
    }
        
    def printInfo  {
        models.foreach (x => { x.printInfo; println("----------------\n") } )
    }
    
    def saveToFile(path : String) = {
        val js = new JavaSerializer(null, null)
        //val os = new DataOutputStream(new FileOutputStream(path))

        val os = new ObjectOutputStream(new FileOutputStream(path))
        os.writeObject(this)
        //js.writeObject(os, this)
        os.close
    }
    
    def loadModelFromFile(path: String) : ModelCenter = {
        val js = new JavaSerializer(null, null)

        val ois = new ObjectInputStream(new FileInputStream(path)) {
            override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
                try { Class.forName(desc.getName, false, getClass.getClassLoader) }
                catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
            }
        }

        var rt = ois.readObject().asInstanceOf[ModelCenter]        
        ois.close()
        rt
    }
}
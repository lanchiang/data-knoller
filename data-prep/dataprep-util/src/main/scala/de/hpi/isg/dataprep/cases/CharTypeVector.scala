package de.hpi.isg.dataprep.cases

import org.apache.spark.ml.linalg.{DenseVector, Vectors}

case class CharTypeVector(lowerCaseChars:Int, upperCaseChars:Int, numbers:Int, specialChars:Int) {
  def allChars:Int = lowerCaseChars + upperCaseChars

  def toDenseVector: org.apache.spark.ml.linalg.Vector = {
    Vectors.dense(allChars, numbers, specialChars)
  }
}

object CharTypeVector{
  def fromString(input:String): CharTypeVector ={
    val lcPattern = "([a-z])".r
    val ucPattern = "([A-Z])".r
    val numPattern = "([0-9])".r

    val b = input.toCharArray.map(c => {
      c match {
        case lcPattern(_) => List(1,0,0,0)
        case ucPattern(_) => List(0,1,0,0)
        case numPattern(_) => List(0,0,1,0)
        case _ => List(0,0,0,1)
      }
    })

    val reducedList = b.reduce((a,b) => a.zip(b).map{case(x,y) => x+y})

    CharTypeVector(reducedList(0), reducedList(1), reducedList(2), reducedList(3))
  }


}

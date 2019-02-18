package de.hpi.isg.dataprep.cases

import org.apache.spark.ml.linalg.Vectors

case class CharTypeVector(lowerCaseChars:Int, upperCaseChars:Int, numbers:Int, specialChars:Int) {
  def allChars:Int = lowerCaseChars + upperCaseChars

  def toDenseVector: org.apache.spark.ml.linalg.Vector = {
    if(allChars == 0 && numbers == 0 && specialChars == 0){
      return Vectors.dense(0.01, 0.01, 0.01)
    }
    Vectors.dense(allChars, numbers, specialChars)
  }

  def simplify: CharTypeVector = {
    val smallestPart = List(allChars, numbers, specialChars).filter(_ != 0).min
    val updatedLowers = simplifySingleValue(lowerCaseChars , smallestPart)
    val updatedUppers = simplifySingleValue(upperCaseChars , smallestPart)
    val updatedNumbers = simplifySingleValue(numbers , smallestPart)
    val updatedSpecials = simplifySingleValue(specialChars , smallestPart)

    CharTypeVector(updatedLowers, updatedUppers, updatedNumbers, updatedSpecials)
  }

  def simplifySingleValue(value:Int, smallestNumber:Int): Int = {
    val updatedVal = Math.round(lowerCaseChars / smallestNumber)
    if(updatedVal > 2){
      return 2
    }
    if(updatedVal == 0){
      return 0
    }
    1
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

    if(b.isEmpty){
      return CharTypeVector(0,0,0,0)
    }

    val reducedList = b.reduce((a,b) => (a,b).zipped.map(_ + _))

    CharTypeVector(reducedList(0), reducedList(1), reducedList(2), reducedList(3))
  }


}
